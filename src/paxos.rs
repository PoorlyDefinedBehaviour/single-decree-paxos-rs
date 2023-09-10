use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io::Cursor, net::SocketAddr};
use tarpc::{client::Config, context, tokio_serde::formats::Json};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

#[tarpc::service]
pub trait AcceptorService {
    async fn prepare(message: PrepareRequest) -> Result<PrepareResponse, String>;
    async fn accept(message: AcceptRequest) -> Result<AcceptResponse, String>;
}

#[derive(Debug)]
pub struct Paxos {
    /// The address of this instance.
    address: SocketAddr,

    /// The next proposal id that will be sent to the acceptors.
    current_proposal_id: u64,

    /// The address of each acceptor.
    acceptors: Vec<SocketAddr>,

    /// Client used to communicate with acceptors.
    acceptor_clients: HashMap<SocketAddr, AcceptorServiceClient>,

    /// The last proposal id this acceptor has seen.
    proposal_id: u64,

    /// The last proposal value this acceptor has received.
    proposal_value: Option<Vec<u8>>,

    /// The file that contains the acceptor state.
    state_file: File,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PrepareRequest {
    pub proposal_id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PrepareResponse {
    pub proposal_id: u64,
    pub proposal_value: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AcceptRequest {
    pub proposal_id: u64,
    pub proposal_value: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AcceptResponse {
    pub proposal_id: u64,
    pub proposal_value: Option<Vec<u8>>,
}

#[derive(Debug)]
struct State {
    proposal_id: u64,
    proposal_value: Option<Vec<u8>>,
}

async fn read_state(file: &mut File) -> Result<Option<State>> {
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)
        .await
        .context("reading file contents to buffer")?;

    if buffer.is_empty() {
        return Ok(None);
    }

    let mut cursor = Cursor::new(buffer);

    // TODO: does not need to be async.
    let proposal_id = cursor
        .read_u64_le()
        .await
        .context("reading proposal id from buffer")?;

    let mut proposal_value = Vec::new();
    cursor.read_to_end(&mut proposal_value).await?;

    Ok(Some(State {
        proposal_id,
        proposal_value: if proposal_value.is_empty() {
            None
        } else {
            Some(proposal_value)
        },
    }))
}

impl Paxos {
    pub async fn new(id: u32, address: SocketAddr, acceptors: Vec<SocketAddr>) -> Result<Self> {
        let mut state_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(format!("acceptor_{id}.state"))
            .await
            .context("opening acceptor state file")?;

        let state = read_state(&mut state_file)
            .await
            .context("reading state from file")?;

        let (proposal_id, proposal_value) = match state {
            None => (0, None),
            Some(state) => (state.proposal_id, state.proposal_value),
        };

        Ok(Self {
            address,
            current_proposal_id: 0,
            acceptors,
            acceptor_clients: HashMap::new(),

            proposal_id,
            proposal_value,
            state_file,
        })
    }

    fn majority(&self) -> usize {
        self.acceptors.len() / 2 + 1
    }

    async fn get_or_init_client(&mut self, acceptor: SocketAddr) -> Result<AcceptorServiceClient> {
        if let Some(client) = self.acceptor_clients.get(&acceptor) {
            return Ok(client.clone());
        }

        let mut transport = tarpc::serde_transport::tcp::connect(acceptor, Json::default);
        transport.config_mut().max_frame_length(usize::MAX);
        let transport = transport.await.context("initializing transport")?;

        let client = AcceptorServiceClient::new(Config::default(), transport).spawn();

        self.acceptor_clients.insert(acceptor, client.clone());

        Ok(client)
    }

    pub async fn propose(&mut self, value: Vec<u8>) -> Result<()> {
        self.current_proposal_id += 1;

        let mut futures = Vec::with_capacity(self.acceptors.len());

        for i in 0..self.acceptors.len() {
            let acceptor_addr = self.acceptors[i];
            if acceptor_addr == self.address {
                continue;
            }

            let client = match self.get_or_init_client(acceptor_addr).await {
                Err(err) => {
                    eprintln!("getting rpc client: acceptor={acceptor_addr} {err:?}");
                    continue;
                }
                Ok(v) => v,
            };

            let request = PrepareRequest {
                proposal_id: self.current_proposal_id,
            };
            futures.push(async move { client.prepare(context::current(), request).await });
        }

        let results = futures::future::join_all(futures).await;

        let _ = self
            .on_prepare(PrepareRequest {
                proposal_id: self.proposal_id,
            })
            .await;

        let mut response_count = 0;
        let mut highest_proposal_id = 0;
        let mut accepted_value = None;

        for result in results {
            let response = match result {
                Err(err) => {
                    eprintln!("rpc error {err:?}");
                    continue;
                }
                Ok(v) => v,
            };

            match response {
                Err(err) => {
                    eprintln!("error response to prepare request: {err:?}");
                    continue;
                }
                Ok(response) => {
                    response_count += 1;
                    highest_proposal_id = std::cmp::max(highest_proposal_id, response.proposal_id);
                    if response.proposal_value.is_some() {
                        accepted_value = response.proposal_value;
                    }
                }
            }
        }

        // -1 To take this proposer into account.
        if response_count < self.majority() - 1 {
            return Err(anyhow!(
                "unable to get response to prepare request from majority of acceptors"
            ));
        }

        self.current_proposal_id = std::cmp::max(self.current_proposal_id, highest_proposal_id);

        match accepted_value {
            None => self
                .accept(value)
                .await
                .context("sending accept requests with proposed value"),
            Some(accepted_value) => {
                self.accept(accepted_value.clone())
                    .await
                    .context("sending accept requests with already accepted value")?;

                Err(anyhow!(
                    "a value has already been accepted: {}",
                    String::from_utf8_lossy(&accepted_value)
                ))
            }
        }
    }

    async fn accept(&mut self, value: Vec<u8>) -> Result<()> {
        let mut futures = Vec::with_capacity(self.acceptors.len());

        for i in 0..self.acceptors.len() {
            let acceptor_addr = self.acceptors[i];
            if acceptor_addr == self.address {
                continue;
            }

            let client = match self.get_or_init_client(acceptor_addr).await {
                Err(err) => {
                    eprintln!("getting rpc client: {err:?}");
                    continue;
                }
                Ok(v) => v,
            };

            let request = AcceptRequest {
                proposal_id: self.current_proposal_id,
                proposal_value: value.clone(),
            };
            futures.push(async move { client.accept(context::current(), request).await });
        }

        let _ = self
            .on_accept(AcceptRequest {
                proposal_id: self.proposal_id,
                proposal_value: value.clone(),
            })
            .await;

        let results = futures::future::join_all(futures).await;

        let mut response_count = 0;
        for result in results {
            let response = match result {
                Err(err) => {
                    eprintln!("rpc error {err:?}");
                    continue;
                }
                Ok(v) => v,
            };

            match response {
                Err(err) => {
                    eprintln!("error response to accept request: {err:?}");
                    continue;
                }
                Ok(response) => {
                    if self.current_proposal_id < response.proposal_id {
                        return Err(anyhow!(
                            "acceptor has seen a proposal id greater than our own"
                        ));
                    }

                    response_count += 1;
                }
            }
        }

        if response_count < self.majority() - 1 {
            return Err(anyhow!(
                "unable to get response to accept request from majority of acceptors"
            ));
        }

        Ok(())
    }

    pub async fn on_prepare(&mut self, message: PrepareRequest) -> Result<PrepareResponse> {
        if message.proposal_id > self.proposal_id {
            self.proposal_id = message.proposal_id;

            self.state_file
                .seek(std::io::SeekFrom::Start(0))
                .await
                .context("seeking to beginning of state file")?;

            self.state_file
                .write_u64_le(message.proposal_id)
                .await
                .context("writing proposal id to disk")?;

            self.state_file
                .sync_all()
                .await
                .context("syncing state file")?;
        }

        Ok(PrepareResponse {
            proposal_id: self.proposal_id,
            proposal_value: self.proposal_value.clone(),
        })
    }

    pub async fn on_accept(&mut self, message: AcceptRequest) -> Result<AcceptResponse> {
        if message.proposal_id < self.proposal_id {
            return Ok(AcceptResponse {
                proposal_id: self.proposal_id,
                proposal_value: self.proposal_value.clone(),
            });
        }

        self.proposal_id = message.proposal_id;
        self.proposal_value = Some(message.proposal_value);

        let mut buffer = Vec::new();
        buffer
            .write_u64_le(self.proposal_id)
            .await
            .context("writing proposal id to buffer")?;
        buffer
            .write_all(self.proposal_value.as_ref().unwrap())
            .await
            .context("writing proposal value to buffer")?;

        self.state_file
            .seek(std::io::SeekFrom::Start(0))
            .await
            .context("seeking to beginning of state file")?;

        self.state_file
            .write_all(&buffer)
            .await
            .context("writing buffer to state file")?;

        self.state_file
            .sync_all()
            .await
            .context("syncing state file")?;

        Ok(AcceptResponse {
            proposal_id: self.proposal_id,
            proposal_value: None,
        })
    }
}
