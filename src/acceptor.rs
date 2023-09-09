use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
    sync::Mutex,
};

#[derive(Debug)]
pub struct Acceptor {
    /// The last proposal id this acceptor has seen.
    proposal_id: u64,

    /// The last proposal value this accept has received.
    proposal_value: Option<Vec<u8>>,

    /// The file that contains the acceptor state.
    state_file: File,
}

#[derive(Debug)]
pub struct PrepareRequest {
    proposal_id: u64,
}

#[derive(Debug)]
pub struct PrepareResponse {
    proposal_id: u64,
    proposal_value: Option<Vec<u8>>,
}

#[derive(Debug)]
pub struct AcceptRequest {
    proposal_id: u64,
    proposal_value: Vec<u8>,
}

#[derive(Debug)]
pub struct AcceptResponse {
    proposal_id: u64,
    proposal_value: Option<Vec<u8>>,
}

impl Acceptor {
    async fn new() -> Result<Arc<Mutex<Self>>> {
        let state_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .mode(0o644)
            .open("./acceptor.state")
            .await
            .context("opening acceptor state file")?;

        Ok(Arc::new(Mutex::new(Self {
            proposal_id: 0,
            proposal_value: None,
            state_file,
        })))
    }

    pub async fn on_prepare(&mut self, message: PrepareRequest) -> Result<PrepareResponse> {
        if message.proposal_id < self.proposal_id {
            return Ok(PrepareResponse {
                proposal_id: self.proposal_id,
                proposal_value: self.proposal_value.clone(),
            });
        }

        self.proposal_id = message.proposal_id;

        self.state_file
            .write_u64_le(message.proposal_id)
            .await
            .context("writing proposal id to disk")?;

        self.state_file
            .sync_all()
            .await
            .context("syncing state file")?;

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
