use anyhow::Result;

use crate::acceptor::{AcceptRequest, AcceptResponse, PrepareRequest, PrepareResponse};

#[tarpc::service]
pub trait AcceptorService {
    async fn prepare(message: PrepareRequest) -> Result<PrepareResponse>;
    async fn accept(message: AcceptRequest) -> Result<AcceptResponse>;
}

#[derive(Debug)]
pub struct Proposer {
    /// The next proposal id that will be sent to the acceptors.
    proposal_id: u64,

    /// Client used to communicate with acceptors.
    acceptor_client: AcceptorServiceClient,
}

impl Proposer {
    pub fn new(acceptor_client: AcceptorServiceClient) -> Self {
        Self {
            proposal_id: 0,
            acceptor_client,
        }
    }

    pub async fn propose(&mut self, value: Vec<u8>) {}
}
