#![feature(inherent_associated_types)]

use tarpc::context;

use std::sync::Arc;

use tokio::sync::Mutex;

mod acceptor;
mod proposer;

use acceptor::{AcceptRequest, AcceptResponse, Acceptor, PrepareRequest, PrepareResponse};

#[derive(Clone)]

struct AcceptorServer {
    acceptor: Arc<Mutex<Acceptor>>,
}

#[tarpc::server]
impl AcceptorServer {
    async fn prepare(
        self,
        _: context::Context,
        request: PrepareRequest,
    ) -> Result<PrepareResponse, String> {
        let mut acceptor = self.acceptor.lock().await;

        acceptor
            .on_prepare(request)
            .await
            .map_err(|err| err.to_string())
    }

    async fn accept(
        self,
        _: context::Context,
        request: AcceptRequest,
    ) -> Result<AcceptResponse, String> {
        let mut acceptor = self.acceptor.lock().await;

        acceptor
            .on_accept(request)
            .await
            .map_err(|err| err.to_string())
    }
}

#[tokio::main]
async fn main() {}
