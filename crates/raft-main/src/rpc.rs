use jsonrpsee::{server::Server, RpcModule};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use tracing_subscriber::FmtSubscriber;
use tracing::subscriber::set_global_default;
use crate::messages::types::AppendEntriesMessage;
use crate::messages::types::CallMessage;
use crate::messages::types::{Message, RequestVoteMessage};

use crate::raft_log;
use crate::raft_sm::handle_append_entries;
use crate::raft_sm::handle_call;
use crate::raft_sm::handle_request_votes;
use crate::raft_sm::RaftServer;
use crate::raft_sm::State;

pub struct RaftRpc {
    rpc_module: RpcModule<Arc<RaftServer>>
}

pub fn set_up_logger() {
    // Subscriber to info
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();

    set_global_default(subscriber)
        .expect("setting default subscriber failed");
}


impl RaftRpc {
    pub async fn new(server: Arc<RaftServer>) -> anyhow::Result<Self> {
        let module = RpcModule::new(server);
        let mut rpc_wrapper = Self {
            rpc_module: module
        };

        rpc_wrapper.register_request_vote().await?;
        rpc_wrapper.register_append_entries().await?;
        rpc_wrapper.register_call().await?;
        rpc_wrapper.register_die().await?;

        Ok(rpc_wrapper)
    }

    pub async fn register_request_vote(&mut self) -> anyhow::Result<()> {
        self.rpc_module.register_method(
            "request_vote",
            |params: jsonrpsee::types::Params<'_>, server| {
                // Make sure that we are properly handling errors!
                let request_vote_message = <RequestVoteMessage as Message>::from_params(params).unwrap();
                let request_vote_response = handle_request_votes(request_vote_message, &mut server.state.lock().unwrap());
                let response = jsonrpsee::types::ResponsePayload::success(request_vote_response);
                return response;
            }
        )?;
        Ok(())
    }

    pub async fn register_append_entries(&mut self) -> anyhow::Result<()> {
        self.rpc_module.register_method(
            "append_entries",
            |params: jsonrpsee::types::Params<'_>, server| {
                // Make sure that we are properly handling errors!
                raft_log!( server.logger, "append entry enterred" );
                let append_entries_message = <AppendEntriesMessage as Message>::from_params(params).unwrap();
                let s = &mut server.state.lock().unwrap();
                let append_entries_response = handle_append_entries(append_entries_message, s);
                let response = jsonrpsee::types::ResponsePayload::success(append_entries_response);
                return response;
            }
        )?;

        Ok(())
    }

    pub async fn register_call(&mut self) -> anyhow::Result<()> {
        self.rpc_module.register_async_method(
            "call",
            |params: jsonrpsee::types::Params<'_>, server| async {
                // Make sure that we are properly handling errors!
                let append_entries_message = <CallMessage as Message>::from_params(params).unwrap();
                let append_entries_response = handle_call(append_entries_message, 
                    Arc::try_unwrap(server).unwrap_or_else(|arc| Arc::clone(&arc))
                ).await;
                let response = jsonrpsee::types::ResponsePayload::success(append_entries_response);
                return response;
            }
        )?;
        Ok(())
    }

    pub async fn register_die(&mut self) -> anyhow::Result<()> {
        self.rpc_module.register_method(
            "die",
            |params: jsonrpsee::types::Params<'_>, server| {
            }
        )?;
        Ok(())
    }

    pub async fn run_server(self, port: u32) -> anyhow::Result<SocketAddr> {
        let server = Server::builder().build(format!("127.0.0.1:{}", port).parse::<SocketAddr>()?).await?;
        let addr = server.local_addr()?;

        let handle = server.start(self.rpc_module);

        tokio::spawn(handle.stopped());

        Ok(addr)
    }

}