// RPC Module - routes messages to the correct functions.
// to handle the response. The raft node object will call the
// RPC module, which will then
use raft_main::{command::Command, messages::types::{CallMessage, Message, RequestVoteMessage}, raft_sm::RaftServer};

// Possible design patterns

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <node_id> <port_id>", args[0]);
        std::process::exit(1);
    }
    let node_id = args[1].parse::<u64>().expect("Node ID must be a valid integer");
    let port_id = args[2].parse::<u32>().expect("Port ID must be a valid integer");


    let rpc_server = RaftServer::new(port_id, node_id).await;
    rpc_server.start().await;

    loop {}

    Ok(())
}

// 
