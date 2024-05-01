//
// Run a small cluster of nodes simultaneously.
// 

use raft_main::{network::PeerConfig, raft_sm::RaftServer, rpc::set_up_logger};

// Modules:
// 

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();

    set_up_logger();

    let network_config = std::fs::read_to_string("network.json")
        .expect("Failed to read network configuration file");

    let network_nodes: Vec<PeerConfig> = serde_json::from_str(&network_config)
        .expect("Failed to parse network configuration");

    for peer in network_nodes {
        let mut rpc_server = RaftServer::new(peer.port, peer.id).await;
        tokio::spawn(async move {
            rpc_server.start().await;
        });
    }

    loop {}

    Ok(())
}

// 
