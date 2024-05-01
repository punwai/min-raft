// Networking module.
// Store all your peers port and address.

use std::{collections::HashMap, fs::File};
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use serde::{Deserialize, Serialize};
use jsonrpsee_core::{client::ClientT, params::ArrayParams, DeserializeOwned};
use crate::messages::types::Message;
use std::io::Read;

struct Peer {
    client: HttpClient,
    config: PeerConfig
}

#[derive(Serialize, Deserialize)]
pub struct PeerConfig {
    pub id: u64,
    pub address: String,
    pub port: u32,
}

pub struct Network {
    peers: HashMap<u64, Peer>,
}

// Broadcast
impl Network {
    pub fn new_empty() -> Self {
        Self { 
            peers: HashMap::new(),
        }
    }

    // Load network peers from file. 
    // Exclude id can be used to exclude self's id from the list.
    pub fn new_from_file(file_path: &str, exclude_id: Option<u64>) -> Self {
        let mut file = File::open(file_path).unwrap();
        let mut file_content = String::new();
        file.read_to_string(&mut file_content).unwrap();

        let peers_vec: Vec<PeerConfig> = serde_json::from_str(&file_content).unwrap();
        let peers = peers_vec
            .into_iter()
            .filter(|p| exclude_id.is_none() || p.id != exclude_id.unwrap())
            .map(|config| {
                let id = config.id;
                let url = format!("http://{}:{}", config.address, config.port);
                let client = HttpClientBuilder::default().build(url).unwrap();
                let peer = Peer {
                    client,
                    config,
                };
                (id, peer)
            })
            .collect();

        Self { peers }
    }

    // Broadcast a message to all peers. The way to send a message to a 
    // peer is asynchronously -- you send it all asynchronously, then you
    // do a handle to join it all. You loop and you start one with a callback.
    // you aggregate votes. The listeners times out after 200ms.

    // Send one message to peer
    async fn send_one<T: Message, R: DeserializeOwned>(&self, peer_id: u64, message: T) -> anyhow::Result<R> {
        let peer = self
            .peers
            .get(&peer_id)
            .unwrap();

        let response: Result<R, _> = peer.client.request::<_, ArrayParams>(T::COMMAND, message.into_params()).await;
        let res = response?;

        Ok(res)
    }

    // Broadcast a message to all peers. The way to send a message to a 
    // peer is asynchronously -- you send it all asynchronously, then you
    // do a handle to join it all. You loop and you start one with a callback.
    // you aggregate votes. The listeners times out after 200ms.

    // Send one message to peer
    pub async fn broadcast<T: Message, R: DeserializeOwned, F>(&self, message: T, mut callback: F) -> anyhow::Result<()> 
        where F: FnMut(R, u64) -> anyhow::Result<()>
    {
        for (&id, _) in self.peers.iter() {
            let result = self.send_one::<T, R>(id, message.clone()).await?;
            callback(result, id)?;
        }
        Ok(())
    }
}

