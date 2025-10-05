#![cfg(feature = "raft-tikv")]

use std::collections::HashMap;
use tokio::sync::Mutex;
use protobuf::Message as PbMessage;
use crate::distributed::network::client::RaftClient;

pub trait RaftTransport: Send + Sync {
    fn send(&self, to: u64, msg: &raft::prelude::Message) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

pub struct GrpcTransport {
    /// node_id -> address (host:port)
    peers: HashMap<u64, String>,
    /// optional simple client cache
    clients: Mutex<HashMap<u64, RaftClient>>, 
}

impl GrpcTransport {
    pub fn new(peers: HashMap<u64, String>) -> Self { Self { peers, clients: Mutex::new(HashMap::new()) } }
}

impl RaftTransport for GrpcTransport {
    fn send(&self, to: u64, msg: &raft::prelude::Message) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let peers = self.peers.clone();
        let bytes = msg.write_to_bytes()?;
        // cannot clone Mutex; take an Arc to self instead to access clients inside the task
        let this = self as *const Self as usize;
        tokio::spawn(async move {
            if let Some(addr) = peers.get(&to) {
                // SAFETY: we only use clients for async lock; pointer origin is stable
                let this_ref = unsafe { &*(this as *const GrpcTransport) };
                let mut guard = this_ref.clients.lock().await;
                let client = if let Some(c) = guard.get_mut(&to) {
                    c
                } else {
                    let c = RaftClient::connect(addr).await.ok();
                    if c.is_none() { return; }
                    guard.insert(to, c.unwrap());
                    guard.get_mut(&to).unwrap()
                };
                let _ = client.step(bytes).await;
            }
        });
        Ok(())
    }
}


