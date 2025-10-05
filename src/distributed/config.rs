use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct PeerCfg {
    pub node_id: u64,
    pub addr: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NodeConfig {
    pub node_id: u64,
    pub data_dir: String,
    pub raft_dir: String,
    pub rpc_addr: String,
    pub http_addr: String,
    pub peers: Vec<PeerCfg>,
    #[serde(default)]
    pub raft_tick_ms: Option<u64>,
    #[serde(default)]
    pub election_tick: Option<u64>,
    #[serde(default)]
    pub heartbeat_tick: Option<u64>,
    #[serde(default)]
    pub max_grpc_msg_bytes: Option<usize>,
}

impl NodeConfig {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let content = std::fs::read_to_string(path)?;
        let cfg: NodeConfig = serde_yaml::from_str(&content)?;
        Ok(cfg)
    }

    pub fn apply_defaults(mut self) -> Self {
        if self.raft_tick_ms.is_none() { self.raft_tick_ms = Some(100); }
        if self.election_tick.is_none() { self.election_tick = Some(10); }
        if self.heartbeat_tick.is_none() { self.heartbeat_tick = Some(1); }
        if self.max_grpc_msg_bytes.is_none() { self.max_grpc_msg_bytes = Some(4 * 1024 * 1024); }
        self
    }

    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.node_id == 0 { return Err("node_id must be > 0".into()); }
        if self.rpc_addr.is_empty() { return Err("rpc_addr must be set".into()); }
        if self.http_addr.is_empty() { return Err("http_addr must be set".into()); }
        Ok(())
    }
}


