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
}

impl NodeConfig {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let content = std::fs::read_to_string(path)?;
        let cfg: NodeConfig = serde_yaml::from_str(&content)?;
        Ok(cfg)
    }
}


