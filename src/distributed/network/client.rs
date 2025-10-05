use tonic::transport::Channel;

use crate::distributed::network::pb::reefdb::raft::raft_client::RaftClient as RaftGrpcClient;
use crate::distributed::network::pb::reefdb::raft::RaftMessage;
use crate::distributed::network::pb::reefdb::sql::sql_client::SqlClient as SqlGrpcClient;
use crate::distributed::network::pb::reefdb::sql::{SqlRequest, SqlResponse};

pub struct RaftClient {
    inner: RaftGrpcClient<Channel>,
}

impl RaftClient {
    pub async fn connect<D: AsRef<str>>(dst: D) -> Result<Self, tonic::transport::Error> {
        let inner = RaftGrpcClient::connect(format!("http://{}", dst.as_ref())).await?;
        Ok(Self { inner })
    }

    pub async fn step(&mut self, data: Vec<u8>) -> Result<Vec<u8>, tonic::Status> {
        let resp = self.inner.step(RaftMessage { data }).await?;
        Ok(resp.into_inner().data)
    }
}

pub struct SqlClient {
    inner: SqlGrpcClient<Channel>,
}

impl SqlClient {
    pub async fn connect<D: AsRef<str>>(dst: D) -> Result<Self, tonic::transport::Error> {
        let inner = SqlGrpcClient::connect(format!("http://{}", dst.as_ref())).await?;
        Ok(Self { inner })
    }

    pub async fn execute(&mut self, sql: &str) -> Result<SqlResponse, tonic::Status> {
        let resp = self.inner.execute(SqlRequest { sql: sql.to_string() }).await?;
        Ok(resp.into_inner())
    }
}


