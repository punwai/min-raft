

// Types of messages

// Allowing converting object to and from params object

use jsonrpsee::{rpc_params, types::Params};
use jsonrpsee_core::params::ArrayParams;
use crate::{command::Command, raft_sm::LogEntry};
use serde::{Serialize, Deserialize};

pub trait Message where Self: Sized + Clone {
    const COMMAND: &'static str;

    fn into_params(&self) -> ArrayParams;
    fn from_params(params: Params<'_>) -> anyhow::Result<Self>;
}

pub trait ResponseMessage where Self: Sized {
    fn into_response(&self) -> ArrayParams;
    // fn from_response(params: ) -> anyhow::Result<Self>;
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct RequestVoteMessage {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

impl Message for RequestVoteMessage {
    const COMMAND: &'static str= "request_vote";

    fn into_params(&self) -> ArrayParams {
        rpc_params![
            self.term, 
            self.candidate_id, 
            self.last_log_index, 
            self.last_log_term
        ]
    }

    fn from_params(params: Params<'_>) -> anyhow::Result<Self> {
        let mut params_sequence = params.sequence();
        let term = params_sequence.next()?;
        let candidate_id = params_sequence.next()?;
        let last_log_index = params_sequence.next()?;
        let last_log_term = params_sequence.next()?;

        Ok(RequestVoteMessage {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        })
    }
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

// impl ResponseMessage for RequestVoteResponse {
//     fn into_response(&self) -> ArrayParams {
//         rpc_params![self.term, self.vote_granted]
//     }

//     fn from_response(params: Params<'_>) -> anyhow::Result<Self> {
//         let mut params_sequence = params.sequence();
//         let term = params_sequence.next()?;
//         let vote_granted = params_sequence.next()?;

//         Ok(RequestVoteResponse { term, vote_granted })
//     }
// }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesMessage {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry<String>>,
    pub leader_commit: u64,
}

impl Message for AppendEntriesMessage {
    const COMMAND: &'static str = "append_entries";

    fn into_params(&self) -> ArrayParams {
        rpc_params![
            self.term, 
            self.leader_id, 
            self.prev_log_index, 
            self.prev_log_term, 
            self.entries.clone(), 
            self.leader_commit
        ]
    }

    fn from_params(params: Params<'_>) -> anyhow::Result<Self> {
        let mut params_sequence = params.sequence();

        let term = params_sequence.next()?;
        let leader_id = params_sequence.next()?;
        let prev_log_index = params_sequence.next()?;
        let prev_log_term = params_sequence.next()?;
        let entries = params_sequence.next()?;
        let leader_commit = params_sequence.next()?;

        Ok(AppendEntriesMessage { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallMessage {
    pub command: Command
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallResponse {
    pub ack: bool
}


impl Message for CallMessage {
    const COMMAND: &'static str = "append_entries";

    fn into_params(&self) -> ArrayParams {
        rpc_params![
            self.command.clone()
        ]
    }

    fn from_params(params: Params<'_>) -> anyhow::Result<Self> {
        let mut params_sequence = params.sequence();

        let command= params_sequence.next()?;

        Ok(CallMessage { command })
    }
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
}
