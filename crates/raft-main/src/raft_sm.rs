
// The Raft state machine.

use std::{fs::File, io::Write, sync::{Arc, Mutex, MutexGuard}};
use crate::{election::Quorum, logger::LockedLogger, messages::types::{AppendEntriesMessage, AppendEntriesResponse, CallMessage, CallResponse, RequestVoteMessage, RequestVoteResponse}, network::Network, raft_log, rpc::RaftRpc};
use rand::Rng;
use serde::{Deserialize, Serialize};

const QUORUM_SIZE: u64 = 3;
type Command = String;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry<T> {
    pub term: u64,
    pub message: T,
}

const ELECTION_TIMEOUT: u128 = 1000000;
const HEARTBEAT_TIMEOUT: u128 = 150000;

pub struct LeaderState {
    next_index: Vec<u64>,
    match_index: Vec<u64>,
}

// Read only variables (no need for mutual exclusion)
pub struct Config {
    pub id: u64,
    pub port_id: u32,
    pub election_timeout: u128
}

pub struct RaftServer {
    // RaftServer stores mutex to the state. The state is apassed
    pub config: Config,
    pub state: Arc<Mutex<State>>,
    pub network: Arc<Network>,
    pub logger: LockedLogger
}

// Initialize the RaftServer
impl RaftServer {
    // Initialize the RaftServer
    pub async fn new(port_id: u32, id: u64) -> Arc<Self> {
        let state = State::new(id, 0, vec![], format!("raft_log_{}.json", id));
        let state = Arc::new(Mutex::new(state));

        let network = 
            Arc::new(Network::new_from_file("network.json", Some(id)));

        let logger = LockedLogger::new(id, None);


        let mut rng = rand::thread_rng(); // Create an instance of a random number generator

        let config = Config {
            election_timeout: ELECTION_TIMEOUT + rng.gen_range(0..500000),
            id,
            port_id,
        };

        Arc::new(RaftServer {
            state,
            network,
            logger,
            config
        })
    }

    // We run a heartbeat to 
    pub async fn start(self: Arc<Self>) {
        let rpc = RaftRpc::new(self.clone()).await.unwrap();
        let server_address = rpc.run_server(self.config.port_id as u32).await.unwrap();

        raft_log! ( self.logger, "Started RPC server with address {:?}", server_address );

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                Self::timer_callback(self.clone()).await;
            }
        });
    }

    // The heartbeat is called
    async fn timer_callback(server: Arc<RaftServer>) {
        let curr_time = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros();

        let state_unlock = server.state.lock().unwrap();
        let status = state_unlock.status;
        let election_timeout = state_unlock.election_timeout;
        drop(state_unlock);

        match status {
            Status::Leader => {
                if curr_time - election_timeout > HEARTBEAT_TIMEOUT {
                    let state = server.state.lock().unwrap();
                    send_heartbeat(&state, server.network.clone(), server.logger.clone());
                }
            },
            Status::Candidate => {
                if curr_time - election_timeout > server.config.election_timeout {
                    begin_election(server);
                }
            },
            Status::Follower => {
                if curr_time - election_timeout > server.config.election_timeout {
                    begin_election(server);
                }
            }
        }
    }
}

// Transition the state to leader
fn transition_to_leader(state: &mut State, network: Arc<Network>, logger: LockedLogger) {
    state.status = Status::Leader;

    // Send out the heartbeat
    send_heartbeat(state, network, logger);
}

fn begin_election(server: Arc<RaftServer>) {
    let msg = {
        let mut state: MutexGuard<'_, State> = server.state.lock().unwrap();

        state.status = Status::Candidate;
        state.current_term += 1;
        state.voted_for = Some(state.id);

        state.election_timeout = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros();
        state.election = Some(Quorum::new(QUORUM_SIZE as usize));
    
        // Vote for yourself
        let id = state.id;
        if let Some(election) = state.election.as_mut() {
            election.vote(id);
        }
        let last_log_index = if state.log.is_empty() { 0 } else { state.log.len() as u64 - 1 };
        let last_log_term = if state.log.is_empty() { 0 } else { state.log[last_log_index as usize].term };

        RequestVoteMessage {
            term: state.current_term,
            candidate_id: id,
            last_log_index,
            last_log_term,
        }
    };
    
    tokio::spawn ( 
        async move {

            let callback = |result: RequestVoteResponse, peer_id: u64| -> anyhow::Result<()> {
                let mut state = server.state.lock().unwrap();
                raft_log!( server.logger, "Vote received from peer {:?}", peer_id );
                // Only allow the vote if it is the current quorum
                if result.vote_granted && state.current_term == result.term {
                    if let Some(election) = &mut state.election {
                        election.vote(peer_id);
                        if election.is_quorum_pass() {
                            transition_to_leader(&mut state, server.network.clone(), server.logger.clone());
                        }
                    }
                }

                Ok(())
            };

            let res = 
                server.network.broadcast::<RequestVoteMessage, RequestVoteResponse, _>(msg, callback).await;
            if let Ok(_) = res {
                raft_log! ( server.logger, "Election broadcast success" );
            } else {
                raft_log! ( server.logger, "Election broadcast success" );
            }
        }
    );
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Status {
    Follower,
    Candidate,
    Leader
}

pub struct State {
    pub id: u64,
    pub log_file: File,

    pub status: Status,
    // Persistent State on all servers
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub log: Vec<LogEntry<Command>>,
    // Volatile State on all servers
    pub commit_index: u64,
    pub last_applied: u64,
    pub leader_state: Option<LeaderState>,
    // Timer-related
    pub election_timeout: u128,
    pub election: Option<Quorum>,
}

impl State {
    pub fn new(id: u64, current_term: u64, log: Vec<LogEntry<Command>>, log_file: String) -> Self {
        let election_timeout = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros();
        let log_file = File::create(log_file).unwrap();

        Self {
            id,
            log_file,

            status: Status::Follower,
            current_term,
            voted_for: None,
            log,
            commit_index: 0,
            last_applied: 0,
            leader_state: None,
            // An election is only started if we are a candidate
            election: None, 
            // 
            election_timeout: election_timeout,
        }
    }

    pub fn add_logs(&mut self, log_entries: Vec<LogEntry<Command>>) {
        for log_entry in log_entries{
            let json = serde_json::to_string(&log_entry).unwrap();
            self.log_file.write_all(json.as_bytes()).unwrap();
            self.log_file.write_all("\n".as_bytes()).unwrap();
            self.log.push(log_entry);
        }
    }

    // Prune the log to the given length
    pub fn prune_logs(&mut self, length: u64) {
        self.log.truncate(length as usize);
        self.write_logs();
    }

    fn write_logs(&mut self) {
        for entry in self.log.iter() {
            let json = serde_json::to_string(&entry).unwrap();
            self.log_file.write_all(json.as_bytes()).unwrap();
            self.log_file.write_all("\n".as_bytes()).unwrap();
        }
    }
}

//
pub fn handle_request_votes(request_vote_message: RequestVoteMessage, state: &mut MutexGuard<State>) -> RequestVoteResponse {
    let RequestVoteMessage { 
        term, 
        candidate_id, 
        last_log_index, 
        last_log_term 
    } = request_vote_message;

    if term < state.current_term {
        return RequestVoteResponse { term: state.current_term, vote_granted: false };
    }

    // Check if the logs are up to date 
    if state.log.is_empty() || last_log_term >= state.log[last_log_index as usize].term {
        if term > state.current_term || state.voted_for.is_none() {
            if term > state.current_term {
                state.current_term = term;
            }
        
            state.voted_for = Some(candidate_id);
            println!("{:?} voting for {:?} on term {:?}", state.id, candidate_id, term);
            return RequestVoteResponse { term: state.current_term, vote_granted: true };
        }
    }

    RequestVoteResponse {
        term: state.current_term,
        vote_granted: false
    }
}


pub fn check_status(state: &mut MutexGuard<State>, term: u64) -> anyhow::Result<()> {
    match state.status {
        Status::Leader => {
            // If someone else is the leader, then we obviously want to
            // convert to the new status
            if state.current_term == term {
                return Err(anyhow::anyhow!("Error - another node has become the leader, violating safety property."));
            }
            if state.current_term < term {
                state.status = Status::Follower;
            }
        },
        Status::Candidate => {
            // Someone has been elected as a leader, so we revert to
            // the follower status
            if state.current_term <= term {
                state.status = Status::Follower;
            }
        },
        _ => (),
    }
    Ok(())
}

pub fn handle_append_entries(append_entries_message: AppendEntriesMessage, state: &mut MutexGuard<State>) -> AppendEntriesResponse {
    // Reset election timeout
    state.election_timeout = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros();

    let AppendEntriesMessage { 
        term, 
        leader_id: _leader_id, 
        prev_log_index, 
        prev_log_term, 
        entries, 
        leader_commit 
    } = append_entries_message;

    if check_status(state, term).is_err() {
        return AppendEntriesResponse { term: state.current_term, success: false };
    }

    if term < state.current_term {
        return AppendEntriesResponse { term: state.current_term, success: false };
    }

    if term < state.current_term || (
        prev_log_index < state.log.len() as u64 &&
        prev_log_term != state.log[prev_log_index as usize].term
    ) {
        return AppendEntriesResponse { term: state.current_term, success: false };
    }

    // if there is an index conflict
    let curr_log_index = prev_log_index + 1;

    // We discard all the ones that are not used
    if state.log.len() > curr_log_index as usize && state.log[curr_log_index as usize].term != term {
        // prune the state log -- discard all the entries that are curr_log_index and after
        state.prune_logs(curr_log_index);
    }

    state.add_logs(entries);

    // We update the commit inex to be the last thing that we've updated it to.
    // Alternatively, 
    if leader_commit > state.commit_index {
        let last_log = if state.log.is_empty() { 0 } else { state.log.len() as u64 - 1 };
        state.commit_index = std::cmp::min(leader_commit, last_log);
    }

    AppendEntriesResponse {
        term: state.current_term,
        success: true 
    }
}


pub async fn handle_call(call_message: CallMessage, server: Arc<RaftServer>) -> CallResponse {
    // Reset election timeout
    let message = {
        let mut state: MutexGuard<'_, State> = server.state.lock().unwrap();

        state.election_timeout = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros();
        let CallMessage { 
            command 
        } = call_message;
    
        // serialize command
        let command_str = serde_json::to_string(&command).unwrap();
        if state.status != Status::Leader {
            return CallResponse { ack: false };
        }
    
        // If we are the leader, add this to our logs
        let term = state.current_term;
        let log_entry = LogEntry { term, message: command_str };
        state.add_logs(vec![log_entry.clone()]);

        AppendEntriesMessage {
            term,
            leader_id: state.id,
            prev_log_index: state.log.len() as u64 - 1,
            prev_log_term: state.log[state.log.len() as usize - 1].term,
            entries: vec![log_entry],
            leader_commit: state.commit_index
        }
    };

    let quorum = Arc::new(Mutex::new(Quorum::new(QUORUM_SIZE as usize)));

    let callback = |result: AppendEntriesResponse, peer_id: u64| -> anyhow::Result<()> {
        let mut state = server.state.lock().unwrap();
        if result.success {
            let mut quorum = quorum.lock().unwrap();
            quorum.vote(peer_id);
            if quorum.is_quorum_pass() {
                state.commit_index = std::cmp::max(state.commit_index, 1);
            }
        };
        Ok(())
    };

    server.network.broadcast(
        message, callback
    ).await.unwrap();

    // Condition variable that only wakes up when we have a response
    CallResponse {
        ack: true
    }
}

// Given readable State and Network, send out a heartbeat.
fn send_heartbeat(state: &State, network: Arc<Network>, logger: LockedLogger) {
    raft_log!( logger, "sending out heartbeat {:?}", state.current_term );

    let prev_log_index = if state.log.is_empty() { 0 } else { state.log.len() as u64 - 1 };
    let prev_log_term = if state.log.is_empty() { 0 } else { state.log[state.log.len() as usize - 1].term };

    let msg = {
        AppendEntriesMessage {
            term: state.current_term, 
            leader_id: state.id,
            prev_log_index,
            prev_log_term,
            entries: vec![],
            leader_commit: state.commit_index
        }
    };

    let callback = move |_result: AppendEntriesResponse, _peer_id: u64| -> anyhow::Result<()> {
        Ok(())
    };

    tokio::spawn(async move {
        network
            .clone()
            .broadcast::<AppendEntriesMessage, AppendEntriesResponse, _>(msg, callback)
            .await
            .unwrap()
    });
}
