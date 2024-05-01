
// The Raft state machine.

use std::{net::SocketAddr, sync::{Arc, Mutex, MutexGuard}};
use crate::{election::Quorum, logger::{LockedLogger, Logger}, messages::types::{AppendEntriesMessage, AppendEntriesResponse, RequestVoteMessage, RequestVoteResponse}, network::Network, raft_log, rpc::RaftRpc};
use rand::Rng;

const QUORUM_SIZE: u64 = 3;
type Command = String;

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
    pub election_timeout: u128
}

pub struct RaftServer {
    // RaftServer stores mutex to the state. The state is apassed
    pub config: Config,
    pub state: Arc<Mutex<State>>,
    pub server_address: SocketAddr,
    pub network: Arc<Network>,
    pub logger: LockedLogger
}

// Initialize the RaftServer
impl RaftServer {
    // Initialize the RaftServer
    pub async fn new(port_id: u32, id: u64) -> Arc<Self> {
        let state = State::new(id, 0, vec![]);
        let state = Arc::new(Mutex::new(state));
        let rpc = RaftRpc::new(state.clone()).await.unwrap();


        let server_address = rpc.run_server(port_id).await.unwrap();
        println!("RPC started with address {:?}", server_address);

        let network = 
            Arc::new(Network::new_from_file("network.json", Some(id)));

        let logger = LockedLogger::new(id, None);


        let mut rng = rand::thread_rng(); // Create an instance of a random number generator

        let config = Config {
            election_timeout: ELECTION_TIMEOUT + rng.gen_range(0..200000)
        };

        Arc::new(RaftServer {
            state,
            server_address,
            network,
            logger,
            config
        })
    }

    // We run a heartbeat to 
    pub async fn start(self: Arc<Self>) {
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

#[derive(Copy, Clone, Debug)]
pub enum Status {
    Follower,
    Candidate,
    Leader
}

pub struct State {
    pub id: u64, // The server's own ID

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
    pub fn new(id: u64, current_term: u64, log: Vec<LogEntry<Command>>) -> Self {
        let election_timeout = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros();
        Self {
            id,
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

    pub fn write_persistent(&mut self) {
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
        state.log.truncate(curr_log_index as usize);
        let log_entry_list: Vec<_> = entries.iter().map(|x| 
            LogEntry { term: term, message: x.to_string() }
        ).collect();
        state.log.extend(log_entry_list);
    }

    // We update the commit inex to be the last thing that we've updated it to.
    // Alternatively, 
    if leader_commit > state.commit_index {
        state.commit_index = std::cmp::min(leader_commit, state.log.len() as u64 - 1);
    }

    AppendEntriesResponse {
        term: state.current_term,
        success: true 
    }
}

// Given readable State and Network, send out a heartbeat.
fn send_heartbeat(state: &State, network: Arc<Network>, logger: LockedLogger) {
    raft_log!( logger, "sending out heartbeat {:?}", state.current_term );

    let prev_log_index = if state.log.is_empty() { 0 } else { state.log.len() as u64 - 1 };
    let prev_log_term = if state.log.is_empty() { 0 } else { state.log[prev_log_index as usize - 1].term };

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
