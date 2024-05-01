

// We start an election when we increase our status to candidate.
// We should asynchronously spawn a thread for each peer, to ask
// for a vote.

use std::collections::HashSet;

pub struct Quorum {
    _quorum_size: usize,
    quorum_pass: usize,
    peers: HashSet<u64>
}

impl Quorum {
    pub fn new(quorum_size: usize) -> Self {
        Self {
            _quorum_size: quorum_size,
            quorum_pass: (quorum_size + 1) / 2,
            peers: HashSet::new(),
        }
    }

    pub fn vote(&mut self, id: u64) {
        self.peers.insert(id);
    }

    pub fn is_quorum_pass(&self) -> bool {
        self.peers.len() >= self.quorum_pass
    }
}

