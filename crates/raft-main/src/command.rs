/*
 * Executable commands in the core Raft system
 */

use serde::{Deserialize, Serialize};

// 
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Command {
    Get(String),
    Set(String, String),
}

// Make it so 
impl Command {
    // pub fn execute(&self, state: &mut StateMachine) {
    //     match self {
    //         SetCommand(key, value) => state.set(key, value)
    //     }
    // }
}

