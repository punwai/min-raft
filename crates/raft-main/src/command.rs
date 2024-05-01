/*
 * Executable commands in the core Raft system
 */

use crate::sm::StateMachine;

// 
pub enum Commands {
    GetCommand(u64),
    SetCommand(u64, u64),
}

// Make it so 
impl Commands {
    pub fn execute(&self, state: &mut StateMachine) {
        match self {
            SetCommand(key, value) => state.set(key, value)
        }
    }
}

