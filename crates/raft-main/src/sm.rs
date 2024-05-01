/*
 * The state machine or the "program". This is passed into each of the functions
 * This is the program that we are trying to edit -- it will be altered through a
 * sequence of "commands" or "transactions".
 */


pub struct StateMachine {
    pub hashmap: HashMap<String, String>,
}

// Very simple state-machine. Only operable by one
impl StateMachine {
    pub fn new() -> Self {
        Self {
        }
    }

    pub fn get(&mut self, key: String) -> Option<String> {
        self.hashmap.get(&key).cloned()
    }

    pub fn set(&mut self, key: String, value: String) {
        self.hashmap.insert(key, value);
    }
}

