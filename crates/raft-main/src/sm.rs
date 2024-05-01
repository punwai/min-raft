/*
 * The state machine or the "program". This is passed into each of the functions
 * This is the program that we are trying to edit -- it will be altered through a
 * sequence of "commands" or "transactions".
 */

use std::collections::HashMap;

use crate::command::Command;

pub trait StateMachine {
    type Command;

    fn execute_command(&mut self, command: Self::Command) {
        self.execute_command(command);
    }
}

// Define the default StateMachine. This will be a HashMap of Strings to Strings

struct HashMapStateMachine {
    hashmap: HashMap<String, String>,
}

impl StateMachine for HashMap<String, String> {
    type Command = Command;

    fn execute_command(&mut self, command: Self::Command) {
        match command {
            Command::Set(key, value) => self.insert(key, value),
            Command::Get(key) => self.get(&key).map(|value| value.to_string()),
        };
    }
}

