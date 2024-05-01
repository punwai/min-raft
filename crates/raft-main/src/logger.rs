// 
// Logging module
// 
use std::fs::File;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct LockedLogger(pub Arc<Mutex<Logger>>);

pub struct Logger {
    // Option to write logs to file.
    pub id: u64,
    pub output: Option<File>,
}

impl LockedLogger {
    pub fn new(id: u64, output: Option<File>) -> Self {
        Self(Arc::new(Mutex::new(Logger { id, output })))
    }

    pub fn log(&self, message: &str) -> io::Result<()> {
        let start = SystemTime::now();
        let since_the_epoch = start.duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        
        let timestamp = since_the_epoch.as_secs();

        let mut logger = self.0.lock().unwrap();

        if let Some(file) = &mut logger.output {
            writeln!(file, "{} - {}", timestamp, message)?;
        } else {
            println!("t:{} id:{} - {}", timestamp, logger.id, message);
        }

        Ok(())
    }
}

#[macro_export]
macro_rules! raft_log {
    ($logger:expr, $($arg:tt)*) => {{
        let message = format!($($arg)*);
        $logger.log(&message).unwrap();
    }};
}
