use crate::aol::{LogCommand, RemoveCommand, SetCommand};
use crate::key_db;
use crate::utils::int::read_be_u32;
use log::{debug, info};
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::exceptions::{PyKeyError, PyTypeError};
use pyo3::prelude::*;
use signal_hook::consts::TERM_SIGNALS;
use signal_hook::flag;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Display;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::BufWriter;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::channel;
use std::sync::mpsc::{self, Receiver};
use std::sync::{atomic, Arc};
use std::thread;

const MAX_LOG_SIZE: usize = 100; // Mazimum no of logs to hold in memory ater which they are dumped

pub struct BackgroundThread {
    name: String,
    is_running: AtomicBool,
}

create_exception!(key_db, ConnectionClosedException, PyException);

#[derive(Debug, Clone)]
pub struct ConnectionClosed;

impl Display for ConnectionClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cant Perform Action on closed database")
    }
}

impl From<ConnectionClosed> for PyErr {
    fn from(e: ConnectionClosed) -> Self {
        ConnectionClosedException::new_err(e.to_string())
    }
}
//
// impl From<DbError> for PyErr {
//     fn from( e : DbError) -> Self {
//         match e {
//           DbError::ConnectionClosed(v) => v
//         }
//     }
// }
//
// pub enum DbError {
//     ConnectionClosed,
// }

#[pyclass]
pub struct Db {
    background_threads: Vec<Arc<BackgroundThread>>,
    data: HashMap<String, PyObject>,
    location: String,
    is_open: Arc<AtomicBool>,
    logs_tx: mpsc::Sender<LogCommand>,
}

#[pymethods]
impl Db {
    pub fn get(&self, key: String) -> PyResult<Option<&PyObject>> {
        self.connection_is_open()?;
        let value = self.data.get(&key);
        Ok(value)
    }

    /// checks if db is open else returns a error
    fn connection_is_open(&self) -> Result<(), ConnectionClosed> {
        if !self.is_open.load(atomic::Ordering::Relaxed) {
            return Err(ConnectionClosed);
        };
        Ok(())
    }

    pub fn set(&mut self, key: String, value: PyObject) -> Result<bool, ConnectionClosed> {
        self.connection_is_open()?;
        self.data.insert(key.clone(), value.clone());
        self.logs_tx
            .send(LogCommand::Set(SetCommand { key, value }))
            .map(|_| true)
            .or(Ok(false))
    }

    pub fn remove(&mut self, key: String) -> PyResult<PyObject> {
        self.connection_is_open()?;

        let removed = self.data.remove(&key);

        match removed {
            Some(k) => {
                self.logs_tx
                    .send(LogCommand::Remove(RemoveCommand { key }))
                    .unwrap();
                Ok(k)
            }

            None => Err(PyKeyError::new_err("Key not found")),
        }
    }

    pub fn close(&mut self) -> PyResult<()> {
        self.connection_is_open()?;

        self.is_open.store(false, atomic::Ordering::Relaxed);
        loop {
            if self.running_thread_count() == 0 {
                println!("All threads Closed");
                break;
            }
        }

        Ok(())
    }

    fn running_thread_count(&self) -> u8 {
        self.background_threads
            .iter()
            .filter(|t| t.is_running.load(atomic::Ordering::Relaxed))
            .count() as u8
    }
}

fn log_file_to_data(f: File) -> Result<HashMap<String, PyObject>, String> {
    let mut data: HashMap<String, PyObject> = HashMap::new();

    let mut logs = vec![];
    let mut log = vec![];
    let mut size = 0;

    for byte in f.bytes() {
        log.push(byte.map_err(|e| e.to_string())?);
        match log.len().cmp(&4) {
            Ordering::Equal => {
                size = read_be_u32(&mut &log[0..4]);
            }
            Ordering::Greater => {
                if log.len() == 4 + (size as usize) {
                    logs.push(LogCommand::from_log_bytes(&log));
                    log.clear();
                }
            }

            _ => {}
        }
    }

    for log in logs {
        
        match log? {
            LogCommand::Set(c) => {
                println!("setting key {}",c.key.clone());
                data.insert(c.key, c.value);
            }
            LogCommand::Remove(c) => {

                println!("removing key {}",c.key.clone());
                data.remove(&c.key).unwrap();
            }
        }
    }

    Ok(data)
}

fn spawn_persisting_thread(
    logs_rx: Receiver<LogCommand>,
    location: String,
    is_open: Arc<AtomicBool>,
    thread: Arc<BackgroundThread>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        //println!("Spawning Persisting Thread for db at {}", location);

        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(location)
            .unwrap();

        let mut log_writer = BufWriter::new(log_file);

        loop {
            let c = logs_rx.try_recv();

            match c {
                Ok(command) => {
                    println!("Saving {:?}", command);
                    let _ = log_writer.write(&command.to_log_bytes()).unwrap();
                }

                Err(_) => {
                    if !is_open.load(atomic::Ordering::Relaxed) {
                        println!("Closing Persisting Thread");
                        break;
                    }
                }
            }
        }
        
        println!("flushing to file");
        log_writer.flush().unwrap();
        thread.is_running.store(false, atomic::Ordering::Relaxed);
    })
}

fn gracefull_shutown(is_open: Arc<AtomicBool>) -> Result<(), std::io::Error> {
    for sig in TERM_SIGNALS {
        // When terminated by a second term signal, exit with exit code 1.
        // This will do nothing the first time (because term_now is false).
        flag::register_conditional_shutdown(*sig, 1, Arc::clone(&is_open))?;
        // But this will "arm" the above for the second time, by setting it to true.
        // The order of registering these is important, if you put this one first, it will
        // first arm and then terminate ‒ all in the first round.
        flag::register(*sig, Arc::clone(&is_open))?;
    }
    Ok(())
}

#[pyfunction]
pub fn load(path: String) -> PyResult<Db> {
    let file = File::open(path.clone());

    let data: HashMap<String, PyObject> = match file {
        Ok(f) => log_file_to_data(f).map_err(PyTypeError::new_err)?,

        Err(_) => HashMap::new(),
    };

    let (logs_tx, logs_rx) = channel();

    let is_open = Arc::new(AtomicBool::new(true));

    let persisting_thread = Arc::new(BackgroundThread {
        is_running: AtomicBool::new(true),
        name: "persisting_thread".to_owned(),
    });

    spawn_persisting_thread(
        logs_rx,
        path.clone(),
        Arc::clone(&is_open),
        Arc::clone(&persisting_thread),
    );
    gracefull_shutown(Arc::clone(&is_open))?;

    let db = Db {
        background_threads: vec![persisting_thread],
        data,
        location: path,
        is_open: Arc::clone(&is_open),
        logs_tx,
    };
    Ok(db)
}
