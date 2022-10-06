use crate::aol::{LogCommand, RemoveCommand, SetCommand};
use crate::utils::int::read_be_u32;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::exceptions::{PyKeyError, PyTypeError};
use pyo3::prelude::*;
use pythonize::{depythonize, pythonize};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Display;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufWriter;
use std::io::{prelude::*, Error};
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::channel;
use std::sync::mpsc::{self, Receiver};
use std::sync::{atomic, Arc};
use std::thread::{self, JoinHandle};

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

#[pyclass]
pub struct Db {
    data: HashMap<String, PyObject>,
    is_open: Arc<AtomicBool>,
    logs_tx: Option<mpsc::Sender<LogCommand>>,
    persist_handler: Option<JoinHandle<()>>,
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

        let value: Value = Python::with_gil(|py| depythonize(value.as_ref(py)).unwrap());

        self.logs_tx
            .as_ref()
            .unwrap()
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
                    .as_ref()
                    .expect("Db Closed")
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

        if let Some(tx) = self.logs_tx.take() {
            println!("Droping tx");
            drop(tx);
        };

        self.persist_handler
            .take()
            .map(JoinHandle::join)
            .unwrap()
            .unwrap();
        println!("closed");
        Ok(())
    }
}

fn log_file_to_data(f: File) -> Result<HashMap<String, PyObject>, String> {
   let commands = log_bytes_to_commands(f.bytes())?;
   log_commands_to_data(commands)
}

fn log_bytes_to_commands<T>(bytes: T) -> Result<Vec<LogCommand>, String>
where
    T: IntoIterator<Item = Result<u8, Error>>,
{
    let mut logs = vec![];
    let mut log = vec![];
    let mut size = 0;

    for byte in bytes {
        log.push(byte.map_err(|e| e.to_string())?);
        match log.len().cmp(&4) {
            Ordering::Equal => {
                size = read_be_u32(&mut &log[0..4]);
            }
            Ordering::Greater => {
                if log.len() == 4 + (size as usize) {
                    logs.push(LogCommand::from_log_bytes(&log)?);
                    log.clear();
                }
            }

            _ => {}
        }
    }

    Ok(logs)
}

fn log_commands_to_data(commands: Vec<LogCommand>) -> Result<HashMap<String, PyObject>, String> {
    let mut data: HashMap<String, PyObject> = HashMap::new();

    for command in commands {
        match command {
            LogCommand::Set(c) => {
                let pythonized_value = Python::with_gil(|py| {
                    pythonize(py, &c.value).unwrap_or_else(|_| panic!("Corrup log cannot pythonize {c:?}")) 
                });
                data.insert(c.key, pythonized_value);
            }
            LogCommand::Remove(c) => {
                data.remove(&c.key).unwrap();
            }
        }
    }

    Ok(data)
}

fn spawn_persisting_thread(
    logs_rx: Receiver<LogCommand>,
    location: String,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        dump(logs_rx, &location);
        println!("Finished Persisting Thread");
    })
}

fn dump<T>(logs: T, location: &str)
where
    T: IntoIterator<Item = LogCommand>,
{
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(location)
        .unwrap();

    let mut log_writer = BufWriter::new(log_file);

    for command in logs.into_iter() {
        let _ = log_writer.write(&command.to_log_bytes()).unwrap();
    }

    println!("flushed");
    log_writer.flush().unwrap();
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

    let persist_handler = Some(spawn_persisting_thread(logs_rx, path));

    let db = Db {
        data,
        is_open: Arc::clone(&is_open),
        logs_tx: Some(logs_tx),
        persist_handler,
    };
    Ok(db)
}
