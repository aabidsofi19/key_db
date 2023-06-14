use crate::aol::commands::{LogCommand, RemoveCommand, SetCommand};
use crate::aol::log_io::{load_log_file,dump};
use pyo3::exceptions::{PyKeyError, PyTypeError};
use pyo3::prelude::*;
use pythonize::depythonize;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::channel;
use std::sync::mpsc::{self, Receiver};
use std::sync::{atomic, Arc};
use std::thread::{self, JoinHandle};

use crate::errors::{ConnectionClosedError, SetError};

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
        self.check_connection()?;
        let value = self.data.get(&key);
        Ok(value)
    }

    /// checks if db is open else returns a error
    fn check_connection(&self) -> Result<(), ConnectionClosedError> {
        if !self.is_open.load(atomic::Ordering::Relaxed) {
            return Err(ConnectionClosedError);
        };
        Ok(())
    }

    pub fn set(&mut self, key: String, value: PyObject) -> Result<bool, SetError> {
        self.check_connection()
            .map_err(|_| SetError::ConnectionClosed)?;

        let value_deserialized: Value = Python::with_gil(|py| {
            depythonize(value.as_ref(py)).map_err(|e| SetError::InvalidDataType(e.to_string()))
        })?;

        self.data.insert(key.clone(), value);

        self.logs_tx
            .as_ref()
            .unwrap()
            .send(LogCommand::Set(SetCommand {
                key,
                value: value_deserialized,
            }))
            .map(|_| true)
            .or(Ok(false))
    }

    pub fn remove(&mut self, key: String) -> PyResult<PyObject> {
        self.check_connection()?;

        let removed = self.data.remove(&key);

        match removed {
            Some(k) => {
                self.logs_tx
                    .as_ref()
                    .ok_or(ConnectionClosedError)
                    .map(|tx| tx.send(LogCommand::Remove(RemoveCommand { key })).unwrap())?;
                Ok(k)
            }

            None => Err(PyKeyError::new_err("Key not found")),
        }
    }

    pub fn close(&mut self) -> PyResult<()> {
        self.check_connection()?;
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

fn spawn_persisting_thread(
    logs_rx: Receiver<LogCommand>,
    location: String,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        dump(logs_rx, &location);
        println!("Finished Persisting Thread");
    })
}

#[pyfunction]
pub fn load(path: String) -> PyResult<Db> {
    let file = File::open(path.clone());

    let data: HashMap<String, PyObject> = match file {
        Ok(f) => load_log_file(f).map_err(PyTypeError::new_err)?,

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
