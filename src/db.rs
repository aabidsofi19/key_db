use crate::aol::{LogCommand, Loggable, SetCommand};
use crate::utils::int::read_be_u32;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::hash::Hash;
use std::io::prelude::*;
use std::io::BufWriter;
use std::sync::mpsc::channel;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;

const MAX_LOG_SIZE: usize = 100; // Mazimum no of logs to hold in memory ater which they are dumped

#[pyclass]
pub struct Db {
    data: HashMap<String, PyObject>,
    location: String,
    log: Vec<LogCommand>,
    logs_tx: mpsc::Sender<Vec<LogCommand>>,
}

#[pymethods]
impl Db {
    pub fn get(&self, key: String) -> PyResult<PyObject> {
        let value = self.data.get(&key).unwrap();
        Ok(value.to_owned())
    }

    pub fn set(&mut self, key: String, value: PyObject) {
        // let v: Value = Python::with_gil(|py| depythonize(value.as_ref(py)).unwrap());
        self.data.insert(key.clone(), value.clone());
        self.log.push(LogCommand::Set(SetCommand { key, value }));

        if self.log.len() >= MAX_LOG_SIZE {
            self.dump();
        }
    }

    pub fn dump(&mut self) {
        self.logs_tx.send(self.log.clone()).unwrap();
        self.log.clear();
    }

    pub fn close(&mut self) {
        println!("Closing Database and persisting the data");
        dump(&self.log, &self.location);
        self.log.clear();
    }
}

fn dump(logs: &[LogCommand], location: &str) {
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(location)
        .unwrap();

    let mut log_writer = BufWriter::new(log_file);

    for command in logs.iter() {
        match command {
            LogCommand::Set(v) => {
                let _ = log_writer.write(&v.to_log()).unwrap();
            }
        }
    }

    log_writer.flush().unwrap();
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
                data.insert(c.key, c.value);
            }
        }
    }

    Ok(data)
}

fn spawn_persisting_thread(
    logs_rx: Receiver<Vec<LogCommand>>,
    location: String,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        // println!("Spawned Persisting Thread");
        for logs in logs_rx {
            dump(&logs, &location);
        }
    })
}

#[pyfunction]
pub fn load(path: String) -> PyResult<Db> {
    let file = File::open(path.clone());

    let data: HashMap<String, PyObject> = match file {
        Ok(f) => log_file_to_data(f).map_err(PyTypeError::new_err)?,

        Err(_) => HashMap::new(),
    };

    let (logs_tx, logs_rx) = channel();
    spawn_persisting_thread(logs_rx, path.clone());

    Ok(Db {
        data,
        logs_tx,
        location: path,
        log: vec![],
    })
}
