use crate::aol::{LogCommand, Loggable, RemoveCommand, SetCommand};
use crate::utils::int::read_be_u32;
use log::{info, trace, warn};
use pyo3::exceptions::{PyKeyError, PyTypeError};
use pyo3::prelude::*;
use signal_hook::consts::signal::*;
use signal_hook::consts::TERM_SIGNALS;
use signal_hook::flag;
use signal_hook::iterator::Signals;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::BufWriter;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::channel;
use std::sync::mpsc::{self, Receiver};
use std::sync::{atomic, Arc, Mutex};
use std::thread;

const MAX_LOG_SIZE: usize = 100; // Mazimum no of logs to hold in memory ater which they are dumped

pub struct BackgroundThread {
    name: String,
    is_running: AtomicBool,
}

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
    pub fn get(&self, key: String) -> PyResult<PyObject> {
        let value = self.data.get(&key).unwrap();
        Ok(value.to_owned())
    }

    pub fn set(&mut self, key: String, value: PyObject) {
        // let v: Value = Python::with_gil(|py| depythonize(value.as_ref(py)).unwrap());
        self.data.insert(key.clone(), value.clone());
        self.logs_tx
            .send(LogCommand::Set(SetCommand { key, value }))
            .unwrap();
    }

    pub fn remove(&mut self, key: String) -> PyResult<PyObject> {
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

    pub fn close(&mut self) {
        self.is_open.store(false, atomic::Ordering::Relaxed);
        loop {
            if self.running_thread_count() == 0 {
                break;
            }
        }
    }

    fn running_thread_count(&self) -> u8 {
        self.background_threads
            .iter()
            .filter(|t| t.is_running.load(atomic::Ordering::Relaxed))
            .count() as u8
    }
}
//
// fn dump<T>(logs: T, location: &str)
// where
//     T: IntoIterator<Item = LogCommand>,
// {
//     let log_file = OpenOptions::new()
//         .create(true)
//         .append(true)
//         .open(location)
//         .unwrap();
//
//     let mut log_writer = BufWriter::new(log_file);
//
//     for command in logs.into_iter() {
//         match command {
//             LogCommand::Set(v) => {
//                 let _ = log_writer.write(&v.to_log()).unwrap();
//             }
//         }
//     }
//
//     log_writer.flush().unwrap();
// }

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
            LogCommand::Remove(c) => {
                data.remove(&c.key);
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
                    //println!("Saving {:?}", command);
                    let _ = log_writer.write(&command.to_log_bytes()).unwrap();
                }

                Err(_) => {
                    if !is_open.load(atomic::Ordering::Relaxed) {
                        //println!("Closing Persisting Thread");
                        thread.is_running.store(false, atomic::Ordering::Relaxed);
                        break;
                    }
                }
            }
        }

        log_writer.flush().unwrap();
    })
}

fn gracefull_shutown(is_open: Arc<AtomicBool>) -> Result<(), std::io::Error> {
    // let term_now = Arc::new(AtomicBool::new(false));

    for sig in TERM_SIGNALS {
        // When terminated by a second term signal, exit with exit code 1.
        // This will do nothing the first time (because term_now is false).
        flag::register_conditional_shutdown(*sig, 1, Arc::clone(&is_open))?;
        // But this will "arm" the above for the second time, by setting it to true.
        // The order of registering these is important, if you put this one first, it will
        // first arm and then terminate ‒ all in the first round.
        flag::register(*sig, Arc::clone(&is_open))?;
    }
    //
    // let handler = thread::spawn(move || loop {
    //     if term_now.load(atomic::Ordering::Relaxed) || !is_open.load(atomic::Ordering::Relaxed) {
    //         //println!("Shutting Down");
    //         is_open.store(false, atomic::Ordering::Relaxed);
    //         //println!("Done Shutdown");
    //         break;
    //     }
    // });
    //
    // Ok(handler)
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
        is_running: AtomicBool::new(false),
        name: "persisting_thread".to_owned(),
    });

    let persisting_handler = spawn_persisting_thread(
        logs_rx,
        path.clone(),
        Arc::clone(&is_open),
        Arc::clone(&persisting_thread),
    );
    gracefull_shutown(Arc::clone(&is_open))?;

    let db = Db {
        data,
        logs_tx,
        is_open: Arc::clone(&is_open),
        location: path.clone(),
        background_threads: vec![persisting_thread],
    };
    Ok(db)
}
