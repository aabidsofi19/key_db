use crate::aol::commands::LogCommand;
use crate::utils::int::read_be_u32;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pythonize::pythonize;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::File;
use std::io::{prelude::*, Error};
use std::io::BufWriter;
use crate::errors::CorruptLogException;
use std::fs::OpenOptions;

pub fn load_log_file(f: File) -> PyResult<HashMap<String, PyObject>> {
    let commands = parse_bytes_to_commands(f.bytes()).map_err(PyException::new_err)?;
    generate_data_from_commands(commands)
}

pub fn parse_bytes_to_commands<T>(bytes: T) -> Result<Vec<LogCommand>, String>

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

pub fn generate_data_from_commands(commands: Vec<LogCommand>) -> PyResult<HashMap<String, PyObject>> {
    let mut data: HashMap<String, PyObject> = HashMap::new();

    for command in commands {
        println!("loading command {command:?}");
        match command {
            LogCommand::Set(c) => {
                let pythonized_value = Python::with_gil(|py| {
                    pythonize(py, &c.value)
                        .map_err(|_| CorruptLogException::new_err(format!("Cant Pythonize {c:?}")))
                })?;
                data.insert(c.key, pythonized_value);
            }
            LogCommand::Remove(c) => {
                data.remove(&c.key).unwrap();
            }
        }
    }

    Ok(data)
}

pub fn dump<T>(logs: T, location: &str)
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


