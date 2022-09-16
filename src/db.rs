use crate::utils::int::read_be_u32;
use crate::aol::{SetCommand,Loggable,LogCommand};
use pyo3::prelude::*;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::BufWriter;



#[pyclass]
pub struct Db {
    data: HashMap<String, PyObject>,
    location: String,
    log: Vec<LogCommand>,
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
        //self.dump();
    }

    pub fn dump(&self) {
        println!("Dumping to disk");
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.location.clone())
            .unwrap();

        let mut log_writer = BufWriter::new(log_file);

        for command in self.log.iter() {
            println!("command := {:?}",command);
            match command {
                LogCommand::Set(v) => {
                    let log = v.to_log() ;
                    println!("Log := {:?}" ,log);
                    let _ = log_writer.write(&v.to_log()).unwrap();
                }
            }
        }

        log_writer.flush().unwrap();
    }
}

#[pyfunction]
pub fn load(path: String) -> PyResult<Db> {
    let file = File::open(path.clone());

    let data: HashMap<String, PyObject> = match file {
        Ok(f) => {
            let mut data: HashMap<String, PyObject> = HashMap::new();

            let mut logs = vec![];
            let mut log = vec![];
            let mut size = 0;

            for byte in f.bytes() {
                println!("Log : {:?}",log);
                println!("Total size {:?}",size);
                log.push(byte.unwrap());
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
                println!("Log : {:?}",log);
                match log.unwrap() {
                    LogCommand::Set(c) => {
                        data.insert(c.key, c.value);
                    }
                }
            }

            data
        }

        Err(_) => HashMap::new(),
    };

    Ok(Db {
        data,
        location: path,
        log: vec![],
    })
}


