use crate::utils::int::read_be_u32;
use core::ops::Range;
use pyo3::prelude::*;
use pyo3::Python;
use pythonize::{depythonize, pythonize};
use serde_json::Value;

const KEY_SIZE_BYTES: usize = 1;
const VALUE_SIZE_BYTES: usize = 4;
const COMMAND_SIZE: usize = 1;
const COMMAND_INDEX: usize = 4;
const TOTAL_SIZE_INDEX: Range<usize> = 0..4;

// Command Values

pub trait Loggable<T> {
    // add code here
    //
    fn action_value() -> u8 ;
    fn to_log(&self) -> Vec<u8>;
    fn from_log(log: &[u8]) -> Result<T, String>;
}

#[derive(Debug)]
pub struct SetCommand {
    pub key: String,
    pub value: PyObject,
}



impl Loggable<SetCommand> for SetCommand {

    // add code here
    fn action_value() -> u8 {
        1
    }

    fn to_log(&self) -> Vec<u8> {
        // Set log structure
        //  [total size u32][command_value i.e 1][key size
        //  u8][k][e][y][value][size][i][32][v][a][l][u][e]

        let mut log: Vec<u8> = vec![];

        let v: Value = Python::with_gil(|py| depythonize(self.value.as_ref(py)).unwrap());
        let value = serde_json::to_string(&v).unwrap().into_bytes();
        let value_length = value.len() as u32;
        let key = self.key.as_bytes();

        let key_len = key.len() as u8;
        let total_size: u32 = (COMMAND_SIZE
            + KEY_SIZE_BYTES
            + key_len as usize
            + VALUE_SIZE_BYTES
            + value_length as usize) as u32;

        log.extend_from_slice(&total_size.to_be_bytes());
        log.push(SetCommand::action_value()); // Command Action (1 for set)
        log.extend_from_slice(&key_len.to_be_bytes());
        log.extend_from_slice(key);
        log.extend_from_slice(&value_length.to_be_bytes());
        log.extend_from_slice(&value);

        log
    }

    fn from_log(log: &[u8]) -> Result<SetCommand, String> {
        let total_size = read_be_u32(&mut &log[TOTAL_SIZE_INDEX]) as usize;
        let key_size_index = 5;
        let key_starts_at = key_size_index + 1;
        let key_ends_at = key_starts_at + (log[key_size_index] as usize);
        let value_size_ends_at = key_ends_at + 4;
        let value_size = read_be_u32(&mut &log[key_ends_at..value_size_ends_at]) as usize;

        let value_starts_at = value_size_ends_at;
        let value_ends_at = value_size_ends_at + value_size;

        if log.len() - 4 != total_size {
            return Err("Corupt Log".to_string());
        };

        if log[COMMAND_INDEX] != SetCommand::action_value() {
            return Err("Log command is not Set".to_string());
        };

        let key = String::from_utf8(log[key_starts_at..key_ends_at].into()).unwrap();
        let value = String::from_utf8(log[value_starts_at..value_ends_at].into()).unwrap();

        let deserialized_value: Value = serde_json::from_str(&value).unwrap();
        let pythonized_value = Python::with_gil(|py| pythonize(py, &deserialized_value).unwrap());

        Ok(SetCommand {
            key,
            value: pythonized_value,
        })
    }
}

#[derive(Debug)]
pub enum LogCommand {
    Set(SetCommand),
}


impl LogCommand {
    

    pub fn from_log_bytes(log: &[u8]) -> Result<LogCommand, String> {
        match log[COMMAND_INDEX] {
            1 => {
                let set_command = SetCommand::from_log(log).unwrap();
                println!("Set : {} {:?}", set_command.key, set_command.value);
                Ok(LogCommand::Set(set_command))
            }
            _ => Err("Unknown Command".to_string()),
        }
    }
}
