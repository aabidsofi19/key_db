use crate::utils::int::read_be_u32;
use core::ops::Range;
use serde_json::Value;

const KEY_SIZE_BYTES_LENGTH: usize = 1;
const VALUE_SIZE_BYTES_LENGTH: usize = 4;
const COMMAND_BYTE_LENGTH: usize = 1;
const COMMAND_INDEX_OFFSET: usize = 4;
const TOTAL_SIZE_INDEX: Range<usize> = 0..4;

pub trait Loggable<T> {
    fn action_value() -> u8;
    fn to_log(&self) -> Vec<u8>;
    fn from_log(log: &[u8]) -> Result<T, String>;
}

#[derive(Debug, Clone)]
pub struct SetCommand {
    pub key: String,
    pub value: Value,
}

#[derive(Debug, Clone)]
pub struct RemoveCommand {
    pub key: String,
}

impl Loggable<RemoveCommand> for RemoveCommand {
    fn action_value() -> u8 {
        2
    }

    fn to_log(&self) -> Vec<u8> {
        // RemoveCommand Log Structure:
        // [Total_size: u32 | Command_value: u8 | Key_size: u8 | Key: ...up to u8 bytes]
        // [ 1,  0,  0,   1 |                 1 |            3 |             10, 12, 11]

        // Components:
        // - Total_size: 32-bit unsigned integer representing the total size of the log entry in bytes.
        // - Command_value: 8-bit unsigned integer representing the command value for RemoveCommand (2).
        // - Key_size: 8-bit unsigned integer representing the size of the key in bytes.
        // - Key: The key value itself, up to u8 bytes in length.

        let mut log: Vec<u8> = vec![];

        let key = self.key.as_bytes();
        let key_size = key.len() as u8;

        let total_size = (COMMAND_BYTE_LENGTH + KEY_SIZE_BYTES_LENGTH + (key_size as usize)) as u32;

        log.extend_from_slice(&total_size.to_be_bytes());

        log.push(RemoveCommand::action_value());
        log.extend_from_slice(&key_size.to_be_bytes());
        log.extend_from_slice(key);

        log
    }

    fn from_log(log: &[u8]) -> Result<RemoveCommand, String> {
        let total_size = read_be_u32(&mut &log[TOTAL_SIZE_INDEX]) as usize;
        let key_size_index = 5;
        let key_starts_at = key_size_index + 1;
        let key_ends_at = key_starts_at + (log[key_size_index] as usize);

        if log.len() - 4 != total_size {
            return Err("Corupt Log".to_string());
        };

        if log[COMMAND_INDEX_OFFSET] != RemoveCommand::action_value() {
            return Err("Log command is not Remove".to_string());
        };

        let key = String::from_utf8(log[key_starts_at..key_ends_at].into()).unwrap();

        Ok(RemoveCommand { key })
    }
}

impl Loggable<SetCommand> for SetCommand {
    // add code here
    fn action_value() -> u8 {
        1
    }

    fn to_log(&self) -> Vec<u8> {

        // Set log structure:
        // [total size: u32 | command_value: u8 | key size: u8 | key      | value size: u32 | value  ]
        // [     1,  0,  0 1|                 1 |           3  | 10,1,22  |      3          |   1,2,4]

        // Components:
        // - total size: 32-bit unsigned integer representing the total size of the log entry in bytes.
        // - command_value: 8-bit unsigned integer representing the command value for SetCommand (1).
        // - key size: 8-bit unsigned integer representing the size of the key in bytes.
        // - key: The key value itself.
        // - value size: 32-bit unsigned integer representing the size of the value in bytes.
        // - value: The value associated with the key.
        
        
        let mut log: Vec<u8> = vec![];
        let value = serde_json::to_string(&self.value).unwrap().into_bytes();
        let value_length = value.len() as u32;
        let key = self.key.as_bytes();

        let key_len = key.len() as u8;
        let total_size: u32 = (COMMAND_BYTE_LENGTH
            + KEY_SIZE_BYTES_LENGTH
            + key_len as usize
            + VALUE_SIZE_BYTES_LENGTH
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

        if log[COMMAND_INDEX_OFFSET] != SetCommand::action_value() {
            return Err("Log command is not Set".to_string());
        };

        let key = String::from_utf8(log[key_starts_at..key_ends_at].into()).unwrap();
        let value = String::from_utf8(log[value_starts_at..value_ends_at].into()).unwrap();

        let deserialized_value: Value = serde_json::from_str(&value).unwrap();
        // let pythonized_value = Python::with_gil(|py| pythonize(py, &deserialized_value).unwrap());

        Ok(SetCommand {
            key,
            value: deserialized_value,
        })
    }
}

#[derive(Debug, Clone)]
pub enum LogCommand {
    Set(SetCommand),
    Remove(RemoveCommand),
}

impl LogCommand {
    pub fn to_log_bytes(&self) -> Vec<u8> {
        match self {
            LogCommand::Set(v) => v.to_log(),

            Self::Remove(v) => v.to_log(),
        }
    }

    pub fn from_log_bytes(log: &[u8]) -> Result<LogCommand, String> {
        match log[COMMAND_INDEX_OFFSET] {
            1 => {
                let set_command = SetCommand::from_log(log).unwrap();
                Ok(LogCommand::Set(set_command))
            }

            2 => {
                let remove_command = RemoveCommand::from_log(log).unwrap();
                Ok(LogCommand::Remove(remove_command))
            }
            _ => Err("Unknown Command".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {

    use std::vec;

    use super::*;
    use serde_json::json;

    fn set_command_bytes() -> Vec<u8> {
        vec![
            0, 0, 0, 16, 1, 4, 116, 101, 115, 116, 0, 0, 0, 6, 34, 116, 101, 115, 116, 34,
        ]
    }

    fn set_command() -> LogCommand {
        LogCommand::Set(SetCommand {
            key: "test".to_string(),
            value: json!("test".to_string()),
        })
    }

    fn remove_command() -> LogCommand {
        LogCommand::Remove(RemoveCommand {
            key: "test".to_string(),
        })
    }

    fn remove_command_bytes() -> Vec<u8> {
        vec![0, 0, 0, 6, 2, 4, 116, 101, 115, 116]
    }

    #[test]
    fn set_command_to_bytes() {
        let bytes = set_command().to_log_bytes();
        assert_eq!(bytes.len(), set_command_bytes().len());
        assert_eq!(bytes, set_command_bytes());
    }

    #[test]
    fn set_command_from_bytes() {
        let command_res = LogCommand::from_log_bytes(&set_command_bytes());

        assert!(command_res.is_ok());
        let command = command_res.unwrap();
        assert_eq!(command.to_log_bytes(), set_command_bytes());
    }

    #[test]
    fn remove_command_to_bytes() {
        let bytes = remove_command().to_log_bytes();

        // assert_eq!(bytes.len(),remove_command_bytes().len()) ;
        assert_eq!(bytes, remove_command_bytes());
    }
}
