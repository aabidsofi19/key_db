use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use std::fmt::Display;

// Generates Public Python Exceptions
create_exception!(layer_db, ConnectionClosedException, PyException);
create_exception!(layer_db, InvalidDatatypeException, PyException);
create_exception!(layer_db, CorruptLogException, PyException);



#[derive(Debug, Clone)]
pub struct ConnectionClosedError;

impl Display for ConnectionClosedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cant Perform Action on closed database")
    }
}

impl From<ConnectionClosedError> for PyErr {
    fn from(e: ConnectionClosedError) -> Self {
        ConnectionClosedException::new_err(e.to_string())
    }
}

impl std::error::Error for ConnectionClosedError {}


#[derive(Debug)]
pub enum SetError {
    ConnectionClosed,
    InvalidDataType(String),
}

impl Display for SetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SetError::ConnectionClosed => write!(f, "{ConnectionClosedError}"),
            SetError::InvalidDataType(message) => write!(f, "{message}"),
        }
    }
}

impl From<SetError> for PyErr {
    fn from(e: SetError) -> Self {
        match e {
            SetError::ConnectionClosed => PyErr::from(ConnectionClosedError),
            SetError::InvalidDataType(msg) => InvalidDatatypeException::new_err(msg),
        }
    }
}

impl std::error::Error for SetError {}


