mod aol;
mod db;
mod utils;
mod errors;

use crate::db::{load, Db};
use pyo3::prelude::*;
use pyo3::Python;

#[pymodule]
fn layer_db(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(load, m)?)?;
    m.add_class::<Db>()?;
    Ok(())
}
