pub mod int {

    pub fn read_be_u32(input: &mut &[u8]) -> u32 {
        let (int_bytes, rest) = input.split_at(std::mem::size_of::<u32>());
        *input = rest;
        u32::from_be_bytes(int_bytes.try_into().unwrap())
    }
}

pub mod python {
    use pyo3::prelude::*;
    use pyo3::Python;
    use serde::Serialize;
    use serde_json::Value;
    use std::collections::HashMap;

    pub fn value_to_object(val: &Value, py: Python<'_>) -> PyObject {
        match val {
            Value::Null => py.None(),
            Value::Bool(x) => x.to_object(py),
            Value::Number(x) => {
                let oi64 = x.as_i64().map(|i| i.to_object(py));
                let ou64 = x.as_u64().map(|i| i.to_object(py));
                let of64 = x.as_f64().map(|i| i.to_object(py));
                oi64.or(ou64).or(of64).expect("number too large")
            }
            Value::String(x) => x.to_object(py),
            Value::Array(x) => {
                let inner: Vec<_> = x.iter().map(|x| value_to_object(x, py)).collect();
                inner.to_object(py)
            }
            Value::Object(x) => {
                let inner: HashMap<_, _> =
                    x.iter().map(|(k, v)| (k, value_to_object(v, py))).collect();
                inner.to_object(py)
            }
        }
    }

    #[repr(transparent)]
    #[derive(Clone, Debug, Serialize)]
    pub struct ParsedValue(Value);

    impl ToPyObject for ParsedValue {
        fn to_object(&self, py: Python<'_>) -> PyObject {
            value_to_object(&self.0, py)
        }
    }
}
