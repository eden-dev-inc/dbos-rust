use base64::Engine;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::{DbosError, Result};
use crate::types::{PortableWorkflowArgs, PortableWorkflowError};

pub const DBOS_JSON: &str = "DBOS_JSON";
pub const PORTABLE_JSON: &str = "portable_json";
pub const NIL_MARKER: &str = "__DBOS_NIL";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EncodedValue {
    pub data: Option<String>,
    pub serialization: String,
}

pub trait CustomSerializer: Send + Sync {
    fn name(&self) -> &str;
    fn encode_value(&self, value: &Value) -> Result<EncodedValue>;
    fn decode_value(&self, encoded: &EncodedValue) -> Result<Value>;
}

#[derive(Debug, Clone, Default)]
pub struct JsonSerializer;

impl JsonSerializer {
    pub fn encode<T: Serialize>(value: &T) -> Result<EncodedValue> {
        let value = serde_json::to_value(value)?;
        encode_json_value(&value)
    }

    pub fn decode<T: DeserializeOwned>(encoded: &EncodedValue) -> Result<T> {
        let value = decode_json_value(encoded)?;
        serde_json::from_value(value).map_err(DbosError::from)
    }
}

pub fn encode_json_value(value: &Value) -> Result<EncodedValue> {
    if value.is_null() {
        return Ok(EncodedValue {
            data: Some(NIL_MARKER.to_string()),
            serialization: DBOS_JSON.to_string(),
        });
    }

    let bytes = serde_json::to_vec(value)?;
    Ok(EncodedValue {
        data: Some(base64::engine::general_purpose::STANDARD.encode(bytes)),
        serialization: DBOS_JSON.to_string(),
    })
}

pub fn decode_json_value(encoded: &EncodedValue) -> Result<Value> {
    let Some(data) = &encoded.data else {
        return Ok(Value::Null);
    };
    if data == NIL_MARKER {
        return Ok(Value::Null);
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data.as_bytes())
        .map_err(|err| DbosError::serialization(format!("failed to decode base64 data: {err}")))?;
    serde_json::from_slice(&bytes).map_err(DbosError::from)
}

pub fn encode_portable<T: Serialize>(value: &T) -> Result<EncodedValue> {
    let value = serde_json::to_value(value)?;
    Ok(EncodedValue {
        data: Some(serde_json::to_string(&value)?),
        serialization: PORTABLE_JSON.to_string(),
    })
}

pub fn decode_portable<T: DeserializeOwned>(encoded: &EncodedValue) -> Result<T> {
    let Some(data) = &encoded.data else {
        return serde_json::from_value(Value::Null).map_err(DbosError::from);
    };
    if data == "null" {
        return serde_json::from_value(Value::Null).map_err(DbosError::from);
    }
    serde_json::from_str(data).map_err(DbosError::from)
}

pub fn encode_portable_args<T: Serialize>(value: &T) -> Result<EncodedValue> {
    let value = serde_json::to_value(value)?;
    if serde_json::from_value::<PortableWorkflowArgs>(value.clone()).is_ok() {
        return Ok(EncodedValue {
            data: Some(serde_json::to_string(&value)?),
            serialization: PORTABLE_JSON.to_string(),
        });
    }
    let args = PortableWorkflowArgs { positional_args: vec![value], named_args: Default::default() };
    encode_portable(&args)
}

pub fn decode_stored<T: DeserializeOwned>(encoded: &EncodedValue) -> Result<T> {
    decode_stored_with_serializer(encoded, None)
}

pub fn decode_stored_with_serializer<T: DeserializeOwned>(encoded: &EncodedValue, serializer: Option<&dyn CustomSerializer>) -> Result<T> {
    match encoded.serialization.as_str() {
        "" | DBOS_JSON => JsonSerializer::decode(encoded),
        PORTABLE_JSON => decode_portable(encoded),
        other if serializer.is_some_and(|serializer| serializer.name() == other) => {
            let serializer = serializer.ok_or_else(|| DbosError::serialization(format!("unknown serialization format {other}")))?;
            let value = serializer.decode_value(encoded)?;
            serde_json::from_value(value).map_err(DbosError::from)
        }
        other => Err(DbosError::serialization(format!("unknown serialization format {other}"))),
    }
}

pub fn serialize_workflow_error(error: &str, serialization: &str) -> String {
    if serialization != PORTABLE_JSON {
        return error.to_string();
    }
    let portable = PortableWorkflowError {
        name: "Portable Error".to_string(),
        message: error.to_string(),
        code: None,
        data: None,
    };
    serde_json::to_string(&portable).unwrap_or_else(|_| error.to_string())
}
