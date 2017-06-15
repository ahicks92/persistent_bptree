use serde::{self, Serialize};
use serde::de::DeserializeOwned;
use std::error::{self, Error};
use std::io::{self, Read, Write, Seek};
use bincode;

/** A storage backend, capable of encoding and decoding values to u64 keys.

Every key must be unique. Keys need not necessarily occur in ascending order.

Assuming that the data the backend stores is matched with a tree of the same type from run to run, this crate guarantees that:

- Any request for the value of a key occurs strictly after a request to store it, either in this run of the program or in a previous run of the program.
- Any request for a key will ask for a key of the right type.
*/
pub trait StorageBackend {
    type EncodingError: error::Error;
    type DecodingError: error::Error;
    fn load<V: DeserializeOwned>(&self, key: u64) -> Result<V, Self::DecodingError>;
    fn store<V: Serialize>(&mut self, value: &V) -> Result<u64, Self::EncodingError>;
}

#[derive(Debug)]
pub enum EncodingError {
    Unknown(String),
    IoError(io::Error),
}

#[derive(Debug)]
pub enum DecodingError {
    Corrupt(String),
    IoError(io::Error),
}

fn encode_reader<O: serde::Serialize, W: Write+Seek>(writer: &mut W, obj: &O) -> Result<u64, EncodingError> {
    let offset = writer.seek(io::SeekFrom::End(0)).map_err(|x| EncodingError::IoError(x))?;
    bincode::serialize_into(writer, obj, bincode::Infinite)
    .map_err(|x| {
        match *x {
            bincode::ErrorKind::IoError(y) => EncodingError::IoError(y),
            _ => EncodingError::Unknown(x.description().to_string()),
        }
    }).map(|_| offset)
}

fn decode<O: serde::de::DeserializeOwned, R: Read+Seek>(reader: &mut R, offset: u64) -> Result<O, DecodingError> {
    reader.seek(io::SeekFrom::Start(offset)).map_err(|x| DecodingError::IoError(x))?;
    bincode::deserialize_from(    reader, bincode::Infinite)
    .map_err(|x| {
        match *x {
            bincode::ErrorKind::IoError(y) => DecodingError::IoError(y),
            _ => DecodingError::Corrupt(x.description().to_string()),
        }
    })
}
