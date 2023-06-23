use std::{num::ParseIntError, str::Utf8Error, string::FromUtf8Error};
use tokio::io;

use hex::FromHexError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error, {0}")]
    IoError(#[from] io::Error),
    #[error("utf8 error, {0}")]
    Utf8Error(#[from] Utf8Error),
    #[error("can't convert from utf8, {0}")]
    FromUtf8Error(#[from] FromUtf8Error),
    #[error("can't convert from hex, {0}")]
    FromHexError(#[from] FromHexError),
    #[error("can't parse int, {0}")]
    ParseIntError(#[from] ParseIntError),
    #[error("{0}")]
    String(String),
}
