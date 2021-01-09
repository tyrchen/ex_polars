use rustler::{Encoder, Env, Term};
use std::io;
use thiserror::Error;

rustler::atoms! {
    ok,
    error
}

#[derive(Error, Debug)]
pub enum ExPolarsError {
    #[error("IO Error")]
    Io(#[from] io::Error),
    #[error("Json serialize/deserialize Error")]
    Json(#[from] serde_json::Error),
    #[error("Polars Error")]
    Polars(#[from] polars::prelude::PolarsError),
    #[error("Internal Error")]
    Internal,
    #[error("Other error: {0}")]
    Other(String),
    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}

impl<'a> Encoder for ExPolarsError {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        match self {
            ExPolarsError::Internal => (error(), self.to_string()).encode(env),
            ExPolarsError::Io(_) => (error(), self.to_string()).encode(env),
            ExPolarsError::Json(_) => (error(), self.to_string()).encode(env),
            ExPolarsError::Polars(_) => (error(), self.to_string()).encode(env),
            ExPolarsError::Other(_) => (error(), self.to_string()).encode(env),
            ExPolarsError::Unknown(_) => (error(), self.to_string()).encode(env),
        }
    }
}
