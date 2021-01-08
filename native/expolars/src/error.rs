use thiserror::Error;
use std::io;
use rustler::{Encoder, Env, Term};

rustler::atoms! {
    ok,
    error
}

#[derive(Error, Debug)]
pub enum ExPolarsError {
    #[error("IO Error")]
    Io(#[from] io::Error),
    #[error("Polars Error")]
    Polars(#[from] polars::prelude::PolarsError),
    #[error("Internal Error")]
    Internal,
}

impl<'a> Encoder for ExPolarsError {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        match self {
            ExPolarsError::Internal => (error(), self.to_string()).encode(env),
            ExPolarsError::Io(_) => (error(), self.to_string()).encode(env),
            ExPolarsError::Polars(_) => (error(), self.to_string()).encode(env),

        }
    }
}
