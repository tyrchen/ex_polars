// use rustler::resource::ResourceArc;
// use rustler::{Env, Term};
// use polars::prelude::*;
// use polars::frame::ser::csv::CsvEncoding;

// use std::result::Result;

mod error;
mod datatypes;
mod dataframe;
pub(crate) mod series;

pub use error::ExPolarsError;
pub use datatypes::{DataType, ExSeries, ExSeriesRef, ExDataFrame, ExDataFrameRef};

#[macro_export]
macro_rules! df_read {
    ($data: ident, $df: ident, $body: block) => {
        match $data.inner.0.read() {
            Ok($df) => $body,
            Err(_) => Err(ExPolarsError::Internal),
        }
    };
}

#[macro_export]
macro_rules! df_read_read {
    ($data: ident, $other: ident, $df: ident, $df1: ident, $body: block) => {
        match ($data.inner.0.read(), $other.inner.0.read()) {
            (Ok($df), Ok($df1)) => $body,
            _ => Err(ExPolarsError::Internal),
        }
    };
}

#[macro_export]
macro_rules! df_write {
    ($data: ident, $df: ident, $body: block) => {
        match $data.inner.0.write() {
            Ok(mut $df) => $body,
            Err(_) => Err(ExPolarsError::Internal),
        }
    };
}

#[macro_export]
macro_rules! df_write_read {
    ($data: ident, $other: ident, $df: ident, $df1: ident, $body: block) => {
        match ($data.inner.0.write(), $other.inner.0.read()) {
            (Ok(mut $df), Ok($df1)) => $body,
            _ => Err(ExPolarsError::Internal),
        }
    };
}
