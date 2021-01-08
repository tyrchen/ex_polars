// use rustler::resource::ResourceArc;
// use rustler::{Env, Term};
// use polars::prelude::*;
// use polars::frame::ser::csv::CsvEncoding;

// use std::result::Result;

mod error;
mod datatypes;
mod dataframe;

pub use error::ExPolarsError;
pub use datatypes::DataType;
