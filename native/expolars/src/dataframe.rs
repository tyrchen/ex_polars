// This file is modified based on: https://github.com/ritchie46/polars/blob/master/py-polars/src/dataframe.rs

use polars::prelude::*;
use rustler::resource::ResourceArc;
use rustler::{Env, Term, NifStruct};
use std::result::Result;
use std::fs::File;
use std::sync::RwLock;

use crate::{DataType,  ExPolarsError};
use polars::frame::ser::csv::CsvEncoding;

pub struct ExDataFrameRef(RwLock<DataFrame>);
pub struct ExSeriesRef(Series);

#[derive(NifStruct)]
#[module = "ExPolars.DataFrame"]
pub struct ExDataFrame {
    pub inner: ResourceArc<ExDataFrameRef>,
}

#[derive(NifStruct)]
#[module = "ExPolars.Series"]
pub struct ExSeries {
    pub inner: ResourceArc<ExSeriesRef>,
}



#[rustler::nif]
fn read_csv(
    filename: &str,
    infer_schema_length: usize,
    batch_size: usize,
    has_header: bool,
    ignore_errors: bool,
    stop_after_n_rows: Option<usize>,
    skip_rows: usize,
    projection: Option<Vec<usize>>,
    sep: &str,
    rechunk: bool,
    columns: Option<Vec<String>>,
    encoding: &str) -> Result<ExDataFrame, ExPolarsError> {
        let encoding = match encoding {
            "utf8-lossy" => CsvEncoding::LossyUtf8,
            _ => CsvEncoding::Utf8,
        };
        let df = CsvReader::from_path(filename)?
                .infer_schema(Some(infer_schema_length))
                .has_header(has_header)
                .with_stop_after_n_rows(stop_after_n_rows)
                .with_delimiter(sep.as_bytes()[0])
                .with_skip_rows(skip_rows)
                .with_ignore_parser_errors(ignore_errors)
                .with_projection(projection)
                .with_rechunk(rechunk)
                .with_batch_size(batch_size)
                .with_encoding(encoding)
                .with_columns(columns)
                .finish()?;
        let lock = RwLock::new(df);
        Ok(ExDataFrame {inner: ResourceArc::new(ExDataFrameRef(lock))})
}

#[rustler::nif]
pub fn read_parquet(filename: &str) -> Result<ExDataFrame, ExPolarsError> {
    let f = File::open(filename)?;
    let df = ParquetReader::new(f).finish()?;
    let lock = RwLock::new(df);
    Ok(ExDataFrame {inner: ResourceArc::new(ExDataFrameRef(lock))})
}

#[rustler::nif]
pub fn to_csv(
    data: ExDataFrame,
    filename: &str,
    batch_size: usize,
    has_headers: bool,
    delimiter: u8,
) -> Result<(), ExPolarsError> {
    match data.inner.0.write() {
        Ok(mut df) => {
            let mut f = File::create(filename)?;
            CsvWriter::new(&mut f)
                .has_headers(has_headers)
                .with_delimiter(delimiter)
                .with_batch_size(batch_size)
                .finish(&mut df)?;
            Ok(())
        }
        Err(_) => Err(ExPolarsError::Internal),
    }
}

#[rustler::nif]
pub fn add(data: ExDataFrame, s: ExSeries) -> Result<ExDataFrame, ExPolarsError> {
    match data.inner.0.read() {
        Ok(df) => {
            let new_df = (&*df + &s.inner.0)?;
            let lock = RwLock::new(new_df);
            Ok(ExDataFrame {inner: ResourceArc::new(ExDataFrameRef(lock))})
        }
        Err(_) => Err(ExPolarsError::Internal)
    }
}

#[rustler::nif]
/// Format `DataFrame` as String
pub fn as_str(data: ExDataFrame) -> Result<String, ExPolarsError> {
    match data.inner.0.read() {
        Ok(df) => {
            Ok(format!("{:?}", &*df))
        }
        Err(_) => Err(ExPolarsError::Internal)
    }
}

fn on_load(env: Env, _info: Term) -> bool {
    rustler::resource!(ExDataFrameRef, env);
    rustler::resource!(ExSeriesRef, env);
    true
}
rustler::init!("Elixir.ExPolars.DataFrame", [
    read_csv,
    read_parquet,
    to_csv,
    as_str,
    add,
], load = on_load);
