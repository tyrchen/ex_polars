// This file is modified based on: https://github.com/ritchie46/polars/blob/master/py-polars/src/dataframe.rs

use polars::prelude::*;
use rustler::{Env, Term};
use std::result::Result;
use std::fs::File;
use polars::frame::ser::csv::CsvEncoding;

use crate::series::{to_series_collection, to_ex_series_collection};

use crate::{DataType, ExDataFrame, ExDataFrameRef, ExSeries, ExSeriesRef, ExPolarsError};



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
    do_rechunk: bool,
    column_names: Option<Vec<String>>,
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
                .with_rechunk(do_rechunk)
                .with_batch_size(batch_size)
                .with_encoding(encoding)
                .with_columns(column_names)
                .finish()?;
        Ok(ExDataFrame::new(df))
}

#[rustler::nif]
pub fn read_parquet(filename: &str) -> Result<ExDataFrame, ExPolarsError> {
    let f = File::open(filename)?;
    let df = ParquetReader::new(f).finish()?;
    Ok(ExDataFrame::new(df))
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
/// Format `DataFrame` as String
pub fn as_str(data: ExDataFrame) -> Result<String, ExPolarsError> {
    match data.inner.0.read() {
        Ok(df) => {
            Ok(format!("{:?}", &*df))
        }
        Err(_) => Err(ExPolarsError::Internal)
    }
}

#[rustler::nif]
pub fn add(data: ExDataFrame, s: ExSeries) -> Result<ExDataFrame, ExPolarsError> {
    match data.inner.0.read() {
        Ok(df) => {
            let new_df = (&*df + &s.inner.0)?;
            Ok(ExDataFrame::new(new_df))
        }
        Err(_) => Err(ExPolarsError::Internal)
    }
}

#[rustler::nif]
pub fn sub(data: ExDataFrame, s: ExSeries) -> Result<ExDataFrame, ExPolarsError> {
    match data.inner.0.read() {
        Ok(df) => {
            let new_df = (&*df - &s.inner.0)?;
            Ok(ExDataFrame::new(new_df))
        }
        Err(_) => Err(ExPolarsError::Internal)
    }
}

#[rustler::nif]
pub fn div(data: ExDataFrame, s: ExSeries) -> Result<ExDataFrame, ExPolarsError> {
    match data.inner.0.read() {
        Ok(df) => {
            let new_df = (&*df / &s.inner.0)?;
            Ok(ExDataFrame::new(new_df))
        }
        Err(_) => Err(ExPolarsError::Internal)
    }
}

#[rustler::nif]
pub fn mul(data: ExDataFrame, s: ExSeries) -> Result<ExDataFrame, ExPolarsError> {
    match data.inner.0.read() {
        Ok(df) => {
            let new_df = (&*df * &s.inner.0)?;
            Ok(ExDataFrame::new(new_df))
        }
        Err(_) => Err(ExPolarsError::Internal)
    }
}

#[rustler::nif]
pub fn rem(data: ExDataFrame, s: ExSeries) -> Result<ExDataFrame, ExPolarsError> {
    match data.inner.0.read() {
        Ok(df) => {
            let new_df = (&*df % &s.inner.0)?;
            Ok(ExDataFrame::new(new_df))
        }
        Err(_) => Err(ExPolarsError::Internal)
    }
}

#[rustler::nif]
pub fn sample_n(data: ExDataFrame, n: usize, with_replacement: bool) -> Result<ExDataFrame, ExPolarsError> {
    match data.inner.0.read() {
        Ok(df) => {
            let new_df = (&*df).sample_n(n, with_replacement)?;
            Ok(ExDataFrame::new(new_df))
        }
        Err(_) => Err(ExPolarsError::Internal)
    }
}

#[rustler::nif]
pub fn sample_frac(data: ExDataFrame, frac: f64, with_replacement: bool) -> Result<ExDataFrame, ExPolarsError> {
    match data.inner.0.read() {
        Ok(df) => {
            let new_df = (&*df).sample_frac(frac, with_replacement)?;
            Ok(ExDataFrame::new(new_df))
        }
        Err(_) => Err(ExPolarsError::Internal)
    }
}

#[rustler::nif]
pub fn rechunk(data: ExDataFrame) -> Result<(), ExPolarsError> {
    match data.inner.0.write() {
        Ok(df) => {
            (*df).agg_chunks();
            Ok(())
        }
        Err(_) => Err(ExPolarsError::Internal)
    }
}

#[rustler::nif]
pub fn fill_none(data: ExDataFrame, strategy: &str) -> Result<ExDataFrame, ExPolarsError> {
    let strat = match strategy {
        "backward" => FillNoneStrategy::Backward,
        "forward" => FillNoneStrategy::Forward,
        "min" => FillNoneStrategy::Min,
        "max" => FillNoneStrategy::Max,
        "mean" => FillNoneStrategy::Mean,
        s => return Err(ExPolarsError::Other(format!("Strategy {} not supported", s)).into()),
    };
    match data.inner.0.read() {
        Ok(df) => {
            let new_df = (&*df).fill_none(strat)?;
            Ok(ExDataFrame::new(new_df))
        }
        Err(_) => Err(ExPolarsError::Internal)
    }
}

#[rustler::nif]
pub fn join(
    data: ExDataFrame,
    other: ExDataFrame,
    left_on: Vec<&str>,
    right_on: Vec<&str>,
    how: &str,
) -> Result<ExDataFrame, ExPolarsError> {
    let how = match how {
        "left" => JoinType::Left,
        "inner" => JoinType::Inner,
        "outer" => JoinType::Outer,
        _ => return Err(ExPolarsError::Other(format!("Join method {} not supported", how)).into()),
    };

    match (data.inner.0.read(), other.inner.0.read())  {
        (Ok(df), Ok(df1)) => {
            let new_df = (&*df).join(&*df1, left_on, right_on, how)?;
            Ok(ExDataFrame::new(new_df))
        }
        _ => Err(ExPolarsError::Internal)
    }

}

#[rustler::nif]
pub fn get_columns(data: ExDataFrame) -> Vec<ExSeries> {
    let df = &*data.inner.0.read().unwrap();
    to_ex_series_collection(df.get_columns().clone())
}

/// Get column names
#[rustler::nif]
pub fn columns(data: ExDataFrame) -> Vec<String> {
    let df = &*data.inner.0.read().unwrap();
    let cols = df.get_column_names();
    cols.into_iter().map(|s| s.to_owned()).collect()
}

/// set column names
#[rustler::nif]
pub fn set_column_names(data: ExDataFrame, names: Vec<&str>) -> Result<(), ExPolarsError> {
    match data.inner.0.write() {
        Ok(mut df) => {
            df.set_column_names(&names)?;
            Ok(())
        }
        Err(_) => Err(ExPolarsError::Internal),
    }
}

/// Get datatypes
#[rustler::nif]
pub fn dtypes(data: ExDataFrame) -> Vec<u8> {
    let df = &*data.inner.0.read().unwrap();
    df
        .dtypes()
        .iter()
        .map(|arrow_dtype| {
            let dt: DataType = arrow_dtype.into();
            dt as u8
        })
        .collect()
}

#[rustler::nif]
pub fn n_chunks(data: ExDataFrame) -> Result<usize, ExPolarsError> {
    let df = &*data.inner.0.read().unwrap();
    Ok(df.n_chunks()?)
}

#[rustler::nif]
pub fn shape(data: ExDataFrame) -> (usize, usize) {
    let df = &*data.inner.0.read().unwrap();
    df.shape()
}

#[rustler::nif]
pub fn height(data: ExDataFrame) -> usize {
    let df = &*data.inner.0.read().unwrap();
    df.height()
}

#[rustler::nif]
pub fn width(data: ExDataFrame) -> usize {
    let df = &*data.inner.0.read().unwrap();
    df.width()
}

#[rustler::nif]
pub fn hstack_mut(data: ExDataFrame, cols: Vec<ExSeries>) -> Result<(), ExPolarsError> {
    let cols = to_series_collection(cols);
    match data.inner.0.write() {
        Ok(mut df) => {
            df.hstack_mut(&cols)?;
            Ok(())
        }
        Err(_) => Err(ExPolarsError::Internal),
    }
}

#[rustler::nif]
pub fn hstack(data: ExDataFrame, cols: Vec<ExSeries>) -> Result<ExDataFrame, ExPolarsError> {
    let cols = to_series_collection(cols);
    let df = &*data.inner.0.read().unwrap();
    let new_df = df.hstack(&cols)?;
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif]
pub fn vstack(data: ExDataFrame, other: ExDataFrame) -> Result<(), ExPolarsError> {
    match (data.inner.0.write(), other.inner.0.read())  {
        (Ok(mut df), Ok(df1)) => {
            df.vstack_mut(&*df1)?;
            Ok(())
        }
        _ => Err(ExPolarsError::Internal),
    }
}

#[rustler::nif]
pub fn drop_in_place(data: ExDataFrame, name: &str) -> Result<ExSeries, ExPolarsError> {
    match data.inner.0.write()  {
        Ok(mut df) => {
            let s = df.drop_in_place(name)?;
            Ok(ExSeries::new(s))
        }
        Err(_) => Err(ExPolarsError::Internal),
    }
}

#[rustler::nif]
pub fn drop_nulls(data: ExDataFrame, subset: Option<Vec<String>>) -> Result<ExDataFrame, ExPolarsError> {
    let df = &*data.inner.0.read().unwrap();
    let new_df = df.drop_nulls(subset.as_ref().map(|s| s.as_ref()))?;
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif]
pub fn drop(data: ExDataFrame, name: &str) -> Result<ExDataFrame, ExPolarsError> {
    let df = &*data.inner.0.read().unwrap();
    let new_df = df.drop(name)?;
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif]
pub fn select_at_idx(data: ExDataFrame, idx: usize) -> Option<ExSeries> {
    let df = &*data.inner.0.read().unwrap();
    df.select_at_idx(idx).map(|s| ExSeries::new(s.clone()))
}

#[rustler::nif]
pub fn find_idx_by_name(data: ExDataFrame, name: &str) -> Option<usize> {
    let df = &*data.inner.0.read().unwrap();
    df.find_idx_by_name(name)
}

#[rustler::nif]
pub fn column(data: ExDataFrame, name: &str) -> Result<ExSeries, ExPolarsError> {
    let df = &*data.inner.0.read().unwrap();
    let series =
        df
        .column(name)
        .map(|s| ExSeries::new(s.clone()))?;
    Ok(series)
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
    sub,
    mul,
    div,
    rem,
    sample_n,
    sample_frac,
    rechunk,
    fill_none,
    join,
    get_columns,
    columns,
    set_column_names,
    dtypes,
    n_chunks,
    shape,
    width,
    height,
    hstack_mut,
    hstack,
    vstack,
    drop_in_place,
    drop_nulls,
    drop,
    select_at_idx,
    find_idx_by_name,
    column,
], load = on_load);
