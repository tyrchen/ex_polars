
use polars::prelude::*;
use std::result::Result;

use crate::{ExSeries, ExPolarsError};

pub(crate) fn to_series_collection(s: Vec<ExSeries>) -> Vec<Series> {
    s.into_iter().map(|c| c.inner.0.clone()).collect()
}

pub(crate) fn to_ex_series_collection(s: Vec<Series>) -> Vec<ExSeries> {
    s.into_iter().map(|c| ExSeries::new(c)).collect()
}

#[rustler::nif]
/// Format `DataFrame` as String
pub fn s_as_str(data: ExSeries) -> Result<String, ExPolarsError> {
    Ok(format!("{:?}", data.inner.0))

}
