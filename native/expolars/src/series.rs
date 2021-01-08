
use polars::prelude::*;
use rustler::resource::ResourceArc;

use crate::{ExSeries, ExSeriesRef};

pub(crate) fn to_series_collection(s: Vec<ExSeries>) -> Vec<Series> {
    s.into_iter().map(|c| c.inner.0.clone()).collect()
}

pub(crate) fn to_ex_series_collection(s: Vec<Series>) -> Vec<ExSeries> {
    s.into_iter().map(|c| ExSeries { inner: ResourceArc::new(ExSeriesRef(c))}).collect()
}
