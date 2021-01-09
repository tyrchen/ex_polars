use polars::prelude::*;
use std::result::Result;

use crate::{DataType, ExDataFrame, ExPolarsError, ExSeries};

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

// Init with arrays
macro_rules! init_method {
    ($name:ident, $type:ty) => {
        #[rustler::nif]
        pub fn $name(name: &str, val: Vec<$type>) -> ExSeries {
            ExSeries::new(Series::new(name, val.as_slice()))
        }
    };
}

init_method!(s_new_i8, i8);
init_method!(s_new_i16, i16);
init_method!(s_new_i32, i32);
init_method!(s_new_i64, i64);
init_method!(s_new_bool, bool);
init_method!(s_new_u8, u8);
init_method!(s_new_u16, u16);
init_method!(s_new_u32, u32);
init_method!(s_new_u64, u64);
init_method!(s_new_date32, i32);
init_method!(s_new_date64, i64);
init_method!(s_new_duration_ns, i64);
init_method!(s_new_time_ns, i64);
init_method!(s_new_f32, f32);
init_method!(s_new_f64, f64);

#[rustler::nif]
pub fn s_parse_date32_from_str_slice(name: &str, val: Vec<&str>, fmt: &str) -> ExSeries {
    let parsed = Date32Chunked::parse_from_str_slice(name, &val, fmt);
    ExSeries::new(parsed.into_series())
}

#[rustler::nif]
pub fn s_new_str(name: &str, val: Vec<&str>) -> ExSeries {
    let chunked: Utf8Chunked = ChunkedArray::new_from_slice(name, &val);
    ExSeries::new(chunked.into_series())
}

#[rustler::nif]
pub fn s_rechunk(data: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let series = s.rechunk(None).expect("should not fail");
    Ok(ExSeries::new(series))
}

#[rustler::nif]
pub fn s_chunk_lengths(data: ExSeries) -> Result<Vec<usize>, ExPolarsError> {
    Ok(data.inner.0.chunk_lengths().clone())
}

#[rustler::nif]
pub fn s_name(data: ExSeries) -> Result<String, ExPolarsError> {
    Ok(data.inner.0.name().to_owned())
}

#[rustler::nif]
pub fn s_rename(data: ExSeries, name: &str) -> Result<ExSeries, ExPolarsError> {
    let mut s = data.inner.0.clone();
    s.rename(name);
    Ok(ExSeries::new(s))
}

#[rustler::nif]
pub fn s_dtype(data: ExSeries) -> Result<u8, ExPolarsError> {
    let s = &data.inner.0;
    let dt: DataType = s.dtype().into();
    Ok(dt as u8)
}

#[rustler::nif]
pub fn s_n_chunks(data: ExSeries) -> Result<usize, ExPolarsError> {
    let s = &data.inner.0;
    Ok(s.n_chunks())
}

#[rustler::nif]
pub fn s_limit(data: ExSeries, num_elements: usize) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let series = s.limit(num_elements)?;
    Ok(ExSeries::new(series))
}

#[rustler::nif]
pub fn s_slice(data: ExSeries, offset: usize, length: usize) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let series = s.slice(offset, length)?;
    Ok(ExSeries::new(series))
}

#[rustler::nif]
pub fn s_append(data: ExSeries, other: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let mut s = data.inner.0.clone();
    let s1 = &other.inner.0;
    s.append(s1)?;
    Ok(ExSeries::new(s))
}

#[rustler::nif]
pub fn s_filter(data: ExSeries, filter: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = &filter.inner.0;
    if let Ok(ca) = s1.bool() {
        let series = s.filter(ca)?;
        Ok(ExSeries::new(series))
    } else {
        Err(ExPolarsError::Other("Expected a boolean mask".into()))
    }
}

#[rustler::nif]
pub fn s_add(data: ExSeries, other: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = &other.inner.0;
    Ok(ExSeries::new(s + s1))
}

#[rustler::nif]
pub fn s_sub(data: ExSeries, other: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = &other.inner.0;
    Ok(ExSeries::new(s - s1))
}

#[rustler::nif]
pub fn s_mul(data: ExSeries, other: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = &other.inner.0;
    Ok(ExSeries::new(s * s1))
}

#[rustler::nif]
pub fn s_div(data: ExSeries, other: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = &other.inner.0;
    Ok(ExSeries::new(s / s1))
}

#[rustler::nif]
pub fn s_head(data: ExSeries, length: Option<usize>) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    Ok(ExSeries::new(s.head(length)))
}

#[rustler::nif]
pub fn s_tail(data: ExSeries, length: Option<usize>) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    Ok(ExSeries::new(s.tail(length)))
}

#[rustler::nif]
pub fn s_sort(data: ExSeries, reverse: bool) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    Ok(ExSeries::new(s.sort(reverse)))
}

#[rustler::nif]
pub fn s_argsort(data: ExSeries, reverse: bool) -> Result<Vec<usize>, ExPolarsError> {
    let s = &data.inner.0;
    Ok(s.argsort(reverse))
}

#[rustler::nif]
pub fn s_unique(data: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let unique = s.unique()?;
    Ok(ExSeries::new(unique))
}

#[rustler::nif]
pub fn s_value_counts(data: ExSeries) -> Result<ExDataFrame, ExPolarsError> {
    let s = &data.inner.0;
    let df = s.value_counts()?;
    Ok(ExDataFrame::new(df))
}

#[rustler::nif]
pub fn s_arg_unique(data: ExSeries) -> Result<Vec<usize>, ExPolarsError> {
    let s = &data.inner.0;
    let arg_unique = s.arg_unique()?;
    Ok(arg_unique)
}

#[rustler::nif]
pub fn s_take(data: ExSeries, indices: Vec<usize>) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = s.take(&indices);
    Ok(ExSeries::new(s1))
}

#[rustler::nif]
pub fn s_take_with_series(data: ExSeries, indices: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = &indices.inner.0;
    let idx = s1.u32()?;
    let s2 = s.take(&idx);
    Ok(ExSeries::new(s2))
}

#[rustler::nif]
pub fn s_null_count(data: ExSeries) -> Result<usize, ExPolarsError> {
    let s = &data.inner.0;
    Ok(s.null_count())
}

#[rustler::nif]
pub fn s_is_null(data: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    Ok(ExSeries::new(s.is_null().into_series()))
}

#[rustler::nif]
pub fn s_is_not_null(data: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    Ok(ExSeries::new(s.is_not_null().into_series()))
}

#[rustler::nif]
pub fn s_is_unique(data: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let ca = s.is_unique()?;
    Ok(ExSeries::new(ca.into_series()))
}

#[rustler::nif]
pub fn s_arg_true(data: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let ca = s.arg_true()?;
    Ok(ExSeries::new(ca.into_series()))
}

#[rustler::nif]
pub fn s_sample_n(
    data: ExSeries,
    n: usize,
    with_replacement: bool,
) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = s.sample_n(n, with_replacement)?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif]
pub fn s_sample_frac(
    data: ExSeries,
    frac: f64,
    with_replacement: bool,
) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = s.sample_frac(frac, with_replacement)?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif]
pub fn s_is_duplicated(data: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let ca = s.is_duplicated()?;
    Ok(ExSeries::new(ca.into_series()))
}

#[rustler::nif]
pub fn s_explode(data: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = s.explode()?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif]
pub fn s_take_every(data: ExSeries, n: usize) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = s.take_every(n);
    Ok(ExSeries::new(s1))
}

#[rustler::nif]
pub fn s_series_equal(
    data: ExSeries,
    other: ExSeries,
    null_equal: bool,
) -> Result<bool, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = &other.inner.0;
    let result = if null_equal {
        s.series_equal_missing(s1)
    } else {
        s.series_equal(s1)
    };
    Ok(result)
}

#[rustler::nif]
pub fn s_eq(data: ExSeries, rhs: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = &rhs.inner.0;
    Ok(ExSeries::new(s.eq(s1).into_series()))
}

#[rustler::nif]
pub fn s_neq(data: ExSeries, rhs: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = &rhs.inner.0;
    Ok(ExSeries::new(s.neq(s1).into_series()))
}

#[rustler::nif]
pub fn s_gt(data: ExSeries, rhs: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = &rhs.inner.0;
    Ok(ExSeries::new(s.gt(s1).into_series()))
}

#[rustler::nif]
pub fn s_gt_eq(data: ExSeries, rhs: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = &rhs.inner.0;
    Ok(ExSeries::new(s.gt_eq(s1).into_series()))
}

#[rustler::nif]
pub fn s_lt(data: ExSeries, rhs: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = &rhs.inner.0;
    Ok(ExSeries::new(s.lt(s1).into_series()))
}

#[rustler::nif]
pub fn s_lt_eq(data: ExSeries, rhs: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = &rhs.inner.0;
    Ok(ExSeries::new(s.lt_eq(s1).into_series()))
}

#[rustler::nif]
pub fn s_not(data: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let bool = s.bool()?;
    Ok(ExSeries::new((!bool).into_series()))
}

#[rustler::nif]
pub fn s_len(data: ExSeries) -> Result<usize, ExPolarsError> {
    let s = &data.inner.0;
    Ok(s.len())
}

// TODO(tchen): Find an elegant way later
#[rustler::nif]
pub fn s_to_json(data: ExSeries) -> Result<String, ExPolarsError> {
    let s = &data.inner.0;

    let st = match s.dtype() {
        ArrowDataType::Boolean => {
            let arr: Vec<bool> = s
                .bool()
                .unwrap()
                .into_iter()
                .map(|item| item.unwrap())
                .collect();
            serde_json::to_string(&arr)
        }
        ArrowDataType::Utf8 => {
            let arr: Vec<String> = s
                .utf8()
                .unwrap()
                .into_iter()
                .map(|item| item.unwrap().into())
                .collect();
            serde_json::to_string(&arr)
        }
        ArrowDataType::UInt8 => serde_json::to_string(&s.u8().unwrap().data_views()),
        ArrowDataType::UInt16 => serde_json::to_string(&s.u16().unwrap().data_views()),
        ArrowDataType::UInt32 => serde_json::to_string(&s.u32().unwrap().data_views()),
        ArrowDataType::UInt64 => serde_json::to_string(&s.u64().unwrap().data_views()),
        ArrowDataType::Int8 => serde_json::to_string(&s.i8().unwrap().data_views()),
        ArrowDataType::Int16 => serde_json::to_string(&s.i16().unwrap().data_views()),
        ArrowDataType::Int32 => serde_json::to_string(&s.i32().unwrap().data_views()),
        ArrowDataType::Int64 => serde_json::to_string(&s.i64().unwrap().data_views()),
        ArrowDataType::Float32 => serde_json::to_string(&s.f32().unwrap().data_views()),
        ArrowDataType::Float64 => serde_json::to_string(&s.f64().unwrap().data_views()),
        ArrowDataType::Date32(DateUnit::Day) => {
            serde_json::to_string(&s.date32().unwrap().data_views())
        }
        ArrowDataType::Date64(DateUnit::Millisecond) => {
            serde_json::to_string(&s.date64().unwrap().data_views())
        }
        ArrowDataType::Time64(TimeUnit::Nanosecond) => {
            serde_json::to_string(&s.time64_nanosecond().unwrap().data_views())
        }
        ArrowDataType::Duration(TimeUnit::Nanosecond) => {
            serde_json::to_string(&s.duration_nanosecond().unwrap().data_views())
        }
        ArrowDataType::Duration(TimeUnit::Millisecond) => {
            serde_json::to_string(&s.duration_millisecond().unwrap().data_views())
        }
        ArrowDataType::Binary => {
            let mut v = Vec::with_capacity(s.len());
            for i in 0..s.len() {
                let val = s.get_as_any(i).downcast_ref::<Vec<u8>>().unwrap();
                v.push(val);
            }
            serde_json::to_string(&v)
        }
        dt => panic!(format!("to_list() not implemented for {:?}", dt)),
    };

    Ok(st?)
}

#[rustler::nif]
pub fn s_drop_nulls(data: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    Ok(ExSeries::new(s.drop_nulls()))
}

#[rustler::nif]
pub fn s_fill_none(data: ExSeries, strategy: &str) -> Result<ExSeries, ExPolarsError> {
    let strat = match strategy {
        "backward" => FillNoneStrategy::Backward,
        "forward" => FillNoneStrategy::Forward,
        "min" => FillNoneStrategy::Min,
        "max" => FillNoneStrategy::Max,
        "mean" => FillNoneStrategy::Mean,
        s => return Err(ExPolarsError::Other(format!("Strategy {} not supported", s)).into()),
    };

    let s = &data.inner.0;
    let s1 = s.fill_none(strat)?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif]
pub fn s_clone(data: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    Ok(ExSeries::new(s.clone()))
}

#[rustler::nif]
pub fn s_shift(data: ExSeries, periods: i32) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = s.shift(periods)?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif]
pub fn s_zip_with(
    data: ExSeries,
    mask: ExSeries,
    other: ExSeries,
) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let m = &mask.inner.0;
    let s1 = &other.inner.0;
    let msk = m.bool()?;
    let s2 = s.zip_with(msk, s1)?;
    Ok(ExSeries::new(s2))
}

#[rustler::nif]
pub fn s_str_lengths(data: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let ca = s.utf8()?;
    let s1 = ca.str_lengths().into_series();
    Ok(ExSeries::new(s1))
}

#[rustler::nif]
pub fn s_str_contains(data: ExSeries, pat: &str) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let ca = s.utf8()?;
    let s1 = ca.contains(pat)?.into_series();
    Ok(ExSeries::new(s1))
}

#[rustler::nif]
pub fn s_str_replace(data: ExSeries, pat: &str, val: &str) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let ca = s.utf8()?;
    let s1 = ca.replace(pat, val)?.into_series();
    Ok(ExSeries::new(s1))
}

#[rustler::nif]
pub fn s_str_replace_all(data: ExSeries, pat: &str, val: &str) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let ca = s.utf8()?;
    let s1 = ca.replace_all(pat, val)?.into_series();
    Ok(ExSeries::new(s1))
}

#[rustler::nif]
pub fn s_str_to_uppercase(data: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let ca = s.utf8()?;
    let s1 = ca.to_uppercase().into_series();
    Ok(ExSeries::new(s1))
}

#[rustler::nif]
pub fn s_str_to_lowercase(data: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let ca = s.utf8()?;
    let s1 = ca.to_lowercase().into_series();
    Ok(ExSeries::new(s1))
}

#[rustler::nif]
pub fn s_str_parse_date32(data: ExSeries, fmt: Option<&str>) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    if let Ok(ca) = s.utf8() {
        let ca = ca.as_date32(fmt)?;
        Ok(ExSeries::new(ca.into_series()))
    } else {
        Err(ExPolarsError::Other("cannot parse date32 expected utf8 type".into()).into())
    }
}

#[rustler::nif]
pub fn s_str_parse_date64(data: ExSeries, fmt: Option<&str>) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    if let Ok(ca) = s.utf8() {
        let ca = ca.as_date64(fmt)?;
        Ok(ExSeries::new(ca.into_series()))
    } else {
        Err(ExPolarsError::Other("cannot parse date64 expected utf8 type".into()).into())
    }
}

#[rustler::nif]
pub fn s_datetime_str_fmt(data: ExSeries, fmt: &str) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = s.datetime_str_fmt(fmt)?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif]
pub fn s_as_duration(data: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    match s.dtype() {
        ArrowDataType::Date64(_) => {
            let ca = s.date64().unwrap().as_duration();
            Ok(ExSeries::new(ca.into_series()))
        }
        ArrowDataType::Date32(_) => {
            let ca = s.date32().unwrap().as_duration();
            Ok(ExSeries::new(ca.into_series()))
        }
        _ => Err(ExPolarsError::Other(
            "Only date32 and date64 can be transformed as duration".into(),
        )
        .into()),
    }
}

#[rustler::nif]
pub fn s_to_dummies(data: ExSeries) -> Result<ExDataFrame, ExPolarsError> {
    let s = &data.inner.0;
    let df = s.to_dummies()?;
    Ok(ExDataFrame::new(df))
}

#[rustler::nif]
pub fn s_get_list(data: ExSeries, index: usize) -> Option<ExSeries> {
    let s = &data.inner.0;
    if let Ok(ca) = s.list() {
        let s = ca.get(index);
        s.map(|s| ExSeries::new(s))
    } else {
        None
    }
}

#[rustler::nif]
pub fn s_rolling_sum(
    data: ExSeries,
    window_size: usize,
    weight: Option<Vec<f64>>,
    ignore_null: bool,
) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = s.rolling_sum(window_size, weight.as_deref(), ignore_null)?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif]
pub fn s_rolling_mean(
    data: ExSeries,
    window_size: usize,
    weight: Option<Vec<f64>>,
    ignore_null: bool,
) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = s.rolling_mean(window_size, weight.as_deref(), ignore_null)?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif]
pub fn s_rolling_max(
    data: ExSeries,
    window_size: usize,
    weight: Option<Vec<f64>>,
    ignore_null: bool,
) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = s.rolling_max(window_size, weight.as_deref(), ignore_null)?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif]
pub fn s_rolling_min(
    data: ExSeries,
    window_size: usize,
    weight: Option<Vec<f64>>,
    ignore_null: bool,
) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = s.rolling_min(window_size, weight.as_deref(), ignore_null)?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif]
pub fn s_year(data: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = s.year()?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif]
pub fn s_month(data: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = s.month()?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif]
pub fn s_day(data: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = s.day()?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif]
pub fn s_ordinal_day(data: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = s.ordinal_day()?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif]
pub fn s_hour(data: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = s.hour()?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif]
pub fn s_minute(data: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = s.minute()?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif]
pub fn s_second(data: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = s.second()?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif]
pub fn s_nanosecond(data: ExSeries) -> Result<ExSeries, ExPolarsError> {
    let s = &data.inner.0;
    let s1 = s.nanosecond()?;
    Ok(ExSeries::new(s1))
}

macro_rules! impl_set_with_mask {
    ($name:ident, $native:ty, $cast:ident, $variant:ident) => {
        #[rustler::nif]
        pub fn $name(
            data: ExSeries,
            filter: ExSeries,
            value: Option<$native>,
        ) -> Result<ExSeries, ExPolarsError> {
            let s = &data.inner.0;
            let f = &filter.inner.0;
            let mask = f.bool()?;
            let ca = s.$cast()?;
            let new = ca.set(mask, value)?;
            let series = new.into_series();

            Ok(ExSeries::new(series))
        }
    };
}

impl_set_with_mask!(s_set_with_mask_str, &str, utf8, Utf8);
impl_set_with_mask!(s_set_with_mask_f64, f64, f64, Float64);
impl_set_with_mask!(s_set_with_mask_f32, f32, f32, Float32);
impl_set_with_mask!(s_set_with_mask_u8, u8, u8, UInt8);
impl_set_with_mask!(s_set_with_mask_u16, u16, u16, UInt16);
impl_set_with_mask!(s_set_with_mask_u32, u32, u32, UInt32);
impl_set_with_mask!(s_set_with_mask_u64, u64, u64, UInt64);
impl_set_with_mask!(s_set_with_mask_i8, i8, i8, Int8);
impl_set_with_mask!(s_set_with_mask_i16, i16, i16, Int16);
impl_set_with_mask!(s_set_with_mask_i32, i32, i32, Int32);
impl_set_with_mask!(s_set_with_mask_i64, i64, i64, Int64);

macro_rules! impl_get {
    ($name:ident, $series_variant:ident, $type:ty) => {
        #[rustler::nif]
        pub fn $name(data: ExSeries, index: usize) -> Option<$type> {
            let s = &data.inner.0;
            if let Ok(ca) = s.$series_variant() {
                ca.get(index).map(|item| item.into())
            } else {
                None
            }
        }
    };
}

impl_get!(s_get_f32, f32, f32);
impl_get!(s_get_f64, f64, f64);
impl_get!(s_get_u8, u8, u8);
impl_get!(s_get_u16, u16, u16);
impl_get!(s_get_u32, u32, u32);
impl_get!(s_get_u64, u64, u64);
impl_get!(s_get_i8, i8, i8);
impl_get!(s_get_i16, i16, i16);
impl_get!(s_get_i32, i32, i32);
impl_get!(s_get_i64, i64, i64);
impl_get!(s_get_str, utf8, String);
impl_get!(s_get_date32, date32, i32);
impl_get!(s_get_date64, date64, i64);

macro_rules! impl_cast {
    ($name:ident, $type:ty) => {
        #[rustler::nif]
        pub fn $name(data: ExSeries) -> Result<ExSeries, ExPolarsError> {
            let s = &data.inner.0;
            let s1 = s.cast::<$type>()?;
            Ok(ExSeries::new(s1))
        }
    };
}

impl_cast!(s_cast_u8, UInt8Type);
impl_cast!(s_cast_u16, UInt16Type);
impl_cast!(s_cast_u32, UInt32Type);
impl_cast!(s_cast_u64, UInt64Type);
impl_cast!(s_cast_i8, Int8Type);
impl_cast!(s_cast_i16, Int16Type);
impl_cast!(s_cast_i32, Int32Type);
impl_cast!(s_cast_i64, Int64Type);
impl_cast!(s_cast_f32, Float32Type);
impl_cast!(s_cast_f64, Float64Type);
impl_cast!(s_cast_date32, Date32Type);
impl_cast!(s_cast_date64, Date64Type);
impl_cast!(s_cast_time64ns, Time64NanosecondType);
impl_cast!(s_cast_duration_ns, DurationNanosecondType);
impl_cast!(s_cast_str, Utf8Type);

macro_rules! impl_op_i64 {
    ($name:ident, $operand:tt) => {
        #[rustler::nif]
        pub fn $name(data: ExSeries, other: i64) -> Result<ExSeries, ExPolarsError> {
            let s = &data.inner.0;
            Ok(ExSeries::new(s $operand other))
        }
    };
}

macro_rules! impl_op_f64 {
    ($name:ident, $operand:tt) => {
        #[rustler::nif]
        pub fn $name(data: ExSeries, other: f64) -> Result<ExSeries, ExPolarsError> {
            let s = &data.inner.0;
            Ok(ExSeries::new(s $operand other))
        }
    };
}

impl_op_i64!(s_add_i64, +);
impl_op_i64!(s_sub_i64, -);
impl_op_i64!(s_mul_i64, *);
impl_op_i64!(s_div_i64, /);

impl_op_f64!(s_add_f64, +);
impl_op_f64!(s_sub_f64, -);
impl_op_f64!(s_mul_f64, *);
impl_op_f64!(s_div_f64, /);

macro_rules! impl_rhs_i64 {
    ($name:ident, $operand:ident) => {
        #[rustler::nif]
        pub fn $name(data: ExSeries, other: i64) -> Result<ExSeries, ExPolarsError> {
            let s = &data.inner.0;
            Ok(ExSeries::new(other.$operand(s)))
        }
    };
}

macro_rules! impl_rhs_f64 {
    ($name:ident, $operand:ident) => {
        #[rustler::nif]
        pub fn $name(data: ExSeries, other: f64) -> Result<ExSeries, ExPolarsError> {
            let s = &data.inner.0;
            Ok(ExSeries::new(other.$operand(s)))
        }
    };
}

impl_rhs_i64!(s_add_i64_rhs, add);
impl_rhs_i64!(s_sub_i64_rhs, sub);
impl_rhs_i64!(s_mul_i64_rhs, mul);
impl_rhs_i64!(s_div_i64_rhs, div);

impl_rhs_f64!(s_add_f64_rhs, add);
impl_rhs_f64!(s_sub_f64_rhs, sub);
impl_rhs_f64!(s_mul_f64_rhs, mul);
impl_rhs_f64!(s_div_f64_rhs, div);

macro_rules! impl_agg {
    ($name:ident, $type:ty, $operand:ident) => {
        #[rustler::nif]
        pub fn $name(data: ExSeries) -> Result<Option<$type>, ExPolarsError> {
            let s = &data.inner.0;
            Ok(s.$operand())
        }
    };
}

impl_agg!(s_sum_u8, u8, sum);
impl_agg!(s_sum_u16, u16, sum);
impl_agg!(s_sum_u32, u32, sum);
impl_agg!(s_sum_u64, u64, sum);
impl_agg!(s_sum_i8, i8, sum);
impl_agg!(s_sum_i16, i16, sum);
impl_agg!(s_sum_i32, i32, sum);
impl_agg!(s_sum_i64, i64, sum);
impl_agg!(s_sum_f32, f32, sum);
impl_agg!(s_sum_f64, f64, sum);

impl_agg!(s_min_u8, u8, min);
impl_agg!(s_min_u16, u16, min);
impl_agg!(s_min_u32, u32, min);
impl_agg!(s_min_u64, u64, min);
impl_agg!(s_min_i8, i8, min);
impl_agg!(s_min_i16, i16, min);
impl_agg!(s_min_i32, i32, min);
impl_agg!(s_min_i64, i64, min);
impl_agg!(s_min_f32, f32, min);
impl_agg!(s_min_f64, f64, min);

impl_agg!(s_max_u8, u8, max);
impl_agg!(s_max_u16, u16, max);
impl_agg!(s_max_u32, u32, max);
impl_agg!(s_max_u64, u64, max);
impl_agg!(s_max_i8, i8, max);
impl_agg!(s_max_i16, i16, max);
impl_agg!(s_max_i32, i32, max);
impl_agg!(s_max_i64, i64, max);
impl_agg!(s_max_f32, f32, max);
impl_agg!(s_max_f64, f64, max);

impl_agg!(s_mean_u8, u8, mean);
impl_agg!(s_mean_u16, u16, mean);
impl_agg!(s_mean_u32, u32, mean);
impl_agg!(s_mean_u64, u64, mean);
impl_agg!(s_mean_i8, i8, mean);
impl_agg!(s_mean_i16, i16, mean);
impl_agg!(s_mean_i32, i32, mean);
impl_agg!(s_mean_i64, i64, mean);
impl_agg!(s_mean_f32, f32, mean);
impl_agg!(s_mean_f64, f64, mean);

macro_rules! impl_cmp_u8 {
    ($name:ident, $operand:ident) => {
        #[rustler::nif]
        pub fn $name(data: ExSeries, rhs: u8) -> Result<ExSeries, ExPolarsError> {
            let s = &data.inner.0;
            Ok(ExSeries::new(s.$operand(rhs).into_series()))
        }
    };
}

macro_rules! impl_cmp_u16 {
    ($name:ident, $operand:ident) => {
        #[rustler::nif]
        pub fn $name(data: ExSeries, rhs: u16) -> Result<ExSeries, ExPolarsError> {
            let s = &data.inner.0;
            Ok(ExSeries::new(s.$operand(rhs).into_series()))
        }
    };
}

macro_rules! impl_cmp_u32 {
    ($name:ident, $operand:ident) => {
        #[rustler::nif]
        pub fn $name(data: ExSeries, rhs: u32) -> Result<ExSeries, ExPolarsError> {
            let s = &data.inner.0;
            Ok(ExSeries::new(s.$operand(rhs).into_series()))
        }
    };
}

macro_rules! impl_cmp_u64 {
    ($name:ident, $operand:ident) => {
        #[rustler::nif]
        pub fn $name(data: ExSeries, rhs: u64) -> Result<ExSeries, ExPolarsError> {
            let s = &data.inner.0;
            Ok(ExSeries::new(s.$operand(rhs).into_series()))
        }
    };
}

macro_rules! impl_cmp_i8 {
    ($name:ident, $operand:ident) => {
        #[rustler::nif]
        pub fn $name(data: ExSeries, rhs: i8) -> Result<ExSeries, ExPolarsError> {
            let s = &data.inner.0;
            Ok(ExSeries::new(s.$operand(rhs).into_series()))
        }
    };
}

macro_rules! impl_cmp_i16 {
    ($name:ident, $operand:ident) => {
        #[rustler::nif]
        pub fn $name(data: ExSeries, rhs: i16) -> Result<ExSeries, ExPolarsError> {
            let s = &data.inner.0;
            Ok(ExSeries::new(s.$operand(rhs).into_series()))
        }
    };
}

macro_rules! impl_cmp_i32 {
    ($name:ident, $operand:ident) => {
        #[rustler::nif]
        pub fn $name(data: ExSeries, rhs: i32) -> Result<ExSeries, ExPolarsError> {
            let s = &data.inner.0;
            Ok(ExSeries::new(s.$operand(rhs).into_series()))
        }
    };
}

macro_rules! impl_cmp_i64 {
    ($name:ident, $operand:ident) => {
        #[rustler::nif]
        pub fn $name(data: ExSeries, rhs: i64) -> Result<ExSeries, ExPolarsError> {
            let s = &data.inner.0;
            Ok(ExSeries::new(s.$operand(rhs).into_series()))
        }
    };
}

macro_rules! impl_cmp_f32 {
    ($name:ident, $operand:ident) => {
        #[rustler::nif]
        pub fn $name(data: ExSeries, rhs: f32) -> Result<ExSeries, ExPolarsError> {
            let s = &data.inner.0;
            Ok(ExSeries::new(s.$operand(rhs).into_series()))
        }
    };
}

macro_rules! impl_cmp_f64 {
    ($name:ident, $operand:ident) => {
        #[rustler::nif]
        pub fn $name(data: ExSeries, rhs: f64) -> Result<ExSeries, ExPolarsError> {
            let s = &data.inner.0;
            Ok(ExSeries::new(s.$operand(rhs).into_series()))
        }
    };
}

macro_rules! impl_cmp_str {
    ($name:ident, $operand:ident) => {
        #[rustler::nif]
        pub fn $name(data: ExSeries, rhs: &str) -> Result<ExSeries, ExPolarsError> {
            let s = &data.inner.0;
            Ok(ExSeries::new(s.$operand(rhs).into_series()))
        }
    };
}

impl_cmp_u8!(s_eq_u8, eq);
impl_cmp_u8!(s_neq_u8, neq);
impl_cmp_u8!(s_gt_u8, gt);
impl_cmp_u8!(s_lt_u8, lt);
impl_cmp_u8!(s_gt_eq_u8, gt_eq);
impl_cmp_u8!(s_lt_eq_u8, lt_eq);

impl_cmp_u16!(s_eq_u16, eq);
impl_cmp_u16!(s_neq_u16, neq);
impl_cmp_u16!(s_gt_u16, gt);
impl_cmp_u16!(s_lt_u16, lt);
impl_cmp_u16!(s_gt_eq_u16, gt_eq);
impl_cmp_u16!(s_lt_eq_u16, lt_eq);

impl_cmp_u32!(s_eq_u32, eq);
impl_cmp_u32!(s_neq_u32, neq);
impl_cmp_u32!(s_gt_u32, gt);
impl_cmp_u32!(s_lt_u32, lt);
impl_cmp_u32!(s_gt_eq_u32, gt_eq);
impl_cmp_u32!(s_lt_eq_u32, lt_eq);

impl_cmp_u64!(s_eq_u64, eq);
impl_cmp_u64!(s_neq_u64, neq);
impl_cmp_u64!(s_gt_u64, gt);
impl_cmp_u64!(s_lt_u64, lt);
impl_cmp_u64!(s_gt_eq_u64, gt_eq);
impl_cmp_u64!(s_lt_eq_u64, lt_eq);

impl_cmp_i8!(s_eq_i8, eq);
impl_cmp_i8!(s_neq_i8, neq);
impl_cmp_i8!(s_gt_i8, gt);
impl_cmp_i8!(s_lt_i8, lt);
impl_cmp_i8!(s_gt_eq_i8, gt_eq);
impl_cmp_i8!(s_lt_eq_i8, lt_eq);

impl_cmp_i16!(s_eq_i16, eq);
impl_cmp_i16!(s_neq_i16, neq);
impl_cmp_i16!(s_gt_i16, gt);
impl_cmp_i16!(s_lt_i16, lt);
impl_cmp_i16!(s_gt_eq_i16, gt_eq);
impl_cmp_i16!(s_lt_eq_i16, lt_eq);

impl_cmp_i32!(s_eq_i32, eq);
impl_cmp_i32!(s_neq_i32, neq);
impl_cmp_i32!(s_gt_i32, gt);
impl_cmp_i32!(s_lt_i32, lt);
impl_cmp_i32!(s_gt_eq_i32, gt_eq);
impl_cmp_i32!(s_lt_eq_i32, lt_eq);

impl_cmp_i64!(s_eq_i64, eq);
impl_cmp_i64!(s_neq_i64, neq);
impl_cmp_i64!(s_gt_i64, gt);
impl_cmp_i64!(s_lt_i64, lt);
impl_cmp_i64!(s_gt_eq_i64, gt_eq);
impl_cmp_i64!(s_lt_eq_i64, lt_eq);

impl_cmp_f32!(s_eq_f32, eq);
impl_cmp_f32!(s_neq_f32, neq);
impl_cmp_f32!(s_gt_f32, gt);
impl_cmp_f32!(s_lt_f32, lt);
impl_cmp_f32!(s_gt_eq_f32, gt_eq);
impl_cmp_f32!(s_lt_eq_f32, lt_eq);

impl_cmp_f64!(s_eq_f64, eq);
impl_cmp_f64!(s_neq_f64, neq);
impl_cmp_f64!(s_gt_f64, gt);
impl_cmp_f64!(s_lt_f64, lt);
impl_cmp_f64!(s_gt_eq_f64, gt_eq);
impl_cmp_f64!(s_lt_eq_f64, lt_eq);

impl_cmp_str!(s_eq_str, eq);
impl_cmp_str!(s_neq_str, neq);
impl_cmp_str!(s_gt_str, gt);
impl_cmp_str!(s_lt_str, lt);
impl_cmp_str!(s_gt_eq_str, gt_eq);
impl_cmp_str!(s_lt_eq_str, lt_eq);
