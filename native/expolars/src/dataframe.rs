// This file is modified based on: https://github.com/ritchie46/polars/blob/master/py-polars/src/dataframe.rs

use polars::prelude::*;

use std::fs::File;
use std::io::Cursor;
use std::result::Result;

use crate::series::{to_ex_series_collection, to_series_collection};

use crate::{DataType, ExDataFrame, ExPolarsError, ExSeries};

use crate::{df_read, df_read_read, df_write, df_write_read};

#[rustler::nif]
pub fn df_read_csv(
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
    encoding: &str,
) -> Result<ExDataFrame, ExPolarsError> {
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
pub fn df_read_parquet(filename: &str) -> Result<ExDataFrame, ExPolarsError> {
    let f = File::open(filename)?;
    let df = ParquetReader::new(f).finish()?;
    Ok(ExDataFrame::new(df))
}

#[rustler::nif]
pub fn df_read_json(
    filename: &str,
    line_delimited_json: bool,
) -> Result<ExDataFrame, ExPolarsError> {
    let infer_size = 3;
    let batch_size = 100;
    let f = File::open(filename)?;
    let df = if line_delimited_json {
        JsonReader::new(f)
            .infer_schema(Some(infer_size))
            .with_batch_size(batch_size)
            .finish()?
    } else {
        let v: serde_json::Value = serde_json::from_reader(f)?;
        let items: Vec<String> = v
            .as_array()
            .unwrap()
            .iter()
            .map(|item| serde_json::to_string(&item).unwrap())
            .collect();

        JsonReader::new(Cursor::new(items.join("\n")))
            .infer_schema(Some(infer_size))
            .with_batch_size(batch_size)
            .finish()?
    };

    Ok(ExDataFrame::new(df))
}

#[rustler::nif]
pub fn df_to_csv(
    data: ExDataFrame,
    batch_size: usize,
    has_headers: bool,
    delimiter: u8,
) -> Result<String, ExPolarsError> {
    df_write!(data, df, {
        let mut buf: Vec<u8> = Vec::with_capacity(81920);
        CsvWriter::new(&mut buf)
            .has_headers(has_headers)
            .with_delimiter(delimiter)
            .with_batch_size(batch_size)
            .finish(&mut *df)?;

        let s = String::from_utf8(buf)?;
        Ok(s)
    })
}

#[rustler::nif]
pub fn df_to_csv_file(
    data: ExDataFrame,
    filename: &str,
    batch_size: usize,
    has_headers: bool,
    delimiter: u8,
) -> Result<(), ExPolarsError> {
    df_write!(data, df, {
        let mut f = File::create(filename)?;
        CsvWriter::new(&mut f)
            .has_headers(has_headers)
            .with_delimiter(delimiter)
            .with_batch_size(batch_size)
            .finish(&mut *df)?;
        Ok(())
    })
}

#[rustler::nif]
/// Format `DataFrame` as String
pub fn df_as_str(data: ExDataFrame) -> Result<String, ExPolarsError> {
    df_read!(data, df, { Ok(format!("{:?}", &*df)) })
}

#[rustler::nif]
pub fn df_add(data: ExDataFrame, s: ExSeries) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let new_df = (&*df + &s.inner.0)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_sub(data: ExDataFrame, s: ExSeries) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let new_df = (&*df - &s.inner.0)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_div(data: ExDataFrame, s: ExSeries) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let new_df = (&*df / &s.inner.0)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_mul(data: ExDataFrame, s: ExSeries) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let new_df = (&*df * &s.inner.0)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_rem(data: ExDataFrame, s: ExSeries) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let new_df = (&*df % &s.inner.0)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_sample_n(
    data: ExDataFrame,
    n: usize,
    with_replacement: bool,
) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let new_df = df.sample_n(n, with_replacement)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_sample_frac(
    data: ExDataFrame,
    frac: f64,
    with_replacement: bool,
) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let new_df = df.sample_frac(frac, with_replacement)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_rechunk(data: ExDataFrame) -> Result<(), ExPolarsError> {
    df_write!(data, df, {
        (&mut *df).agg_chunks();
        Ok(())
    })
}

#[rustler::nif]
pub fn df_fill_none(data: ExDataFrame, strategy: &str) -> Result<ExDataFrame, ExPolarsError> {
    let strat = match strategy {
        "backward" => FillNoneStrategy::Backward,
        "forward" => FillNoneStrategy::Forward,
        "min" => FillNoneStrategy::Min,
        "max" => FillNoneStrategy::Max,
        "mean" => FillNoneStrategy::Mean,
        s => return Err(ExPolarsError::Other(format!("Strategy {} not supported", s)).into()),
    };
    df_read!(data, df, {
        let new_df = df.fill_none(strat)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_join(
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

    df_read_read!(data, other, df, df1, {
        let new_df = df.join(&*df1, left_on, right_on, how)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_get_columns(data: ExDataFrame) -> Result<Vec<ExSeries>, ExPolarsError> {
    df_read!(data, df, {
        Ok(to_ex_series_collection(df.get_columns().clone()))
    })
}

/// Get column names
#[rustler::nif]
pub fn df_columns(data: ExDataFrame) -> Result<Vec<String>, ExPolarsError> {
    df_read!(data, df, {
        let cols = df.get_column_names();
        Ok(cols.into_iter().map(|s| s.to_owned()).collect())
    })
}

/// set column names
#[rustler::nif]
pub fn df_set_column_names(data: ExDataFrame, names: Vec<&str>) -> Result<(), ExPolarsError> {
    df_write!(data, df, {
        (&mut *df).set_column_names(&names)?;
        Ok(())
    })
}

/// Get datatypes
#[rustler::nif]
pub fn df_dtypes(data: ExDataFrame) -> Result<Vec<u8>, ExPolarsError> {
    df_read!(data, df, {
        let result = df
            .dtypes()
            .iter()
            .map(|dtype| {
                let arrow_dtype = &dtype.to_arrow();
                let dt: DataType = arrow_dtype.into();
                dt as u8
            })
            .collect();
        Ok(result)
    })
}

#[rustler::nif]
pub fn df_n_chunks(data: ExDataFrame) -> Result<usize, ExPolarsError> {
    df_read!(data, df, { Ok(df.n_chunks()?) })
}

#[rustler::nif]
pub fn df_shape(data: ExDataFrame) -> Result<(usize, usize), ExPolarsError> {
    df_read!(data, df, { Ok(df.shape()) })
}

#[rustler::nif]
pub fn df_height(data: ExDataFrame) -> Result<usize, ExPolarsError> {
    df_read!(data, df, { Ok(df.height()) })
}

#[rustler::nif]
pub fn df_width(data: ExDataFrame) -> Result<usize, ExPolarsError> {
    df_read!(data, df, { Ok(df.width()) })
}

#[rustler::nif]
pub fn df_hstack_mut(data: ExDataFrame, cols: Vec<ExSeries>) -> Result<(), ExPolarsError> {
    let cols = to_series_collection(cols);
    df_write!(data, df, {
        (&mut *df).hstack_mut(&cols)?;
        Ok(())
    })
}

#[rustler::nif]
pub fn df_hstack(data: ExDataFrame, cols: Vec<ExSeries>) -> Result<ExDataFrame, ExPolarsError> {
    let cols = to_series_collection(cols);
    df_read!(data, df, {
        let new_df = df.hstack(&cols)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_vstack(data: ExDataFrame, other: ExDataFrame) -> Result<(), ExPolarsError> {
    df_write_read!(data, other, df, df1, {
        (&mut *df).vstack_mut(&df1)?;
        Ok(())
    })
}

#[rustler::nif]
pub fn df_drop_in_place(data: ExDataFrame, name: &str) -> Result<ExSeries, ExPolarsError> {
    df_write!(data, df, {
        let s = (&mut *df).drop_in_place(name)?;
        Ok(ExSeries::new(s))
    })
}

#[rustler::nif]
pub fn df_drop_nulls(
    data: ExDataFrame,
    subset: Option<Vec<String>>,
) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let new_df = df.drop_nulls(subset.as_ref().map(|s| s.as_ref()))?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_drop(data: ExDataFrame, name: &str) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let new_df = (&*df).drop(name)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_select_at_idx(data: ExDataFrame, idx: usize) -> Result<Option<ExSeries>, ExPolarsError> {
    df_read!(data, df, {
        let result = df.select_at_idx(idx).map(|s| ExSeries::new(s.clone()));
        Ok(result)
    })
}

#[rustler::nif]
pub fn df_find_idx_by_name(data: ExDataFrame, name: &str) -> Result<Option<usize>, ExPolarsError> {
    df_read!(data, df, { Ok(df.find_idx_by_name(name)) })
}

#[rustler::nif]
pub fn df_column(data: ExDataFrame, name: &str) -> Result<ExSeries, ExPolarsError> {
    df_read!(data, df, {
        let series = df.column(name).map(|s| ExSeries::new(s.clone()))?;
        Ok(series)
    })
}

#[rustler::nif]
pub fn df_select(data: ExDataFrame, selection: Vec<&str>) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let new_df = df.select(&selection)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_filter(data: ExDataFrame, mask: ExSeries) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let filter_series = &mask.inner.0;
        if let Ok(ca) = filter_series.bool() {
            let new_df = df.filter(ca)?;
            Ok(ExDataFrame::new(new_df))
        } else {
            Err(ExPolarsError::Other("Expected a boolean mask".into()))
        }
    })
}

#[rustler::nif]
pub fn df_take(data: ExDataFrame, indices: Vec<usize>) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let new_df = df.take(&indices);
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_take_with_series(
    data: ExDataFrame,
    indices: ExSeries,
) -> Result<ExDataFrame, ExPolarsError> {
    let idx = indices.inner.0.u32()?;
    df_read!(data, df, {
        let new_df = df.take(&idx);
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_sort_new(
    data: ExDataFrame,
    by_column: &str,
    reverse: bool,
) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let new_df = df.sort(by_column, reverse)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_sort_in_place(
    data: ExDataFrame,
    by_column: &str,
    reverse: bool,
) -> Result<(), ExPolarsError> {
    df_write!(data, df, {
        (&mut *df).sort_in_place(by_column, reverse)?;
        Ok(())
    })
}

#[rustler::nif]
pub fn df_replace(data: ExDataFrame, col: &str, new_col: ExSeries) -> Result<(), ExPolarsError> {
    df_write!(data, df, {
        (&mut *df).replace(col, new_col.inner.0.clone())?;
        Ok(())
    })
}

#[rustler::nif]
pub fn df_replace_at_idx(
    data: ExDataFrame,
    index: usize,
    new_col: ExSeries,
) -> Result<(), ExPolarsError> {
    df_write!(data, df, {
        (&mut *df).replace_at_idx(index, new_col.inner.0.clone())?;
        Ok(())
    })
}

#[rustler::nif]
pub fn df_insert_at_idx(
    data: ExDataFrame,
    index: usize,
    new_col: ExSeries,
) -> Result<(), ExPolarsError> {
    df_write!(data, df, {
        (&mut *df).insert_at_idx(index, new_col.inner.0.clone())?;
        Ok(())
    })
}

#[rustler::nif]
pub fn df_slice(
    data: ExDataFrame,
    offset: usize,
    length: usize,
) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let new_df = df.slice(offset, length)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_head(data: ExDataFrame, length: Option<usize>) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let new_df = df.head(length);
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_tail(data: ExDataFrame, length: Option<usize>) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let new_df = df.tail(length);
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_is_unique(data: ExDataFrame) -> Result<ExSeries, ExPolarsError> {
    df_read!(data, df, {
        let mask = df.is_unique()?;
        Ok(ExSeries::new(mask.into_series()))
    })
}

#[rustler::nif]
pub fn df_is_duplicated(data: ExDataFrame) -> Result<ExSeries, ExPolarsError> {
    df_read!(data, df, {
        let mask = df.is_unique()?;
        Ok(ExSeries::new(mask.into_series()))
    })
}

#[rustler::nif]
pub fn df_frame_equal(
    data: ExDataFrame,
    other: ExDataFrame,
    null_equal: bool,
) -> Result<bool, ExPolarsError> {
    df_read_read!(data, other, df, df1, {
        let result = if null_equal {
            df.frame_equal_missing(&*df1)
        } else {
            df.frame_equal(&*df1)
        };
        Ok(result)
    })
}

#[rustler::nif]
pub fn df_groupby(
    data: ExDataFrame,
    by: Vec<&str>,
    sel: Option<Vec<String>>,
    agg: &str,
) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let gb = df.groupby(&by)?;
        let selection = match sel.as_ref() {
            Some(s) => gb.select(s),
            None => gb,
        };
        let result = match agg {
            "min" => selection.min(),
            "max" => selection.max(),
            "mean" => selection.mean(),
            "first" => selection.first(),
            "last" => selection.last(),
            "sum" => selection.sum(),
            "count" => selection.count(),
            "n_unique" => selection.n_unique(),
            "median" => selection.median(),
            "agg_list" => selection.agg_list(),
            "groups" => selection.groups(),
            "std" => selection.std(),
            "var" => selection.var(),
            a => Err(PolarsError::Other(
                format!("agg fn {} does not exists", a).into(),
            )),
        };
        Ok(ExDataFrame::new(result?))
    })
}

#[rustler::nif]
pub fn df_groupby_agg(
    data: ExDataFrame,
    by: Vec<&str>,
    column_to_agg: Vec<(&str, Vec<&str>)>,
) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let gb = df.groupby(&by)?;
        let new_df = gb.agg(&column_to_agg)?;
        Ok(ExDataFrame::new(new_df))
    })
}

// TODO(tchen): groupby_apply(data: ExDataFrame, by: Vec<&str>, lambda: Fun) -> Result<ExDataFrame, ExPolarsError> not implemented
// I don't know how to pass an elixir function to rust for execution

#[rustler::nif]
pub fn df_groupby_quantile(
    data: ExDataFrame,
    by: Vec<&str>,
    sel: Vec<String>,
    quant: f64,
) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let gb = df.groupby(&by)?;
        let selection = gb.select(&sel);
        let new_df = selection.quantile(quant)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_pivot(
    data: ExDataFrame,
    by: Vec<String>,
    pivot_column: &str,
    values_column: &str,
    agg: &str,
) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let mut gb = df.groupby(&by)?;
        let pivot = gb.pivot(pivot_column, values_column);
        let result = match agg {
            "first" => pivot.first(),
            "min" => pivot.min(),
            "max" => pivot.max(),
            "mean" => pivot.mean(),
            "median" => pivot.median(),
            "sum" => pivot.sum(),
            "count" => pivot.count(),
            a => Err(PolarsError::Other(
                format!("agg fn {} does not exists", a).into(),
            )),
        };
        Ok(ExDataFrame::new(result?))
    })
}

#[rustler::nif]
pub fn df_clone(data: ExDataFrame) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, { Ok(ExDataFrame::new(df.clone())) })
}

#[rustler::nif]
pub fn df_explode(data: ExDataFrame, cols: Vec<String>) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let new_df = df.explode(&cols)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_melt(
    data: ExDataFrame,
    id_vars: Vec<&str>,
    value_vars: Vec<&str>,
) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let new_df = df.melt(id_vars, value_vars)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_shift(data: ExDataFrame, periods: i32) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let new_df = df.shift(periods)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_drop_duplicates(
    data: ExDataFrame,
    maintain_order: bool,
    subset: Option<Vec<String>>,
) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let new_df = df.drop_duplicates(maintain_order, subset.as_ref().map(|v| v.as_ref()))?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_max(data: ExDataFrame) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, { Ok(ExDataFrame::new(df.max())) })
}

#[rustler::nif]
pub fn df_min(data: ExDataFrame) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, { Ok(ExDataFrame::new(df.min())) })
}

#[rustler::nif]
pub fn df_sum(data: ExDataFrame) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, { Ok(ExDataFrame::new(df.sum())) })
}

#[rustler::nif]
pub fn df_mean(data: ExDataFrame) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, { Ok(ExDataFrame::new(df.mean())) })
}

#[rustler::nif]
pub fn df_stdev(data: ExDataFrame) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, { Ok(ExDataFrame::new(df.std())) })
}

#[rustler::nif]
pub fn df_var(data: ExDataFrame) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, { Ok(ExDataFrame::new(df.var())) })
}

#[rustler::nif]
pub fn df_median(data: ExDataFrame) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, { Ok(ExDataFrame::new(df.median())) })
}

#[rustler::nif]
pub fn df_quantile(data: ExDataFrame, quant: f64) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let new_df = df.quantile(quant)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_to_dummies(data: ExDataFrame) -> Result<ExDataFrame, ExPolarsError> {
    df_read!(data, df, {
        let new_df = df.to_dummies()?;
        Ok(ExDataFrame::new(new_df))
    })
}
