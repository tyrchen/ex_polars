use rustler::{Env, Term};

mod dataframe;
mod datatypes;
mod error;
mod series;

use dataframe::*;
pub use datatypes::{DataType, ExDataFrame, ExDataFrameRef, ExSeries, ExSeriesRef};
pub use error::ExPolarsError;
use series::*;

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

// rustler initialization

fn on_load(env: Env, _info: Term) -> bool {
    rustler::resource!(ExDataFrameRef, env);
    rustler::resource!(ExSeriesRef, env);
    true
}

rustler::init!(
    "Elixir.ExPolars.Native",
    [
        df_read_csv,
        df_read_parquet,
        df_to_csv,
        df_as_str,
        df_add,
        df_sub,
        df_mul,
        df_div,
        df_rem,
        df_sample_n,
        df_sample_frac,
        df_rechunk,
        df_fill_none,
        df_join,
        df_get_columns,
        df_columns,
        df_set_column_names,
        df_dtypes,
        df_n_chunks,
        df_shape,
        df_width,
        df_height,
        df_hstack_mut,
        df_hstack,
        df_vstack,
        df_drop_in_place,
        df_drop_nulls,
        df_drop,
        df_select_at_idx,
        df_find_idx_by_name,
        df_column,
        df_select,
        df_filter,
        df_take,
        df_take_with_series,
        df_sort_new,
        df_sort_in_place,
        df_replace,
        df_replace_at_idx,
        df_insert_at_idx,
        df_slice,
        df_head,
        df_tail,
        df_is_unique,
        df_is_duplicated,
        df_frame_equal,
        df_groupby,
        df_groupby_agg,
        df_groupby_quantile,
        df_pivot,
        df_clone,
        df_explode,
        df_melt,
        df_shift,
        df_drop_duplicates,
        df_max,
        df_min,
        df_sum,
        df_mean,
        df_stdev,
        df_var,
        df_median,
        df_quantile,
        df_to_dummies,
        // series
        s_as_str,
        s_new_i8,
        s_new_i16,
        s_new_i32,
        s_new_i64,
        s_new_bool,
        s_new_u8,
        s_new_u16,
        s_new_u32,
        s_new_u64,
        s_new_date32,
        s_new_date64,
        s_new_duration_ns,
        s_new_f32,
        s_new_f64,
        s_parse_date32_from_str_slice,
        s_new_str,
        s_to_json,
    ],
    load = on_load
);
