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
            Err(_) => Err(ExPolarsError::Internal(
                "Failed to take read lock for df".into(),
            )),
        }
    };
}

#[macro_export]
macro_rules! df_read_read {
    ($data: ident, $other: ident, $df: ident, $df1: ident, $body: block) => {
        match ($data.inner.0.read(), $other.inner.0.read()) {
            (Ok($df), Ok($df1)) => $body,
            _ => Err(ExPolarsError::Internal(
                "Failed to take read locks for df and df1".into(),
            )),
        }
    };
}

#[macro_export]
macro_rules! df_write {
    ($data: ident, $df: ident, $body: block) => {
        match $data.inner.0.write() {
            Ok(mut $df) => $body,
            Err(_) => Err(ExPolarsError::Internal(
                "Failed to take write lock for df".into(),
            )),
        }
    };
}

#[macro_export]
macro_rules! df_write_read {
    ($data: ident, $other: ident, $df: ident, $df1: ident, $body: block) => {
        match ($data.inner.0.write(), $other.inner.0.read()) {
            (Ok(mut $df), Ok($df1)) => $body,
            _ => Err(ExPolarsError::Internal(
                "Failed to take write and read lock for df and df1".into(),
            )),
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
        df_read_json,
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
        s_rechunk,
        s_chunk_lengths,
        s_name,
        s_rename,
        s_dtype,
        s_n_chunks,
        s_limit,
        s_slice,
        s_append,
        s_filter,
        s_add,
        s_sub,
        s_mul,
        s_div,
        s_head,
        s_tail,
        s_sort,
        s_argsort,
        s_unique,
        s_value_counts,
        s_arg_unique,
        s_take,
        s_take_with_series,
        s_null_count,
        s_is_null,
        s_is_not_null,
        s_is_unique,
        s_arg_true,
        s_sample_n,
        s_sample_frac,
        s_is_duplicated,
        s_explode,
        s_take_every,
        s_series_equal,
        s_eq,
        s_neq,
        s_gt,
        s_gt_eq,
        s_lt,
        s_lt_eq,
        s_not,
        s_len,
        s_to_json,
        s_drop_nulls,
        s_fill_none,
        s_clone,
        s_shift,
        s_zip_with,
        s_str_lengths,
        s_str_contains,
        s_str_replace,
        s_str_replace_all,
        s_str_to_uppercase,
        s_str_to_lowercase,
        s_str_parse_date32,
        s_str_parse_date64,
        s_datetime_str_fmt,
        s_as_duration,
        s_to_dummies,
        s_get_list,
        s_rolling_sum,
        s_rolling_mean,
        s_rolling_max,
        s_rolling_min,
        s_year,
        s_month,
        s_day,
        s_ordinal_day,
        s_hour,
        s_minute,
        s_second,
        s_nanosecond,
        s_set_with_mask_str,
        s_set_with_mask_f64,
        s_set_with_mask_f32,
        s_set_with_mask_u8,
        s_set_with_mask_u16,
        s_set_with_mask_u32,
        s_set_with_mask_u64,
        s_set_with_mask_i8,
        s_set_with_mask_i16,
        s_set_with_mask_i32,
        s_set_with_mask_i64,
        s_get_f32,
        s_get_f64,
        s_get_u8,
        s_get_u16,
        s_get_u32,
        s_get_u64,
        s_get_i8,
        s_get_i16,
        s_get_i32,
        s_get_i64,
        s_get_date32,
        s_get_date64,
        s_cast_u8,
        s_cast_u16,
        s_cast_u32,
        s_cast_u64,
        s_cast_i8,
        s_cast_i16,
        s_cast_i32,
        s_cast_i64,
        s_cast_f32,
        s_cast_f64,
        s_cast_date32,
        s_cast_date64,
        s_cast_time64ns,
        s_cast_duration_ns,
        s_cast_str,
        s_add_i64,
        s_sub_i64,
        s_mul_i64,
        s_div_i64,
        s_add_f64,
        s_sub_f64,
        s_mul_f64,
        s_div_f64,
        s_add_i64_rhs,
        s_sub_i64_rhs,
        s_mul_i64_rhs,
        s_div_i64_rhs,
        s_add_f64_rhs,
        s_sub_f64_rhs,
        s_mul_f64_rhs,
        s_div_f64_rhs,
        s_sum_u8,
        s_sum_u16,
        s_sum_u32,
        s_sum_u64,
        s_sum_i8,
        s_sum_i16,
        s_sum_i32,
        s_sum_i64,
        s_sum_f32,
        s_sum_f64,
        s_min_u8,
        s_min_u16,
        s_min_u32,
        s_min_u64,
        s_min_i8,
        s_min_i16,
        s_min_i32,
        s_min_i64,
        s_min_f32,
        s_min_f64,
        s_max_u8,
        s_max_u16,
        s_max_u32,
        s_max_u64,
        s_max_i8,
        s_max_i16,
        s_max_i32,
        s_max_i64,
        s_max_f32,
        s_max_f64,
        s_mean_u8,
        s_mean_u16,
        s_mean_u32,
        s_mean_u64,
        s_mean_i8,
        s_mean_i16,
        s_mean_i32,
        s_mean_i64,
        s_mean_f32,
        s_mean_f64,
        s_eq_u8,
        s_neq_u8,
        s_gt_u8,
        s_lt_u8,
        s_gt_eq_u8,
        s_lt_eq_u8,
        s_eq_u16,
        s_neq_u16,
        s_gt_u16,
        s_lt_u16,
        s_gt_eq_u16,
        s_lt_eq_u16,
        s_eq_u32,
        s_neq_u32,
        s_gt_u32,
        s_lt_u32,
        s_gt_eq_u32,
        s_lt_eq_u32,
        s_eq_u64,
        s_neq_u64,
        s_gt_u64,
        s_lt_u64,
        s_gt_eq_u64,
        s_lt_eq_u64,
        s_eq_i8,
        s_neq_i8,
        s_gt_i8,
        s_lt_i8,
        s_gt_eq_i8,
        s_lt_eq_i8,
        s_eq_i16,
        s_neq_i16,
        s_gt_i16,
        s_lt_i16,
        s_gt_eq_i16,
        s_lt_eq_i16,
        s_eq_i32,
        s_neq_i32,
        s_gt_i32,
        s_lt_i32,
        s_gt_eq_i32,
        s_lt_eq_i32,
        s_eq_i64,
        s_neq_i64,
        s_gt_i64,
        s_lt_i64,
        s_gt_eq_i64,
        s_lt_eq_i64,
        s_eq_f32,
        s_neq_f32,
        s_gt_f32,
        s_lt_f32,
        s_gt_eq_f32,
        s_lt_eq_f32,
        s_eq_f64,
        s_neq_f64,
        s_gt_f64,
        s_lt_f64,
        s_gt_eq_f64,
        s_lt_eq_f64,
        s_eq_str,
        s_neq_str,
        s_gt_str,
        s_lt_str,
        s_gt_eq_str,
        s_lt_eq_str,
    ],
    load = on_load
);

#[cfg(test)]
mod test {
    use serde_json::Value;
    use std::fs::File;
    #[test]
    fn read_json() {
        let f = File::open("crimea.json").unwrap();
        let v: Value = serde_json::from_reader(f).unwrap();
        let items: Vec<String> = v
            .as_array()
            .unwrap()
            .iter()
            .map(|item| serde_json::to_string(&item).unwrap())
            .collect();
        for line in items {
            println!("{}", line);
        }
    }
}
