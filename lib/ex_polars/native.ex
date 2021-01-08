defmodule ExPolars.Native do
  @moduledoc """
  Documentation for `Native`.
  """

  use Rustler, otp_app: :ex_polars, crate: :expolars

  defstruct [:inner]

  def df_read_csv(
        _filename,
        _infer_schema_length \\ 100,
        _batch_size \\ 64,
        _has_header \\ true,
        _ignore_errors \\ false,
        _stop_after_n_rows \\ nil,
        _skip_rows \\ 0,
        _projection \\ nil,
        _sep \\ ",",
        _rechunk \\ true,
        _columns \\ nil,
        _encoding \\ "utf8"
      ),
      do: err()

  def df_read_parquet(_filename), do: err()

  def df_to_csv(_df, _filename, _batch_size \\ 100_000, _has_headers \\ true, _delimiter \\ ?,),
    do: err()

  def df_as_str(_df), do: err()

  def df_add(_df, _s), do: err()
  def df_sub(_df, _s), do: err()
  def df_div(_df, _s), do: err()
  def df_mul(_df, _s), do: err()
  def df_rem(_df, _s), do: err()
  def df_sample_n(_df, _n, _with_replacement), do: err()
  def df_sample_frac(_df, _f, _with_replacement), do: err()
  def df_rechunk(_df), do: err()
  def df_fill_none(_df, _strategy), do: err()
  def df_join(_df, _other, _left_on, _right_on, _how), do: err()
  def df_get_columns(_df), do: err()
  def df_columns(_def), do: err()
  def df_set_column_names(_df, _names), do: err()
  def df_dtypes(_df), do: err()
  def df_n_chunks(_df), do: err()
  def df_shape(_df), do: err()
  def df_height(_df), do: err()
  def df_width(_df), do: err()
  def df_hstack_mut(_df, _cols), do: err()
  def df_hstack(_df, _cols), do: err()
  def df_vstack(_df, _other), do: err()
  def df_drop_in_place(_df, _name), do: err()
  def df_drop_nulls(_df, _subset), do: err()
  def df_drop(_df, _name), do: err()
  def df_select_at_idx(_df, _idx), do: err()
  def df_find_idx_by_name(_df, _name), do: err()
  def df_column(_df, _name), do: err()
  def df_select(_df, _selection), do: err()
  def df_filter(_df, _mask), do: err()
  def df_take(_df, _indices), do: err()
  def df_take_with_series(_df, _indices), do: err()
  def df_sort_new(_df, _by_column, _reverse), do: err()
  def df_sort_in_place(_df, _by_column, _reverse), do: err()
  def df_replace(_df, _col, _new_col), do: err()
  def df_replace_at_idx(_df, _index, _new_col), do: err()
  def df_insert_at_idx(_df, _index, _new_col), do: err()
  def df_slice(_df, _offset, _length), do: err()
  def df_head(_df, _length \\ 5), do: err()
  def df_tail(_df, _length \\ 5), do: err()
  def df_is_unique(_df), do: err()
  def df_is_duplicated(_df), do: err()
  def df_frame_equal(_df, _other, _null_equal \\ false), do: err()
  def df_groupby(_df, _by, _sel, _agg), do: err()
  def df_groupby_agg(_df, _by, _column_to_agg), do: err()
  def df_groupby_quantile(_df, _by, _sel, _quant), do: err()
  def df_pivot(_df, _by, _pivot_column, _values_column, _agg), do: err()
  def df_clone(_df), do: err()
  def df_explode(_df, _cols), do: err()
  def df_melt(_df, _id_vars, _value_vars), do: err()
  def df_shift(_df, _periods), do: err()
  def df_drop_duplicates(_df, _maintain_order \\ true, _subset \\ nil), do: err()
  def df_max(_df), do: err()
  def df_min(_df), do: err()
  def df_sum(_df), do: err()
  def df_mean(_df), do: err()
  def df_stdev(_df), do: err()
  def df_var(_df), do: err()
  def df_median(_df), do: err()
  def df_quantile(_df, _quant), do: err()
  def df_to_dummies(_df), do: err()
  def s_as_str(_s), do: err()

  defp err, do: :erlang.nif_error(:nif_not_loaded)
end
