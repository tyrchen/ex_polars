defmodule ExPolars.DataFrame do
  @moduledoc """
  Documentation for `ExPolars`.
  """

  use Rustler, otp_app: :ex_polars, crate: :expolars

  defstruct [:inner]

  def read_csv(
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

  def read_parquet(_filename), do: err()

  def to_csv(_df, _filename, _batch_size \\ 100_000, _has_headers \\ true, _delimiter \\ ?,),
    do: err()

  def as_str(_df), do: err()

  def add(_df, _s), do: err()
  def sub(_df, _s), do: err()
  def div(_df, _s), do: err()
  def mul(_df, _s), do: err()
  def rem(_df, _s), do: err()
  def sample_n(_df, _n, _with_replacement), do: err()
  def sample_frac(_df, _f, _with_replacement), do: err()
  def rechunk(_df), do: err()
  def fill_none(_df, _strategy), do: err()
  def join(_df, _other, _left_on, _right_on, _how), do: err()
  def get_columns(_df), do: err()
  def columns(_def), do: err()
  def set_column_names(_df, _names), do: err()
  def dtypes(_df), do: err()
  def n_chunks(_df), do: err()
  def shape(_df), do: err()
  def height(_df), do: err()
  def width(_df), do: err()
  def hstack_mut(_df, _cols), do: err()
  def hstack(_df, _cols), do: err()
  def vstack(_df, _other), do: err()
  def drop_in_place(_df, _name), do: err()
  def drop_nulls(_df, _subset), do: err()
  def drop(_df, _name), do: err()
  def select_at_idx(_df, _idx), do: err()
  def find_idx_by_name(_df, _name), do: err()
  def column(_df, _name), do: err()
  def select(_df, _selection), do: err()
  def filter(_df, _mask), do: err()
  def take(_df, _indices), do: err()
  def take_with_series(_df, _indices), do: err()
  def sort_new(_df, _by_column, _reverse), do: err()
  def sort_in_place(_df, _by_column, _reverse), do: err()
  def replace(_df, _col, _new_col), do: err()
  def replace_at_idx(_df, _index, _new_col), do: err()
  def insert_at_idx(_df, _index, _new_col), do: err()
  def slice(_df, _offset, _length), do: err()
  def head(_df, _length), do: err()
  def tail(_df, _length), do: err()
  def is_unique(_df), do: err()
  def is_duplicated(_df), do: err()
  def frame_equal(_df, _other, _null_equal \\ false), do: err()
  def groupby(_df, _by, _sel, _agg), do: err()
  def groupby_agg(_df, _by, _column_to_agg), do: err()
  def groupby_quantile(_df, _by, _sel, _quant), do: err()
  def pivot(_df, _by, _pivot_column, _values_column, _agg), do: err()
  def clone(_df), do: err()
  def explode(_df, _cols), do: err()
  def melt(_df, _id_vars, _value_vars), do: err()
  def shift(_df, _periods), do: err()
  def drop_duplicates(_df, _maintain_order \\ true, _subset \\ nil), do: err()
  def max(_df), do: err()
  def min(_df), do: err()
  def sum(_df), do: err()
  def mean(_df), do: err()
  def stdev(_df), do: err()
  def var(_df), do: err()
  def median(_df), do: err()
  def quantile(_df, _quant), do: err()
  def to_dummies(_df), do: err()

  def sample(df, n_or_frac, with_replacement \\ false) do
    case is_integer(n_or_frac) do
      true -> sample_n(df, n_or_frac, with_replacement)
      _ -> sample_frac(df, n_or_frac, with_replacement)
    end
  end

  def sort(df, by_column, inplace \\ false, reverse \\ false) do
    case inplace do
      true -> sort_in_place(df, by_column, reverse)
      _ -> sort_new(df, by_column, reverse)
    end
  end

  defp err, do: :erlang.nif_error(:nif_not_loaded)
end
