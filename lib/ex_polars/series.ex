defmodule ExPolars.Series do
  @moduledoc """
  Documentation for `Series`.
  """
  import Kernel, except: [+: 2, -: 2, *: 2, /: 2, ==: 2, <>: 2, >: 2, >=: 2, <: 2, <=: 2]

  alias ExPolars.Native
  defstruct [:inner]

  @type t :: ExPolars.DataFrame
  @type s :: ExPolars.Series

  @dtype_strs %{
    0 => "i8",
    1 => "i16",
    2 => "i32",
    3 => "i64",
    4 => "u8",
    5 => "u16",
    6 => "u32",
    7 => "u64",
    8 => "f32",
    9 => "f64",
    10 => "bool",
    11 => "str",
    12 => "list",
    13 => "date32",
    14 => "date64",
    15 => "time64_nanosecond",
    16 => "duration_nanosecond",
    17 => "duration_millisecond",
    18 => "object"
  }

  @spec new(String.t(), list(String.t() | integer() | float() | boolean())) ::
          {:ok, s()} | {:error, term}
  def new(name, data) do
    [first | _] = data

    cond do
      is_integer(first) -> Native.s_new_i64(name, data)
      is_float(first) -> Native.s_new_f64(name, data)
      is_boolean(first) -> Native.s_new_bool(name, data)
      is_binary(first) -> Native.s_new_str(name, data)
      true -> raise "Unspported datetype: #{inspect(first)}"
    end
  end

  @spec new_duration_ns(String.t(), list(integer())) :: {:ok, s()} | {:error, term}
  defdelegate new_duration_ns(name, data), to: Native, as: :s_new_duration_ns

  @spec to_list(s() | {:ok, s()}) :: {:ok, list()} | {:error, term}
  def to_list({:ok, s}), do: to_list(s)

  def to_list(s) do
    with {:ok, json} <- Native.s_to_json(s),
         {:ok, data} <- Jason.decode(json) do
      {:ok, data}
    else
      e -> e
    end
  end

  @spec rechunk(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def rechunk({:ok, s}), do: rechunk(s)
  defdelegate rechunk(s), to: Native, as: :s_rechunk

  @spec chunk_lengths(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def chunk_lengths({:ok, s}), do: chunk_lengths(s)
  defdelegate chunk_lengths(s), to: Native, as: :s_chunk_lengths

  @spec name(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def name({:ok, s}), do: name(s)
  defdelegate name(s), to: Native, as: :s_name

  @spec rename(s() | {:ok, s()}, String.t()) :: {:ok, s()} | {:error, term}
  def rename({:ok, s}, name), do: rename(s, name)
  defdelegate rename(s, name), to: Native, as: :s_rename

  @spec dtype(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def dtype({:ok, s}), do: dtype(s)
  defdelegate dtype(s), to: Native, as: :s_dtype

  @spec dtype_str(s() | {:ok, s()}) :: String.t()
  def dtype_str({:ok, s}), do: dtype_str(s)

  def dtype_str(s) do
    case dtype(s) do
      {:ok, t} ->
        case t > 18 or t < 0 do
          true -> @dtype_strs[18]
          false -> @dtype_strs[t]
        end

      _ ->
        @dtype_strs[18]
    end
  end

  @spec n_chunks(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  @doc """
  Get the number of chunks that this Series contains.
  """
  def n_chunks({:ok, s}), do: n_chunks(s)
  defdelegate n_chunks(s), to: Native, as: :s_n_chunks

  @spec limit(s() | {:ok, s()}, integer()) :: {:ok, s()} | {:error, term}
  @doc """
  Take n elements from this Series.

  Parameters
  ----------
  num_elements
      Amount of elements to take.
  """
  def limit({:ok, s}, num_elements), do: limit(s, num_elements)
  defdelegate limit(s, num_elements), to: Native, as: :s_limit

  @spec slice(s() | {:ok, s()}, integer(), integer()) :: {:ok, s()} | {:error, term}
  @doc """
  Get a slice of this Series

  Parameters
  ----------
  offset
      Offset index.
  length
      Length of the slice.
  """
  def slice({:ok, s}, offset, length), do: slice(s, offset, length)
  defdelegate slice(s, offset, length), to: Native, as: :s_slice

  @spec append(s() | {:ok, s()}, s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  @doc """
  Append a Series to this one.

  Parameters
  ----------
  other
      Series to append
  """
  def append({:ok, s}, {:ok, other}), do: append(s, other)
  def append(s, {:ok, other}), do: append(s, other)
  def append({:ok, s}, other), do: append(s, other)
  defdelegate append(s, other), to: Native, as: :s_append

  @spec filter(s() | {:ok, s()}, s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  @doc """
  Filter elements by a boolean mask

  Parameters
  ----------
  filter
      Boolean mask
  """
  def filter({:ok, s}, {:ok, filter}), do: filter(s, filter)
  def filter(s, {:ok, filter}), do: filter(s, filter)
  def filter({:ok, s}, filter), do: filter(s, filter)
  defdelegate filter(s, filter), to: Native, as: :s_filter

  @spec add(s() | {:ok, s()}, s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def add({:ok, s}, {:ok, other}), do: add(s, other)
  def add({:ok, s}, other), do: add(s, other)
  def add(s, {:ok, other}), do: add(s, other)
  defdelegate add(s, other), to: Native, as: :s_add

  @spec sub(s() | {:ok, s()}, s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def sub({:ok, s}, {:ok, other}), do: sub(s, other)
  def sub({:ok, s}, other), do: sub(s, other)
  def sub(s, {:ok, other}), do: sub(s, other)
  defdelegate sub(s, other), to: Native, as: :s_sub

  @spec mul(s() | {:ok, s()}, s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def mul({:ok, s}, {:ok, other}), do: mul(s, other)
  def mul({:ok, s}, other), do: mul(s, other)
  def mul(s, {:ok, other}), do: mul(s, other)
  defdelegate mul(s, other), to: Native, as: :s_mul

  @spec divide(s() | {:ok, s()}, s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def divide({:ok, s}, {:ok, other}), do: divide(s, other)
  def divide({:ok, s}, other), do: divide(s, other)
  def divide(s, {:ok, other}), do: divide(s, other)
  defdelegate divide(s, other), to: Native, as: :s_div

  @spec head(s() | {:ok, s()}, integer()) :: {:ok, s()} | {:error, term}
  def head(s, length \\ 5)
  def head({:ok, s}, length), do: head(s, length)
  def head(s, {:ok, length}), do: head(s, length)
  defdelegate head(s, length), to: Native, as: :s_head

  @spec tail(s() | {:ok, s()}, integer()) :: {:ok, s()} | {:error, term}
  def tail(s, length \\ 5)
  def tail({:ok, s}, length), do: tail(s, length)
  defdelegate tail(s, length), to: Native, as: :s_tail

  @spec sort(s() | {:ok, s()}, boolean()) :: {:ok, s()} | {:error, term}
  def sort(s, reverse \\ false)
  def sort({:ok, s}, reverse), do: sort(s, reverse)
  defdelegate sort(s, reverse), to: Native, as: :s_sort

  @spec argsort(s() | {:ok, s()}, boolean()) :: {:ok, s()} | {:error, term}
  def argsort(s, reverse \\ false)
  def argsort({:ok, s}, reverse), do: argsort(s, reverse)
  defdelegate argsort(s, reverse), to: Native, as: :s_argsort

  @spec unique(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def unique({:ok, s}), do: unique(s)
  defdelegate unique(s), to: Native, as: :s_unique

  @spec value_counts(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def value_counts({:ok, s}), do: value_counts(s)
  defdelegate value_counts(s), to: Native, as: :s_value_counts

  @spec arg_unique(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def arg_unique({:ok, s}), do: arg_unique(s)
  defdelegate arg_unique(s), to: Native, as: :s_arg_unique

  @spec take(s() | {:ok, s()}, list(integer())) :: {:ok, s()} | {:error, term}
  def take({:ok, s}, indeces), do: take(s, indeces)
  defdelegate take(s, indeces), to: Native, as: :s_take

  @spec take_with_series(s() | {:ok, s()}, s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def take_with_series({:ok, s}, {:ok, indeces}), do: take_with_series(s, indeces)
  def take_with_series(s, {:ok, indeces}), do: take_with_series(s, indeces)
  def take_with_series({:ok, s}, indeces), do: take_with_series(s, indeces)
  defdelegate take_with_series(s, indeces), to: Native, as: :s_take_with_series

  @spec null_count(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def null_count({:ok, s}), do: null_count(s)
  defdelegate null_count(s), to: Native, as: :s_null_count

  @spec is_null(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def is_null({:ok, s}), do: is_null(s)
  defdelegate is_null(s), to: Native, as: :s_is_null

  @spec is_not_null(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def is_not_null({:ok, s}), do: is_not_null(s)
  defdelegate is_not_null(s), to: Native, as: :s_is_not_null

  @spec is_unique(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def is_unique({:ok, s}), do: is_unique(s)
  defdelegate is_unique(s), to: Native, as: :s_is_unique

  @spec arg_true(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def arg_true({:ok, s}), do: arg_true(s)
  defdelegate arg_true(s), to: Native, as: :s_arg_true

  @spec sample(s() | {:ok, s()}, integer() | float(), boolean()) :: {:ok, s()} | {:error, term}
  def sample(s, n_or_frac, with_replacement \\ false)
  def sample({:ok, s}, n_or_frac, with_replacement), do: sample(s, n_or_frac, with_replacement)

  def sample(s, n_or_frac, with_replacement) do
    case is_integer(n_or_frac) do
      true -> Native.s_sample_n(s, n_or_frac, with_replacement)
      _ -> Native.s_sample_frac(s, n_or_frac, with_replacement)
    end
  end

  @spec is_duplicated(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def is_duplicated({:ok, s}), do: is_duplicated(s)
  defdelegate is_duplicated(s), to: Native, as: :s_is_duplicated

  @spec explode(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def explode({:ok, s}), do: explode(s)
  defdelegate explode(s), to: Native, as: :s_explode

  @spec take_every(s() | {:ok, s()}, integer()) :: {:ok, s()} | {:error, term}
  def take_every({:ok, s}, n), do: take_every(s, n)
  defdelegate take_every(s, n), to: Native, as: :s_take_every

  @spec series_equal(s() | {:ok, s()}, s() | {:ok, s()}, boolean()) :: {:ok, s()} | {:error, term}
  def series_equal(s, other, null_equal \\ false)
  def series_equal({:ok, s}, {:ok, other}, null_equal), do: series_equal(s, other, null_equal)
  def series_equal(s, {:ok, other}, null_equal), do: series_equal(s, other, null_equal)
  def series_equal({:ok, s}, other, null_equal), do: series_equal(s, other, null_equal)
  defdelegate series_equal(s, other, null_equal), to: Native, as: :s_series_equal

  @spec eq(s() | {:ok, s()}, s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def eq({:ok, s}, {:ok, rhs}), do: eq(s, rhs)
  def eq(s, {:ok, rhs}), do: eq(s, rhs)
  def eq({:ok, s}, rhs), do: eq(s, rhs)
  defdelegate eq(s, rhs), to: Native, as: :s_eq

  @spec neq(s() | {:ok, s()}, s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def neq({:ok, s}, {:ok, rhs}), do: neq(s, rhs)
  def neq(s, {:ok, rhs}), do: neq(s, rhs)
  def neq({:ok, s}, rhs), do: neq(s, rhs)
  defdelegate neq(s, rhs), to: Native, as: :s_neq

  @spec gt(s() | {:ok, s()}, s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def gt({:ok, s}, {:ok, rhs}), do: gt(s, rhs)
  def gt(s, {:ok, rhs}), do: gt(s, rhs)
  def gt({:ok, s}, rhs), do: gt(s, rhs)
  defdelegate gt(s, rhs), to: Native, as: :s_gt

  @spec gt_eq(s() | {:ok, s()}, s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def gt_eq({:ok, s}, {:ok, rhs}), do: gt_eq(s, rhs)
  def gt_eq(s, {:ok, rhs}), do: gt_eq(s, rhs)
  def gt_eq({:ok, s}, rhs), do: gt_eq(s, rhs)
  defdelegate gt_eq(s, rhs), to: Native, as: :s_gt_eq

  @spec lt(s() | {:ok, s()}, s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def lt({:ok, s}, {:ok, rhs}), do: lt(s, rhs)
  def lt(s, {:ok, rhs}), do: lt(s, rhs)
  def lt({:ok, s}, rhs), do: lt(s, rhs)
  defdelegate lt(s, rhs), to: Native, as: :s_lt

  @spec lt_eq(s() | {:ok, s()}, s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def lt_eq({:ok, s}, {:ok, rhs}), do: lt_eq(s, rhs)
  def lt_eq(s, {:ok, rhs}), do: lt_eq(s, rhs)
  def lt_eq({:ok, s}, rhs), do: lt_eq(s, rhs)
  defdelegate lt_eq(s, rhs), to: Native, as: :s_lt_eq

  @spec not_(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def not_({:ok, s}), do: not_(s)
  defdelegate not_(s), to: Native, as: :s_not

  @spec len(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def len({:ok, s}), do: len(s)
  defdelegate len(s), to: Native, as: :s_len

  @spec drop_nulls(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def drop_nulls({:ok, s}), do: drop_nulls(s)
  defdelegate drop_nulls(s), to: Native, as: :s_drop_nulls

  @spec fill_none(s() | {:ok, s()}, String.t()) :: {:ok, s()} | {:error, term}
  def fill_none({:ok, s}, strategy), do: fill_none(s, strategy)
  defdelegate fill_none(s, strategy), to: Native, as: :s_fill_none

  @spec clone(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def clone({:ok, s}), do: clone(s)
  defdelegate clone(s), to: Native, as: :s_clone

  @spec shift(s() | {:ok, s()}, integer()) :: {:ok, s()} | {:error, term}
  def shift({:ok, s}, periods), do: shift(s, periods)
  defdelegate shift(s, periods), to: Native, as: :s_shift

  @spec zip_with(s() | {:ok, s()}, s() | {:ok, s()}, s() | {:ok, s()}) ::
          {:ok, s()} | {:error, term}
  def zip_with({:ok, s}, {:ok, mask}, {:ok, other}), do: zip_with(s, mask, other)
  def zip_with(s, {:ok, mask}, {:ok, other}), do: zip_with(s, mask, other)
  def zip_with({:ok, s}, mask, {:ok, other}), do: zip_with(s, mask, other)
  def zip_with({:ok, s}, {:ok, mask}, other), do: zip_with(s, mask, other)
  def zip_with(s, mask, {:ok, other}), do: zip_with(s, mask, other)
  def zip_with(s, {:ok, mask}, other), do: zip_with(s, mask, other)
  def zip_with({:ok, s}, mask, other), do: zip_with(s, mask, other)
  defdelegate zip_with(s, mask, other), to: Native, as: :s_zip_with

  @spec str_lengths(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def str_lengths({:ok, s}), do: str_lengths(s)
  defdelegate str_lengths(s), to: Native, as: :s_str_lengths

  @spec str_contains(s() | {:ok, s()}, String.t()) :: {:ok, s()} | {:error, term}
  def str_contains({:ok, s}, pat), do: str_contains(s, pat)
  defdelegate str_contains(s, pat), to: Native, as: :s_str_contains

  @spec str_replace(s() | {:ok, s()}, String.t(), String.t()) :: {:ok, s()} | {:error, term}
  def str_replace({:ok, s}, pat, val), do: str_replace(s, pat, val)
  defdelegate str_replace(s, pat, val), to: Native, as: :s_str_replace

  @spec str_replace_all(s() | {:ok, s()}, String.t(), String.t()) :: {:ok, s()} | {:error, term}
  def str_replace_all({:ok, s}, pat, val), do: str_replace_all(s, pat, val)
  defdelegate str_replace_all(s, pat, val), to: Native, as: :s_str_replace_all

  @spec str_to_uppercase(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def str_to_uppercase({:ok, s}), do: str_to_uppercase(s)
  defdelegate str_to_uppercase(s), to: Native, as: :s_str_to_uppercase

  @spec str_to_lowercase(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def str_to_lowercase({:ok, s}), do: str_to_lowercase(s)
  defdelegate str_to_lowercase(s), to: Native, as: :s_str_to_lowercase

  @spec str_parse_date32(s() | {:ok, s()}, String.t()) :: {:ok, s()} | {:error, term}
  def str_parse_date32({:ok, s}, fmt), do: str_parse_date32(s, fmt)
  defdelegate str_parse_date32(s, fmt), to: Native, as: :s_str_parse_date32

  @spec str_parse_date64(s() | {:ok, s()}, String.t()) :: {:ok, s()} | {:error, term}
  def str_parse_date64({:ok, s}, fmt), do: str_parse_date64(s, fmt)
  defdelegate str_parse_date64(s, fmt), to: Native, as: :s_str_parse_date64

  @spec datetime_str_fmt(s() | {:ok, s()}, String.t()) :: {:ok, s()} | {:error, term}
  def datetime_str_fmt({:ok, s}, fmt), do: datetime_str_fmt(s, fmt)
  defdelegate datetime_str_fmt(s, fmt), to: Native, as: :s_datetime_str_fmt

  @spec as_duration(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def as_duration({:ok, s}), do: as_duration(s)
  defdelegate as_duration(s), to: Native, as: :s_as_duration

  @spec to_dummies(s() | {:ok, s()}) :: {:ok, t()} | {:error, term}
  def to_dummies({:ok, s}), do: to_dummies(s)
  defdelegate to_dummies(s), to: Native, as: :s_to_dummies

  @spec get_list(s() | {:ok, s()}, integer()) :: s() | {:ok, s()} | nil
  def get_list({:ok, s}, index), do: get_list(s, index)
  defdelegate get_list(s, index), to: Native, as: :s_get_list

  @spec rolling_sum(s() | {:ok, s()}, integer(), nil | list(float()), boolean()) ::
          {:ok, s()} | {:error, term}
  def rolling_sum(s, window_size, weight \\ nil, ignore_null \\ false)

  def rolling_sum({:ok, s}, window_size, weight, ignore_null),
    do: rolling_sum(s, window_size, weight, ignore_null)

  defdelegate rolling_sum(s, window_size, weight, ignore_null), to: Native, as: :s_rolling_sum

  @spec rolling_mean(s() | {:ok, s()}, integer(), nil | list(float()), boolean()) ::
          {:ok, s()} | {:error, term}
  def rolling_mean(s, window_size, weight \\ nil, ignore_null \\ false)

  def rolling_mean({:ok, s}, window_size, weight, ignore_null),
    do: rolling_mean(s, window_size, weight, ignore_null)

  defdelegate rolling_mean(s, window_size, weight, ignore_null), to: Native, as: :s_rolling_mean

  @spec rolling_max(s() | {:ok, s()}, integer(), nil | list(float()), boolean()) ::
          {:ok, s()} | {:error, term}
  def rolling_max(s, window_size, weight \\ nil, ignore_null \\ false)

  def rolling_max({:ok, s}, window_size, weight, ignore_null),
    do: rolling_max(s, window_size, weight, ignore_null)

  defdelegate rolling_max(s, window_size, weight, ignore_null), to: Native, as: :s_rolling_max

  @spec rolling_min(s() | {:ok, s()}, integer(), nil | list(float()), boolean()) ::
          {:ok, s()} | {:error, term}
  def rolling_min(s, window_size, weight \\ nil, ignore_null \\ false)

  def rolling_min({:ok, s}, window_size, weight, ignore_null),
    do: rolling_min(s, window_size, weight, ignore_null)

  defdelegate rolling_min(s, window_size, weight, ignore_null), to: Native, as: :s_rolling_min

  @spec year(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def year({:ok, s}), do: year(s)
  defdelegate year(s), to: Native, as: :s_year

  @spec month(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def month({:ok, s}), do: month(s)
  defdelegate month(s), to: Native, as: :s_month

  @spec day(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def day({:ok, s}), do: day(s)
  defdelegate day(s), to: Native, as: :s_day

  @spec ordinal_day(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def ordinal_day({:ok, s}), do: ordinal_day(s)
  defdelegate ordinal_day(s), to: Native, as: :s_ordinal_day

  @spec hour(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def hour({:ok, s}), do: hour(s)
  defdelegate hour(s), to: Native, as: :s_hour

  @spec minute(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def minute({:ok, s}), do: minute(s)
  defdelegate minute(s), to: Native, as: :s_minute

  @spec second(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def second({:ok, s}), do: second(s)
  defdelegate second(s), to: Native, as: :s_second

  @spec nanosecond(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def nanosecond({:ok, s}), do: nanosecond(s)
  defdelegate nanosecond(s), to: Native, as: :s_nanosecond

  @spec set(s() | {:ok, s()}, s() | {:ok, s()}, float() | integer()) ::
          {:ok, s()} | {:error, term}
  @doc """
  Set masked values.

  Parameters
  ----------
  filter
      Boolean mask
  value
      Value to replace the the masked values with.
  """
  def set({:ok, s}, {:ok, filter}, value), do: set(s, filter, value)
  def set(s, {:ok, filter}, value), do: set(s, filter, value)
  def set({:ok, s}, filter, value), do: set(s, filter, value)

  def set(s, filter, value) do
    t = dtype_str(s)
    apply(Native, :"s_set_with_mask_#{t}", [s, filter, value])
  end

  @spec get(s() | {:ok, s()}, float() | integer()) :: {:ok, s()} | {:error, term}
  def get({:ok, s}, index), do: get(s, index)

  def get(s, index) do
    t = dtype_str(s)
    apply(Native, :"s_get_#{t}", [s, index])
  end

  @spec cast(s() | {:ok, s()}, :integer | :float | :str) :: {:ok, s()} | {:error, term}
  def cast({:ok, s}, data_type), do: cast(s, data_type)

  def cast(s, data_type) do
    f =
      cond do
        data_type == :integer -> :s_cast_i64
        data_type == :float -> :s_cast_f64
        data_type == :str -> :s_cast_str
        true -> :s_cast_str
      end

    apply(Native, f, [s])
  end

  @spec (s() | {:ok, s()}) + (s() | {:ok, s()}) :: {:ok, s()} | {:error, term()}
  def {:ok, s} + {:ok, other}, do: s + other
  def s + {:ok, other}, do: s + other
  def {:ok, s} + other, do: s + other

  def s + other when is_struct(s) and is_struct(other) do
    add(s, other)
  end

  def s + other when is_struct(s) do
    t = dtype_str(s)
    apply(Native, :"s_add_#{t}", [s, other])
  end

  def _s + _other, do: {:error, "Not supported"}

  @spec (s() | {:ok, s()}) - (s() | {:ok, s()}) :: {:ok, s()} | {:error, term()}
  def {:ok, s} - {:ok, other}, do: s - other
  def s - {:ok, other}, do: s - other
  def {:ok, s} - other, do: s - other

  def s - other when is_struct(s) and is_struct(other) do
    sub(s, other)
  end

  def s - other when is_struct(s) do
    t = dtype_str(s)
    apply(Native, :"s_sub_#{t}", [s, other])
  end

  def _s - _other, do: {:error, "Not supported"}

  @spec (s() | {:ok, s()}) * (s() | {:ok, s()}) :: {:ok, s()} | {:error, term()}
  def {:ok, s} * {:ok, other}, do: s * other
  def s * {:ok, other}, do: s * other
  def {:ok, s} * other, do: s * other

  def s * other when is_struct(s) and is_struct(other) do
    mul(s, other)
  end

  def s * other when is_struct(s) do
    t = dtype_str(s)
    apply(Native, :"s_mul_#{t}", [s, other])
  end

  def _s * _other, do: {:error, "Not supported"}

  @spec (s() | {:ok, s()}) / (s() | {:ok, s()}) :: {:ok, s()} | {:error, term()}
  def {:ok, s} / {:ok, other}, do: s / other
  def s / {:ok, other}, do: s / other
  def {:ok, s} / other, do: s / other

  def s / other when is_struct(s) and is_struct(other) do
    divide(s, other)
  end

  def s / other when is_struct(s) do
    t = dtype_str(s)
    apply(Native, :"s_div_#{t}", [s, other])
  end

  def _s / _other, do: {:error, "Not supported"}

  @spec (s() | {:ok, s()}) == (s() | {:ok, s()}) :: {:ok, s()} | {:error, term()}
  def {:ok, s} == {:ok, other}, do: s == other
  def {:ok, s} == other, do: s == other
  def s == {:ok, other}, do: s == other

  def s == other when is_struct(s) and is_struct(other) do
    eq(s, other)
  end

  def s == other when is_struct(s) do
    t = dtype_str(s)
    apply(Native, :"s_eq_#{t}", [s, other])
  end

  def _s == _other, do: {:error, "Not supported"}

  @spec (s() | {:ok, s()}) <> (s() | {:ok, s()}) :: {:ok, s()} | {:error, term()}
  def {:ok, s} <> {:ok, other}, do: s <> other
  def {:ok, s} <> other, do: s <> other
  def s <> {:ok, other}, do: s <> other

  def s <> other when is_struct(s) and is_struct(other) do
    neq(s, other)
  end

  def s <> other when is_struct(s) do
    t = dtype_str(s)
    apply(Native, :"s_neq_#{t}", [s, other])
  end

  def _s <> _other, do: {:error, "Not supported"}

  @spec (s() | {:ok, s()}) > (s() | {:ok, s()}) :: {:ok, s()} | {:error, term()}
  def {:ok, s} > {:ok, other}, do: s > other
  def {:ok, s} > other, do: s > other
  def s > {:ok, other}, do: s > other

  def s > other when is_struct(s) and is_struct(other) do
    gt(s, other)
  end

  def s > other when is_struct(s) do
    t = dtype_str(s)
    apply(Native, :"s_gt_#{t}", [s, other])
  end

  def _s > _other, do: {:error, "Not supported"}

  @spec (s() | {:ok, s()}) >= (s() | {:ok, s()}) :: {:ok, s()} | {:error, term()}
  def {:ok, s} >= {:ok, other}, do: s >= other
  def {:ok, s} >= other, do: s >= other
  def s >= {:ok, other}, do: s >= other

  def s >= other when is_struct(s) and is_struct(other) do
    gt_eq(s, other)
  end

  def s >= other when is_struct(s) do
    t = dtype_str(s)
    apply(Native, :"s_gt_eq_#{t}", [s, other])
  end

  def _s >= _other, do: {:error, "Not supported"}

  @spec (s() | {:ok, s()}) < (s() | {:ok, s()}) :: {:ok, s()} | {:error, term()}
  def {:ok, s} < {:ok, other}, do: s < other
  def {:ok, s} < other, do: s < other
  def s < {:ok, other}, do: s < other

  def s < other when is_struct(s) and is_struct(other) do
    lt(s, other)
  end

  def s < other when is_struct(s) do
    t = dtype_str(s)
    apply(Native, :"s_lt_#{t}", [s, other])
  end

  def _s < _other, do: {:error, "Not supported"}

  @spec (s() | {:ok, s()}) <= (s() | {:ok, s()}) :: {:ok, s()} | {:error, term()}
  def {:ok, s} <= {:ok, other}, do: s <= other
  def {:ok, s} <= other, do: s <= other
  def s <= {:ok, other}, do: s <= other

  def s <= other when is_struct(s) and is_struct(other) do
    lt_eq(s, other)
  end

  def s <= other when is_struct(s) do
    t = dtype_str(s)
    apply(Native, :"s_lt_eq_#{t}", [s, other])
  end

  def _s <= _other, do: {:error, "Not supported"}

  @spec sum(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  @doc """
  Reduce this Series to the sum value.
  """
  def sum({:ok, s}), do: sum(s)

  def sum(s) do
    t = dtype_str(s)

    t =
      case t do
        "bool" -> "u32"
        "u8" -> "u64"
        "i8" -> "i64"
        _ -> t
      end

    apply(Native, :"s_sum_#{t}", [s])
  end

  @spec mean(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def mean({:ok, s}), do: mean(s)

  def mean(s) do
    t = dtype_str(s)
    apply(Native, :"s_mean_#{t}", [s])
  end

  @spec min(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def min({:ok, s}), do: min(s)

  def min(s) do
    t = dtype_str(s)
    apply(Native, :"s_min_#{t}", [s])
  end

  @spec max(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def max({:ok, s}), do: max(s)

  def max(s) do
    t = dtype_str(s)
    apply(Native, :"s_max_#{t}", [s])
  end

  @spec std(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def std({:ok, s}), do: std(s)

  def std(_s) do
    {:error, "Not implemented"}
  end

  @spec var(s() | {:ok, s()}) :: {:ok, s()} | {:error, term}
  def var({:ok, s}), do: var(s)

  def var(_s) do
    {:error, "Not implemented"}
  end
end

defimpl Inspect, for: ExPolars.Series do
  alias ExPolars.Native

  def inspect(data, _opts) do
    case Native.s_as_str(data) do
      {:ok, s} -> s
      _ -> "Cannot output series"
    end
  end
end
