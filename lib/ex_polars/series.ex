defmodule ExPolars.Series do
  @moduledoc """
  Documentation for `Series`.
  """
  import Kernel, except: [+: 2, -: 2, *: 2, /: 2, ==: 2, <>: 2, >: 2, >=: 2, <: 2, <=: 2]

  alias ExPolars.Native
  defstruct [:inner]

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

  defdelegate new_duration_ns(name, data), to: Native, as: :s_new_duration_ns

  def to_list(s) do
    with {:ok, json} <- Native.s_to_json(s),
         {:ok, data} <- Jason.decode(json) do
      {:ok, data}
    else
      e -> e
    end
  end

  defdelegate rechunk(s), to: Native, as: :s_rechunk
  defdelegate chunk_lengths(s), to: Native, as: :s_chunk_lengths
  defdelegate name(s), to: Native, as: :s_name
  defdelegate rename(s, name), to: Native, as: :s_rename
  defdelegate dtype(s), to: Native, as: :s_dtype

  def dtype_str(s) do
    {:ok, t} = dtype(s)
    @dtype_strs[t]
  end

  defdelegate n_chunks(s), to: Native, as: :s_n_chunks
  defdelegate limit(s, num_elements), to: Native, as: :s_limit
  defdelegate slice(s, offset, length), to: Native, as: :s_slice
  defdelegate append(s, other), to: Native, as: :s_append
  defdelegate filter(s, filter), to: Native, as: :s_filter
  defdelegate add(s, other), to: Native, as: :s_add
  defdelegate sub(s, other), to: Native, as: :s_sub
  defdelegate mul(s, other), to: Native, as: :s_mul
  defdelegate sdiv(s, other), to: Native, as: :s_div
  defdelegate head(s, length \\ 5), to: Native, as: :s_head
  defdelegate tail(s, length \\ 5), to: Native, as: :s_tail
  defdelegate sort(s, reverse \\ false), to: Native, as: :s_sort
  defdelegate argsort(s, reverse \\ false), to: Native, as: :s_argsort
  defdelegate unique(s), to: Native, as: :s_unique
  defdelegate value_counts(s), to: Native, as: :s_value_counts
  defdelegate arg_unique(s), to: Native, as: :s_arg_unique
  defdelegate take(s, indeces), to: Native, as: :s_take
  defdelegate take_with_series(s, indeces), to: Native, as: :s_take_with_series
  defdelegate null_count(s), to: Native, as: :s_null_count
  defdelegate is_null(s), to: Native, as: :s_is_null
  defdelegate is_not_null(s), to: Native, as: :s_is_not_null
  defdelegate is_unique(s), to: Native, as: :s_is_unique
  defdelegate arg_true(s), to: Native, as: :s_arg_true

  def sample(s, n_or_frac, with_replacement \\ false) do
    case is_integer(n_or_frac) do
      true -> Native.s_sample_n(s, n_or_frac, with_replacement)
      _ -> Native.s_sample_frac(s, n_or_frac, with_replacement)
    end
  end

  defdelegate is_duplicated(s), to: Native, as: :s_is_duplicated
  defdelegate explode(s), to: Native, as: :s_explode
  defdelegate take_every(s, n), to: Native, as: :s_take_every
  defdelegate series_equal(s, other, null_equal \\ false), to: Native, as: :s_series_equal
  defdelegate eq(s, rhs), to: Native, as: :s_eq
  defdelegate neq(s, rhs), to: Native, as: :s_neq
  defdelegate gt(s, rhs), to: Native, as: :s_gt
  defdelegate gt_eq(s, rhs), to: Native, as: :s_gt_eq
  defdelegate lt(s, rhs), to: Native, as: :s_lt
  defdelegate lt_eq(s, rhs), to: Native, as: :s_lt_eq
  defdelegate not s, to: Native, as: :s_not
  defdelegate len(s), to: Native, as: :s_len
  defdelegate drop_nulls(s), to: Native, as: :s_drop_nulls
  defdelegate fill_none(s, strategy), to: Native, as: :s_fill_none
  defdelegate clone(s), to: Native, as: :s_clone
  defdelegate shift(s, periods), to: Native, as: :s_shift
  defdelegate zip_with(s, mask, other), to: Native, as: :s_zip_with
  defdelegate str_lengths(s), to: Native, as: :s_str_lengths
  defdelegate str_contains(s, pat), to: Native, as: :s_str_contains
  defdelegate str_replace(s, pat, val), to: Native, as: :s_str_replace
  defdelegate str_replace_all(s, pat, val), to: Native, as: :s_str_replace_all
  defdelegate str_to_uppercase(s), to: Native, as: :s_str_to_uppercase
  defdelegate str_to_lowercase(s), to: Native, as: :s_str_to_lowercase
  defdelegate str_parse_date32(s, fmt), to: Native, as: :s_str_parse_date32
  defdelegate str_parse_date64(s, fmt), to: Native, as: :s_str_parse_date64
  defdelegate datetime_str_fmt(s, fmt), to: Native, as: :s_datetime_str_fmt
  defdelegate as_duration(s), to: Native, as: :s_as_duration
  defdelegate to_dummies(s), to: Native, as: :s_to_dummies
  defdelegate get_list(s, index), to: Native, as: :s_get_list

  defdelegate rolling_sum(s, window_size, weight \\ nil, ignore_null \\ false),
    to: Native,
    as: :s_rolling_sum

  defdelegate rolling_mean(s, window_size, weight \\ nil, ignore_null \\ false),
    to: Native,
    as: :s_rolling_mean

  defdelegate rolling_max(s, window_size, weight \\ nil, ignore_null \\ false),
    to: Native,
    as: :s_rolling_max

  defdelegate rolling_min(s, window_size, weight \\ nil, ignore_null \\ false),
    to: Native,
    as: :s_rolling_min

  defdelegate year(s), to: Native, as: :s_year
  defdelegate month(s), to: Native, as: :s_month
  defdelegate day(s), to: Native, as: :s_day
  defdelegate ordinal_day(s), to: Native, as: :s_ordinal_day
  defdelegate hour(s), to: Native, as: :s_hour
  defdelegate minute(s), to: Native, as: :s_minute
  defdelegate second(s), to: Native, as: :s_second
  defdelegate nanosecond(s), to: Native, as: :s_nanosecond

  @doc """
  Set masked values.

  Parameters
  ----------
  filter
      Boolean mask
  value
      Value to replace the the masked values with.
  """
  def set(s, filter, value) do
    t = dtype_str(s)
    apply(Native, :"s_set_with_mask_#{t}", [s, filter, value])
  end

  def get(s, index) do
    t = dtype_str(s)
    apply(Native, :"s_get_#{t}", [s, index])
  end

  def cast(s, data_type) do
    f =
      cond do
        data_type == :integer -> :s_cast_i64
        data_type == :float -> :s_cast_f64
        data_type == :str -> :s_cast_str
      end

    apply(Native, f, [s])
  end

  def s + other when is_map(s) and is_map(other) do
    add(s, other)
  end

  def s + other when is_map(s) do
    t = dtype_str(s)
    apply(Native, :"s_add_#{t}", [s, other])
  end

  def s - other when is_map(s) and is_map(other) do
    sub(s, other)
  end

  def s - other when is_map(s) do
    t = dtype_str(s)
    apply(Native, :"s_sub_#{t}", [s, other])
  end

  def s * other when is_map(s) and is_map(other) do
    mul(s, other)
  end

  def s * other when is_map(s) do
    t = dtype_str(s)
    apply(Native, :"s_mul_#{t}", [s, other])
  end

  def s / other when is_map(s) and is_map(other) do
    sdiv(s, other)
  end

  def s / other when is_map(s) do
    t = dtype_str(s)
    apply(Native, :"s_div_#{t}", [s, other])
  end

  def s == other when is_map(s) and is_map(other) do
    eq(s, other)
  end

  def s == other when is_map(s) do
    t = dtype_str(s)
    apply(Native, :"s_eq_#{t}", [s, other])
  end

  def s <> other when is_map(s) and is_map(other) do
    neq(s, other)
  end

  def s <> other when is_map(s) do
    t = dtype_str(s)
    apply(Native, :"s_neq_#{t}", [s, other])
  end

  def s > other when is_map(s) and is_map(other) do
    gt(s, other)
  end

  def s > other when is_map(s) do
    t = dtype_str(s)
    apply(Native, :"s_gt_#{t}", [s, other])
  end

  def s >= other when is_map(s) and is_map(other) do
    gt_eq(s, other)
  end

  def s >= other when is_map(s) do
    t = dtype_str(s)
    apply(Native, :"s_gt_eq_#{t}", [s, other])
  end

  def s < other when is_map(s) and is_map(other) do
    lt(s, other)
  end

  def s < other when is_map(s) do
    t = dtype_str(s)
    apply(Native, :"s_lt_#{t}", [s, other])
  end

  def s <= other when is_map(s) and is_map(other) do
    lt_eq(s, other)
  end

  def s <= other when is_map(s) do
    t = dtype_str(s)
    apply(Native, :"s_lt_eq_#{t}", [s, other])
  end

  @doc """
  Reduce this Series to the sum value.
  """
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

  def mean(s) do
    t = dtype_str(s)
    apply(Native, :"s_mean_#{t}", [s])
  end

  def min(s) do
    t = dtype_str(s)
    apply(Native, :"s_min_#{t}", [s])
  end

  def max(s) do
    t = dtype_str(s)
    apply(Native, :"s_max_#{t}", [s])
  end

  def std(_s) do
    raise "Not implemented"
  end

  def var(_s) do
    raise "Not implemented"
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
