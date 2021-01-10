defmodule ExPolars.DataFrame do
  alias ExPolars.Native

  @type t :: ExPolars.DataFrame
  @type s :: ExPolars.Series

  defstruct [:inner]

  @spec read_csv(
          String.t(),
          integer(),
          integer(),
          boolean(),
          boolean(),
          integer() | nil,
          integer(),
          list(integer()) | nil,
          String.t(),
          boolean(),
          list(String.t()) | nil,
          String.t()
        ) :: {:ok, t()} | {:error, term()}
  defdelegate read_csv(
                filename,
                infer_schema_length \\ 100,
                batch_size \\ 64,
                has_header \\ true,
                ignore_errors \\ false,
                stop_after_n_rows \\ nil,
                skip_rows \\ 0,
                projection \\ nil,
                sep \\ ",",
                rechunk \\ true,
                columns \\ nil,
                encoding \\ "utf8"
              ),
              to: Native,
              as: :df_read_csv

  @spec read_parquet(String.t()) :: {:ok, t()} | {:error, term()}
  defdelegate read_parquet(filename), to: Native, as: :df_read_parquet

  @spec read_json(String.t(), boolean()) :: {:ok, t()} | {:error, term()}
  defdelegate read_json(filename, line_delimited_json \\ false), to: Native, as: :df_read_json

  @spec to_csv(t() | {:ok, t()}, integer(), boolean(), integer()) ::
          {:ok, String.t()} | {:error, term()}
  defdelegate to_csv(
                df,
                batch_size \\ 100_000,
                has_headers \\ true,
                delimiter \\ ?,
              ),
              to: Native,
              as: :df_to_csv

  @spec to_csv_file(t() | {:ok, t()}, String.t(), integer(), boolean(), integer()) ::
          :ok | {:error, term()}
  defdelegate to_csv_file(
                df,
                filename,
                batch_size \\ 100_000,
                has_headers \\ true,
                delimiter \\ ?,
              ),
              to: Native,
              as: :df_to_csv_file

  # defdelegate as_str(df), to: Native, as: :df_as_str

  @spec add(t() | {:ok, t()}, s() | {:ok, s()}) :: {:ok, t()} | {:error, term()}
  def add(df, {:ok, s}), do: add(df, s)
  def add({:ok, df}, {:ok, s}), do: add(df, s)
  def add({:ok, df}, s), do: add(df, s)
  defdelegate add(df, s), to: Native, as: :df_add

  @spec sub(t() | {:ok, t()}, s() | {:ok, s()}) :: {:ok, t()} | {:error, term()}
  def sub(df, {:ok, s}), do: sub(df, s)
  def sub({:ok, df}, {:ok, s}), do: sub(df, s)
  def sub({:ok, df}, s), do: sub(df, s)
  defdelegate sub(df, s), to: Native, as: :df_sub

  @spec divide(t() | {:ok, t()}, s() | {:ok, s()}) :: {:ok, t()} | {:error, term()}
  def divide(df, {:ok, s}), do: divide(df, s)
  def divide({:ok, df}, {:ok, s}), do: divide(df, s)
  def divide({:ok, df}, s), do: divide(df, s)
  defdelegate divide(df, s), to: Native, as: :df_div

  @spec mul(t() | {:ok, t()}, s() | {:ok, s()}) :: {:ok, t()} | {:error, term()}
  def mul(df, {:ok, s}), do: mul(df, s)
  def mul({:ok, df}, {:ok, s}), do: mul(df, s)
  def mul({:ok, df}, s), do: mul(df, s)
  defdelegate mul(df, s), to: Native, as: :df_mul

  @spec remainder(t() | {:ok, t()}, s() | {:ok, s()}) :: {:ok, t()} | {:error, term()}
  def remainder({:ok, df}, {:ok, s}), do: remainder(df, s)
  def remainder(df, {:ok, s}), do: remainder(df, s)
  def remainder({:ok, df}, s), do: remainder(df, s)
  defdelegate remainder(df, s), to: Native, as: :df_rem

  @spec rechunk(t() | {:ok, t()}) :: :ok | {:error, term()}
  def rechunk({:ok, df}), do: rechunk(df)
  defdelegate rechunk(df), to: Native, as: :df_rechunk

  @spec fill_none(t() | {:ok, t()}, String.t()) :: {:ok, t()} | {:error, term()}
  def fill_none({:ok, df}, strategy), do: fill_none(df, strategy)
  defdelegate fill_none(df, strategy), to: Native, as: :df_fill_none

  @spec join(t() | {:ok, t()}, t() | {:ok, t()}, list(String.t()), list(String.t()), String.t()) ::
          {:ok, t()} | {:error, term()}
  def join({:ok, df}, other, left_on, right_on, how), do: join(df, other, left_on, right_on, how)
  defdelegate join(df, other, left_on, right_on, how), to: Native, as: :df_join

  @spec get_columns(t() | {:ok, t()}) :: {:ok, list(s() | {:ok, s()})} | {:error, term()}
  def get_columns({:ok, df}), do: get_columns(df)
  defdelegate get_columns(df), to: Native, as: :df_get_columns

  @spec columns(t() | {:ok, t()}) :: {:ok, list(String.t())} | {:error, term()}
  def columns({:ok, df}), do: columns(df)
  defdelegate columns(def), to: Native, as: :df_columns

  @spec set_column_names(t() | {:ok, t()}, list(String.t())) :: :ok | {:error, term()}
  def set_column_names({:ok, df}, names), do: set_column_names(df, names)
  defdelegate set_column_names(df, names), to: Native, as: :df_set_column_names

  @spec dtypes(t() | {:ok, t()}) :: {:ok, list(integer())} | {:error, term()}
  def dtypes({:ok, df}), do: dtypes(df)
  defdelegate dtypes(df), to: Native, as: :df_dtypes

  @spec n_chunks(t() | {:ok, t()}) :: {:ok, integer()} | {:error, term()}
  defdelegate n_chunks(df), to: Native, as: :df_n_chunks

  @spec shape(t() | {:ok, t()}) :: {:ok, {integer(), integer()}} | {:error, term()}
  def shape({:ok, df}), do: shape(df)
  defdelegate shape(df), to: Native, as: :df_shape

  @spec height(t() | {:ok, t()}) :: {:ok, integer()} | {:error, term()}
  def height({:ok, df}), do: height(df)
  defdelegate height(df), to: Native, as: :df_height

  @spec width(t() | {:ok, t()}) :: {:ok, integer()} | {:error, term()}
  def width({:ok, df}), do: width(df)
  defdelegate width(df), to: Native, as: :df_width

  @spec hstack_mut(t() | {:ok, t()}, list(s() | {:ok, s()})) :: :ok | {:error, term()}
  def hstack_mut({:ok, df}, cols), do: hstack_mut(df, cols)
  defdelegate hstack_mut(df, cols), to: Native, as: :df_hstack_mut

  @spec hstack(t() | {:ok, t()}, list(s() | {:ok, s()})) :: {:ok, t()} | {:error, term()}
  def hstack({:ok, df}, cols), do: hstack(df, cols)
  defdelegate hstack(df, cols), to: Native, as: :df_hstack

  @spec vstack(t() | {:ok, t()}, t() | {:ok, t()}) :: :ok | {:error, term()}
  def vstack({:ok, df}, other), do: vstack(df, other)
  defdelegate vstack(df, other), to: Native, as: :df_vstack

  @spec drop_in_place(t() | {:ok, t()}, String.t()) :: {:ok, s()} | {:error, term()}
  def drop_in_place({:ok, df}, name), do: drop_in_place(df, name)
  defdelegate drop_in_place(df, name), to: Native, as: :df_drop_in_place

  @spec drop_nulls(t() | {:ok, t()}, list(String.t()) | nil) :: {:ok, t()} | {:error, term()}
  def drop_nulls({:ok, df}, subset), do: drop_nulls(df, subset)
  defdelegate drop_nulls(df, subset), to: Native, as: :df_drop_nulls

  @spec drop(t() | {:ok, t()}, String.t()) :: {:ok, t()} | {:error, term()}
  def drop({:ok, df}, name), do: drop(df, name)
  defdelegate drop(df, name), to: Native, as: :df_drop

  @spec select_at_idx(t() | {:ok, t()}, integer()) :: {:ok, s() | nil} | {:error, term()}
  def select_at_idx({:ok, df}, idx), do: select_at_idx(df, idx)
  defdelegate select_at_idx(df, idx), to: Native, as: :df_select_at_idx

  @spec find_idx_by_name(t() | {:ok, t()}, String.t()) ::
          {:ok, integer() | nil} | {:error, term()}
  def find_idx_by_name({:ok, df}, name), do: find_idx_by_name(df, name)
  defdelegate find_idx_by_name(df, name), to: Native, as: :df_find_idx_by_name

  @spec column(t() | {:ok, t()}, String.t()) :: {:ok, s() | nil} | {:error, term()}
  def column({:ok, df}, name), do: column(df, name)
  defdelegate column(df, name), to: Native, as: :df_column

  @spec select(t() | {:ok, t()}, list(String.t())) :: {:ok, t()} | {:error, term()}
  def select({:ok, df}, selection), do: select(df, selection)
  defdelegate select(df, selection), to: Native, as: :df_select

  @spec filter(t() | {:ok, t()}, s() | {:ok, s()}) :: {:ok, t()} | {:error, term()}
  def filter({:ok, df}, {:ok, mask}), do: filter(df, mask)
  def filter(df, {:ok, mask}), do: filter(df, mask)
  def filter({:ok, df}, mask), do: filter(df, mask)
  defdelegate filter(df, mask), to: Native, as: :df_filter

  @spec take(t() | {:ok, t()}, list(integer())) :: {:ok, t()} | {:error, term()}
  def take({:ok, df}, indices), do: take(df, indices)
  defdelegate take(df, indices), to: Native, as: :df_take

  @spec take_with_series(t() | {:ok, t()}, s() | {:ok, s()}) :: {:ok, t()} | {:error, term()}
  def take_with_series({:ok, df}, {:ok, indices}), do: take_with_series(df, indices)
  def take_with_series(df, {:ok, indices}), do: take_with_series(df, indices)
  def take_with_series({:ok, df}, indices), do: take_with_series(df, indices)
  defdelegate take_with_series(df, indices), to: Native, as: :df_take_with_series

  @spec replace(t() | {:ok, t()}, String.t(), s() | {:ok, s()}) :: :ok | {:error, term()}
  def replace({:ok, df}, col, {:ok, new_col}), do: replace(df, col, new_col)
  def replace(df, col, {:ok, new_col}), do: replace(df, col, new_col)
  def replace({:ok, df}, col, new_col), do: replace(df, col, new_col)
  defdelegate replace(df, col, new_col), to: Native, as: :df_replace

  @spec replace_at_idx(t() | {:ok, t()}, integer(), s() | {:ok, s()}) :: :ok | {:error, term()}
  def replace_at_idx({:ok, df}, index, {:ok, new_col}), do: replace_at_idx(df, index, new_col)
  def replace_at_idx(df, index, {:ok, new_col}), do: replace_at_idx(df, index, new_col)
  def replace_at_idx({:ok, df}, index, new_col), do: replace_at_idx(df, index, new_col)
  defdelegate replace_at_idx(df, index, new_col), to: Native, as: :df_replace_at_idx

  @spec insert_at_idx(t() | {:ok, t()}, integer(), s() | {:ok, s()}) :: :ok | {:error, term()}
  def insert_at_idx({:ok, df}, index, {:ok, new_col}), do: insert_at_idx(df, index, new_col)
  def insert_at_idx(df, index, {:ok, new_col}), do: insert_at_idx(df, index, new_col)
  def insert_at_idx({:ok, df}, index, new_col), do: insert_at_idx(df, index, new_col)
  defdelegate insert_at_idx(df, index, new_col), to: Native, as: :df_insert_at_idx

  @spec slice(t() | {:ok, t()}, integer(), integer()) :: {:ok, t()} | {:error, term()}
  def slice({:ok, df}, offset, length), do: slice(df, offset, length)
  defdelegate slice(df, offset, length), to: Native, as: :df_slice

  @spec head(t() | {:ok, t()}, integer()) :: {:ok, t()} | {:error, term()}
  def head(df, length \\ 5)
  def head({:ok, df}, length), do: head(df, length)
  defdelegate head(df, length), to: Native, as: :df_head

  @spec tail(t() | {:ok, t()}, integer()) :: {:ok, t()} | {:error, term()}
  def tail(df, length \\ 5)
  def tail({:ok, df}, length), do: tail(df, length)
  defdelegate tail(df, length), to: Native, as: :df_tail

  @spec is_unique(t() | {:ok, t()}) :: {:ok, s()} | {:error, term()}
  @doc """
  Get a mask of all unique rows in this DataFrame
  """
  def is_unique({:ok, df}), do: is_unique(df)
  defdelegate is_unique(df), to: Native, as: :df_is_unique

  @spec is_duplicated(t() | {:ok, t()}) :: {:ok, s()} | {:error, term()}
  @doc """
  Get a mask of all duplicated rows in this DataFrame
  """
  def is_duplicated({:ok, df}), do: is_duplicated(df)
  defdelegate is_duplicated(df), to: Native, as: :df_is_duplicated

  @spec equal(t() | {:ok, t()}, t() | {:ok, t()}, boolean()) ::
          {:ok, boolean()} | {:error, term()}
  @doc """
  Check if DataFrame is equal to other.

  Parameters
  ----------
  df: DataFrame
  other: DataFrame to compare with.
  null_equal: Consider null values as equal.
  """
  def equal(df, other, null_equal \\ false)
  def equal({:ok, df}, {:ok, other}, null_equal), do: equal(df, other, null_equal)
  def equal(df, {:ok, other}, null_equal), do: equal(df, other, null_equal)
  def equal({:ok, df}, other, null_equal), do: equal(df, other, null_equal)
  defdelegate equal(df, other, null_equal), to: Native, as: :df_frame_equal

  @spec groupby(t() | {:ok, t()}, list(String.t()), list(String.t()) | nil, String.t()) ::
          {:ok, t()} | {:error, term()}
  def groupby({:ok, df}, by, sel, agg), do: groupby(df, by, sel, agg)
  defdelegate groupby(df, by, sel, agg), to: Native, as: :df_groupby

  @spec groupby_agg(
          t() | {:ok, t()},
          list(String.t()),
          %{String.t() => list(String.t())} | list({String.t(), list(String.t())})
        ) ::
          {:ok, t()} | {:error, term()}
  @doc """
  Use multiple aggregations on columns

  Parameters
  ----------
  column_to_agg
      map column to aggregation functions

      Examples:
          [{"foo", ["sum", "n_unique", "min"]},
           {"bar": ["max"]}]

          {"foo": ["sum", "n_unique", "min"],
          "bar": "max"}

  Returns
  -------
  Result of groupby split apply operations.
  """
  def groupby_agg({:ok, df}, by, column_to_agg), do: groupby_agg(df, by, column_to_agg)

  def groupby_agg(df, by, column_to_agg) when is_map(column_to_agg),
    do: groupby_agg(df, by, Enum.into(column_to_agg, []))

  def groupby_agg(df, by, column_to_agg) when is_list(column_to_agg),
    do: Native.df_groupby_agg(df, by, column_to_agg)

  @spec groupby_quantile(t() | {:ok, t()}, list(String.t()), list(String.t()), float()) ::
          {:ok, t()} | {:error, term()}
  @doc """
  Count the unique values per group.
  """
  def groupby_quantile({:ok, df}, by, sel, quant), do: groupby_quantile(df, by, sel, quant)
  defdelegate groupby_quantile(df, by, sel, quant), to: Native, as: :df_groupby_quantile

  @spec pivot(t() | {:ok, t()}, list(String.t()), String.t(), String.t(), String.t()) ::
          {:ok, t()} | {:error, term()}
  @doc """
  Do a pivot operation based on the group key, a pivot column and an aggregation function on the values column.

  Parameters
  ----------
  pivot_column
      Column to pivot.
  values_column
      Column that will be aggregated
  """
  def pivot({:ok, df}, by, pivot_column, values_column, agg),
    do: pivot(df, by, pivot_column, values_column, agg)

  defdelegate pivot(df, by, pivot_column, values_column, agg), to: Native, as: :df_pivot

  @spec clone(t() | {:ok, t()}) :: {:ok, t()} | {:error, term()}
  def clone({:ok, df}), do: clone(df)
  defdelegate clone(df), to: Native, as: :df_clone

  @spec explode(t() | {:ok, t()}, list(String.t())) :: {:ok, t()} | {:error, term()}
  @doc """
  Explode `DataFrame` to long format by exploding a column with Lists.

  Parameters
  ----------
  columns
      Column of LargeList type

  Returns
  -------
  DataFrame
  """
  def explode({:ok, df}, cols), do: explode(df, cols)
  defdelegate explode(df, cols), to: Native, as: :df_explode

  @spec melt(t() | {:ok, t()}, list(String.t()), list(String.t())) ::
          {:ok, t()} | {:error, term()}
  @doc """
  Unpivot DataFrame to long format.

  Parameters
  ----------
  id_vars
      Columns to use as identifier variables

  value_vars
      Values to use as identifier variables

  Returns
  -------

  """
  def melt({:ok, df}, id_vars, value_vars), do: melt(df, id_vars, value_vars)
  defdelegate melt(df, id_vars, value_vars), to: Native, as: :df_melt

  @spec shift(t() | {:ok, t()}, integer()) :: {:ok, t()} | {:error, term()}
  @doc """
  Shift the values by a given period and fill the parts that will be empty due to this operation
  with `Nones`.

  Parameters
  ----------
  periods
      Number of places to shift (may be negative).
  """
  def shift({:ok, df}, periods), do: shift(df, periods)
  defdelegate shift(df, periods), to: Native, as: :df_shift

  @spec drop_duplicates(t() | {:ok, t()}, boolean(), list(String.t()) | nil) ::
          {:ok, t()} | {:error, term()}
  @doc """
  Drop duplicate rows from this DataFrame.
  Note that this fails if there is a column of type `List` in the DataFrame.
  """
  def drop_duplicates(df, maintain_order \\ true, subset \\ nil)

  def drop_duplicates({:ok, df}, maintain_order, subset),
    do: drop_duplicates(df, maintain_order, subset)

  defdelegate drop_duplicates(df, maintain_order, subset),
    to: Native,
    as: :df_drop_duplicates

  @spec max(t() | {:ok, t()}) :: {:ok, t()} | {:error, term()}
  def max({:ok, df}), do: max(df)
  defdelegate max(df), to: Native, as: :df_max

  @spec min(t() | {:ok, t()}) :: {:ok, t()} | {:error, term()}
  def min({:ok, df}), do: min(df)
  defdelegate min(df), to: Native, as: :df_min

  @spec sum(t() | {:ok, t()}) :: {:ok, t()} | {:error, term()}
  def sum({:ok, df}), do: sum(df)
  defdelegate sum(df), to: Native, as: :df_sum

  @spec mean(t() | {:ok, t()}) :: {:ok, t()} | {:error, term()}
  def mean({:ok, df}), do: mean(df)
  defdelegate mean(df), to: Native, as: :df_mean

  @spec std(t() | {:ok, t()}) :: {:ok, t()} | {:error, term()}
  def std({:ok, df}), do: std(df)
  defdelegate std(df), to: Native, as: :df_stdev

  @spec var(t() | {:ok, t()}) :: {:ok, t()} | {:error, term()}
  def var({:ok, df}), do: var(df)
  defdelegate var(df), to: Native, as: :df_var

  @spec median(t() | {:ok, t()}) :: {:ok, t()} | {:error, term()}
  def median({:ok, df}), do: median(df)
  defdelegate median(df), to: Native, as: :df_median

  @spec quantile(t() | {:ok, t()}, float()) :: {:ok, t()} | {:error, term()}
  def quantile({:ok, df}, quant), do: quantile(df, quant)
  defdelegate quantile(df, quant), to: Native, as: :df_quantile

  @spec to_dummies(t() | {:ok, t()}) :: {:ok, t()} | {:error, term()}
  def to_dummies({:ok, df}), do: to_dummies(df)
  defdelegate to_dummies(df), to: Native, as: :df_to_dummies

  @spec sample(t() | {:ok, t()}, integer() | float(), boolean()) :: {:ok, t()} | {:error, term()}
  def sample(df, n_or_frac, with_replacement \\ false)
  def sample({:ok, df}, n_or_frac, with_replacement), do: sample(df, n_or_frac, with_replacement)

  def sample(df, n_or_frac, with_replacement) do
    case is_integer(n_or_frac) do
      true -> Native.df_sample_n(df, n_or_frac, with_replacement)
      _ -> Native.df_sample_frac(df, n_or_frac, with_replacement)
    end
  end

  @spec sort(t() | {:ok, t()}, String.t(), boolean(), boolean()) :: {:ok, t()} | {:error, term()}
  def sort(df, by_column, inplace \\ false, reverse \\ false)
  def sort({:ok, df}, by_column, inplace, reverse), do: sort(df, by_column, inplace, reverse)

  def sort(df, by_column, inplace, reverse) do
    case inplace do
      true -> Native.df_sort_in_place(df, by_column, reverse)
      _ -> Native.df_sort_new(df, by_column, reverse)
    end
  end
end

defimpl Inspect, for: ExPolars.DataFrame do
  alias ExPolars.Native

  def inspect(data, _opts) do
    case Native.df_as_str(data) do
      {:ok, s} -> s
      _ -> "Cannot output dataframe"
    end
  end
end
