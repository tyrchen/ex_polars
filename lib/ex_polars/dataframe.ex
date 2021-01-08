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

  @spec to_csv(t(), String.t(), integer(), boolean(), integer()) :: :ok | {:error, term()}
  defdelegate to_csv(
                df,
                filename,
                batch_size \\ 100_000,
                has_headers \\ true,
                delimiter \\ ?,
              ),
              to: Native,
              as: :df_to_csv

  # defdelegate as_str(df), to: Native, as: :df_as_str

  @spec add(t(), s()) :: {:ok, t()} | {:error, term()}
  defdelegate add(df, s), to: Native, as: :df_add

  @spec sub(t(), s()) :: {:ok, t()} | {:error, term()}
  defdelegate sub(df, s), to: Native, as: :df_sub

  @spec div(t(), s()) :: {:ok, t()} | {:error, term()}
  defdelegate div(df, s), to: Native, as: :df_div

  @spec mul(t(), s()) :: {:ok, t()} | {:error, term()}
  defdelegate mul(df, s), to: Native, as: :df_mul

  @spec rem(t(), s()) :: {:ok, t()} | {:error, term()}
  defdelegate rem(df, s), to: Native, as: :df_rem

  @spec rechunk(t()) :: :ok | {:error, term()}
  defdelegate rechunk(df), to: Native, as: :df_rechunk

  @spec fill_none(t(), String.t()) :: {:ok, t()} | {:error, term()}
  defdelegate fill_none(df, strategy), to: Native, as: :df_fill_none

  @spec join(t(), t(), list(String.t()), list(String.t()), String.t()) ::
          {:ok, t()} | {:error, term()}
  defdelegate join(df, other, left_on, right_on, how), to: Native, as: :df_join

  @spec get_columns(t()) :: {:ok, list(s())} | {:error, term()}
  defdelegate get_columns(df), to: Native, as: :df_get_columns

  @spec columns(t()) :: {:ok, list(String.t())} | {:error, term()}
  defdelegate columns(def), to: Native, as: :df_columns

  @spec set_column_names(t(), list(String.t())) :: :ok | {:error, term()}
  defdelegate set_column_names(df, names), to: Native, as: :df_set_column_names

  @spec dtypes(t()) :: {:ok, list(integer())} | {:error, term()}
  defdelegate dtypes(df), to: Native, as: :df_dtypes

  @spec n_chunks(t()) :: {:ok, integer()} | {:error, term()}
  defdelegate n_chunks(df), to: Native, as: :df_n_chunks

  @spec shape(t()) :: {:ok, {integer(), integer()}} | {:error, term()}
  defdelegate shape(df), to: Native, as: :df_shape

  @spec height(t()) :: {:ok, integer()} | {:error, term()}
  defdelegate height(df), to: Native, as: :df_height

  @spec width(t()) :: {:ok, integer()} | {:error, term()}
  defdelegate width(df), to: Native, as: :df_width

  @spec hstack_mut(t(), list(s())) :: :ok | {:error, term()}
  defdelegate hstack_mut(df, cols), to: Native, as: :df_hstack_mut

  @spec hstack(t(), list(s())) :: {:ok, t()} | {:error, term()}
  defdelegate hstack(df, cols), to: Native, as: :df_hstack

  @spec vstack(t(), t()) :: :ok | {:error, term()}
  defdelegate vstack(df, other), to: Native, as: :df_vstack

  @spec drop_in_place(t(), String.t()) :: {:ok, s()} | {:error, term()}
  defdelegate drop_in_place(df, name), to: Native, as: :df_drop_in_place

  @spec drop_nulls(t(), list(String.t()) | nil) :: {:ok, t()} | {:error, term()}
  defdelegate drop_nulls(df, subset), to: Native, as: :df_drop_nulls

  @spec drop(t(), String.t()) :: {:ok, t()} | {:error, term()}
  defdelegate drop(df, name), to: Native, as: :df_drop

  @spec select_at_idx(t(), integer()) :: {:ok, s() | nil} | {:error, term()}
  defdelegate select_at_idx(df, idx), to: Native, as: :df_select_at_idx

  @spec find_idx_by_name(t(), String.t()) :: {:ok, integer() | nil} | {:error, term()}
  defdelegate find_idx_by_name(df, name), to: Native, as: :df_find_idx_by_name

  @spec column(t(), String.t()) :: {:ok, s() | nil} | {:error, term()}
  defdelegate column(df, name), to: Native, as: :df_column

  @spec select(t(), list(String.t())) :: {:ok, t()} | {:error, term()}
  defdelegate select(df, selection), to: Native, as: :df_select

  @spec filter(t(), s()) :: {:ok, t()} | {:error, term()}
  defdelegate filter(df, mask), to: Native, as: :df_filter

  @spec take(t(), list(integer())) :: {:ok, t()} | {:error, term()}
  defdelegate take(df, indices), to: Native, as: :df_take

  @spec take_with_series(t(), s()) :: {:ok, t()} | {:error, term()}
  defdelegate take_with_series(df, indices), to: Native, as: :df_take_with_series

  @spec replace(t(), String.t(), s()) :: :ok | {:error, term()}
  defdelegate replace(df, col, new_col), to: Native, as: :df_replace

  @spec replace_at_idx(t(), integer(), s()) :: :ok | {:error, term()}
  defdelegate replace_at_idx(df, index, new_col), to: Native, as: :df_replace_at_idx

  @spec insert_at_idx(t(), integer(), s()) :: :ok | {:error, term()}
  defdelegate insert_at_idx(df, index, new_col), to: Native, as: :df_insert_at_idx

  @spec slice(t(), integer(), integer()) :: {:ok, t()} | {:error, term()}
  defdelegate slice(df, offset, length), to: Native, as: :df_slice

  @spec head(t(), integer()) :: {:ok, t()} | {:error, term()}
  defdelegate head(df, length \\ 5), to: Native, as: :df_head

  @spec tail(t(), integer()) :: {:ok, t()} | {:error, term()}
  defdelegate tail(df, length \\ 5), to: Native, as: :df_tail

  @spec is_unique(t()) :: {:ok, s()} | {:error, term()}
  @doc """
  Get a mask of all unique rows in this DataFrame
  """
  defdelegate is_unique(df), to: Native, as: :df_is_unique

  @spec is_duplicated(t()) :: {:ok, s()} | {:error, term()}
  @doc """
  Get a mask of all duplicated rows in this DataFrame
  """
  defdelegate is_duplicated(df), to: Native, as: :df_is_duplicated

  @spec equal(t(), t(), boolean()) :: {:ok, boolean()} | {:error, term()}
  @doc """
  Check if DataFrame is equal to other.

  Parameters
  ----------
  df: DataFrame
  other: DataFrame to compare with.
  null_equal: Consider null values as equal.
  """
  defdelegate equal(df, other, null_equal \\ false), to: Native, as: :df_frame_equal

  @spec groupby(t(), list(String.t()), list(String.t()) | nil, String.t()) ::
          {:ok, t()} | {:error, term()}
  defdelegate groupby(df, by, sel, agg), to: Native, as: :df_groupby

  @spec groupby_agg(
          t(),
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
  def groupby_agg(df, by, column_to_agg) when is_map(column_to_agg),
    do: groupby_agg(df, by, Enum.into(column_to_agg, []))

  def groupby_agg(df, by, column_to_agg) when is_list(column_to_agg),
    do: Native.df_groupby_agg(df, by, column_to_agg)

  @spec groupby_quantile(t(), list(String.t()), list(String.t()), float()) ::
          {:ok, t()} | {:error, term()}
  @doc """
  Count the unique values per group.
  """
  defdelegate groupby_quantile(df, by, sel, quant), to: Native, as: :df_groupby_quantile

  @spec pivot(t(), list(String.t()), String.t(), String.t(), String.t()) ::
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
  defdelegate pivot(df, by, pivot_column, values_column, agg), to: Native, as: :df_pivot

  @spec clone(t()) :: {:ok, t()} | {:error, term()}
  defdelegate clone(df), to: Native, as: :df_clone

  @spec explode(t(), list(String.t())) :: {:ok, t()} | {:error, term()}
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
  defdelegate explode(df, cols), to: Native, as: :df_explode

  @spec melt(t(), list(String.t()), list(String.t())) :: {:ok, t()} | {:error, term()}
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
  defdelegate melt(df, id_vars, value_vars), to: Native, as: :df_melt

  @spec shift(t(), integer()) :: {:ok, t()} | {:error, term()}
  @doc """
  Shift the values by a given period and fill the parts that will be empty due to this operation
  with `Nones`.

  Parameters
  ----------
  periods
      Number of places to shift (may be negative).
  """
  defdelegate shift(df, periods), to: Native, as: :df_shift

  @spec drop_duplicates(t(), boolean(), list(String.t()) | nil) :: {:ok, t()} | {:error, term()}
  @doc """
  Drop duplicate rows from this DataFrame.
  Note that this fails if there is a column of type `List` in the DataFrame.
  """
  defdelegate drop_duplicates(df, maintain_order \\ true, subset \\ nil),
    to: Native,
    as: :df_drop_duplicates

  @spec max(t()) :: {:ok, t()} | {:error, term()}
  defdelegate max(df), to: Native, as: :df_max

  @spec min(t()) :: {:ok, t()} | {:error, term()}
  defdelegate min(df), to: Native, as: :df_min

  @spec sum(t()) :: {:ok, t()} | {:error, term()}
  defdelegate sum(df), to: Native, as: :df_sum

  @spec mean(t()) :: {:ok, t()} | {:error, term()}
  defdelegate mean(df), to: Native, as: :df_mean

  @spec std(t()) :: {:ok, t()} | {:error, term()}
  defdelegate std(df), to: Native, as: :df_stdev

  @spec var(t()) :: {:ok, t()} | {:error, term()}
  defdelegate var(df), to: Native, as: :df_var

  @spec median(t()) :: {:ok, t()} | {:error, term()}
  defdelegate median(df), to: Native, as: :df_median

  @spec quantile(t(), float()) :: {:ok, t()} | {:error, term()}
  defdelegate quantile(df, quant), to: Native, as: :df_quantile

  @spec to_dummies(t()) :: {:ok, t()} | {:error, term()}
  defdelegate to_dummies(df), to: Native, as: :df_to_dummies

  @spec sample(t(), integer() | float(), boolean()) :: {:ok, t()} | {:error, term()}
  def sample(df, n_or_frac, with_replacement \\ false) do
    case is_integer(n_or_frac) do
      true -> Native.df_sample_n(df, n_or_frac, with_replacement)
      _ -> Native.df_sample_frac(df, n_or_frac, with_replacement)
    end
  end

  def sort(df, by_column, inplace \\ false, reverse \\ false) do
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
