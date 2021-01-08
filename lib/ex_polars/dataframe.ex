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

  def add(_df, _s), do: err()

  def as_str(_df), do: err()

  defp err, do: :erlang.nif_error(:nif_not_loaded)
end
