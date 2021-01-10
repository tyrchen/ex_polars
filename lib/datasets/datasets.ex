defmodule ExPolars.Datasets do
  @moduledoc """
  Vega datasets, copied from altair: https://github.com/altair-viz/vega_datasets
  """

  alias ExPolars.DataFrame, as: DF

  path = Application.app_dir(:ex_polars, ["priv"])

  datasets =
    path
    |> Path.join("local_datasets.json")
    |> File.read!()
    |> Jason.decode!()
    |> Enum.reduce(%{}, fn {name, filename}, acc ->
      name = String.replace(name, "-", "_")
      filename = Path.join(path, filename)

      Map.put(acc, name, filename)
    end)

  Enum.each(datasets, fn {name, filename} ->
    fname = String.to_atom(name)

    def unquote(fname)() do
      filename = unquote(filename)

      {:ok, df} =
        case Path.extname(filename) do
          ".json" -> DF.read_json(filename)
          ".csv" -> DF.read_csv(filename)
          ".parquet" -> DF.read_parquet(filename)
          _ -> {:ok, nil}
        end

      df
    end
  end)
end
