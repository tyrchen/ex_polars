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
    |> Enum.reduce(%{}, fn {name, v}, acc ->
      name = String.replace(name, "-", "_")

      result =
        case is_map(v) do
          true -> %{v | "file" => Path.join(path, v["file"])}
          _ -> %{"file" => Path.join(path, v)}
        end

      Map.put(acc, name, result)
    end)

  Enum.each(datasets, fn {name, %{"file" => filename} = v} ->
    fname = String.to_atom(name)
    date = Map.get(v, "date")
    format = Map.get(v, "format")

    def unquote(fname)() do
      filename = unquote(filename)
      date = unquote(date)
      format = unquote(format)

      {:ok, df} =
        case Path.extname(filename) do
          ".json" -> DF.read_json(filename)
          ".csv" -> DF.read_csv(filename)
          ".parquet" -> DF.read_parquet(filename)
          _ -> {:ok, nil}
        end

      case {df, date} do
        {nil, nil} -> nil
        {_, nil} -> df
        {_, _} -> DF.parse_date(df, date, format)
      end

      df
    end
  end)
end
