defmodule ExPolars.Plot do
  @moduledoc """
  Plotting tools
  """

  alias ExPolars.DataFrame, as: DF
  alias Deneb.{Chart, Encoding, Mark, Plot}

  def plot_by_type(df, type, opts \\ []) do
    {:ok, csv} = DF.to_csv(df)
    apply(Plot, type, [csv, opts])
  end

  def plot_single(df, mark, x, y, opts \\ []) do
    default = [width: 800, height: 600]
    color = get_color(df, opts)
    opts = Keyword.merge(default, Keyword.delete(opts, :color))
    xtype = DF.dtype(df, x, :vega)
    ytype = DF.dtype(df, y, :vega)

    {:ok, csv} = DF.to_csv(df)

    mark
    |> Mark.new(true)
    |> Chart.new(
      Encoding.new(%{
        x: %{field: x, type: xtype},
        y: %{field: y, type: ytype},
        color: color
      })
    )
    |> Chart.to_json(opts)
    |> Deneb.to_json(csv)
  end

  def plot_repeat(df, rows, columns, opts \\ []) do
    color = get_color(df, opts)
    default = []
    opts = Keyword.merge(default, Keyword.delete(opts, :color))

    {:ok, csv} = DF.to_csv(df)

    :point
    |> Mark.new(true)
    |> Chart.new(
      Encoding.new(%{
        x: %{field: %{repeat: "column"}, type: "quantitative"},
        y: %{field: %{repeat: "row"}, type: "quantitative", axis: %{minExtend: 30}},
        color: color
      })
    )
    |> Chart.repeat(%{row: rows, column: columns}, opts)
    |> Deneb.to_json(csv)
  end

  defp get_color(df, opts) do
    color_field = opts[:color] || nil

    case color_field do
      nil -> nil
      v -> %{field: v, type: DF.dtype(df, v, :vega)}
    end
  end
end
