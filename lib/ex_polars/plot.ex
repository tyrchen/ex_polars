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

  def plot_repeat(df, mark, rows, columns, opts \\ [])

  def plot_repeat(df, mark, rows, columns, opts) when is_list(rows) and is_list(columns) do
    {color, opts} = get_color(df, opts)
    {xtype, opts} = Keyword.pop(opts, :xtype, "quantitative")
    {ytype, opts} = Keyword.pop(opts, :ytype, "quantitative")
    default = []
    opts = Keyword.merge(default, opts)

    {:ok, csv} = DF.to_csv(df)

    mark
    |> Mark.new(true)
    |> Chart.new(
      Encoding.new(%{
        x: %{field: %{repeat: "column"}, type: xtype},
        y: %{field: %{repeat: "row"}, type: ytype, axis: %{minExtend: 30}},
        color: color
      })
    )
    |> Chart.repeat(%{row: rows, column: columns}, opts)
    |> Deneb.to_json(csv)
  end

  def plot_repeat(df, mark, x, ys, opts) when is_binary(x) and is_list(ys) do
    {color, opts} = get_color(df, opts)
    {columns, opts} = Keyword.pop(opts, :columns, 2)

    {ytype, opts} = Keyword.pop(opts, :ytype, "quantitative")

    default = []
    opts = Keyword.merge(default, opts)

    {:ok, csv} = DF.to_csv(df)

    mark
    |> Mark.new(true)
    |> Chart.new(
      Encoding.new(%{
        x: %{field: x, type: DF.dtype(df, x, :vega)},
        y: %{field: %{repeat: "repeat"}, type: "ytype"},
        color: color
      })
    )
    |> Chart.repeat(ys, opts)
    |> Deneb.to_json(csv)
    |> Map.put("columns", columns)
  end

  def plot_repeat(df, mark, xs, y, opts) when is_list(xs) and is_binary(y) do
    {color, opts} = get_color(df, opts)
    {columns, opts} = Keyword.pop(opts, :columns, 2)
    {xtype, opts} = Keyword.pop(opts, :xtype, "quantitative")

    default = []
    opts = Keyword.merge(default, opts)

    {:ok, csv} = DF.to_csv(df)

    mark
    |> Mark.new(true)
    |> Chart.new(
      Encoding.new(%{
        x: %{field: %{repeat: "repeat"}, type: xtype},
        y: %{field: y, type: DF.dtype(df, y, :vega)},
        color: color
      })
    )
    |> Chart.repeat(xs, opts)
    |> Deneb.to_json(csv)
    |> Map.put("columns", columns)
  end

  defp get_color(df, opts) do
    {color_field, opts} = Keyword.pop(opts, :color)

    case color_field do
      nil -> {nil, opts}
      v -> {%{field: v, type: DF.dtype(df, v, :vega)}, opts}
    end
  end
end
