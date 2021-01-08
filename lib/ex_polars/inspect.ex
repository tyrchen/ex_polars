defimpl Inspect, for: ExPolars.DataFrame do
  alias ExPolars.DataFrame

  def inspect(data, _opts) do
    case DataFrame.as_str(data) do
      {:ok, s} -> s
      _ -> "Cannot output dataframe"
    end
  end
end

defimpl Inspect, for: ExPolars.Series do
  alias ExPolars.DataFrame

  def inspect(data, _opts) do
    case DataFrame.series_as_str(data) do
      {:ok, s} -> s
      _ -> "Cannot output series"
    end
  end
end
