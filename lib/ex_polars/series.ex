defmodule ExPolars.Series do
  @moduledoc """
  Documentation for `Series`.
  """

  defstruct [:inner]
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
