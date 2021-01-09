defmodule ExPolars.Series do
  @moduledoc """
  Documentation for `Series`.
  """
  alias ExPolars.Native
  defstruct [:inner]

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

  def to_list(s) do
    with {:ok, json} <- Native.s_to_json(s),
         {:ok, data} <- Jason.decode(json) do
      {:ok, data}
    else
      e -> e
    end
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
