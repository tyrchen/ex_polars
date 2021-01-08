# ExPolars

Elixir support for [polars](https://github.com/ritchie46/polars), a DataFrame library written in rust.

## Usage

```elixir
alias ExPolars.DataFrame, as: Df
df = Df.read_csv("iris.csv")
Df.head(df)

```

## Installation

To build ex_polars, you need to install rust nightly. This is because polars is using nightly features at the moment.

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `ex_polars` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ex_polars, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/ex_polars](https://hexdocs.pm/ex_polars).
