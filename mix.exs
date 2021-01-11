defmodule ExPolars.MixProject do
  use Mix.Project

  @version "0.3.0-dev"
  def project do
    [
      app: :ex_polars,
      compilers: [:rustler] ++ Mix.compilers(),
      rustler_crates: [
        expolars: [
          path: "native/expolars",
          mode: rustc_mode(Mix.env())
        ]
      ],
      version: @version,
      elixir: "~> 1.11",
      start_permanent: Mix.env() == :prod,
      deps: deps(),

      # Docs
      name: "ExPolars",
      docs: [
        extras: ["README.md"]
      ],
      source_url: "https://github.com/tyrchen/ex_polars",
      homepage_url: "https://github.com/tyrchen/ex_polars",
      description: """
      Elixir support for [polars](https://github.com/ritchie46/polars), a DataFrame library written in rust.
      """,
      package: package()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp rustc_mode(:prod), do: :release
  defp rustc_mode(_), do: :debug

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:rustler, "~> 0.22.0-rc.0"},
      {:jason, "~> 1.2"},
      {:deneb, "~> 0.2"},

      # dev/test deps
      {:ex_doc, "~> 0.23", only: :dev, runtime: false},
      {:credo, "~> 1.5", only: [:dev]}
    ]
  end

  defp package do
    [
      files: ["lib", "priv/*.json", "priv/datasets", "native", "mix.exs", "README*", "LICENSE*"],
      licenses: ["MIT"],
      maintainers: ["tyr.chen@gmail.com"],
      links: %{
        "GitHub" => "https://github.com/tyrchen/ex_polars",
        "Docs" => "https://hexdocs.pm/ex_polars"
      }
    ]
  end
end
