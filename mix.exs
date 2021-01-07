defmodule ExPolars.MixProject do
  use Mix.Project

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
      version: "0.1.0",
      elixir: "~> 1.11",
      start_permanent: Mix.env() == :prod,
      deps: deps()
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
      {:rustler, "~> 0.22.0-rc.0"}
    ]
  end
end
