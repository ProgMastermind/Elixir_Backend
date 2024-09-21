defmodule App.MixProject do
  use Mix.Project

  def project do
    [
      app: :redis,
      version: "1.0.0",
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger, :plug_cowboy],
      mod: {Server, []}
    ]
  end

  defp deps do
    [
      # or any other simple dependency
      {:jason, "~> 1.2"},
      {:plug_cowboy, "~> 2.5"}
    ]
  end
end
