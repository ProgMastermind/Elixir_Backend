defmodule App.MixProject do
  use Mix.Project

  def project do
    [
      app: :redis,
      version: "1.0.0",
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Server, []}
    ]
  end
end
