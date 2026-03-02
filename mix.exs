defmodule Toqueue.MixProject do
  use Mix.Project

  def project do
    [
      app: :snaq,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      escript: escript()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Snaq.Application, []}
    ]
  end

  defp deps, do: []

  defp escript do
    [main_module: Snaq.CLI, app: nil, name: "snaq-cli"]
  end
end
