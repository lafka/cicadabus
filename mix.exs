defmodule CicadaBus.MixProject do
  use Mix.Project

  def project do
    [
      app: :cicadabus,
      version: "0.1.3",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      mod: {CicadaBus.App, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:typed_struct, "~> 0.1"},
      {:path_glob, "~> 0.1.0"}
    ]
  end
end
