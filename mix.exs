defmodule CouchdbAdapter.Mixfile do
  use Mix.Project

  def project do
    [
      app: :ecto_couchdb,
      version: "0.1.0",
      elixir: "~> 1.4",
      build_embedded: Mix.env == :prod,
      start_permanent: Mix.env == :prod,
      test_coverage: [tool: Coverex.Task],
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      {:ecto, "~> 3.0"},
      {:icouch, "~> 0.6"},
      {:credo, "~> 1.1", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 0.5", only: [:dev, :test], runtime: false},
      {:coverex, "~> 1.5", only: :test},
    ]
  end

end
