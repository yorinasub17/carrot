defmodule MacaroniPenguin.Mixfile do
  use Mix.Project

  def project do
    [app: :macaroni_penguin,
     version: "0.1.0",
     elixir: "~> 1.4",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps()]
  end

  # No applications
  def application do
    []
  end

  defp deps do
    []
  end
end
