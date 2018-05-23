defmodule CouchdbAdapter.HttpClient do

  def post(url, params) do
    url
    |> HTTPoison.post(Poison.encode!(params), [{"Content-Type", "application/json; charset=utf-8"}])
    |> process_response
  end

  defp process_response({:ok, %{body: body}}), do: process_response({:ok, Poison.decode!(body)})
  defp process_response({:ok, %{"reason" => reason}}), do: {:error, "Could not fetch (#{reason})"}
  defp process_response({:ok, map}) when is_map(map), do: {:ok, map}
  defp process_response({:error, %{reason: reason}}), do: {:error, "Cound not fetch (#{reason})"}

end
