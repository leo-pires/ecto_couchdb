defmodule CouchdbAdapter.HttpClient do

  def request(method, url, body \\ %{}, headers \\ [{"Content-Type", "application/json; charset=utf-8"}], options \\ []) do
    HTTPoison.request(method, url, Poison.encode!(body), headers, options)
    |> process_response
  end

  def head(url, body \\ %{}), do: request(:head, url, body)
  def get(url, body \\ %{}), do: request(:get, url, body)
  def put(url, body \\ %{}), do: request(:put, url, body)
  def post(url, body \\ %{}), do: request(:post, url, body)
  def delete(url, body \\ %{}), do: request(:delete, url, body)

  defp process_response({:ok, %{status_code: status_code, body: ""}}), do: {:ok, {status_code, %{}}}
  defp process_response({:ok, %{status_code: status_code, body: body}}), do: {:ok, {status_code, Poison.decode!(body)}}
  defp process_response({:error, %{reason: reason}}), do: {:error, "Cound not fetch (#{reason})"}

end
