defmodule CouchdbAdapter.Storage do

  alias CouchdbAdapter.HttpClient

  def db_exists?(options) do
    case CouchdbAdapter.url_for(options) |> HttpClient.head do
      {:ok, {404, _}} -> {:ok, false}
      response -> response |> process_response("verify db")
    end
  end

  def create_db(options) do
    case CouchdbAdapter.url_for(options) |> HttpClient.put do
      {:ok, {412, _}} -> {:ok, false}
      response -> response |> process_response("create db")
    end
  end

  def delete_db(options) do
    case CouchdbAdapter.url_for(options) |> HttpClient.delete do
      {:ok, {404, _}} -> {:ok, false}
      response -> response |> process_response("delete db")
    end
  end

  def create_ddoc(options, ddoc, code) do
    if valid_ddoc_name?(ddoc) do
      "#{CouchdbAdapter.url_for(options)}/#{ddoc}"
      |> HttpClient.put(code)
      |> process_response("create design doc")
    else
      {:error, "Design doc id not present"}
    end
  end

  def create_index(options, data) do
    "#{CouchdbAdapter.url_for(options)}/_index"
    |> HttpClient.post(data)
    |> process_response("create index")
  end


  defp valid_ddoc_name?(ddoc), do: ddoc && ddoc =~ ~r/^_design\/(.+)$/

  defp process_response({:ok, {code, _}}, _action) when div(code, 100) == 2, do: {:ok, true}
  defp process_response({:ok, {code, %{reason: reason}}}, action), do: {:error, "Could not #{action} (#{code} - #{reason})"}
  defp process_response({:ok, {code, %{"reason" => reason}}}, action), do: {:error, "Could not #{action} (#{code} - #{reason})"}
  defp process_response({:ok, {code, _}}, action), do: {:error, "Could not #{action} (#{code})"}
  defp process_response({:error, {code, %{reason: reason}}}, action), do: {:error, "Could not #{action} (#{code} - #{reason})"}
  defp process_response({:error, {code, %{"reason" => reason}}}, action), do: {:error, "Could not #{action} (#{code} - #{reason})"}
  defp process_response({:error, reason}, action), do: {:error, "Could not #{action} (#{reason})"}

end
