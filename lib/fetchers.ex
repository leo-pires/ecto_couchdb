defmodule CouchdbAdapter.Fetchers do

  alias CouchdbAdapter.{HttpClient, HttpResultProcessor, TempResultProcessor}

  def get(repo, schema, id) do
    get(repo, schema, id, [])
  end

  def get(repo, :map, id, _options) do
    with db_props <- CouchdbAdapter.db_props_for(repo),
         {:ok, data} <- Couchdb.Connector.get(db_props, id) do
      data
    else
      {:error, _} -> nil
    end
  end

  def get(repo, schema, id, options) do
    preloads = Keyword.get(options, :preload, [])
    with db_props <- CouchdbAdapter.db_props_for(repo),
         {:ok, data} <- Couchdb.Connector.get(db_props, id)
    do
      data |> CouchdbAdapter.Processors.Helper.process_result(TempResultProcessor, repo, schema, preloads)
    else
      {:error, %{"error" => "not_found", "reason" => "missing"}} -> nil
      {:error, reason} -> raise "Could not get (#{inspect(reason)})"
    end
  end

  def fetch_one(repo, schema, view_name, options \\ []) do
    case fetch_all(repo, schema, view_name, options) do
      {:error, error} -> {:error, error}
      data when length(data) == 1 -> hd(data)
      data when length(data) == 0 -> nil
      _ -> raise "Fetch returning more than one value"
    end
  end

  def fetch_all(repo, schema, view_name, opts \\ []) do
    ddoc = CouchdbAdapter.db_name(schema)
    preloads = Keyword.get(opts, :preload, [])
    query = Keyword.delete(opts, :preload) |> Enum.into(%{})
    with db_props <- CouchdbAdapter.db_props_for(repo),
         {:ok, data} <- Couchdb.Connector.fetch_all(db_props, ddoc, view_name, query)
    do
      data |> CouchdbAdapter.Processors.Helper.process_result(TempResultProcessor, repo, schema, preloads)
    else
      {:error, %{"error" => "not_found", "reason" => "missing_named_view"}} -> raise "View not found (#{ddoc}, #{view_name})"
      {:error, reason} -> raise "Error while fetching (#{inspect(reason)})"
    end
  end

  def multiple_fetch_all(repo, schema, view, params, options \\ []) do
    fetch_keys = Keyword.get(options, :fetch_keys, false)
    ddoc = CouchdbAdapter.db_name(schema)
    url = "#{CouchdbAdapter.url_for(repo)}/_design/#{ddoc}/_view/#{view}"
    schema_to_use =
      if Keyword.get(options, :as_map, false) do
        :map
      else
        schema
      end
    with {:ok, {_, data}} <- url |> HttpClient.post(params),
         result <- data |> CouchdbAdapter.Processors.Helper.process_result(HttpResultProcessor, repo, schema_to_use, [], fetch_keys)
    do
      {:ok, result}
    end
  end

  def find(repo, schema, params, options \\ []) do
    preloads = Keyword.get(options, :preload, [])
    url = "#{CouchdbAdapter.url_for(repo)}/_find"
    with {:ok, {_, data}} <- url |> HttpClient.post(params),
         docs <- data |> CouchdbAdapter.Processors.Helper.process_result(HttpResultProcessor, repo, schema, preloads)
    do
      bookmark = data |> Map.get("bookmark")
      warning = data |> Map.get("warning")
      result = %{
        bookmark: bookmark,
        docs: docs
      }
      result =
        if warning do
          result |> Map.put(:warning, warning)
        else
          result
        end
      {:ok, result}
    end
  end

end
