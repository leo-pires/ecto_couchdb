defmodule CouchdbAdapter.Fetchers do

  alias CouchdbAdapter.{HttpClient, CouchbeamResultProcessor, HttpResultProcessor}

  def get(repo, schema, id) do
    get(repo, schema, id, [])
  end

  def get(repo, :map, id, _options) do
    url = "#{CouchdbAdapter.url_for(repo)}/#{id}"
    with {:ok, {_, data}} <- url |> HttpClient.get,
         result <- data |> HttpResultProcessor.identity_process_result
    do
      result
    else
      {:error, _} -> nil
    end
  end

  def get(repo, schema, id, options) do
    database = repo.config[:database]
    preloads = Keyword.get(options, :preload, [])
    with server <- CouchdbAdapter.server_for(repo),
         {:ok, db} <- :couchbeam.open_db(server, database),
         {:ok, data} <- :couchbeam.open_doc(db, id)
    do
      data |> CouchdbAdapter.Processors.Helper.process_result(CouchbeamResultProcessor, repo, schema, preloads)
    else
      {:error, :not_found} -> nil
      {:error, {:error, reason}} -> raise inspect(reason)
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

  def fetch_all(repo, schema, view_name, options \\ []) do
    type = CouchdbAdapter.db_name(schema)
    view_name = view_name |> Atom.to_string
    database = repo.config[:database]
    preloads = Keyword.get(options, :preload, [])
    with server <- CouchdbAdapter.server_for(repo),
         {:ok, db} <- :couchbeam.open_db(server, database),
         {:ok, data} <- :couchbeam_view.fetch(db, {type, view_name}, options |> fetch_options_humanize)
    do
      data |> CouchdbAdapter.Processors.Helper.process_result(CouchbeamResultProcessor, repo, schema, preloads)
    else
      {:error, :not_found} -> raise "View not found (#{type}, #{view_name})"
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

  @bool_to_atom [:include_docs, :descending]
  defp fetch_options_humanize(options) do
    options
    |> Enum.reduce([], fn
         ({opt, true}, acc) when opt in @bool_to_atom -> [opt | acc]
         ({opt, false}, acc) when opt in @bool_to_atom -> acc
         ({opt, value}, acc) -> [{opt, value} | acc]
       end)
  end

end
