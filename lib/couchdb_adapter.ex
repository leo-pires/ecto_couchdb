defmodule CouchdbAdapter do
  @moduledoc ~S"""
  CouchdbAdapter provides an implementation of the `Ecto.Adapter` behaviour for the Couchdb
  database.
  """
  @behaviour Ecto.Adapter

  defmacro __before_compile__(_env), do: nil

  @doc false
  def autogenerate(:id),        do: nil
  def autogenerate(:embed_id),  do: Ecto.UUID.generate()
  def autogenerate(:binary_id), do: nil

  @doc false
  def loaders({:embed, _} = type, _), do: [&load_embed(type, &1)]
  def loaders(:naive_datetime, type), do: [&load_datetime/1, type]
  def loaders(:date, type), do: [&load_date/1, type]
  def loaders(:time, type), do: [&load_time/1, type]
  def loaders(_, type), do: [type]
  defp load_embed({:embed, %{related: related, cardinality: :one}}, value) do
    {:ok, struct(related, atomize_keys(value))}
  end
  defp load_embed({:embed, %{related: related, cardinality: :many}}, values) do
    {:ok, Enum.map(values, &struct(related, atomize_keys(&1)))}
  end
  defp load_datetime(datetime), do: {:ok, NaiveDateTime.from_iso8601!(datetime) |> NaiveDateTime.to_erl}
  defp load_date(date), do: date |> Date.from_iso8601
  defp load_time(time), do: time |> Time.from_iso8601

  defp atomize_keys({map}), do: atomize_keys(map)
  defp atomize_keys(map), do: for {k, v} <- map, do: {String.to_atom(k), v}

  def dumpers(:naive_datetime, type), do: [type, &dump_naive_datetime/1]
  def dumpers(:date, type), do: [type, &dump_date/1]
  def dumpers(:time, type), do: [type, &dump_time/1]
  def dumpers(_, type), do: [type]
  defp dump_naive_datetime({{_, _, _} = dt, {h, m, s, ms}}), do: {:ok, {dt, {h, m, s}} |> NaiveDateTime.from_erl!({ms, 6}) |> NaiveDateTime.to_iso8601}
  defp dump_date(date), do: {:ok, date |> Date.from_erl! |> Date.to_iso8601}
  defp dump_time({h, m, s, ms}), do: {:ok, {h, m, s} |> Time.from_erl!({ms, 0}) |> Time.to_iso8601}

  def child_spec(repo, _options) do
    :hackney_pool.child_spec(repo, pool_config(repo.config))
  end

  @default_pool_options [max_connections: 20, timeout: 10_000]
  defp pool_config(config) do
    config_options = Keyword.take(config, [:max_connections, :timeout])
    Keyword.merge @default_pool_options, config_options
  end

  # Returns the server connection to use with the given repo
  # TODO: reuse url_for
  defp url_for(repo) do
    config = repo.config
    protocol = Keyword.get(config, :protocol, "http")
    hostname = Keyword.get(config, :hostname, "localhost")
    port = Keyword.get(config, :port, 5984)
    username = Keyword.get(config, :username)
    password = Keyword.get(config, :password)
    database = Keyword.get(config, :database)
    if username && password do
      "#{protocol}://#{username}:#{password}@#{hostname}:#{port}/#{database}"
    else
      "#{protocol}://#{hostname}:#{port}/#{database}"
    end
  end
  defp server_for(repo) do
    config = repo.config
    protocol = Keyword.get(config, :protocol, "http")
    hostname = Keyword.get(config, :hostname, "localhost")
    port = Keyword.get(config, :port, 5984)
    username = Keyword.get(config, :username)
    password = Keyword.get(config, :password)
    base_options = [pool: repo]
    options =
      if username && password do
        base_options |> Keyword.put(:basic_auth, {username, password})
      else
        base_options
      end
    server_url = "#{protocol}://#{hostname}:#{port}"
    :couchbeam.server_connection(server_url, options)
  end

  def ensure_all_started(_repo, type) do
    Application.ensure_all_started(:couchbeam, type)
  end

  def insert(repo, meta, fields, _on_conflict, returning, _options) do
    type = db_name(meta)
    database = repo.config[:database]
    with server <- server_for(repo),
         {:ok, db} <- :couchbeam.open_db(server, database),
         {:ok, {new_fields}} <- :couchbeam.save_doc(db, to_doc(fields |> inject_type(type)))
      do
        {:ok, returning(returning, new_fields)}
      else
        # Map the conflict to the format of SQL constraints
        {:error, :conflict} -> {:invalid, [unique: "#{db_name(meta)}_id_index"]}
        # other errors
        {:error, reason} -> raise "Error while inserting (#{inspect(reason)})"
    end
  end

  def insert_all(repo, schema_meta, _header, list, _on_conflict, returning, _options) do
    type = db_name(schema_meta)
    database = repo.config[:database]
    with server <- server_for(repo),
         {:ok, db} <- :couchbeam.open_db(server, database),
         {:ok, result} <- :couchbeam.save_docs(db, Enum.map(list, &to_doc(&1 |> inject_type(type))))
    do
      if returning == [] do
        {length(result), nil}
      else
        {length(result), Enum.map(result, fn({fields}) -> returning(returning, fields) end)}
      end
    else
      {:error, reason} -> raise "Error while inserting all (#{inspect(reason)})"
    end
  end

  @spec db_name(Ecto.Adapter.schema_meta | Ecto.Adapter.query_meta) :: String.t
  defp db_name(%{schema: schema}), do: schema.__schema__(:source)
  defp db_name(%{sources: {{db_name, _}}}), do: db_name
  defp db_name(module), do: module.__schema__(:source)

  @spec to_doc(Keyword.t | map) :: {[{String.t, any}]}
  def to_doc(fields) do
    kv_list = for {name, value} <- fields do
      {to_string(name), to_doc_value(value)}
    end
    {kv_list}
  end
  defp to_doc_value(list) when is_list(list) do
    values = for i <- list, do: to_doc_value(i)
    {values}
  end
  defp to_doc_value(map) when is_map(map) do
    kv_list = for {name, value} <- map, do: {to_string(name), to_doc_value(value)}
    {kv_list}
  end
  defp to_doc_value(nil), do: :null
  defp to_doc_value(value), do: value

  defp returning(returning, fields) do
    for field_name <- returning, do: normalize(field_name, fields)
  end
  defp normalize(field_name, fields) do
    {_string_key, value} = List.keyfind(fields, to_string(field_name), 0)
    {field_name, value}
  end

  def prepare(:all, _) do
    raise "Unsupported operation in CouchdbAdapter: all"
  end
  def prepare(:delete_all, _) do
    raise "Unsupported operation in CouchdbAdapter: delete_all"
  end

  def delete(repo, _schema_meta, filters, _options) do
    database = repo.config[:database]
    with server <- server_for(repo),
         {:ok, db} <- :couchbeam.open_db(server, database),
         {:ok, [result]} <- :couchbeam.delete_doc(db, to_doc(filters))
    do
      {ok, result} = :couchbeam_doc.take_value("ok", result)
      if ok != :undefined do
        {:ok, _rev: :couchbeam_doc.get_value("rev", result)}
      else
        error = :couchbeam_doc.get_value("error", result)
        {:invalid, [check: error]}
      end
    else
      {:error, reason} -> raise "Error while deleting (#{inspect(reason)})"
    end
  end

  def execute(_repo, _query_meta, _, _params, _preprocess, _options) do
    raise "Unsupported operation in CouchdbAdapter: execute"
  end

  def update(repo, schema_meta, fields, filters, returning, _options) do
    type = db_name(schema_meta)
    database = repo.config[:database]
    with server <- server_for(repo),
         {:ok, db} <- :couchbeam.open_db(server, database),
         {:ok, doc} <- fetch_for_update(db, filters),
         doc <- Enum.reduce(fields |> inject_type(type), doc,
                            fn({key, value}, accum) ->
                              :couchbeam_doc.set_value(to_string(key), to_doc_value(value), accum)
                            end),
         {:ok, doc} <- :couchbeam.save_doc(db, doc)
    do
      fields = for field <- returning, do: {field, :couchbeam_doc.get_value(to_string(field), doc)}
      {:ok, fields}
    end
  end

  # Because Ecto will not give us the full document, we need to retrieve it and then update
  # We try to maintain the Conflict semantics of couchdb and avoid updating documents in a
  # different revision than the one in the filter.
  @spec fetch_for_update(:couchbeam.db, [_id: String.t, _rev: String.t]) :: :couchbeam.doc
  defp fetch_for_update(db, filters) do
    with {:ok, doc} <- :couchbeam.open_doc(db, filters[:_id])
    do
      if :couchbeam_doc.get_rev(doc) == filters[:_rev] do
        {:ok, doc}
      else
        {:error, :stale}
      end
    else
      {:error, :not_found} -> {:error, :stale}
      {:error, reason} -> raise "Error while fetching (#{inspect(reason)})"
    end
  end

  defp inject_type(fields, type) when is_map(fields), do: fields |> Map.put(:type, type)
  defp inject_type(fields, type), do: [{:type, type} | fields]

  # Fetchers for Audo

  alias CouchdbAdapter.{CouchbeamResultProcessor, HttpResultProcessor}

  def get(repo, schema, id, options \\ []) do
    database = repo.config[:database]
    preloads = Keyword.get(options, :preload, []) |> normalize_preloads
    with server <- server_for(repo),
         {:ok, db} <- :couchbeam.open_db(server, database),
         {:ok, data} <- :couchbeam.open_doc(db, id)
    do
      data |> CouchbeamResultProcessor.ecto_process_result(repo, schema, preloads)
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
    type = db_name(schema)
    view_name = view_name |> Atom.to_string
    database = repo.config[:database]
    preloads = Keyword.get(options, :preload, []) |> normalize_preloads
    with server <- server_for(repo),
         {:ok, db} <- :couchbeam.open_db(server, database),
         {:ok, data} <- :couchbeam_view.fetch(db, {type, view_name}, options |> fetch_options_humanize)
    do
      data |> CouchbeamResultProcessor.ecto_process_result(repo, schema, preloads)
    else
      {:error, {:error, reason}} -> raise inspect(reason)
      {:error, reason} -> raise "Error while fetching (#{inspect(reason)})"
    end
  end

  def multiple_fetch_all(repo, schema, view, queries, options \\ []) do
    fetch_keys = Keyword.get(options, :fetch_keys, false)
    ddoc = db_name(schema)
    url = "#{url_for(repo)}/_design/#{ddoc}/_view/#{view}"
    with {:ok, data} <- url |> http_post(%{queries: queries}),
         result <- data |> process_result(HttpResultProcessor, repo, cast_to(schema, options), [], fetch_keys) do
      {:ok, result}
    end
  end

  def find(repo, schema, selector, options \\ []) do
    preloads = Keyword.get(options, :preload, []) |> normalize_preloads
    url = "#{url_for(repo)}/_find"
    with {:ok, data} <- url |> http_post(%{selector: selector}),
         result <- data |> process_result(HttpResultProcessor, repo, schema, preloads) do
      {:ok, result}
    end
  end

  # TODO: better api when using as_map and fetch_keys
  defp process_result(data, processor, repo, schema, preloads) do
    process_result(data, processor, repo, schema, preloads, false)
  end
  defp process_result(data, processor, _repo, :map, _preloads, false) do
    data |> processor.identity_process_result(true)
  end
  defp process_result(data, processor, _repo, :map, _preloads, true) do
    data |> processor.process_result_keys
  end
  defp process_result(data, processor, repo, schema, preloads, false) do
    data |> processor.ecto_process_result(repo, schema, preloads)
  end

  def inject_preloads(map, _repo, _schema, [] = _preloads), do: map
  def inject_preloads(nil, _repo, _schema, _preloads), do: nil
  def inject_preloads(map, repo, schema, preloads) do
    to_inject =
      preloads
      |> Enum.reduce([], fn ({preload_assoc, preload_inject}, acc) ->
          case schema.__schema__(:association, preload_assoc) do
            %Ecto.Association.BelongsTo{owner_key: fk, related: related_schema, field: field} ->
              value = Map.get(map, fk)
              if value do
                to_add = CouchdbAdapter.get(repo, related_schema, value) |> inject_preloads(repo, related_schema, preload_inject)
                [{field, to_add} | acc]
              else
                acc
              end
            _ ->
              raise "Unsupported preload type for #{preload_assoc}"
          end
        end)
      |> Map.new
    Map.merge(map, to_inject)
  end

  def normalize_preloads(f) when is_atom(f), do: [strict_normalize_preloads(f)]
  def normalize_preloads(o), do: strict_normalize_preloads(o)
  defp strict_normalize_preloads(f) when is_atom(f), do: {f, []}
  defp strict_normalize_preloads({f, l}), do: {f, normalize_preloads(l)}
  defp strict_normalize_preloads(l) when is_list(l), do: l |> Enum.map(&(strict_normalize_preloads(&1)))

  defp http_post(url, params) do
    url
    |> HTTPoison.post(Poison.encode!(params), [{"Content-Type", "application/json; charset=utf-8"}])
    |> http_process_response
  end
  defp http_process_response({:ok, %{body: body}}), do: http_process_response({:ok, Poison.decode!(body)})
  defp http_process_response({:ok, %{"reason" => reason}}), do: {:error, "Could not fetch (#{reason})"}
  defp http_process_response({:ok, map}) when is_map(map), do: {:ok, map}
  defp http_process_response({:error, %{reason: reason}}), do: {:error, "Cound not fetch (#{reason})"}

  defp fetch_options_humanize(options) do
    options = options |> Keyword.delete(:preload)
    if options[:descending] do
      [:descending | options |> Keyword.delete(:descending)]
    else
      options
    end
  end
  defp cast_to(schema, options) do
    if Keyword.get(options, :as_map, false) do
      :map
    else
      schema
    end
  end

end
