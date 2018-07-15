defmodule CouchdbAdapter do

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Storage


  defmacro __before_compile__(_env), do: nil

  def child_spec(repo, _options) do
    :hackney_pool.child_spec(repo, pool_config(repo.config))
  end
  @default_pool_options [max_connections: 20, timeout: 10_000]
  defp pool_config(config) do
    config_options = Keyword.take(config, [:max_connections, :timeout])
    Keyword.merge @default_pool_options, config_options
  end

  def ensure_all_started(_repo, type) do
    Application.ensure_all_started(:couchbeam, type)
  end

  # TODO: raise para id?
  def autogenerate(:id),        do: nil
  def autogenerate(:binary_id), do: nil
  def autogenerate(:embed_id),  do: Ecto.UUID.generate()

  def loaders(:naive_datetime, type), do: [&load_datetime/1, type]
  def loaders(:date, type), do: [&load_date/1, type]
  def loaders(:time, type), do: [&load_time/1, type]
  def loaders(_, type), do: [type]
  defp load_datetime(datetime), do: {:ok, NaiveDateTime.from_iso8601!(datetime) |> NaiveDateTime.to_erl}
  defp load_date(date), do: date |> Date.from_iso8601
  defp load_time(time), do: time |> Time.from_iso8601

  def dumpers(:naive_datetime, type), do: [type, &dump_naive_datetime/1]
  def dumpers(:date, type), do: [type, &dump_date/1]
  def dumpers(:time, type), do: [type, &dump_time/1]
  def dumpers(_, type), do: [type]
  defp dump_naive_datetime({{_, _, _} = dt, {h, m, s, ms}}), do: {:ok, {dt, {h, m, s}} |> NaiveDateTime.from_erl!({ms, 6}) |> NaiveDateTime.to_iso8601}
  defp dump_date(date), do: {:ok, date |> Date.from_erl! |> Date.to_iso8601}
  defp dump_time({h, m, s, ms}), do: {:ok, {h, m, s} |> Time.from_erl!({ms, 0}) |> Time.to_iso8601}

  def insert(repo, meta, fields, _on_conflict, returning, _options) do
    type = ddoc_name(meta)
    database = repo.config[:database]
    with server <- server_for(repo),
         {:ok, db} <- :couchbeam.open_db(server, database),
         {:ok, {new_fields}} <- :couchbeam.save_doc(db, to_doc(fields |> inject_type(type)))
      do
        {:ok, returning(returning, new_fields)}
      else
        # Map the conflict to the format of SQL constraints
        {:error, :conflict} -> {:invalid, [unique: "#{type}_id_index"]}
        # other errors
        {:error, reason} -> raise "Error while inserting (#{inspect(reason)})"
    end
  end

  def insert_all(repo, schema_meta, _header, list, _on_conflict, returning, _options) do
    type = ddoc_name(schema_meta)
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

  def update(repo, schema_meta, fields, filters, returning, options) do
    type = ddoc_name(schema_meta)
    database = repo.config[:database]
    with server <- server_for(repo),
         {:ok, db} <- :couchbeam.open_db(server, database),
         {:ok, doc} <- fetch_for_update(db, filters),
         doc <- Enum.reduce(fields |> inject_type(type), doc,
                            fn ({key, value}, accum) ->
                              :couchbeam_doc.set_value(to_string(key), to_doc_value(value), accum)
                            end),
         {:ok, doc} <- :couchbeam.save_doc(db, doc)
    do
      fields = for field <- returning, do: {field, :couchbeam_doc.get_value(to_string(field), doc)}
      {:ok, fields}
    else
        {:error, e} when e in [:conflict, :stale] ->
          if Keyword.get(options, :on_conflict, nil) == :retry do
            server = server_for(repo)
            {:ok, db} = :couchbeam.open_db(server, database)
            {:ok, doc} = :couchbeam.open_doc(db, filters[:_id])
            rev = :couchbeam_doc.get_rev(doc)
            new_filters = filters |> Keyword.put(:_rev, rev)
            update(repo, schema_meta, fields, new_filters, returning, options)
          else
            {:error, :stale}            
          end
      {:error, reason} -> raise "Error while updating (#{inspect(reason)})"
    end
  end

  def delete(repo, schema_meta, filters, options) do
    database = repo.config[:database]
    with server <- server_for(repo),
         {:ok, db} <- :couchbeam.open_db(server, database),
         {:ok, [result]} <- :couchbeam.delete_doc(db, to_doc(filters))
    do
      {ok, result} = :couchbeam_doc.take_value("ok", result)
      if ok != :undefined do
        {:ok, _rev: :couchbeam_doc.get_value("rev", result)}
      else
        case :couchbeam_doc.get_value("error", result) do
          "conflict" ->
            if Keyword.get(options, :on_conflict, nil) == :retry do
              {:ok, doc} = :couchbeam.open_doc(db, filters[:_id])
              rev = :couchbeam_doc.get_rev(doc)
              new_filters = filters |> Keyword.put(:_rev, rev)
              delete(repo, schema_meta, new_filters, options)
            else
              {:error, :stale}
            end
          error ->
            {:invalid, [check: error]}
        end
      end
    else
      {:error, reason} -> raise "Error while deleting (#{inspect(reason)})"
    end
  end

  def prepare(_, _) do
    raise "Unsupported operation in CouchdbAdapter: prepare"
  end

  def execute(_repo, _query_meta, _, _params, _preprocess, _options) do
    raise "Unsupported operation in CouchdbAdapter: execute"
  end

  # Returns the server connection to use with the given repo
  # TODO: reuse
  def server_for(repo) do
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
  def db_props_for(repo) do
    config = repo.config
    protocol = Keyword.get(config, :protocol, "http")
    hostname = Keyword.get(config, :hostname, "localhost")
    port = Keyword.get(config, :port, 5984)
    database = Keyword.get(config, :database)
    username = Keyword.get(config, :username)
    password = Keyword.get(config, :password)
    props = %{protocol: protocol, hostname: hostname, port: port, database: database}
    if username && password do
      props |> Map.merge(%{username: username, password: password})
    else
      props
    end
  end

  @spec ddoc_name(Ecto.Adapter.schema_meta | Ecto.Adapter.query_meta) :: String.t
  def ddoc_name(%{schema: schema}), do: schema.__schema__(:source)
  def ddoc_name(module), do: module.__schema__(:source)

  @spec to_doc(Keyword.t | map) :: {[{String.t, any}]}
  def to_doc(fields) do
    kv_list = for {name, value} <- fields do
      {to_string(name), to_doc_value(value)}
    end
    {kv_list}
  end
  defp to_doc_value([]), do: []
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


  defdelegate storage_up(options), to: CouchdbAdapter.Storage
  defdelegate storage_down(options), to: CouchdbAdapter.Storage

  defdelegate get(repo, schema, id), to: CouchdbAdapter.Fetchers
  defdelegate get(repo, schema, id, options), to: CouchdbAdapter.Fetchers
  defdelegate fetch_one(repo, schema, view_name), to: CouchdbAdapter.Fetchers
  defdelegate fetch_one(repo, schema, view_name, options), to: CouchdbAdapter.Fetchers
  defdelegate fetch_all(repo, schema, view_name), to: CouchdbAdapter.Fetchers
  defdelegate fetch_all(repo, schema, view_name, options), to: CouchdbAdapter.Fetchers
  defdelegate multiple_fetch_all(repo, schema, view_name, queries), to: CouchdbAdapter.Fetchers
  defdelegate multiple_fetch_all(repo, schema, view_name, queries, options), to: CouchdbAdapter.Fetchers
  defdelegate find(repo, schema, params), to: CouchdbAdapter.Fetchers
  defdelegate find(repo, schema, params, options), to: CouchdbAdapter.Fetchers


end
