# TODO: tratamento de transaction?
# TODO: pooling?

defmodule CouchdbAdapter do

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Storage

  @default_pool_options [max_connections: 20, timeout: 10_000]


  defmacro __before_compile__(_env), do: nil

  # TODO: esse pool está funcionando?
  def child_spec(repo, _options) do
    :hackney_pool.child_spec(repo, pool_config(repo.config))
  end
  defp pool_config(config) do
    config_options = Keyword.take(config, [:max_connections, :timeout])
    @default_pool_options |> Keyword.merge(config_options)
  end

  def ensure_all_started(_repo, type) do
    Application.ensure_all_started([:hackney], type)
  end

  # TODO: raise para id?
  def autogenerate(:id),        do: nil
  def autogenerate(:binary_id), do: nil
  def autogenerate(:embed_id),  do: Ecto.UUID.generate()

  def loaders(:naive_datetime, type), do: [&load_naive_datetime/1, type]
  def loaders(:date, type), do: [&load_date/1, type]
  def loaders(:time, type), do: [&load_time/1, type]
  def loaders(_, type), do: [type]
  defp load_naive_datetime(datetime), do: {:ok, datetime |> NaiveDateTime.from_iso8601!}
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
    db_props = db_props_for(repo)
    type = ddoc_name(meta)
    {id, doc} = fields |> prepare_for_couch(type) |> Map.pop("_id")
    with {:ok, %{payload: payload}} <- do_create(db_props, doc, id)
    do
      payload |> prepare_for_returning(type, returning)
    else
      {:error, %{payload: %{"error" => "conflict"}}} ->
        {:invalid, [unique: "#{type}_id_index"]}
      {:error, reason} ->
        raise "Error while inserting (#{inspect(reason)})"
    end
  end
  defp do_create(db_props, doc, nil) do
    Couchdb.Connector.create_generate(db_props, doc)
  end
  defp do_create(db_props, doc, id) do
    Couchdb.Connector.create(db_props, doc, id)
  end

  def update(repo, schema_meta, fields, filters, returning, options) do
    db_props = db_props_for(repo)
    type = ddoc_name(schema_meta)
    with {:ok, fetched_doc} <- do_fetch_for_update(db_props, filters),
         {:ok, %{payload: payload}} <- do_update(db_props, fetched_doc, fields)
    do
      payload |> prepare_for_returning(type, returning)
    else
      {:error, %{"error" => "not_found"}} ->
        {:error, :stale}
      {:error, :stale} ->
        with {:ok, new_filters} <- handle_conflict(repo, filters, options)
        do
          update(repo, schema_meta, fields, new_filters, returning, options)
        end
      {:error, reason} ->
        raise "Error while updating (#{inspect(reason)})"
    end
  end
  defp do_fetch_for_update(db_props, filters) do
    with {:ok, doc} <- Couchdb.Connector.get(db_props, filters[:_id])
    do
      if doc["_rev"] == filters[:_rev] do
        {:ok, doc}
      else
        {:error, :stale}
      end
    end
  end
  defp do_update(db_props, fetched_doc, updated_fields) do
    updated_doc =
      fetched_doc
      |> Map.merge(updated_fields |> prepare_for_couch)
    db_props |> Couchdb.Connector.update(updated_doc)
  end

  def delete(repo, schema_meta, filters, options) do
    db_props = db_props_for(repo)
    type = ddoc_name(schema_meta)
    id = filters |> Keyword.get(:_id)
    rev = filters |> Keyword.get(:_rev)
    with {:ok, %{payload: payload}} <- do_delete(db_props, id, rev)
    do
      # TODO: check what delete should return
      payload |> prepare_for_returning(type, [])
    else
      {:error, %{payload: %{"error" => "not_found"}}} ->
        {:ok, []}
      {:error, %{payload: %{"error" => "conflict"}}} ->
        with {:ok, new_filters} <- handle_conflict(repo, filters, options)
        do
          delete(repo, schema_meta, new_filters, options)
        end
        {:error, :stale}
      {:error, reason} ->
        raise "Error while deleting (#{inspect(reason)})"
    end
  end
  defp do_delete(db_props, id, rev) do
    Couchdb.Connector.destroy(db_props, id, rev)
  end

  # TODO: check what to return in second parameter
  def insert_all(repo, schema_meta, _header, list, _on_conflict, returning, _options) do
    db_props = db_props_for(repo)
    type = ddoc_name(schema_meta)
    prepared = list |> do_prepare_insert_all(type)
    with {:ok, %{payload: data}} <- do_insert_all(db_props, prepared)
    do
      {_return, count} =
        data
        |> Enum.map_reduce(0, fn (doc_return, acc) ->
             case doc_return |> prepare_for_returning(nil, returning) do
               {:ok, return_kw} -> {return_kw, acc + 1}
               {:error, _} -> {[], acc}
             end
           end)
      {count, nil}
    else
      {:error, reason} ->
        raise "Error while deleting (#{inspect(reason)})"
    end
  end
  defp do_prepare_insert_all(list, type) do
    list |> Enum.map(&(&1 |> prepare_for_couch(type)))
  end
  defp do_insert_all(db_props, list) do
    Couchdb.Connector.bulk_docs(db_props, list)
  end

  def prepare(_atom, _query) do
    raise "Unsupported operation in CouchdbAdapter: prepare"
  end

  def execute(_repo, _query_meta, _query, _params, _arg4, _options) do
    raise "Unsupported operation in CouchdbAdapter: execute"
  end

  @spec db_props_for(Ecto.Repo.t) :: Types.db_properties
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

  @spec prepare_for_couch(keyword | map) :: map
  defp prepare_for_couch(fields) do
    fields |> Enum.map(fn {k, v} -> {k |> to_string, v} end) |> Map.new
  end
  @spec prepare_for_couch(keyword | map, String.t) :: map
  defp prepare_for_couch(fields, type) when is_list(fields) do
    prepare_for_couch(fields) |> Map.put("type", type)
  end

  @spec prepare_for_returning(keyword, String.t | nil, keyword) :: keyword
  defp prepare_for_returning(%{"ok" => true} = payload, type, returning) do
    %{_id: payload["id"], _rev: payload["rev"], type: type}
    |> do_prepare_for_returning(returning)
  end
  defp prepare_for_returning(_, _, _) do
    {:error, nil}
  end
  defp do_prepare_for_returning(data, returning) do
    return =
      returning
      |> Enum.map(&({&1, Map.get(data, &1)}))
      |> Keyword.new
    {:ok, return}
  end

  defp handle_conflict(repo, filters, options) do
    if Keyword.get(options, :on_conflict) == :replace_all do
      case db_props_for(repo) |> Couchdb.Connector.get(filters[:_id]) do
        {:ok, doc} ->
          rev = doc["_rev"]
          {:ok, filters |> Keyword.put(:_rev, rev)}
        _ ->
          {:error, :stale}
      end
    else
      {:error, :stale}
    end
  end

  # Storage behaviour
  def storage_up(options) do
    Application.ensure_all_started(:hackney)
    repo_wrap = %{config: options}
    case repo_wrap |> CouchdbAdapter.Storage.create_db do
      {:ok, true} -> :ok
      {:ok, false} -> {:error, :already_up}
      {:error, reason} -> {:error, reason}
    end
  end

  def storage_down(options) do
    Application.ensure_all_started(:hackney)
    repo_wrap = %{config: options}
    case repo_wrap |> CouchdbAdapter.Storage.delete_db do
      {:ok, true} -> :ok
      {:ok, false} -> {:error, :already_down}
      {:error, reason} -> {:error, reason}
    end
  end

end
