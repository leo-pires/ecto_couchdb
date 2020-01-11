defmodule Couchdb.Ecto do

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Storage


  defmacro __before_compile__(_env), do: :ok

  def child_spec(_repo, _options) do
    Supervisor.Spec.supervisor(Supervisor, [[], [strategy: :one_for_one]])
  end

  def ensure_all_started(_repo, _type) do
    {:ok, []}
  end

  def autogenerate(:id),        do: nil
  def autogenerate(:binary_id), do: nil
  def autogenerate(:embed_id),  do: Ecto.UUID.generate()

  def loaders(:utc_datetime, type), do: [&load_utc_datetime/1, type]
  def loaders(:naive_datetime, type), do: [&load_naive_datetime/1, type]
  def loaders(:date, type), do: [&load_date/1, type]
  def loaders(:time, type), do: [&load_time/1, type]
  def loaders(_, type), do: [type]
  defp load_utc_datetime(datetime_str) do
    case DateTime.from_iso8601(datetime_str) do
      {:ok, datetime, _} -> {:ok, datetime}
      _ -> :error
    end
  end
  defp load_naive_datetime(datetime_str) do
    case NaiveDateTime.from_iso8601(datetime_str) do
      {:ok, datetime} -> {:ok, datetime}
      _ -> :error
    end
  end
  defp load_date(date_str) do
    case Date.from_iso8601(date_str) do
      {:ok, date} -> {:ok, date}
      _ -> :error
    end
  end
  defp load_time(time_str) do
    case Time.from_iso8601(time_str) do
      {:ok, time} -> {:ok, time}
      _ -> :error
    end
  end
  def dumpers(:utc_datetime, type), do: [type, &dump_utc_datetime/1]
  def dumpers(:naive_datetime, type), do: [type, &dump_naive_datetime/1]
  def dumpers(:date, type), do: [type, &dump_date/1]
  def dumpers(:time, type), do: [type, &dump_time/1]
  def dumpers(_, type), do: [type]
  defp dump_utc_datetime({{_, _, _} = dt, {h, m, s, ms}}) do
    case NaiveDateTime.from_erl({dt, {h, m, s}}, {ms, 6}) do
      {:ok, naive_datetime} ->
        {:ok, datetime} = DateTime.from_naive(naive_datetime, "Etc/UTC")
        {:ok, datetime |> DateTime.to_iso8601}
      _ -> :error
    end
  end
  defp dump_naive_datetime({{_, _, _} = dt, {h, m, s, ms}}) do
    case NaiveDateTime.from_erl({dt, {h, m, s}}, {ms, 6}) do
      {:ok, datetime} -> {:ok, datetime |> NaiveDateTime.to_iso8601}
      _ -> :error
    end
  end
  defp dump_date(dt) do
    case Date.from_erl(dt) do
      {:ok, date} -> {:ok, date}
      _ -> :error
    end
  end
  defp dump_time({h, m, s, ms}) do
    case Time.from_erl({h, m, s}, {ms, 0}) do
      {:ok, time} -> {:ok, time |> Time.to_iso8601}
      _ -> :error
    end
  end

  def insert(repo, schema_meta, fields, _on_conflict, returning, _options) do
    db_props = db_props_for(repo)
    type = ddoc_name(schema_meta)
    {id, doc} =
      prepare_for_couch(%{}, fields, schema_meta.schema)
      |> Map.put("type", type)
      |> Map.pop("_id")
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
         {:ok, %{payload: payload}} <- do_update(db_props, fetched_doc, fields, schema_meta)
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
  defp do_update(db_props, fetched_doc, updated_fields, schema_meta) do
    updated_doc = prepare_for_couch(fetched_doc, updated_fields, schema_meta.schema)
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
    prepared = list |> do_prepare_insert_all(type, schema_meta.schema)
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
  defp do_prepare_insert_all(list, type, schema) do
    list |> Enum.map(&(prepare_for_couch(%{}, &1, schema) |> Map.put("type", type)))
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
    username = Keyword.get(config, :username) || Keyword.get(config, :user)
    password = Keyword.get(config, :password)
    props = %{protocol: protocol, hostname: hostname, port: port, database: database}
    if username && password do
      props |> Map.merge(%{user: username, password: password})
    else
      props
    end
  end

  def server_connection_from_repo(repo) do
    repo.config |> Keyword.get(:couchdb_url) |> ICouch.server_connection
  end

  def db_from_repo(repo) do
    server_connection_from_repo(repo) |> ICouch.DB.new(repo.config |> Keyword.get(:database))
  end

  @spec ddoc_name(Ecto.Adapter.schema_meta | Ecto.Adapter.query_meta) :: String.t
  def ddoc_name(%{schema: schema}), do: schema.__schema__(:source)
  def ddoc_name(module), do: module.__schema__(:source)

  @spec prepare_for_couch(map, keyword | map, Ecto.Adapter.schema_meta) :: map
  defp prepare_for_couch(fetched_doc, new_fields, schema_meta) do
    {attachments, new_fields} = split_attachments(fetched_doc, new_fields, schema_meta)
    attachments = prepare_attachments(attachments)
    new_fields = new_fields |> Enum.map(fn {k, v} -> {k |> to_string, v} end) |> Map.new
    fetched_doc
    |> Map.merge(new_fields)
    |> Map.merge(%{"_attachments" => attachments})
  end

  defp split_attachments(fetched_doc, all_fields, schema) do
    # split attachment and fields
    {new_attachments, new_fields} =
      all_fields
      |> Enum.split_with(fn {k, _} ->
          schema.__schema__(:type, k) == Couchdb.Ecto.Attachment
         end)
    # merge existing attachments and new ones
    old_attachments = fetched_doc["_attachments"] || %{}
    new_attachments_names = new_attachments |> Enum.map(fn {k, _} -> k |> to_string end)
    all_attachments =
      old_attachments
      |> Map.drop(new_attachments_names)
      |> Map.merge(Enum.into(new_attachments, %{}))
    # return
    {all_attachments, new_fields}
  end
  defp prepare_attachments(attachments) do
    attachments
    |> Enum.reduce(%{}, fn
         ({_, nil}, acc) -> acc
         ({k, v}, acc) -> Map.put(acc, k |> to_string, prepare_attachment(v))
       end)
  end
  defp prepare_attachment(%{"content_type" => content_type, "stub" => true}) do
    do_prepare_attachment(content_type, nil)
  end
  defp prepare_attachment(%{content_type: content_type, data: data}) do
    do_prepare_attachment(content_type, data)
  end
  defp do_prepare_attachment(content_type, nil) do
    %{"content_type" => content_type, "stub" => true}
  end
  defp do_prepare_attachment(content_type, data) do
    %{"content_type" => content_type, "data" => data}
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

  ##
  # Storage behaviour
  ##

  def storage_up(options) do
    repo_wrap = %{config: options}
    case repo_wrap |> Couchdb.Ecto.Storage.create_db do
      {:ok, true} -> :ok
      {:ok, false} -> {:error, :already_up}
      {:error, reason} -> {:error, reason}
    end
  end

  def storage_down(options) do
    repo_wrap = %{config: options}
    case repo_wrap |> Couchdb.Ecto.Storage.delete_db do
      {:ok, true} -> :ok
      {:ok, false} -> {:error, :already_down}
      {:error, reason} -> {:error, reason}
    end
  end

end
