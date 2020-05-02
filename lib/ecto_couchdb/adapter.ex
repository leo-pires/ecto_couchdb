defmodule Couchdb.Ecto do
  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Schema
  @behaviour Ecto.Adapter.Storage

  import Couchdb.Ecto.Helpers


  ###
  # Adapter behaviour
  ###

  @impl true
  defmacro __before_compile__(_env), do: :ok

  @impl true
  def ensure_all_started(_repo, _type), do: {:ok, [:icouch]}

  @impl true
  def init(config) do
    child_spec = Supervisor.Spec.supervisor(Supervisor, [[], [strategy: :one_for_one]])
    server = server_from_config(config)
    db = if server, do: server |> db_from_config(config)
    adapter_meta = %{server: server, db: db}
    {:ok, child_spec, adapter_meta}
  end

  @impl true
  def checkout(_adapter_meta, _config, function), do: function.()

  @impl true
  def dumpers(_primitive_type, type), do: [type]

  @impl true
  def loaders(:utc_datetime_usec, type), do: [&load_datetime/1, type]
  def loaders(:utc_datetime, type), do: [&load_datetime/1, type]
  def loaders(:naive_datetime_usec, type), do: [&load_naive_datetime/1, type]
  def loaders(:naive_datetime, type), do: [&load_naive_datetime/1, type]
  def loaders(:date, type), do: [&load_date/1, type]
  def loaders(:time_usec, type), do: [&load_time/1, type]
  def loaders(:time, type), do: [&load_time/1, type]
  def loaders(_primitive_type, type), do: [type]
  defp load_datetime(datetime_str) do
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

  ###
  # Schema behaviour
  ###

  @impl true
  def autogenerate(:id),        do: raise "Unsupported id type"
  def autogenerate(:binary_id), do: nil
  def autogenerate(:embed_id),  do: Ecto.UUID.generate()

  @impl true
  def insert(%{db: db}, schema_meta, fields, _on_conflict, returning, _options) do
    type = ddoc_name(schema_meta)
    doc = prepare_for_couch(type, fields, schema_meta.schema)
    case db |> ICouch.save_doc(doc) do
      {:ok, doc} -> {:ok, doc |> prepare_for_return(returning)}
      {:error, :conflict} ->
        {:invalid, [unique: "#{type}_id_index"]}
      {:error, reason} ->
        raise "Error while inserting (#{inspect(reason)})"
    end
  end

  @impl true
  def update(%{db: db} = adapter_meta, schema_meta, fields, filters, returning, options) do
    type = ddoc_name(schema_meta)
    with {:ok, fetched_doc} <- do_fetch_for_update(db, filters),
         changes = prepare_for_couch(type, fetched_doc, fields, schema_meta.schema),
         {:ok, doc} <- db |> ICouch.save_doc(changes)
    do
      {:ok, doc |> prepare_for_return(returning)}
    else
      {:error, :not_found} ->
        {:error, :stale}
      {:error, :stale} ->
        case handle_conflict(db, filters, options) do
          {:ok, new_filters} -> update(adapter_meta, schema_meta, fields, new_filters, returning, options)
          other -> other
        end
      {:error, reason} ->
        raise "Error while updating (#{inspect(reason)})"
    end
  end
  defp do_fetch_for_update(db, filters) do
    id = filters[:_id]
    rev = filters[:_rev]
    case db |> ICouch.open_doc(id) do
      {:ok, %ICouch.Document{rev: ^rev} = doc} -> {:ok, doc}
      {:ok, _doc} -> {:error, :stale}
      other -> other
    end
  end

  @impl true
  def delete(%{db: db} = adapter_meta, schema_meta, filters, options) do
    id = filters |> Keyword.get(:_id)
    rev = filters |> Keyword.get(:_rev)
    case db |> ICouch.delete_doc(%{"_id" => id, "_rev" => rev}) do
      {:ok, _doc} -> {:ok, []}
      {:error, :not_found} ->
        {:ok, []}
      {:error, :conflict} ->
        case handle_conflict(db, filters, options) do
          {:ok, new_filters} -> delete(adapter_meta, schema_meta, new_filters, options)
          other -> other
        end
        {:error, :stale}
      {:error, reason} ->
        raise "Error while deleting (#{inspect(reason)})"
    end
  end

  @impl true
  def insert_all(%{db: db}, schema_meta, _header, list, _on_conflict, _returning, _options) do
    type = ddoc_name(schema_meta)
    docs = list |> Enum.map(&(prepare_for_couch(type, &1, schema_meta.schema)))
    case db |> ICouch.save_docs(docs) do
      {:ok, docs} -> {docs |> length, nil}
      {:error, reason} -> raise "Error while inserting all (#{inspect(reason)})"
    end
  end

  defp prepare_for_couch(type, existing_doc \\ ICouch.Document.new, new_fields, schema_meta) do
    {attachments, regular_fields} = split_attachments(existing_doc, new_fields, schema_meta)
    attachments = prepare_attachments(attachments)
    [{:type, type}, {:_attachments, attachments} | regular_fields] |> Enum.reduce(existing_doc, fn {k, v}, doc ->
      doc |> ICouch.Document.put(k |> Atom.to_string, v)
    end)
  end

  defp prepare_for_return(%ICouch.Document{} = doc, returning) do
    returning |> Enum.map(fn field -> {field, doc |> ICouch.Document.get(field |> Atom.to_string, :error)} end)
  end

  defp handle_conflict(db, filters, options) do
    id = filters[:_id]
    if Keyword.get(options, :on_conflict) == :replace_all do
      case db |> ICouch.get_doc_rev(id) do
        {:ok, rev} -> {:ok, filters |> Keyword.put(:_rev, rev)}
        _other -> {:error, :stale}
      end
    else
      {:error, :stale}
    end
  end

  defp split_attachments(fetched_doc, all_fields, schema) do
    # split attachment and fields
    {new_attachments, new_fields} = all_fields |> Enum.split_with(fn {k, _} ->
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
    attachments |> Enum.reduce(%{}, fn
      {_, nil}, acc -> acc
      {k, v}, acc -> Map.put(acc, k |> to_string, prepare_attachment(v))
    end)
  end
  # TODO: handle attachments with ICouch
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

  ##
  # Storage behaviour
  ##

  @impl true
  def storage_status(config) do
    case config |> server_from_config |> db_from_config(config) |> ICouch.db_info do
      {:ok, _info} -> :up
      {:error, :not_found} -> :down
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def storage_up(config) do
    database_name = config |> Keyword.get(:database)
    case config |> server_from_config |> ICouch.create_db(database_name) do
      {:ok, _db} -> :ok
      {:error, :precondition_failed} -> {:error, :already_up}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def storage_down(config) do
    case config |> server_from_config |> db_from_config(config) |> ICouch.delete_db do
      :ok -> :ok
      {:error, :not_found} -> {:error, :already_down}
      {:error, reason} -> {:error, reason}
    end
  end

end
