defmodule Couchdb.Ecto do

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Storage


  defmacro __before_compile__(_env), do: :ok

  def child_spec(_repo, _options) do
    Supervisor.Spec.supervisor(Supervisor, [[], [strategy: :one_for_one]])
  end

  def ensure_all_started(_repo, _type) do
    {:ok, [:icouch]}
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
    db = repo |> db_from_repo
    type = ddoc_name(schema_meta)
    with doc = prepare_for_couch(type, fields, schema_meta.schema),
         {:ok, doc} <- db |> ICouch.save_doc(doc)
    do
      {:ok, doc |> prepare_for_returning(returning)}
    else
      {:error, :conflict} ->
        {:invalid, [unique: "#{type}_id_index"]}
      {:error, reason} ->
        raise "Error while inserting (#{inspect(reason)})"
    end
  end

  def update(repo, schema_meta, fields, filters, returning, options) do
    db = repo |> db_from_repo
    type = ddoc_name(schema_meta)
    with {:ok, fetched_doc} <- do_fetch_for_update(db, filters),
         changes = prepare_for_couch(type, fetched_doc, fields, schema_meta.schema),
         {:ok, doc} <- db |> ICouch.save_doc(changes)
    do
      {:ok, doc |> prepare_for_returning(returning)}
    else
      {:error, :not_found} ->
        {:error, :stale}
      {:error, :stale} ->
        case handle_conflict(db, filters, options) do
          {:ok, new_filters} -> update(repo, schema_meta, fields, new_filters, returning, options)
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

  def delete(repo, schema_meta, filters, options) do
    db = repo |> db_from_repo
    id = filters |> Keyword.get(:_id)
    rev = filters |> Keyword.get(:_rev)
    with {:ok, _doc} <- db |> ICouch.delete_doc(%{"_id" => id, "_rev" => rev})
    do
      {:ok, []}
    else
      {:error, :not_found} ->
        {:ok, []}
      {:error, :conflict} ->
        case handle_conflict(repo, filters, options) do
          {:ok, new_filters} -> delete(repo, schema_meta, new_filters, options)
          other -> other
        end
        {:error, :stale}
      {:error, reason} ->
        raise "Error while deleting (#{inspect(reason)})"
    end
  end

  def insert_all(repo, schema_meta, _header, list, _on_conflict, _returning, _options) do
    db = repo |> db_from_repo
    type = ddoc_name(schema_meta)
    with docs = list |> Enum.map(&(prepare_for_couch(type, &1, schema_meta.schema))),
         {:ok, docs} <- db |> ICouch.save_docs(docs)
    do
      {docs |> length, nil}
    else
      {:error, reason} -> raise "Error while inserting all (#{inspect(reason)})"
    end
  end

  def prepare(_atom, _query) do
    raise "Unsupported operation by #{__MODULE__}: prepare"
  end

  def execute(_repo, _query_meta, _query, _params, _arg4, _options) do
    raise "Unsupported operation by #{__MODULE__}: execute"
  end

  def server_connection_from_repo(repo) do
    repo.config |> Keyword.get(:couchdb_url) |> ICouch.server_connection
  end

  def db_from_repo(repo) do
    server_connection_from_repo(repo) |> ICouch.DB.new(repo.config |> Keyword.get(:database))
  end

  def view_from_repo(repo, ddoc, view_name, params \\ []) do
    %ICouch.View{db: db_from_repo(repo), ddoc: ddoc, name: view_name, params: params}
  end

  # TODO: não deveria ser type from_schema_meta?
  @spec ddoc_name(Ecto.Adapter.schema_meta | Ecto.Adapter.query_meta) :: String.t
  def ddoc_name(%{schema: schema}), do: schema.__schema__(:source)
  def ddoc_name(module), do: module.__schema__(:source)

  defp prepare_for_couch(type, existing_doc \\ ICouch.Document.new, new_fields, schema_meta) do
    {attachments, regular_fields} = split_attachments(existing_doc, new_fields, schema_meta)
    attachments = prepare_attachments(attachments)
    [{:type, type}, {:_attachments, attachments} | regular_fields] |> Enum.reduce(existing_doc, fn {k, v}, doc ->
      doc |> ICouch.Document.put(k |> Atom.to_string, v)
    end)
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

  defp prepare_for_returning(%ICouch.Document{} = doc, returning) do
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
