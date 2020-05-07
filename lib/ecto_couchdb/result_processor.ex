defmodule Couchdb.Ecto.ResultProcessor do
  alias Couchdb.Ecto.Fetchers

  # TODO: typespec
  def process_result(result_type, result, repo, schema_map, opts) do
    payload = %{
      repo: repo,
      schema_map: schema_map,
      prefix: opts |> Keyword.get(:prefix),
      preloads: opts |> Keyword.get(:preload, []) |> normalize_preloads,
      return_keys: opts |> Keyword.get(:return_keys, false)
    }
    process(result_type, result, payload)
  end

  # TODO: typespec
  def check_schema_map(map, schema_map_fun) when is_function(schema_map_fun) do
    case schema_map_fun.(map) do
      nil -> raise "Invalid schema mapping"
      schema -> schema
    end
  end
  def check_schema_map(_map, schema) do
    schema
  end

  # TODO: typespec
  def normalize_preloads(f) when is_atom(f), do: [strict_normalize_preloads(f)]
  def normalize_preloads(o), do: strict_normalize_preloads(o)

  ###

  defp process(:get, doc, payload) do
    doc |> process_doc(payload)
  end
  defp process(:get_many, docs, payload) do
    docs |> Enum.map(&(process_doc(&1, payload)))
  end
  defp process(:all, %ICouch.View{} = view, payload) do
    process_rows(view.rows, payload)
  end
  defp process(:multiple_all, %{results: results}, payload) do
    results |> Enum.map(&(process_rows(&1.rows, payload)))
  end
  defp process(:find, %{docs: docs, bookmark: bookmark} = result, payload) do
    %{docs: process_docs(docs, payload), bookmark: bookmark, warning: Map.get(result, :warning)}
  end
  defp process(:search, %{docs: docs, bookmark: bookmark, total_rows: total_rows}, payload) do
    %{docs: process_docs(docs, payload), bookmark: bookmark, total_rows: total_rows}
  end

  ###

  defp process_rows(rows, payload) when is_list(rows), do: rows |> Enum.map(&(process_row(&1, payload)))

  defp process_row(%{"key" => key, "doc" => doc}, payload) do
    row_result(key, doc |> process_doc(payload), payload)
  end
  defp process_row(%{"key" => key, "value" => value}, payload) do
    row_result(key, value, payload)
  end
  # TODO: refactor return_keys to mimics db return
  defp row_result(key, returning, %{return_keys: true}), do: {key, returning}
  defp row_result(_key, returning, %{return_keys: false}), do: returning

  defp process_docs(docs, payload) when is_list(docs), do: docs |> Enum.map(&(process_doc(&1, payload)))

  defp process_doc(%ICouch.Document{} = doc, payload) do
    regular_fields = doc.fields |> Map.keys |> Enum.reduce([], fn
      "_attachments", acc ->
        acc
      field_name, acc ->
        value = doc |> ICouch.Document.get(field_name)
        [{field_name, value} |> process_field(payload) | acc]
    end)
    attachments_fields = doc.attachment_order |> Enum.reduce([], fn attachment_name, acc ->
      [{attachment_name, doc |> process_attachment(attachment_name)} | acc]
    end)
    (regular_fields ++ attachments_fields) |> Map.new |> postprocess_doc(payload)
  end
  defp process_doc(value, _payload) do
    value
  end

  defp process_field(nil, _), do: nil
  defp process_field(map, payload) when is_map(map), do: map |> process_doc(payload)
  defp process_field(list, payload) when is_list(list), do: list |> Enum.map(&(&1 |> process_doc(payload)))
  defp process_field(raw, _payload), do: raw

  defp process_attachment(doc, attachment_name) do
    case doc |> ICouch.Document.get_attachment(attachment_name) do
      {%{"content_type" => content_type, "revpos" => revpos}, data} ->
        %{content_type: content_type, revpos: revpos, data: data}
      _other -> :error
    end
  end

  defp postprocess_doc(map, %{schema_map: :raw}) do
    map
  end
  defp postprocess_doc(map, %{schema_map: schema_map_fun} = payload) when is_function(schema_map_fun) do
    schema = check_schema_map(map, schema_map_fun)
    postprocess_doc(map, payload |> Map.put(:schema_map, schema))
  end
  defp postprocess_doc(map, %{schema_map: schema, prefix: prefix} = payload) do
    loader = &Ecto.Type.adapter_load(Couchdb.Ecto, &1, &2)
    Ecto.Schema.Loader.unsafe_load(schema, map, loader)
    |> apply_prefix(prefix)
    |> inject_preloads(payload)
  end
  defp apply_prefix(%{__meta__: meta} = data, prefix) do
    %{data | __meta__: %{meta | prefix: prefix}}
  end

  ###

  defp strict_normalize_preloads(f) when is_atom(f), do: {f, []}
  defp strict_normalize_preloads({f, l}), do: {f, normalize_preloads(l)}
  defp strict_normalize_preloads(l) when is_list(l), do: l |> Enum.map(&(strict_normalize_preloads(&1)))

  defp inject_preloads(nil, _), do: nil
  defp inject_preloads(map, %{preloads: []}), do: map
  defp inject_preloads(map, %{repo: repo, schema_map: schema_map, preloads: preloads}) do
    schema = check_schema_map(map, schema_map)
    to_inject = preloads |> Enum.reduce([], fn ({preload_assoc, preload_inject}, acc) ->
      association = schema.__schema__(:association, preload_assoc)
      case map |> Map.get(association.owner_key) |> inject_preload(repo, preload_inject, association) do
        nil -> acc
        to_add -> [{association.field, to_add} | acc]
      end
    end)
    |> Map.new
    Map.merge(map, to_inject)
  end

  defp inject_preload(nil, _repo, _preload, _association), do: nil
  defp inject_preload(value, repo, preload, %Ecto.Association.BelongsTo{related: related_schema}) do
    {:ok, fetched} = Fetchers.get(repo, related_schema, value, [])
    fetched |> inject_preloads(%{repo: repo, schema_map: related_schema, preloads: preload})
  end
  defp inject_preload(value, repo, preload, %Ecto.Association.Has{cardinality: :one, queryable: queryable}) do
    {ddoc_view, related_schema} = queryable
    {:ok, fetched} = Fetchers.one(repo, related_schema, ddoc_view, [key: value, include_docs: true])
    fetched |> inject_preloads(%{repo: repo, schema_map: related_schema, preloads: preload})
  end
  defp inject_preload(value, repo, preload, %Ecto.Association.Has{cardinality: :many, queryable: queryable}) do
    {ddoc_view, related_schema} = queryable
    {:ok, fetched} = Fetchers.all(repo, related_schema, ddoc_view, [key: value, include_docs: true])
    fetched |> Enum.map(&(&1 |> inject_preloads(%{repo: repo, schema_map: related_schema, preloads: preload})))
  end
  defp inject_preload(_, _, _, association) do
    raise "Unsupported preload type #{inspect association}"
  end

end
