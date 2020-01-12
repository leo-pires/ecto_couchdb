defmodule Couchdb.Ecto.ResultProcessor do

  alias Couchdb.Ecto.Fetchers

  def process_result(result_type, result, repo, schema, opts) do
    payload = %{
      repo: repo, schema: schema,
      preloads: opts |> Keyword.get(:preload, []) |> normalize_preloads,
      return_keys: opts |> Keyword.get(:return_keys, false)
    }
    process(result_type, result, payload)
  end

  ###

  def process(:get, doc, payload) do
    doc |> process_doc(payload)
  end
  def process(:fetch_all, %ICouch.View{} = view, payload) do
    process_rows(view.rows, payload)
  end
  def process(:multiple_fetch_all, %{results: results}, payload) do
    results |> Enum.map(&(process_rows(&1.rows, payload)))
  end
  def process(:find, %{docs: docs, bookmark: bookmark} = result, payload) do
    %{docs: process_docs(docs, payload), bookmark: bookmark, warning: Map.get(result, :warning)}
  end

  ###

  def process_rows(rows, payload) when is_list(rows), do: rows |> Enum.map(&(process_row(&1, payload)))

  defp process_row(%{"key" => _key, "doc" => doc}, payload), do: doc |> process_doc(payload)
  defp process_row(%{"key" => key, "value" => value}, %{return_keys: true}), do: {key, value}
  defp process_row(%{"key" => _key, "value" => value}, _payload), do: value

  def process_docs(docs, payload) when is_list(docs), do: docs |> Enum.map(&(process_doc(&1, payload)))

  def process_doc(%ICouch.Document{} = doc, payload) do
    regular_fields = doc.fields |> Map.keys |> Enum.reduce([], fn
      "_attachments", acc ->
        acc
      raw_field_name, acc ->
        field_name = raw_field_name |> field_name
        value = doc |> ICouch.Document.get(raw_field_name)
        [{field_name, value} |> process_field(payload) | acc]
    end)
    attachments_fields = doc.attachment_order |> Enum.reduce([], fn raw_attachment_name, acc ->
      [{raw_attachment_name |> field_name, doc |> process_attachment(raw_attachment_name)} | acc]
    end)
    (regular_fields ++ attachments_fields) |> Map.new |> postprocess_doc(payload)
  end
  def process_doc(value, _payload) do
    value
  end
  defp field_name(field_str), do: field_str |> String.to_atom

  defp process_field(nil, _), do: nil
  defp process_field(map, payload) when is_map(map), do: map |> process_doc(payload)
  defp process_field(list, payload) when is_list(list), do: list |> Enum.map(&(&1 |> process_doc(payload)))
  defp process_field(raw, _payload), do: raw

  defp process_attachment(doc, raw_attachment_name) do
    case doc |> ICouch.Document.get_attachment(raw_attachment_name) do
      {%{"content_type" => content_type, "revpos" => revpos}, data} ->
        %{content_type: content_type, revpos: revpos, data: data}
      _other -> :error
    end
  end

  defp postprocess_doc(map, payload), do: map |> postprocess_load(payload) |> inject_preloads(payload)

  defp postprocess_load(map, %{schema: schema}), do: Ecto.Repo.Schema.load(Couchdb.Ecto, schema, map)

  ###

  def normalize_preloads(f) when is_atom(f), do: [strict_normalize_preloads(f)]
  def normalize_preloads(o), do: strict_normalize_preloads(o)
  defp strict_normalize_preloads(f) when is_atom(f), do: {f, []}
  defp strict_normalize_preloads({f, l}), do: {f, normalize_preloads(l)}
  defp strict_normalize_preloads(l) when is_list(l), do: l |> Enum.map(&(strict_normalize_preloads(&1)))

  defp inject_preloads(nil, _), do: nil
  defp inject_preloads(map, %{preloads: []}), do: map
  defp inject_preloads(map, %{repo: repo, schema: schema, preloads: preloads}) do
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

  defp inject_preload(nil, _, _, _), do: nil
  defp inject_preload(value, repo, preload, %Ecto.Association.BelongsTo{related: related_schema}) do
    {:ok, fetched} = Fetchers.get(repo, related_schema, value)
    fetched |> inject_preloads(%{repo: repo, schema: related_schema, preloads: preload})
  end
  defp inject_preload(value, repo, preload, %Ecto.Association.Has{cardinality: :one, queryable: queryable}) do
    {view_name, related_schema} = related_view(queryable)
    {:ok, fetched} = Fetchers.fetch_one(repo, related_schema, view_name, key: value, include_docs: true)
    fetched |> inject_preloads(%{repo: repo, schema: related_schema, preloads: preload})
  end
  defp inject_preload(value, repo, preload, %Ecto.Association.Has{cardinality: :many, queryable: queryable}) do
    {view_name, related_schema} = related_view(queryable)
    {:ok, fetched} = Fetchers.fetch_all(repo, related_schema, view_name, key: value, include_docs: true)
    fetched |> Enum.map(&(&1 |> inject_preloads(%{repo: repo, schema: related_schema, preloads: preload})))
  end
  defp inject_preload(_, _, _, association), do: raise "Unsupported preload type #{inspect association}"

  defp related_view({view_name_str, related_schema}), do: {view_name_str |> String.to_atom, related_schema}
  defp related_view(queryable), do: "Invalid queryable (#{inspect queryable}), should be (\"view_name\", schema)"

end
