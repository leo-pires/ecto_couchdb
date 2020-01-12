defmodule Couchdb.Ecto.ResultProcessor do

  alias Couchdb.Ecto.Fetchers

  def process_result(result_type, result, repo, schema, opts) do
    payload = %{
      repo: repo, schema: schema,
      preloads: opts |> Keyword.get(:preload, []) |> normalize_preloads,
      return_keys: opts |> Keyword.get(:return_keys, false),
      as_map: opts |> Keyword.get(:as_map, false)
    }
    process(result_type, result, payload)
  end

  ###

  def process(:get, row, payload) do
    row |> process_doc(payload)
  end
  def process(:fetch_all, %ICouch.View{} = view, payload) do
    process(:fetch_all, %{"rows" => view.rows}, payload)
  end
  def process(:fetch_all, %{"rows" => _rows} = result, payload) do
    process_rows(result, payload)
  end
  def process(:multiple_fetch_all, %{"results" => results}, payload) do
    results |> Enum.map(fn result ->
      process(:fetch_all, result, payload)
    end)
  end
  def process(:find, %{"docs" => _docs, "bookmark" => bookmark} = result, payload) do
    %{
      docs: process_docs(result, payload),
      bookmark: bookmark,
      warning: Map.get(result, "warning")
    }
  end

  ###

  def process_rows(%{"rows" => rows}, payload) when is_list(rows) do
    rows |> Enum.map(&(process_row(&1, payload)))
  end

  defp process_row(%{"key" => key, "doc" => doc}, payload) do
    process_row(key, doc, payload)
  end
  defp process_row(%{"key" => key, "value" => doc}, payload) do
    process_row(key, doc, payload)
  end
  defp process_row(%{"id" => key, "doc" => doc}, payload) do
    process_row(key, doc, payload)
  end
  defp process_row(key, doc, payload) do
    doc |> process_doc(payload) |> prepare_row_result(key, payload)
  end
  defp prepare_row_result(doc, key, %{return_keys: true}), do: {key, doc}
  defp prepare_row_result(doc, _, %{return_keys: false}), do: doc

  def process_docs(%{"docs" => docs}, payload) when is_list(docs) do
    docs |> Enum.map(&(process_doc(&1, payload)))
  end

  def process_doc(values, payload) when is_list(values) do
    values |> Enum.map(&(process_doc(&1, payload)))
  end
  def process_doc(%ICouch.Document{} = doc, payload) do
    as_map = payload[:as_map]
    regular_fields = doc.fields |> Map.keys |> Enum.reduce([], fn
      "_attachments", acc ->
        acc
      raw_field_name, acc ->
        field_name = raw_field_name |> field_name(as_map)
        value = doc |> ICouch.Document.get(raw_field_name)
        [{field_name, value} |> process_field(payload) | acc]
    end)
    attachments_fields = doc.attachment_order |> Enum.reduce([], fn raw_attachment_name, acc ->
      [{raw_attachment_name |> field_name(as_map), doc |> process_attachment(raw_attachment_name)} | acc]
    end)
    (regular_fields ++ attachments_fields) |> Map.new |> postprocess_doc(payload)
  end
  def process_doc(value, _payload) do
    value
  end

  defp process_field(nil, _) do
    nil
  end
  defp process_field(map, payload) when is_map(map) do
    map |> process_doc(payload)
  end
  defp process_field(list, payload) when is_list(list) do
    list |> Enum.map(&(&1 |> process_doc(payload)))
  end
  defp process_field(raw, _payload) do
    raw
  end

  defp process_attachment(doc, raw_attachment_name) do
    case doc |> ICouch.Document.get_attachment(raw_attachment_name) do
      {%{"content_type" => content_type, "revpos" => revpos}, data} ->
        %{content_type: content_type, revpos: revpos, data: data}
      _other -> :error
    end
  end

  defp postprocess_doc(map, payload) do
    map |> postprocess_wrap(payload) |> inject_preloads(payload)
  end

  defp postprocess_wrap(map, %{as_map: v}) when v in [true, :raw] do
    map
  end
  defp postprocess_wrap(map, %{as_map: false, schema: schema}) do
    Ecto.Repo.Schema.load(Couchdb.Ecto, schema, map)
  end

  defp field_name(field_str, :raw), do: field_str
  defp field_name(field_str, b) when is_boolean(b), do: field_str |> String.to_atom

  ###

  def inject_preloads(nil, _), do: nil
  def inject_preloads(map, %{preloads: []}), do: map
  def inject_preloads(map, %{repo: repo, schema: schema, preloads: preloads}) do
    to_inject = preloads |> Enum.reduce([], fn ({preload_assoc, preload_inject}, acc) ->
      association = schema.__schema__(:association, preload_assoc)
      to_add = map |> Map.get(association.owner_key) |> inject_preload(repo, preload_inject, association)
      if to_add do
        [{association.field, to_add} | acc]
      else
        acc
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

  def normalize_preloads(f) when is_atom(f), do: [strict_normalize_preloads(f)]
  def normalize_preloads(o), do: strict_normalize_preloads(o)
  defp strict_normalize_preloads(f) when is_atom(f), do: {f, []}
  defp strict_normalize_preloads({f, l}), do: {f, normalize_preloads(l)}
  defp strict_normalize_preloads(l) when is_list(l), do: l |> Enum.map(&(strict_normalize_preloads(&1)))

end
