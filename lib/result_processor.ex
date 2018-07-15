defmodule CouchdbAdapter.ResultProcessor do

  alias CouchdbAdapter.Fetchers

  def process_result(type, result, repo, schema, opts) do
    preloads = opts |> Keyword.get(:preload, []) |> normalize_preloads
    return_keys = opts |> Keyword.get(:return_keys, false)
    as_map = opts |> Keyword.get(:as_map, false)

    processor =
      case type do
        :get -> &process_get/2
        :fetch_all -> &process_fetch_all/2
        :multiple_fetch_all -> &process_multiple_fetch_all/2
        :find -> &process_find/2
      end

    payload =
      %{
        repo: repo,
        schema: schema,
        preloads: preloads,
        return_keys: return_keys,
        as_map: as_map
      }

    result |> processor.(payload)
  end

  ###

  def process_get(row, payload) when is_map(row) do
    row |> process_doc(payload)
  end

  def process_fetch_all(%{"rows" => rows} = result, payload) when is_list(rows) do
    process_rows(result, payload)
  end

  def process_multiple_fetch_all(%{"results" => results}, payload) when is_list(results) do
    results
    |> Enum.map(fn result ->
         process_fetch_all(result, payload)
       end)
  end

  def process_find(%{"docs" => docs, "bookmark" => bookmark} = result, payload) when is_list(docs) do
    %{
      docs: process_docs(result, payload),
      bookmark: bookmark,
      warning: Map.get(result, "warning")
    }
  end

  ###

  def process_rows(%{"rows" => rows}, %{return_keys: return_keys} = payload) when is_list(rows) do
    rows
    |> Enum.map(fn (%{"key" => key, "value" => value}) ->
         value
         |> process_doc(payload)
         |> prepare_row_result(key, return_keys)
       end)
  end
  defp prepare_row_result(doc, key, true), do: {key, doc}
  defp prepare_row_result(doc, _, false), do: doc

  def process_docs(%{"docs" => docs}, payload) when is_list(docs) do
    docs |> Enum.map(&(process_doc(&1, payload)))
  end

  def process_doc(values, payload) when is_list(values) do
    values |> Enum.map(&(process_doc(&1, payload)))
  end
  def process_doc(map, %{as_map: as_map} = payload) when is_map(map) do
    map
    |> Map.keys
    |> Enum.reduce([], fn raw_field, acc ->
         raw_value = map |> Map.get(raw_field)
         field = raw_field |> field_name(as_map)
         [{field, raw_value} |> process_field(payload) | acc]
       end)
    |> Map.new
    |> postprocess_doc(payload)
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
  defp process_field(raw, _) do
    raw
  end

  defp postprocess_doc(map, payload) do
    map
    |> postprocess_wrap(payload)
    |> inject_preloads(payload)
  end

  defp postprocess_wrap(map, %{as_map: v}) when v in [true, :raw] do
    map
  end
  defp postprocess_wrap(map, %{as_map: false, schema: schema}) do
    Ecto.Repo.Schema.load(CouchdbAdapter, schema, map)
  end

  defp field_name(field_str, :raw), do: field_str
  defp field_name(field_str, b) when is_boolean(b), do: field_str |> String.to_atom

  ###

  def inject_preloads(nil, _), do: nil
  def inject_preloads(map, %{preloads: []}), do: map
  def inject_preloads(map, %{repo: repo, schema: schema, preloads: preloads}) do
    to_inject =
      preloads
      |> Enum.reduce([], fn ({preload_assoc, preload_inject}, acc) ->
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
    fetched
    |> inject_preloads(%{repo: repo, schema: related_schema, preloads: preload})
  end
  defp inject_preload(value, repo, preload, %Ecto.Association.Has{cardinality: :one, queryable: queryable}) do
    {view_name, related_schema} = related_view(queryable)
    {:ok, fetched} = Fetchers.fetch_one(repo, related_schema, view_name, key: value, include_docs: true)
    fetched
    |> inject_preloads(%{repo: repo, schema: related_schema, preloads: preload})
  end
  defp inject_preload(value, repo, preload, %Ecto.Association.Has{cardinality: :many, queryable: queryable}) do
    {view_name, related_schema} = related_view(queryable)
    {:ok, fetched} = Fetchers.fetch_all(repo, related_schema, view_name, key: value, include_docs: true)
    fetched
    |> Enum.map(&(&1 |> inject_preloads(%{repo: repo, schema: related_schema, preloads: preload})))
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
