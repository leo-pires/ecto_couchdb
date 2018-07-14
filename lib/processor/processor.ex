defmodule CouchdbAdapter.ResultProcessor do

  alias CouchdbAdapter.{EctoCast, IdentityCast}
  alias Ecto.Association.{BelongsTo, Has}

  def process_result(type, result, repo, schema, opts) do
    preloads = opts |> Keyword.get(:preload, []) |> normalize_preloads
    return_keys = opts |> Keyword.get(:return_keys, false)
    as_map = opts |> Keyword.get(:as_map, false)
    # get processor
    processor =
      case type do
        :get -> &process_get/4
        :fetch_all -> &process_fetch_all/4
        :multiple_fetch_all -> &process_multiple_fetch_all/4
        :find -> &process_find/4
      end
    # get cast, pp functions and payload
    # TODO: transformar em behaviour?
    {cast_fun, pp_fun, payload} =
      case as_map do
        true ->
          {&IdentityCast.cast_fun/2, &IdentityCast.cast_pp/2, %{atomize: true}}
        :raw ->
          {&IdentityCast.cast_fun/2, &IdentityCast.cast_pp/2, %{atomize: false}}
        _ ->
          {&EctoCast.cast_fun/2, &EctoCast.pp_fun/2, %{repo: repo, schema: schema, preloads: preloads}}
      end
    payload = payload |> Map.put(:return_keys, return_keys)
    # call according processor and cast functions
    result |> processor.(cast_fun, pp_fun, payload)
  end

  ###

  def process_get(row, cast_fun, pp_fun, payload) when is_map(row) do
    row |> process_doc(cast_fun, pp_fun, payload)
  end

  def process_fetch_all(%{"rows" => rows} = result, cast_fun, pp_fun, payload) when is_list(rows) do
    process_rows(result, cast_fun, pp_fun, payload)
  end

  def process_multiple_fetch_all(%{"results" => results}, cast_fun, pp_fun, payload) when is_list(results) do
    results
    |> Enum.map(fn result ->
         process_fetch_all(result, cast_fun, pp_fun, payload)
       end)
  end

  def process_find(%{"docs" => docs, "bookmark" => bookmark} = result, cast_fun, pp_fun, payload) when is_list(docs) do
    %{
      docs: process_docs(result, cast_fun, pp_fun, payload),
      bookmark: bookmark,
      warning: Map.get(result, "warning")
    }
  end

  ###

  def process_rows(%{"rows" => rows}, cast_fun, pp_fun, %{return_keys: return_keys} = payload) when is_list(rows) do
    rows
    |> Enum.map(fn (%{"key" => key, "value" => value}) ->
         value
         |> process_doc(cast_fun, pp_fun, payload)
         |> prepare_row_result(key, return_keys)
       end)
  end
  defp prepare_row_result(doc, key, true), do: {key, doc}
  defp prepare_row_result(doc, _, false), do: doc

  def process_docs(%{"docs" => docs}, cast_fun, pp_fun, payload) when is_list(docs) do
    docs |> Enum.map(&(process_doc(&1, cast_fun, pp_fun, payload)))
  end

  def process_doc(values, cast_fun, pp_fun, payload) when is_list(values) do
    values |> Enum.map(&(process_doc(&1, cast_fun, pp_fun, payload)))
  end
  def process_doc(map, cast_fun, pp_fun, payload) when is_map(map) do
    map
    |> Map.keys
    |> Enum.reduce([], fn raw_field, acc ->
       raw_value = map |> Map.get(raw_field)
         [{raw_field, raw_value} |> process_field(cast_fun, pp_fun, payload) | acc]
       end)
    |> Map.new
    |> pp_fun.(payload)
  end
  def process_doc(value, _cast_fun, _pp_fun, _payload) do
    value
  end

  def process_field({raw_field, nil}, _cast_fun, _pp_fun, _payload) do
    {raw_field, nil}
  end
  def process_field({raw_field, raw_value}, cast_fun, _pp_fun, payload) do
    {raw_field, raw_value} |> cast_fun.(payload)
  end

  ###

  def inject_preloads(nil, _, _, _), do: nil
  def inject_preloads(map, _, _, [] = _preloads), do: map
  def inject_preloads(map, repo, schema, preloads) do
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
  defp inject_preload(value, repo, preload, %BelongsTo{related: related_schema}) do
    CouchdbAdapter.get(repo, related_schema, value)
    |> inject_preloads(repo, related_schema, preload)
  end
  defp inject_preload(value, repo, preload, %Has{cardinality: :one, queryable: queryable}) do
    {view_name, related_schema} = related_view(queryable)
    CouchdbAdapter.fetch_one(repo, related_schema, view_name, key: value, include_docs: true)
    |> inject_preloads(repo, related_schema, preload)
  end
  defp inject_preload(value, repo, preload, %Has{cardinality: :many, queryable: queryable}) do
    {view_name, related_schema} = related_view(queryable)
    CouchdbAdapter.fetch_all(repo, related_schema, view_name, key: value, include_docs: true)
    |> Enum.map(&(&1 |> inject_preloads(repo, related_schema, preload)))
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
