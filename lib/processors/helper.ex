defmodule CouchdbAdapter.Processors.Helper do

  # TODO: better api when using as_map and fetch_keys
  def process_result(data, processor, repo, schema, preloads) do
    normalized_preloads = preloads |> normalize_preloads
    process_result(data, processor, repo, schema, normalized_preloads, false)
  end
  def process_result(data, processor, _repo, :map, _preloads, false) do
    data |> processor.identity_process_result(true)
  end
  def process_result(data, processor, _repo, :map, _preloads, true) do
    data |> processor.process_result_keys
  end
  def process_result(data, processor, repo, schema, preloads, false) do
    data |> processor.ecto_process_result(repo, schema, preloads)
  end

  def inject_preloads(map, _repo, _schema, [] = _preloads), do: map
  def inject_preloads(nil, _repo, _schema, _preloads), do: nil
  def inject_preloads(map, repo, schema, preloads) do
    to_inject =
      preloads
      |> Enum.reduce([], fn ({preload_assoc, preload_inject}, acc) ->
          case schema.__schema__(:association, preload_assoc) do
            %Ecto.Association.BelongsTo{owner_key: fk, related: related_schema, field: field} ->
              value = Map.get(map, fk)
              if value do
                to_add = CouchdbAdapter.get(repo, related_schema, value) |> inject_preloads(repo, related_schema, preload_inject)
                [{field, to_add} | acc]
              else
                acc
              end
            %Ecto.Association.Has{owner_key: fk, queryable: queryable, cardinality: cardinality, field: field} ->
              {view_name, related_schema} =
                case queryable do
                  {view_name_str, related_schema} -> {view_name_str |> String.to_atom, related_schema}
                  _ -> raise "Invalid queryable (#{inspect queryable}), should be (\"view_name\", schema)"
                end
              value = Map.get(map, fk)
              if value do
                to_add =
                  case cardinality do
                    :one ->
                      CouchdbAdapter.fetch_one(repo, related_schema, view_name, key: value, include_docs: true) |> inject_preloads(repo, related_schema, preload_inject)
                    :many ->
                      CouchdbAdapter.fetch_all(repo, related_schema, view_name, key: value, include_docs: true)
                      |> Enum.map(&(&1 |> inject_preloads(repo, related_schema, preload_inject)))
                  end
                [{field, to_add} | acc]
              else
                acc
              end
            _ ->
              raise "Unsupported preload type for #{preload_assoc}"
          end
        end)
      |> Map.new
    Map.merge(map, to_inject)
  end

  def normalize_preloads(f) when is_atom(f), do: [strict_normalize_preloads(f)]
  def normalize_preloads(o), do: strict_normalize_preloads(o)
  defp strict_normalize_preloads(f) when is_atom(f), do: {f, []}
  defp strict_normalize_preloads({f, l}), do: {f, normalize_preloads(l)}
  defp strict_normalize_preloads(l) when is_list(l), do: l |> Enum.map(&(strict_normalize_preloads(&1)))

end