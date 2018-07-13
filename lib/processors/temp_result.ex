defmodule CouchdbAdapter.TempResultProcessor do

  def process_result(%{"results" => rows}, cast_fun, pp_fun, payload) when is_list(rows) do
    rows
    |> Enum.map(fn (%{"rows" => rows} when is_list(rows)) ->
         rows
         |> Enum.map(fn (%{"value" => value}) ->
              case value do
                value when is_map(value) -> value |> process_doc(cast_fun, pp_fun, payload)
                _ -> value
              end
            end)
       end)
  end
  def process_result(%{"docs" => docs}, cast_fun, pp_fun, payload) when is_list(docs) do
    docs |> Enum.map(&(&1 |> process_doc(cast_fun, pp_fun, payload)))
  end
  def process_result(map, cast_fun, pp_fun, payload) when is_map(map) do
    map |> process_doc(cast_fun, pp_fun, payload)
  end
  def process_result_map(map, _cast_fun, _pp_fun, _payload) when is_map(map) do
    map
  end

  def process_result_keys(%{"results" => rows}) when is_list(rows) do
    rows
    |> Enum.map(fn (%{"rows" => rows} when is_list(rows)) ->
         rows
         |> Enum.map(fn (%{"key" => key}) -> key end)
       end)
  end

  def process_doc(nil, _cast_fun, _pp_fun, _payload), do: nil
  def process_doc(values, cast_fun, pp_fun, payload) when is_list(values) do
    values |> Enum.map(&(process_doc(&1, cast_fun, pp_fun, payload)))
  end
  def process_doc(map, cast_fun, pp_fun, payload) when is_map(map) do
    map
    |> Map.keys
    |> Enum.reduce([], fn (raw_field, acc) ->
       raw_value = map |> Map.get(raw_field)
         [{raw_field, raw_value} |> process_field(cast_fun, pp_fun, payload) | acc]
       end)
    |> Map.new
    |> pp_fun.(payload)
  end

  def process_field({raw_field, nil}, _cast_fun, _pp_fun, _payload), do: {raw_field, nil}
  def process_field({raw_field, raw_value}, cast_fun, _pp_fun, payload) do
    {raw_field, raw_value} |> cast_fun.(payload)
  end

  # Identity cast and post processor functions

  def identity_process_result(map, atomize \\ false) when is_map(map) do
    map |> process_result_map(&identity_cast_fun/2, &identity_cast_pp/2, %{atomize: atomize})
  end

  def identity_cast_fun({field, map}, %{atomize: atomize}) when is_map(map) do
    {field |> identity_field_name(atomize), map |> identity_process_result(atomize)}
  end
  def identity_cast_fun({field, list}, %{atomize: atomize}) when is_list(list) do
    {field |> identity_field_name(atomize), list |> Enum.map(&(&1 |> identity_process_result(atomize)))}
  end
  def identity_cast_fun({field, raw}, %{atomize: atomize}) do
    {field |> identity_field_name(atomize), raw}
  end

  def identity_cast_pp(map, _payload), do: map

  def identity_field_name(field_str, true), do: field_str |> String.to_atom
  def identity_field_name(field_str, false), do: field_str

  # Process Couchbeam results to opinated Ecto structs

  def ecto_process_result(result, repo, schema, preloads) do
    result |> process_result(&ecto_cast_fun/2, &ecto_pp_fun/2, %{repo: repo, schema: schema, preloads: preloads})
  end

  def ecto_process_doc(result, payload) do
    result |> process_doc(&ecto_cast_fun/2, &ecto_pp_fun/2, payload)
  end

  def ecto_cast_fun({field_str, raw_value}, %{schema: schema, repo: repo}) do
     field = field_str |> String.to_atom
     type = schema.__schema__(:type, field)
     if is_nil(type), do: raise "Field #{field} doesnt exists in #{schema}"
     value =
       case type do
         {:embed, %{related: related_schema}} ->
           raw_value |> ecto_process_doc(%{repo: repo, schema: related_schema, preloads: []})
         {:array, _} ->
           raw_value
         :map ->
           raw_value |> identity_process_result
         _ ->
          case Ecto.Type.cast(type, raw_value) do
            {:ok, result} -> result
            :error -> raise "Invalid cast for #{field} (#{inspect raw_value})"
          end
       end
    {field, value}
  end

  def ecto_pp_fun(map, %{repo: repo, schema: schema, preloads: preloads}) do
    Kernel.struct(schema, map |> CouchdbAdapter.Processors.Helper.inject_preloads(repo, schema, preloads))
  end

end
