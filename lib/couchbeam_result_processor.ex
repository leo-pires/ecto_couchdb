defmodule CouchdbAdapter.CouchbeamResultProcessor do

  # Generic functions to process Couchbeam results

  def process_result(row, cast_fun, pp_fun, payload) when is_tuple(row) do
    row |> process_doc(cast_fun, pp_fun, payload)
  end
  def process_result(rows, cast_fun, pp_fun, payload) when is_list(rows) do
    rows
    |> Enum.map(fn ({[{"id", _id}, {"key", _key}, {"value", value}]}) ->
         value |> process_doc(cast_fun, pp_fun, payload)
       end)
  end

  def process_doc(:null, _cast_fun, _pp_fun, _payload), do: nil
  def process_doc(values, cast_fun, pp_fun, payload) when is_list(values) do
    values |> Enum.map(&(process_doc(&1, cast_fun, pp_fun, payload)))
  end
  def process_doc({fields}, cast_fun, pp_fun, payload) do
    fields
    |> Enum.reduce([], fn ({raw_field, raw_value}, acc) ->
         [{raw_field, raw_value} |> process_field(cast_fun, pp_fun, payload) | acc]
       end)
    |> Map.new
    |> pp_fun.(payload)
  end

  def process_field({raw_field, :null}, _cast_fun, _pp_fun, _payload), do: {raw_field, nil}
  def process_field({raw_field, raw_value}, cast_fun, _pp_fun, payload) do
    {raw_field, raw_value} |> cast_fun.(payload)
  end

  # Identity cast and post processor functions

  def identity_process_result(result) when is_tuple(result) do
    result |> process_doc(&identity_cast_fun/2, &identity_cast_pp/2, nil)
  end

  def identity_cast_fun({field, tuple}, _payload) when is_tuple(tuple) and is_list(elem(tuple, 0)) do
    {field, tuple |> identity_process_result}
  end
  def identity_cast_fun({field, list}, _payload) when is_list(list) and is_tuple(hd(list)) do
    {field, list |> Enum.map(&(&1 |> identity_process_result))}
  end
  def identity_cast_fun({field, raw}, _payload) do
    {field, raw}
  end

  def identity_cast_pp(map, _payload), do: map

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
          raise "Not implemented... yet"
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
    Kernel.struct(schema, map |> CouchdbAdapter.inject_preloads(repo, schema, preloads))
  end

end
