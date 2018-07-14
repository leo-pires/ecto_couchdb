defmodule CouchdbAdapter.EctoCast do

  alias CouchdbAdapter.{ResultProcessor, IdentityCast}


  def process_doc(result, payload) do
    result |> ResultProcessor.process_doc(&cast_fun/2, &pp_fun/2, payload)
  end

  def cast_fun({field_str, raw_value}, %{schema: schema, repo: repo}) do
     field = field_str |> String.to_atom
     type = schema.__schema__(:type, field)
     if is_nil(type), do: raise "Field #{field} doesnt exists in #{schema}"
     value =
       case type do
         {:embed, %{related: related_schema}} ->
           raw_value |> process_doc(%{repo: repo, schema: related_schema, preloads: []})
         {:array, _} ->
           raw_value
         :map ->
           raw_value |> IdentityCast.process_doc(%{atomize: false})
         _ ->
          case Ecto.Type.cast(type, raw_value) do
            {:ok, result} -> result
            :error -> raise "Invalid cast for #{field} (#{inspect raw_value})"
          end
       end
    {field, value}
  end

  def pp_fun(map, %{repo: repo, schema: schema, preloads: preloads}) do
    Kernel.struct(schema, map |> ResultProcessor.inject_preloads(repo, schema, preloads))
  end

end
