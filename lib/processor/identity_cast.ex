defmodule CouchdbAdapter.IdentityCast do

  alias CouchdbAdapter.ResultProcessor


  def process_doc(map, payload \\ %{}) do
    map |> ResultProcessor.process_doc(&cast_fun/2, &cast_pp/2, payload)
  end

  def cast_fun({field, map}, %{atomize: atomize} = payload) when is_map(map) do
    {field |> identity_field_name(atomize), map |> process_doc(payload)}
  end
  def cast_fun({field, list}, %{atomize: atomize} = payload) when is_list(list) do
    {field |> identity_field_name(atomize), list |> Enum.map(&(&1 |> process_doc(payload)))}
  end
  def cast_fun({field, raw}, %{atomize: atomize}) do
    {field |> identity_field_name(atomize), raw}
  end

  # TODO: preload
  def cast_pp(map, _payload), do: map

  defp identity_field_name(field_str, true), do: field_str |> String.to_atom
  defp identity_field_name(field_str, false), do: field_str

end
