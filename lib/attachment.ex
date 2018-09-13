defmodule CouchdbAdapter.Attachment do

  alias CouchdbAdapter.Attachment

  @behaviour Ecto.Type

  @enforce_keys [:content_type, :data]
  defstruct [:content_type, :data, :revpos, :digest, :length, :stub]


  def type, do: :map

  def cast(%Attachment{} = data) do
    {:ok, data}
  end
  def cast(%{content_type: content_type, data: data}) do
    {:ok, %Attachment{content_type: content_type, data: data}}
  end
  def cast(_), do: :error

  def dump(%Attachment{content_type: "application/json", data: data}) do
    case Poison.encode(data) do
      {:ok, json} -> {:ok, do_dump("application/json", json)}
      {:error, _} -> :error
    end
  end
  def dump(%Attachment{content_type: content_type, data: data}) do
    {:ok, do_dump(content_type, data)}
  end
  def dump(_), do: :error
  defp do_dump(content_type, data) do
    %{type: :couch_attachment, content_type: content_type, data: data |> Base.encode64}
  end

  def load(data) do
    IO.inspect(["load", data])
    {:ok, data}
  end

end
