defmodule Couchdb.Ecto.Attachment do

  alias Couchdb.Ecto.Attachment

  @behaviour Ecto.Type

  @enforce_keys [:content_type, :data]
  defstruct [:content_type, :data, :revpos]


  def type, do: :map

  def cast(%Attachment{} = data) do
    {:ok, data}
  end
  def cast(%{content_type: content_type, data: data}) do
    {:ok, %Attachment{content_type: content_type, data: data}}
  end
  def cast(%{"content_type" => content_type, "data" => data}) do
    {:ok, %Attachment{content_type: content_type, data: data}}
  end
  def cast(_), do: :error

  def dump(%Attachment{content_type: "application/json", data: data}) do
    case Poison.encode(data) do
      {:ok, json} -> do_dump("application/json", json)
      {:error, _} -> :error
    end
  end
  def dump(%Attachment{content_type: content_type, data: data}) do
    do_dump(content_type, data)
  end
  def dump(_), do: :error
  defp do_dump(content_type, data) do
    try do
      {:ok, %{content_type: content_type, data: data |> Base.encode64}}
    rescue
      _ -> :error
    end
  end

  def load(%{"content_type" => content_type, "data" => data, "revpos" => revpos}) do
    %Attachment{content_type: content_type, data: data, revpos: revpos}
    |> do_load
  end
  def load(%{"content_type" => content_type, "revpos" => revpos}) do
    {:ok, %Attachment{content_type: content_type, data: nil, revpos: revpos}}
  end
  defp do_load(%Attachment{content_type: "application/json", data: data} = attachment) do
    with {:ok, base_decoded} <- Base.decode64(data),
         {:ok, json_decoded} <- Poison.decode(base_decoded)
    do
      {:ok, Map.put(attachment, :data, json_decoded)}
    else
      _ -> :error
    end
  end
  defp do_load(%Attachment{data: data} = attachment) do
    with {:ok, base_decoded} <- Base.decode64(data) do
      {:ok, Map.put(attachment, :data, base_decoded)}
    else
      _ -> :error
    end
  end

end
