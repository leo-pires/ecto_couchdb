defmodule Couchdb.Ecto.Attachment do
  @behaviour Ecto.Type

  @enforce_keys [:content_type, :data]
  defstruct [:content_type, :data]


  @impl true
  def type, do: :map

  @impl true
  def cast(%__MODULE__{} = data) do
    {:ok, data}
  end
  def cast(%{"content_type" => content_type, "data" => data}) do
    {:ok, %__MODULE__{content_type: content_type, data: data}}
  end
  def cast(%{content_type: content_type, data: data}) do
    {:ok, %__MODULE__{content_type: content_type, data: data}}
  end
  def cast(_), do: :error

  @impl true
  def dump(%__MODULE__{content_type: "application/json", data: data}) do
    case Jason.encode(data) do
      {:ok, json} -> do_dump("application/json", json)
      {:error, _} -> :error
    end
  end
  def dump(%__MODULE__{content_type: content_type, data: data}) do
    do_dump(content_type, data)
  end
  def dump(_other), do: :error
  defp do_dump(content_type, data) do
    {:ok, %{content_type: content_type, data: data |> Base.encode64}}
  end

  @impl true
  def load(%{content_type: content_type, data: data}) do
    %__MODULE__{content_type: content_type, data: data} |> do_load
  end
  defp do_load(%__MODULE__{content_type: "application/json", data: data} = attachment) when not is_nil(data) do
    case Jason.decode(data) do
      {:ok, json} -> {:ok, Map.put(attachment, :data, json)}
      _error -> :error
    end
  end
  defp do_load(attachment) do
    {:ok, attachment}
  end

  @impl true
  def equal?(term1, term2), do: term1 == term2

  @impl true
  def embed_as(_format), do: raise "#{__MODULE__} cannot be embeded"

end
