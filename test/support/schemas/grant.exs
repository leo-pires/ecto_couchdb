defmodule Grant do
  use Ecto.Schema
  embedded_schema do
    field :user, :string
    field :access, :string
  end
  def changeset(struct, params) do
    struct
    |> Ecto.Changeset.cast(params, [:user, :access])
  end
end
