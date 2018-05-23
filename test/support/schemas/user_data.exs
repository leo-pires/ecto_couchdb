defmodule UserData do
  use Ecto.Schema

  @primary_key false
  @foreign_key_type :binary_id
  schema "UserData" do
    field :_id, :binary_id, primary_key: true, autogenerate: false
    field :_rev, :string, primary_key: true, read_after_writes: true
    field :type, :string, read_after_writes: true
    field :extra, :string
    belongs_to :user, User, references: :_id
    timestamps()

    def changeset(struct, params) do
      struct
      |> Ecto.Changeset.cast(params, [:_id, :extra])
    end
  end
end
