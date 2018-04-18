defmodule User do
  use Ecto.Schema
  use Couchdb.Design

  @primary_key false
  schema "User" do
    field :_id, :binary_id, primary_key: true, autogenerate: false
    field :_rev, :string, primary_key: true, read_after_writes: true
    field :type, :string, read_after_writes: true
    field :username, :string
    field :email, :string
    timestamps()

    designs do
      design __MODULE__ do
        view :all, [:string]
      end
    end

    def changeset(struct, params) do
      struct
      |> Ecto.Changeset.cast(params, [:_id, :username, :email])
    end
  end
end
