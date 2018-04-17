defmodule Post do
  use Ecto.Schema
  use Couchdb.Design

  @primary_key false
  @foreign_key_type :binary_id
  schema "posts" do
    field :_id, :binary_id, autogenerate: true, primary_key: true
    field :_rev, :string, read_after_writes: true, primary_key: true
    field :type, :string, read_after_writes: true
    field :title, :string
    field :body, :string
    belongs_to :user, User, references: :_id, on_replace: :update
    embeds_many :grants, Grant, on_replace: :delete
    embeds_one :stats, Stats, on_replace: :delete
    timestamps()

    designs do
      design __MODULE__ do
        view :by_title, [:string]
        view :all, [:string]
      end
      design "secondary" do
        view :by_other, [:string]
      end
    end

    def changeset(struct, params) do
      struct
      |> Ecto.Changeset.cast(params, [:title, :body])
      |> Ecto.Changeset.cast_assoc(:user)
    end
  end
end
