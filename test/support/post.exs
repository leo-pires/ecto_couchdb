defmodule Post do
  use Ecto.Schema

  @primary_key false
  @foreign_key_type :binary_id
  schema "Post" do
    field :_id, :binary_id, autogenerate: true, primary_key: true
    field :_rev, :string, read_after_writes: true, primary_key: true
    field :type, :string, read_after_writes: true
    field :title, :string
    field :body, :string
    belongs_to :user, User, references: :_id, on_replace: :update
    embeds_many :grants, Grant, on_replace: :delete
    embeds_one :stats, Stats, on_replace: :delete
    timestamps()
  end

  def changeset(struct, params) do
    struct
    |> Ecto.Changeset.cast(params, [:title, :body, :user_id])
  end

  def changeset_user(struct, params) do
    struct
    |> Ecto.Changeset.cast(params, [:title, :body])
    |> Ecto.Changeset.cast_assoc(:user)
  end

end
