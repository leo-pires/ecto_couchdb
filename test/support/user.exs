defmodule User do
  use Ecto.Schema

  @primary_key false
  @foreign_key_type :binary_id
  schema "User" do
    field :_id, :binary_id, primary_key: true, autogenerate: false
    field :_rev, :string, primary_key: true, read_after_writes: true
    field :type, :string, read_after_writes: true

    field :username, :string
    field :email, :string
    has_many :posts, {"by_user_id", Post}, references: :_id
    has_one :user_data, {"by_user_id", UserData}, references: :_id, foreign_key: :user_id
    timestamps()
  end

  def changeset(struct, params) do
    struct
    |> Ecto.Changeset.cast(params, [:_id, :username, :email])
  end

  def changeset_user_data(struct, params) do
    struct
    |> Ecto.Changeset.cast(params, [:_id, :username, :email])
    |> Ecto.Changeset.cast_assoc(:user_data)
  end

end
