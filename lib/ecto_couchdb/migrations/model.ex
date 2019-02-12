# TODO: receber log com parâmetro para testes sem output

defmodule Couchdb.Ecto.Migration.MigrationModel do
  use Ecto.Schema
  alias Ecto.Changeset
  alias Couchdb.Ecto.Migration.MigrationModel

  @primary_key false
  @foreign_key_type :binary_id
  schema "CouchDbEctoMigration" do
    field :_id, :binary_id, primary_key: true, autogenerate: false
    field :_rev, :string, primary_key: true, read_after_writes: true
    field :type, :string, read_after_writes: true

    field :version, :integer

    timestamps()
  end

  def changeset(version) do
    %MigrationModel{}
    |> Changeset.change(version: version)
    |> Changeset.put_change(:_id, generate_id(version))
  end

  def generate_id(version), do: "#{__MODULE__.__schema__(:source)}:#{version}"
	
end
