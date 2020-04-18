defmodule Couchdb.Ecto.Helpers do

  def server_from_config(config) do
    config |> Keyword.get(:couchdb_url) |> ICouch.server_connection
  end
  def db_from_config(config) do
    config |> server_from_config |> ICouch.DB.new(config |> Keyword.get(:database))
  end
  def view_from_config(config, ddoc, view_name, params \\ []) do
    %ICouch.View{db: db_from_config(config), ddoc: ddoc, name: view_name, params: params}
  end

  def server_from_repo(repo) do
    server_from_config(repo.config)
  end
  def db_from_repo(repo) do
    db_from_config(repo.config)
  end
  def view_from_repo(repo, ddoc, view_name, params \\ []) do
    view_from_config(repo.config, ddoc, view_name, params)
  end

  @spec ddoc_name(Ecto.Schema.schema()) :: String.t
  def ddoc_name(%{schema: schema}), do: schema.__schema__(:source)
  def ddoc_name(module), do: module.__schema__(:source)

  @spec split_ddoc_view(Ecto.Schema.schema(), {String.t, String.t} | String.t) :: {String.t, String.t}
  def split_ddoc_view(_schema, {ddoc, view_name}), do: {ddoc, view_name}
  def split_ddoc_view(schema, view_name), do: {ddoc_name(schema), view_name}

end
