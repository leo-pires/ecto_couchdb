# TODO: typespec
defmodule Couchdb.Ecto.Helpers do

  def server_from_config(config, try_session? \\ false) do
    url = config |> Keyword.get(:couchdb_url)
    server = url |> ICouch.server_connection
    if try_session? do
      try_session(server, url)
    else
      server
    end
  end
  defp try_session(orig_server, url) do
    try do
      {user, password} = orig_server |> ICouch.Server.credentials
      body = %{name: user, password: password} |> Poison.encode!
      headers = [{"Content-Type", "application/json"}]
      {:ok, {resp_headers, _resp_body}} =
        orig_server
        |> ICouch.Server.send_raw_req("_session", :post, body, headers)
      {_k, auth_session} = resp_headers |> Enum.find(fn {k, _v} ->
        k |> to_string |> String.match?(~r/^set-cookie$/i)
      end)
      [_, session] = Regex.run(~r/^AuthSession=(.+?);/, auth_session |> to_string)
      cookie = session |> to_string
      ICouch.server_connection(url, cookie: cookie)
    rescue
      _error ->
        orig_server
    end
  end

  def server_from_repo(repo) do
    {_repo, %{server: server}} = Ecto.Repo.Registry.lookup(repo)
    server
  end

  def db_from_config(server, config) do
    server |> ICouch.DB.new(config |> Keyword.get(:database))
  end

  def db_from_repo(repo) do
    {_repo, %{db: db}} = Ecto.Repo.Registry.lookup(repo)
    db
  end

  def view_from_db(db, ddoc, view_name, params \\ []) do
    %ICouch.View{db: db, ddoc: ddoc, name: view_name, params: params}
  end

  @spec ddoc_name(Ecto.Schema.schema()) :: String.t
  def ddoc_name(%{schema: schema}), do: schema.__schema__(:source)
  def ddoc_name(module), do: module.__schema__(:source)

  @spec split_ddoc_view(Ecto.Schema.schema(), {String.t, String.t} | String.t) :: {String.t, String.t}
  def split_ddoc_view(_schema, {ddoc, view_name}), do: {ddoc, view_name}
  def split_ddoc_view(:raw, _view_name), do: raise "Invalid ddoc_view, for :raw use {ddoc, view_name}"
  def split_ddoc_view(schema, view_name), do: {ddoc_name(schema), view_name}

end
