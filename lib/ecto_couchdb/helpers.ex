defmodule Couchdb.Ecto.Helpers do

  @spec ddoc_doc_id(ddoc :: String.t()) :: String.t()
  def ddoc_doc_id(ddoc), do: "_design/#{ddoc}"

  @spec server_from_config(config :: any()) :: ICouch.Server.t() | nil
  def server_from_config(config) do
    case config |> Keyword.get(:couchdb_url) do
      nil -> nil
      url -> url |> ICouch.server_connection
    end
  end

  @spec try_session(orig_server :: ICouch.Server.t(), url :: String.t() | URI.t()) :: ICouch.Server.t()
  def try_session(orig_server, url) do
    try do
      {user, password} = orig_server |> ICouch.Server.credentials
      body = %{name: user, password: password} |> Jason.encode!
      headers = [{"Content-Type", "application/json"}]
      {:ok, {resp_headers, _resp_body}} =
        orig_server |> ICouch.Server.send_raw_req("_session", :post, body, headers)
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

  @spec db_from_config(server :: ICouch.Server.t(), config :: any()) :: ICouch.Db.t()
  def db_from_config(server, config) do
    db_name = config |> Keyword.get(:database)
    prefix = config |> Keyword.get(:prefix)
    db_with_prefix(server, db_name, prefix)
  end

  @spec db_from_repo(repo :: Ecto.Repo.t(), opts :: Keyword.t()) :: ICouch.Db.t()
  def db_from_repo(repo, opts \\ []) do
    {_repo, %{server: server}} = Ecto.Repo.Registry.lookup(repo)
    repo_config = repo.config
    db_name = repo_config |> Keyword.get(:database)
    prefix = Keyword.get(opts, :prefix) || Keyword.get(repo.config, :prefix)
    db_with_prefix(server, db_name, prefix)
  end

  @spec db_from_meta(adapter_meta :: Ecto.Adapter.Schema.adapter_meta(), schema_meta :: Ecto.Adapter.Schema.schema_meta()) :: ICouch.Db.t()
  def db_from_meta(%{server: server, repo: repo}, %{prefix: schema_prefix}) do
    repo_config = repo.config
    db_name = repo_config |> Keyword.get(:database)
    prefix = schema_prefix || Keyword.get(repo_config, :prefix)
    db_with_prefix(server, db_name, prefix)
  end

  @spec view_from_db(db :: ICouch.DB.t(), ddoc :: String.t(), name :: String.t(), params :: map()) :: ICouch.View.t()
  def view_from_db(db, ddoc, view_name, params \\ %{}) do
    %ICouch.View{db: db, ddoc: ddoc, name: view_name, params: params}
  end

  @spec type_from_schema(Ecto.Schema.schema()) :: String.t()
  def type_from_schema(%{schema: schema}), do: schema.__schema__(:source)
  def type_from_schema(module), do: module.__schema__(:source)

  @spec split_ddoc_view(Ecto.Schema.schema(), {String.t(), String.t()} | String.t()) :: {String.t(), String.t()}
  def split_ddoc_view(_schema, {ddoc, view_name}), do: {ddoc, view_name}
  def split_ddoc_view(:raw, _view_name), do: raise "Invalid ddoc_view, for :raw use {ddoc, view_name}"
  def split_ddoc_view(schema, view_name), do: {type_from_schema(schema), view_name}


  defp db_with_prefix(server, db_name, prefix), do: server |> ICouch.DB.new("#{prefix}#{db_name}")

end
