defmodule DatabaseCleaner do
  def ensure_clean_db!(repo) do
    config = Application.get_env(:couchdb_adapter, repo)
    opts =
      if config[:user] && config[:password] do
        [{:basic_auth, {config[:user], config[:password]}}]
      else
        []
      end
    server = :couchbeam.server_connection(config[:hostname], config[:port], "", opts)
    database = config[:database]
    try do
      if :couchbeam.db_exists(server, database) do
        :couchbeam.delete_db(server, database)
      end
      {:ok, db} = :couchbeam.create_db(server, database)
      db
    rescue
      _ -> ensure_clean_db!(repo)
    end
  end
end
