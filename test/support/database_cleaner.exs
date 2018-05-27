defmodule DatabaseCleaner do
  def ensure_clean_db!(repo) do
    config = repo.config
    opts =
      if config[:username] && config[:password] do
        [{:basic_auth, {config[:username], config[:password]}}]
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
