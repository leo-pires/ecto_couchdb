defmodule Mix.Tasks.Couchdb.Migrate do
  use Mix.Task
  import Mix.Couchdb
  alias Couchdb.Ecto.Migration.Migrator

  @shortdoc "Runs the CouchDb migrations"


  def run(args) do
    repos = parse_repo(args)
    Enum.each repos, fn repo ->
      # ensure adapter is started and priv repo dir is created
      ensure_repo(repo, args)
      path = ensure_migrations_path(repo)
      {:ok, pid, apps} = ensure_started(repo, all: true)
      # run migrations
      migrated = Migrator.run(repo, path)
      # stop adapter and restart apps
      pid && repo.stop()
      restart_apps_if_migrated(apps, migrated)
    end
  end

end
