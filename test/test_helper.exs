Application.put_env(
  :ecto_couchdb,
  :ecto_repos, [TestRepo]
)
Application.put_env(
  :ecto_couchdb,
  TestRepo, [
    adapter: Couchdb.Ecto,
    couchdb_url: "http://admin:admin@127.0.0.1:5984",
    database: "ecto_couchdb_test",
    priv: "test/priv/repo"
  ]
)

defmodule TestRepo do
  use Ecto.Repo,
    adapter: Couchdb.Ecto,
    otp_app: :ecto_couchdb
  use Couchdb.Ecto.RepoFetchers
end

# Load support files
Path.wildcard("#{__DIR__}/support/**/*.exs") |> Enum.each(&Code.require_file(&1, __DIR__))

{:ok, _pid} = TestRepo.start_link()

ExUnit.start()
