ExUnit.start()

Application.put_env(
  :ecto_couchdb,
  TestRepo, [
    adapter: Couchdb.Ecto,
    couchdb_url: "http://admin:admin@127.0.0.1:5984",
    database: "ecto_couchdb_test",
    priv: "test/priv/repo"
  ]
)
Application.put_env(
  :ecto_couchdb,
  :ecto_repos, [TestRepo]
)

defmodule TestRepo do
  use Ecto.Repo, otp_app: :ecto_couchdb
  use Couchdb.Ecto.RepoFetchersHelper
end

# Load support files
Path.wildcard("#{__DIR__}/support/**/*.exs") |> Enum.each(&Code.require_file(&1, __DIR__))

{:ok, pid} = TestRepo.start_link()
:ok = TestRepo.stop(pid, :infinity)
{:ok, _pid} = TestRepo.start_link()
