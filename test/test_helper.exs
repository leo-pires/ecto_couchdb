ExUnit.start()

Application.put_env(
	:ecto_couchdb,
  TestRepo, [
    adapter: Couchdb.Ecto,
    protocol: "http",
    hostname: "localhost",
    port: 5984,
    username: "admin",
    password: "admin",
    database: "ecto_couchdb_test",
    pool_size: 5,
    pool_timeout: 2000,
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
files = Path.wildcard("#{__DIR__}/support/**/*.exs")
Enum.each files, &Code.require_file(&1, __DIR__)

{:ok, pid} = TestRepo.start_link()
:ok = TestRepo.stop(pid, :infinity)
{:ok, _pid} = TestRepo.start_link()
