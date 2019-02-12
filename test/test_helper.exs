ExUnit.start()

defmodule Repo do
  use Ecto.Repo, otp_app: :ecto_couchdb
  use CouchdbAdapter.CouchdbFetchersRepoHelper
end

# Load support files
files = Path.wildcard("#{__DIR__}/support/**/*.exs")
Enum.each files, &Code.require_file(&1, __DIR__)
