defmodule Mix.Tasks.Couchdb.Migrations do
  use Mix.Task
  import Mix.Couchdb
  alias Couchdb.Ecto.Migration.Migrator

  @shortdoc "Displays the CouchDb migration status"


  def run(args, puts \\ &IO.puts/1) do
    repos = parse_repo(args)
    result =
      Enum.map(repos, fn repo ->
        # ensure adapter is started
        ensure_repo(repo, args)
        path = ensure_migrations_path(repo)
        {:ok, pid, _} = ensure_started(repo, all: true)
        # fetch migrations from database
        repo_status = Migrator.migrations(repo, path)
        # stop adapter
        pid && repo.stop()
        # prepare result
        """
        Repo: #{inspect(repo)}
          Status    Migration ID    Migration Name
        --------------------------------------------------
        """ <>
          Enum.map_join(repo_status, "\n", fn {status, number, name, _} ->
            "  #{format(status, 10)}#{format(number, 16)}#{name}"
          end) <> "\n"
      end)
    puts.(Enum.join(result, "\n"))
  end

  defp format(content, pad) do
    content
    |> to_string
    |> String.pad_trailing(pad)
  end

end
