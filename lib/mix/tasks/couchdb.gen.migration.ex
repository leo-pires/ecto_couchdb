defmodule Mix.Tasks.Couchdb.Gen.Migration do
  use Mix.Task
  import Macro, only: [camelize: 1, underscore: 1]
  import Mix.Generator
  import Mix.Couchdb

  @shortdoc "Generates a new migration for CouchDb"


  def run(args) do
    no_umbrella!("couchdb.gen.migration")
    repos = parse_repo(args)
    Enum.map repos, fn repo ->
      name = length(args) == 1 and hd(args)
      if name do
        # ensure priv repo dir is created
        ensure_repo(repo, args)
        path = Path.join(source_repo_priv(repo), "migrations")
        base_name = "#{underscore(name)}.exs"
        file = Path.join(path, "#{timestamp()}_#{base_name}")
        unless File.dir?(path), do: create_directory path
        # check if migration already created
        fuzzy_path = Path.join(path, "*_#{base_name}")
        if Path.wildcard(fuzzy_path) != [] do
          Mix.raise "migration can't be created, there is already a migration file with name #{name}."
        end
        # create migration file
        assigns = [mod: Module.concat([repo, Migrations, camelize(name)])]
        create_file file, migration_template(assigns)
        # return created file
        file
      else
        Mix.raise "expected couchdb.gen.migration to receive the migration file name, " <>
                  "got: #{inspect Enum.join(args, " ")}"
      end
    end
  end

  defp timestamp do
    {{y, m, d}, {hh, mm, ss}} = :calendar.universal_time()
    "#{y}#{pad(m)}#{pad(d)}#{pad(hh)}#{pad(mm)}#{pad(ss)}"
  end

  defp pad(i) when i < 10, do: << ?0, ?0 + i >>
  defp pad(i), do: to_string(i)

  defp migration_module do
    case Application.get_env(:ecto_sql, :migration_module, Ecto.Migration) do
      migration_module when is_atom(migration_module) -> migration_module
      other -> Mix.raise "Expected :migration_module to be a module, got: #{inspect(other)}"
    end
  end

  embed_template :migration, """
  defmodule <%= @mod %> do
    use <%= inspect migration_module() %>

    def change do
      # Code goes here
    end
  end
  """

end
