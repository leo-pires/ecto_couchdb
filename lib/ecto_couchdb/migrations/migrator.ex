# TODO: receber log com parâmetro para testes sem output

defmodule Couchdb.Ecto.Migration.Migrator do
  require Logger
  alias Couchdb.Ecto.Migration.MigrationModel


  @spec migrations(Ecto.Repo.t, String.t) :: [{:up | :down, id :: integer(), name :: String.t}]
  def migrations(repo, migration_source) do
    migrations_files = migrations_for(migration_source)
    repo
    |> migrated_versions(migrations_files)
    |> collect_migrations(migrations_files)
    |> Enum.sort_by(fn {_, version, _, _} -> version end)
  end

  @spec run(Ecto.Repo.t, String.t) :: [integer]
  def run(repo, migration_source) do
    # find migrations files and already migrateds versions
    migrations_files = migrations_for(migration_source)
    migrated_versions = migrated_versions(repo, migrations_files)
    # check pending migrations, migrate and write trackings
    check_migrations(migrations_files, migrated_versions, :down)
    |> migrate
    |> write_trackings(repo)
  end

  @spec migrated_versions(Ecto.Repo.t, String.t) :: [integer]
  def migrated_versions(repo, migrations_files) do
    # TODO: use _all_docs
    {ok, error} =
      migrations_files
      |> Enum.map(fn {version, _, _} ->
           id = MigrationModel.generate_id(version)
           Couchdb.Ecto.Fetchers.get(repo, MigrationModel, id)
         end)
      |> Enum.split_with(fn {status, _} -> status == :ok end)
    if error == [] do
      ok
      |> Enum.filter(fn {_, doc} -> doc != nil end)
      |> Enum.map(fn {_, doc} -> doc.version end)
    else
      reason =
        error
        |> Enum.map(fn {:error, error} -> "#{inspect error}" end)
        |> Enum.join("; ")
      raise "Could not fetch migrated versions (#{inspect reason})!"
    end
  end

  @spec migrations_for(String.t | [String.t]) :: [String.t]
  # This function will match directories passed into `Migrator.run`.
  def migrations_for(migration_source) when is_binary(migration_source) do
    Path.join([migration_source, "**", "*.exs"])
    |> Path.wildcard()
    |> Enum.map(&extract_migration_info/1)
    |> Enum.filter(& &1)
    |> Enum.sort()
  end
  # This function will match specific version/modules passed into `Migrator.run`.
  def migrations_for(migration_source) when is_list(migration_source) do
    Enum.map migration_source, fn {version, module} -> {version, module, module} end
  end

  @spec extract_migration_info(String.t) :: {String.t, integer, String.t}
  def extract_migration_info(file) do
    case Integer.parse(Path.rootname(Path.basename(file))) do
      {integer, "_" <> name} ->
        {integer, name, file}
      _ ->
        nil
    end
  end

  defp collect_migrations(migrated_versions, migrations_files) do
    ups = check_migrations(migrations_files, migrated_versions, :up)
    downs = check_migrations(migrations_files, migrated_versions, :down)
    ups ++ downs
  end

  defp check_migrations(migrations_files, migrated_versions, :up) do
    migrations_files
    |> Enum.filter(fn {version, _name, _file} -> version in migrated_versions end)
    |> Enum.map(fn {version, name, file} -> {:up, version, name, file} end)
  end
  defp check_migrations(migrations_files, migrated_versions, :down) do
    migrations_files
    |> Enum.filter(fn {version, _name, _file} -> not (version in migrated_versions) end)
    |> Enum.map(fn {version, name, file} -> {:down, version, name, file} end)
  end

  defp migrate([]) do
    Logger.log(:info, "Migrated!")
    []
  end
  defp migrate(migrations) do
    with :ok <- ensure_no_duplication(migrations),
         versions when is_list(versions) <- do_migrate(migrations)
    do
      Enum.reverse(versions)
    end
  end

  defp do_migrate(migrations) do
    migrations
    |> Enum.map(&load_migration/1)
    |> Enum.reduce_while([], fn {version, file_or_mod, modules}, versions ->
      with {:ok, mod} <- find_migration_module(modules, file_or_mod),
           :ok <- do_change(version, mod)
      do
        {:cont, [version | versions]}
      else
        _ -> {:halt, versions}
      end
    end)
  end

  defp write_trackings(versions, repo) do
    Enum.each(versions, fn version ->
      {inserted, reason} = MigrationModel.changeset(version) |> repo.insert
      if inserted == :error do
        Logger.error("Could not write migration tracking for #{version} (#{inspect reason})")
      end
    end)
    versions
  end

  defp ensure_no_duplication([{_, version, name, _} | t]) do
    cond do
      List.keyfind(t, version, 0) ->
        message = "migrations can't be executed, migration version #{version} is duplicated"
        {:error, Ecto.MigrationError.exception(message)}
      List.keyfind(t, name, 1) ->
        message = "migrations can't be executed, migration name #{name} is duplicated"
        {:error, Ecto.MigrationError.exception(message)}
      true ->
        ensure_no_duplication(t)
    end
  end
  defp ensure_no_duplication([]), do: :ok

  defp load_migration({_, version, _, mod}) when is_atom(mod) do
    {version, mod, [mod]}
  end
  defp load_migration({_, version, _, file}) when is_binary(file) do
    {version, file, Code.load_file(file) |> Enum.map(&elem(&1, 0))}
  end

  defp find_migration_module(modules, file_or_mod) do
    cond do
      mod = Enum.find(modules, &function_exported?(&1, :__migration__, 0)) ->
        {:ok, mod}
      is_binary(file_or_mod) ->
        message = "file #{Path.relative_to_cwd(file_or_mod)} does not define an Ecto.Migration"
        {:error, Ecto.MigrationError.exception(message)}
      is_atom(file_or_mod) ->
        message = "module #{inspect(file_or_mod)} is not an Ecto.Migration"
        {:error, Ecto.MigrationError.exception(message)}
    end
  end

  defp do_change(version, mod) do
    if Code.ensure_loaded?(mod) and function_exported?(mod, :change, 0) do
      Logger.info("== Running #{version} #{inspect mod}.change/0")
      {time, result} = :timer.tc(fn ->
        try do
          apply(mod, :change, [])
          :ok
        rescue
          error ->
            formated_error = Exception.format(:error, error, __STACKTRACE__)
            Logger.error("#{inspect mod} returned a error while executing\n#{formated_error}")
            :error
        end
      end)
      if result == :ok do
        Logger.info("== Migrated #{version} in #{inspect(div(time, 100_000) / 10)}s")
      end
      result
    else
      {:error, Ecto.MigrationError.exception("#{inspect mod} does not implement a `change/0` function")}
    end
  end

end
