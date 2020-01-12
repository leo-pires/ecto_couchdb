defmodule TestSupport do
  import ExUnit.Assertions
  import Couchdb.Ecto.Helpers

  @repo TestRepo


  def clear_db! do
    server = @repo.config |> server_from_config
    database_name = @repo.config |> Keyword.get(:database)
    case server |> ICouch.delete_db(database_name) do
      {:error, :not_found} -> :ok
      :ok -> :ok
      _error -> clear_db!()
    end
    case server |> ICouch.create_db(database_name) do
      {:ok, _db} -> :ok
      _error -> clear_db!()
    end
    :ok
  end

  def create_views!(design_docs) do
    design_docs |> Enum.map(fn {ddoc, code} ->
      @repo |> Couchdb.Ecto.Storage.create_ddoc(ddoc, code)
    end)
  end

  def insert_docs!(docs) do
    {:ok, docs} = @repo |> db_from_repo |> ICouch.save_docs(docs)
    docs |> Enum.map(fn return ->
      %{"ok" => true, "id" => id, "rev" => rev} = return
      %{_id: id, _rev: rev}
    end)
  end

  def has_id_and_rev?(resource) do
    assert resource._id
    assert resource._rev
  end

  def atomize_keys!(list) when is_list(list), do: list |> Enum.map(&(&1 |> atomize_keys!))
  def atomize_keys!(map) when is_map(map), do: map |> Enum.map(fn {k, v} -> {k |> String.to_atom, v} end) |> Map.new

end
