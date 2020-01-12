defmodule TestSupport do

  def clear_db!(repo) do
    {:ok, _} = repo |> Couchdb.Ecto.Storage.delete_db
    case repo |> Couchdb.Ecto.Storage.create_db do
      {:ok, true} -> :ok
      {:error, reason} -> raise reason
      _ -> clear_db!(repo)
    end
  end

  def create_views!(repo, design_docs) do
    design_docs |> Enum.map(fn {ddoc, code} ->
      repo |> Couchdb.Ecto.Storage.create_ddoc(ddoc, code)
    end)
  end

  def insert_docs!(repo, docs) do
    {:ok, docs} = repo |> Couchdb.Ecto.db_from_repo |> ICouch.save_docs(docs)
    docs |> Enum.map(fn return ->
      %{"ok" => true, "id" => id, "rev" => rev} = return
      %{_id: id, _rev: rev}
    end)
  end

  def atomize_keys!(list) when is_list(list), do: list |> Enum.map(&(&1 |> atomize_keys!))
  def atomize_keys!(map) when is_map(map), do: map |> Enum.map(fn {k, v} -> {k |> String.to_atom, v} end) |> Map.new

end
