defmodule CouchdbAdapter.Storage do

  import Couchdb.Connector.AsMap

  
  @spec create_db(Ecto.Repo.t) :: {:ok, boolean} | {:error, term()}
  def create_db(repo) do
    case repo |> CouchdbAdapter.db_props_for |> Couchdb.Connector.Storage.storage_up |> as_map do
      {:ok, %{"ok" => true}} -> {:ok, true}
      {:error, %{"error" => "file_exists"}} -> {:ok, false}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec delete_db(Ecto.Repo.t) :: {:ok, boolean} | {:error, term()}
  def delete_db(repo) do
    case repo |> CouchdbAdapter.db_props_for |> Couchdb.Connector.Storage.storage_down |> as_map do
      {:ok, %{"ok" => true}} -> {:ok, true}
      {:error, %{"error" => "not_found"}} -> {:ok, false}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec create_ddoc(Ecto.Repo.t, String.t, String.t | map) :: {:ok, boolean} | {:error, term()}
  def create_ddoc(repo, ddoc, code) do
    case repo |> CouchdbAdapter.db_props_for |> Couchdb.Connector.View.create_view(ddoc, code) |> as_map do
      {:ok, %{"ok" => true}} -> {:ok, true}
      {:error, %{"error" => _, "reason" => reason}} -> {:error, reason}
    end
  end

  @spec create_index(Ecto.Repo.t, String.t | map) :: {:ok, boolean} | {:error, term()}
  def create_index(repo, data) do
    case repo |> CouchdbAdapter.db_props_for |> Couchdb.Connector.View.create_index(data) |> as_map do
      {:ok, %{"result" => _}} -> {:ok, true}
      {:error, %{"error" => _, "reason" => reason}} -> {:error, reason}
    end
  end

end
