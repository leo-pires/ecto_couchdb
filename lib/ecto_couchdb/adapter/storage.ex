defmodule Couchdb.Ecto.Storage do

  @spec create_db(Ecto.Repo.t) :: {:ok, boolean} | {:error, term()}
  def create_db(repo) do
    database_name = repo.config |> Keyword.get(:database)
    case repo |> Couchdb.Ecto.server_connection_from_repo |> ICouch.create_db(database_name) do
      {:ok, _db} -> {:ok, true}
      {:error, :precondition_failed} -> {:ok, false}
      other -> other
    end
  end

  @spec delete_db(Ecto.Repo.t) :: {:ok, boolean} | {:error, term()}
  def delete_db(repo) do
    case repo |> Couchdb.Ecto.db_from_repo |> ICouch.delete_db do
      :ok -> {:ok, true}
      {:error, :not_found} -> {:ok, false}
      other -> other
    end
  end

  @spec fetch_ddoc(Ecto.Repo.t, String.t) :: {:ok, :not_found} | {:ok, map} | {:error, term()}
  def fetch_ddoc(repo, ddoc) do
    case repo |> Couchdb.Ecto.db_from_repo |> ICouch.open_doc(ddoc_doc_id(ddoc)) do
      {:ok, doc} -> {:ok, doc}
      {:error, :not_found} -> {:ok, :not_found}
      other -> other
    end
  end

  @spec create_ddoc(Ecto.Repo.t, String.t, String.t | map) :: {:ok, boolean} | {:error, term()}
  def create_ddoc(repo, ddoc, code) when is_map(code) do
    code = code |> Map.put("_id", ddoc_doc_id(ddoc))
    case repo |> Couchdb.Ecto.db_from_repo |> ICouch.save_doc(code) do
      {:ok, _doc} -> {:ok, true}
      other -> other
    end
  end

  @spec drop_ddoc(Ecto.Repo.t, String.t) :: {:ok, boolean} | {:error, term()}
  def drop_ddoc(repo, ddoc) do
    db = repo |> Couchdb.Ecto.db_from_repo
    case db |> ICouch.open_doc(ddoc_doc_id(ddoc)) do
      {:ok, doc} ->
        case db |> ICouch.delete_doc(doc) do
          {:ok, _doc_map} -> {:ok, true}
          other -> other
        end
      {:error, :not_found} -> {:ok, :not_found}
      other -> other
    end
  end

  @spec create_index(Ecto.Repo.t, String.t | map) :: {:ok, boolean} | {:error, term()}
  def create_index(repo, code) when is_map(code) do
    case repo |> Couchdb.Ecto.db_from_repo |> ICouch.DB.send_req("_index", :post, code) do
      {:ok, %{"result" => "created"}} -> {:ok, true}
      other -> other
    end
  end

  def ddoc_doc_id(ddoc), do: "_design/#{ddoc}"

end
