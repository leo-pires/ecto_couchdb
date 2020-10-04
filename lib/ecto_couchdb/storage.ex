defmodule Couchdb.Ecto.Storage do
  import Couchdb.Ecto.Helpers


  @spec fetch_ddoc(Ecto.Repo.t, String.t) :: {:ok, :not_found} | {:ok, map} | {:error, term()}
  def fetch_ddoc(repo, ddoc, opts \\ []) do
    prefix = opts |> Keyword.get(:prefix)
    case repo |> db_from_repo(prefix: prefix) |> ICouch.open_doc(ddoc_doc_id(ddoc)) do
      {:ok, doc} -> {:ok, doc}
      {:error, :not_found} -> {:ok, :not_found}
      other -> other
    end
  end

  @spec create_ddoc(Ecto.Repo.t, String.t, String.t | map) :: {:ok, boolean} | {:error, term()}
  def create_ddoc(repo, ddoc, code, opts \\ []) when is_map(code) do
    prefix = opts |> Keyword.get(:prefix)
    code = code |> Map.put("_id", ddoc_doc_id(ddoc))
    case repo |> db_from_repo(prefix: prefix) |> ICouch.save_doc(code) do
      {:ok, _doc} -> {:ok, true}
      other -> other
    end
  end

  @spec drop_ddoc(Ecto.Repo.t, String.t) :: {:ok, boolean} | {:error, term()}
  def drop_ddoc(repo, ddoc, opts \\ []) do
    prefix = opts |> Keyword.get(:prefix)
    db = repo |> db_from_repo(prefix: prefix)
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
  def create_index(repo, code, opts \\ []) when is_map(code) do
    prefix = opts |> Keyword.get(:prefix)
    case repo |> db_from_repo(prefix: prefix) |> ICouch.DB.send_req("_index", :post, code) do
      {:ok, %{"result" => "created"}} -> {:ok, true}
      other -> other
    end
  end

end
