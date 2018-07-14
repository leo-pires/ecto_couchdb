defmodule CouchdbAdapter.Storage do

  import Couchdb.Connector.AsMap

  
  def storage_up(options) do
    Application.ensure_all_started(:hackney)
    case CouchdbAdapter.Storage.create_db(options) do
      {:ok, true} -> :ok
      {:ok, false} -> {:error, :already_down}
      {:error, reason} -> {:error, reason}
    end
  end
  def storage_down(options) do
    Application.ensure_all_started(:hackney)
    case CouchdbAdapter.Storage.delete_db(options) do
      {:ok, true} -> :ok
      {:ok, false} -> {:error, :already_up}
      {:error, reason} -> {:error, reason}
    end
  end


  def create_db(options) do
    case options |> CouchdbAdapter.db_props_for |> Couchdb.Connector.Storage.storage_up |> as_map do
      {:ok, %{"ok" => true}} -> {:ok, true}
      {:error, %{"error" => "file_exists"}} -> {:ok, false}
      {:error, reason} -> {:error, reason}
    end
  end

  def delete_db(options) do
    case options |> CouchdbAdapter.db_props_for |> Couchdb.Connector.Storage.storage_down |> as_map do
      {:ok, %{"ok" => true}} -> {:ok, true}
      {:error, %{"error" => "not_found"}} -> {:ok, false}
      {:error, reason} -> {:error, reason}
    end
  end

  def create_ddoc(options, ddoc, code) do
    case options |> CouchdbAdapter.db_props_for |> Couchdb.Connector.View.create_view(ddoc, code) |> as_map do
      {:ok, %{"ok" => true}} -> {:ok, true}
      {:error, %{"error" => _, "reason" => reason}} -> {:error, reason}
    end
  end

  def create_index(options, data) do
    case options |> CouchdbAdapter.db_props_for |> Couchdb.Connector.View.create_index(data) |> as_map do
      {:ok, %{"result" => _}} -> {:ok, true}
      {:error, %{"error" => _, "reason" => reason}} -> {:error, reason}
    end
  end

end
