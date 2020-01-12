defmodule Couchdb.Ecto.StorageTest do
  use ExUnit.Case, async: false
  use TestModelCase


  describe "storage" do

    setup do
      config = %{config: TestRepo.config |> Keyword.put(:database, "xpto_123")}
      wrong_config = config |> put_in([:config, :database], "wrong_xpto_321")
      config |> clear_db!
      %{config: config, wrong_config: wrong_config}
    end

    test "drop db", %{config: config} do
      assert {:ok, true} = Couchdb.Ecto.Storage.delete_db(config)
    end

    test "doesnt drop db that doesnt exists", %{wrong_config: wrong_config} do
      assert {:ok, false} = Couchdb.Ecto.Storage.delete_db(wrong_config)
    end

    test "create db", %{config: config} do
      assert {:ok, true} = Couchdb.Ecto.Storage.delete_db(config)
      assert {:ok, true} = Couchdb.Ecto.Storage.create_db(config)
    end

    test "doesnt create db that already exists", %{config: config} do
      assert {:ok, false} = Couchdb.Ecto.Storage.create_db(config)
    end

  end

  describe "design docs" do

    setup do
      config = %{config: TestRepo.config |> Keyword.put(:database, "xpto_123")}
      config |> clear_db!
      %{config: config}
    end

    test "fetch unexisting ddoc", %{config: config} do
      assert {:ok, :not_found} = Couchdb.Ecto.Storage.fetch_ddoc(config, "xpto")
    end

    test "fetch existing ddoc", %{config: config} do
      assert {:ok, true} = Couchdb.Ecto.Storage.create_ddoc(config, @ddoc_doc_id, @ddoc_doc_id_code)
      {:ok, fetched} = Couchdb.Ecto.Storage.fetch_ddoc(config, @ddoc_doc_id)
      assert ICouch.Document.equal_content?(fetched, @ddoc_doc_id_code)
    end

    test "create design doc", %{config: config} do
      assert {:ok, true} = Couchdb.Ecto.Storage.create_ddoc(config, @ddoc_doc_id, @ddoc_doc_id_code)
    end

    test "drop unexisting design doc", %{config: config} do
      assert {:ok, _} = Couchdb.Ecto.Storage.drop_ddoc(config, "xpto")
    end

    test "drop design doc", %{config: config} do
      assert {:ok, true} = Couchdb.Ecto.Storage.create_ddoc(config, @ddoc_doc_id, @ddoc_doc_id_code)
      assert {:ok, true} = Couchdb.Ecto.Storage.drop_ddoc(config, @ddoc_doc_id)
    end

    test "doesnt create invalid design doc", %{config: config} do
      assert {:error, _} = Couchdb.Ecto.Storage.create_ddoc(config, "", %{})
    end

  end

  describe "indexes" do

    setup do
      config = %{config: TestRepo.config |> Keyword.put(:database, "xpto_123")}
      config |> clear_db!
      %{config: config}
    end

    test "create index", %{config: config} do
      assert {:ok, true} = Couchdb.Ecto.Storage.create_index(config, @index_code)
    end

    test "doesnt create invalid index", %{config: config} do
      assert {:error, _} = Couchdb.Ecto.Storage.create_index(config, %{})
    end

  end

end
