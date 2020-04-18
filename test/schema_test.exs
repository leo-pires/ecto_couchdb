defmodule Couchdb.Ecto.SchemaTest do
  use Couchdb.Ecto.ModelCase, async: false


  setup do
    clear_db!()
    :ok
  end

  describe "design docs" do

    test "create design doc" do
      # assert {:ok, true} = TestRepo |> Couchdb.Ecto.Storage.create_ddoc(@ddoc_doc_id, @ddoc_doc_id_code)
    end

    test "create design doc fails for invalid doc" do
      assert {:error, _} = TestRepo |> Couchdb.Ecto.Storage.create_ddoc("", %{})
    end

    test "fetch unexisting ddoc" do
      assert {:ok, :not_found} = TestRepo |> Couchdb.Ecto.Storage.fetch_ddoc("xpto")
    end

    test "fetch existing ddoc" do
      assert {:ok, true} = TestRepo |> Couchdb.Ecto.Storage.create_ddoc(@ddoc_doc_id, @ddoc_doc_id_code)
      {:ok, fetched} = TestRepo |> Couchdb.Ecto.Storage.fetch_ddoc(@ddoc_doc_id)
      assert ICouch.Document.equal_content?(fetched, @ddoc_doc_id_code)
    end

    test "drop unexisting design doc" do
      assert {:ok, _} = TestRepo |> Couchdb.Ecto.Storage.drop_ddoc("xpto")
    end

    test "drop design doc" do
      assert {:ok, true} = TestRepo |> Couchdb.Ecto.Storage.create_ddoc(@ddoc_doc_id, @ddoc_doc_id_code)
      assert {:ok, true} = TestRepo |> Couchdb.Ecto.Storage.drop_ddoc(@ddoc_doc_id)
    end

  end

  describe "indexes" do

    test "create index" do
      assert {:ok, true} = TestRepo |> Couchdb.Ecto.Storage.create_index(@index_code)
    end

    test "doesnt create invalid index" do
      assert {:error, _} = TestRepo |> Couchdb.Ecto.Storage.create_index(%{})
    end

  end

end
