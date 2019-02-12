defmodule Couchdb.Ecto.SchemaTest do

  use ExUnit.Case, async: true
  import TestSupport

  @sample_ddoc %{
    _id: nil,
    language: "javascript",
    views: %{
      all: %{
        map: "function(doc) { if (doc.type === 'Post') emit(doc._id, doc) }"
      }
    }
  }

  describe "storage" do
    setup do
      config_wrapper = %{config: TestRepo.config |> Keyword.put(:database, "xpto_123")}
      wrong_config_wrapper = config_wrapper |> put_in([:config, :database], "wrong_xpto_321")
      config_wrapper |> clear_db!
      %{
        config_wrapper: config_wrapper,
        wrong_config_wrapper: wrong_config_wrapper
      }
    end

    test "drop db", %{config_wrapper: config_wrapper} do
      assert {:ok, true} = Couchdb.Ecto.Storage.delete_db(config_wrapper)
    end

    test "doesnt drop db that doesnt exists", %{wrong_config_wrapper: wrong_config_wrapper} do
      assert {:ok, false} = Couchdb.Ecto.Storage.delete_db(wrong_config_wrapper)
    end

    test "create db", %{config_wrapper: config_wrapper} do
      assert {:ok, true} = Couchdb.Ecto.Storage.delete_db(config_wrapper)
      assert {:ok, true} = Couchdb.Ecto.Storage.create_db(config_wrapper)
    end

    test "doesnt create db that already exists", %{config_wrapper: config_wrapper} do
      assert {:ok, false} = Couchdb.Ecto.Storage.create_db(config_wrapper)
    end

  end

  describe "design docs and indexes" do
    setup do
      config_wrapper = %{config: TestRepo.config |> Keyword.put(:database, "xpto_123")}
      config_wrapper |> clear_db!
      %{
        config_wrapper: config_wrapper
      }
    end

    test "create design doc", %{config_wrapper: config_wrapper} do
      ddoc = "TestPost"
      code = @sample_ddoc |> Map.put(:_id, ddoc) |> Poison.encode!
      assert {:ok, true} = Couchdb.Ecto.Storage.create_ddoc(config_wrapper, ddoc, code)
    end

    test "drop design doc", %{config_wrapper: config_wrapper} do
      ddoc = "TestPost"
      code = @sample_ddoc |> Map.put(:_id, ddoc) |> Poison.encode!
      assert {:ok, true} = Couchdb.Ecto.Storage.create_ddoc(config_wrapper, ddoc, code)
      assert {:ok, true} = Couchdb.Ecto.Storage.drop_ddoc(config_wrapper, ddoc)
    end

    test "doesnt create invalid design doc", %{config_wrapper: config_wrapper} do
      assert {:error, _} = Couchdb.Ecto.Storage.create_ddoc(config_wrapper, "", "")
    end

    test "create index", %{config_wrapper: config_wrapper} do
      schema = %{
        index: %{
          fields: ["name"]
        },
        ddoc: "TestPostIndex",
        name: "test1"
      } |> Poison.encode!
      assert {:ok, true} = Couchdb.Ecto.Storage.create_index(config_wrapper, schema)
    end

    test "doesnt create invalid index", %{config_wrapper: config_wrapper} do
      assert {:error, _} = Couchdb.Ecto.Storage.create_index(config_wrapper, "")
    end

  end

end
