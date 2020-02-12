defmodule Couchdb.Ecto.StorageTest do
  use Couchdb.Ecto.TestModelCase, async: false


  describe "storage" do

    setup do
      %{wrong_server_config: TestRepo.config |> Keyword.put(:couchdb_url, "http://127.0.0.1:9999/")}
    end

    test "db status", %{wrong_server_config: wrong_server_config} do
      assert :up = TestRepo.__adapter__.storage_status(TestRepo.config)
      :ok = TestRepo |> db_from_repo |> ICouch.delete_db
      assert :down = TestRepo.__adapter__.storage_status(TestRepo.config)
      assert {:error, _} = TestRepo.__adapter__.storage_status(wrong_server_config)
    end

    test "drop db", %{wrong_server_config: wrong_server_config} do
      assert :ok = TestRepo.__adapter__.storage_down(TestRepo.config)
      assert {:error, :already_down} = TestRepo.__adapter__.storage_down(TestRepo.config)
      assert {:error, _} = TestRepo.__adapter__.storage_down(wrong_server_config)
    end

    test "create db", %{wrong_server_config: wrong_server_config} do
      assert {:error, :already_up} = TestRepo.__adapter__.storage_up(TestRepo.config)
      :ok = TestRepo |> db_from_repo |> ICouch.delete_db
      assert :ok = TestRepo.__adapter__.storage_up(TestRepo.config)
      assert {:error, _} = TestRepo.__adapter__.storage_up(wrong_server_config)
    end

  end

end
