defmodule Couchdb.Ecto.MigrationsTest do
  use ExUnit.Case, async: false
  import TestSupport
  alias Couchdb.Ecto.Fetchers
  alias Couchdb.Ecto.Migration.{Migrator, MigrationModel}


  def no_output(s), do: s

  setup do
    TestRepo |> clear_db!
  end

  describe "ecto.gen.migration" do

    test "should not execute if no file named inputed" do
      assert_raise Mix.Error, fn -> Mix.Tasks.Couchdb.Gen.Migration.run([]) end
    end

    test "should execute" do
      file = Mix.Tasks.Couchdb.Gen.Migration.run(["TestGenMigration"])

      assert File.exists?(file)
      output = File.read!(file)
      assert output =~ ~r/def change/

      File.rm!(file)
    end

  end

  describe "ecto.migrations" do

    test "should return nothing when there isn't migrations files" do
      output = Mix.Tasks.Couchdb.Migrations.run([], &no_output/1) |> String.split("\n")
      assert length(output) == 5
    end

    test "should return when there is migrations files" do
      file1 = Mix.Tasks.Couchdb.Gen.Migration.run(["TestMigrations1"])
      :timer.sleep(1000)
      file2 = Mix.Tasks.Couchdb.Gen.Migration.run(["TestMigrations2"])
      :timer.sleep(1000)
      file3 = Mix.Tasks.Couchdb.Gen.Migration.run(["TestMigrations3"])
      {number1, _, _} = Migrator.extract_migration_info(file1)
      {number2, _, _} = Migrator.extract_migration_info(file2)
      {number3, _, _} = Migrator.extract_migration_info(file3)

      number1_doc = MigrationModel.changeset(number1) |> TestRepo.insert!

      output = Mix.Tasks.Couchdb.Migrations.run([], &no_output/1) |> String.split("\n")
      assert Enum.at(output, 3) =~ ~r/up\s+#{number1}/
      assert Enum.at(output, 4) =~ ~r/down\s+#{number2}/
      assert Enum.at(output, 5) =~ ~r/down\s+#{number3}/

      File.rm!(file1)
      File.rm!(file2)
      File.rm!(file3)
      number1_doc |> TestRepo.delete!
    end

  end

  describe "ecto.migrate" do

    test "should run when there isn't pending migrations" do
      assert Mix.Tasks.Couchdb.Migrate.run([]) == :ok
    end

    test "should save tracking when successfuly migrate" do
      file = Mix.Tasks.Couchdb.Gen.Migration.run(["TestMigrate1"])
      {number, _, _} = Migrator.extract_migration_info(file)
      id = MigrationModel.generate_id(number)

      assert Fetchers.get(TestRepo, MigrationModel, id) == {:ok, nil}

      Mix.Tasks.Couchdb.Migrate.run([])

      {:ok, tracking_after} = Fetchers.get(TestRepo, MigrationModel, id)
      assert tracking_after != nil
      assert tracking_after.version == number

      File.rm!(file)
    end

    test "shouldn't save tracking when error" do
      file1 = Mix.Tasks.Couchdb.Gen.Migration.run(["TestMigrate2"])
      :timer.sleep(1000)
      file2 = Mix.Tasks.Couchdb.Gen.Migration.run(["TestMigrate3"])
      {number1, _, _} = Migrator.extract_migration_info(file1)
      {number2, _, _} = Migrator.extract_migration_info(file2)
      id1 = MigrationModel.generate_id(number1)
      id2 = MigrationModel.generate_id(number2)

      assert Fetchers.get(TestRepo, MigrationModel, id1) == {:ok, nil}
      assert Fetchers.get(TestRepo, MigrationModel, id2) == {:ok, nil}

      new_code =
        File.read!(file1)
        |> String.replace("# Code goes here", "raise \"oops\"")
      :ok = File.write(file1, new_code, [:utf8])

      Mix.Tasks.Couchdb.Migrate.run([])

      assert Fetchers.get(TestRepo, MigrationModel, id1) == {:ok, nil}
      assert Fetchers.get(TestRepo, MigrationModel, id2) == {:ok, nil}

      File.rm!(file1)
      File.rm!(file2)
    end


  end

end
