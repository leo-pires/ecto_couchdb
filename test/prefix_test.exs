defmodule Couchdb.Ecto.PrefixTest do
  use Couchdb.Ecto.DataCase, async: false
  alias Couchdb.Ecto.{Helpers, Fetchers}


  setup do
    clear_db!()
    :ok
  end

  describe "schema operations" do

    setup do
      clear_db!()
      :ok
    end

    test "insert" do
      # prepare db
      prefix = "prefix1"
      clear_db!(prefix)
      assert {:ok, inserted} = Post.changeset(%Post{}, %{title: "test1"}) |> TestRepo.insert(prefix: prefix)
      # fails to fetch on "main db", but succeds on "prefix db"
      assert {:ok, nil} = Fetchers.get(TestRepo, Post, inserted._id)
      assert {:ok, fetched} = TestRepo |> Helpers.db_from_repo(prefix: prefix) |> ICouch.open_doc(inserted._id)
      assert fetched.rev == inserted._rev
      assert ICouch.Document.get(fetched, "title") == inserted.title
    end

    test "update" do
      # prepare db
      prefix = "prefix2"
      clear_db!(prefix)
      # insert using prefix
      assert {:ok, inserted} = Post.changeset(%Post{}, %{title: "test2"}) |> TestRepo.insert(prefix: prefix)
      # update using the builtin prefix
      assert {:ok, updated} = Post.changeset(inserted, %{title: "test3"}) |> TestRepo.update
      # fails to fetch on "main db", but succeds on "prefix db"
      assert {:ok, nil} = Fetchers.get(TestRepo, Post, inserted._id)
      assert {:ok, fetched2} = TestRepo |> Helpers.db_from_repo(prefix: prefix) |> ICouch.open_doc(inserted._id)
      assert fetched2.rev == updated._rev
      assert ICouch.Document.get(fetched2, "title") == updated.title
    end

    test "delete" do
      # prepare db
      prefix = "prefix3"
      clear_db!(prefix)
      # insert using prefix
      assert {:ok, inserted} = Post.changeset(%Post{}, %{title: "test4"}) |> TestRepo.insert(prefix: prefix)
      # delete using the builtin prefix
      assert {:ok, _} = inserted |> TestRepo.delete
      # fails to fetch after deleted
      assert {:error, _} = TestRepo |> Helpers.db_from_repo(prefix: prefix) |> ICouch.open_doc(inserted._id)
    end

    test "insert_all" do
      # prepare db and viewes
      prefix = "prefix4"
      clear_db!(prefix)
      create_views!(@schema_design_docs, prefix: prefix)
      # insert
      posts = Enum.map(["test4", "test4"], fn title ->
        %{title: title}
      end)
      assert {2, nil} == TestRepo.insert_all(Post, posts, prefix: prefix)
      # check
      {:ok, %{rows: [_post1, _post2]}} = TestRepo |> Helpers.db_from_repo(prefix: prefix) |> ICouch.open_view!("Post/all") |> ICouch.View.fetch
    end

    test "raises error if db not found" do
      assert_raise RuntimeError, fn ->
        Post.changeset(%Post{}, %{title: "test1"}) |> TestRepo.insert(prefix: "xpto")
      end
    end

  end

  describe "fetchers" do

    test "get" do
      # prepare db
      prefix = "prefix5"
      clear_db!(prefix)
      assert {:ok, inserted} = Post.changeset(%Post{}, %{title: "test1"}) |> TestRepo.insert(prefix: prefix)
      # fetch
      assert {:ok, fetched} = Fetchers.get(TestRepo, Post, inserted._id, prefix: prefix)
      # check
      assert fetched == inserted
    end

    test "all" do
      # prepare db and viewes
      prefix = "prefix6"
      clear_db!(prefix)
      create_views!(@schema_design_docs, prefix: prefix)
      # insert
      assert {:ok, inserted} = Post.changeset(%Post{}, %{title: "test1"}) |> TestRepo.insert(prefix: prefix)
      # check
      assert {:ok, [fetched]} = Fetchers.all(TestRepo, Post, {"Post", "all"}, include_docs: true, prefix: prefix)
      assert fetched == inserted
    end

  end

end
