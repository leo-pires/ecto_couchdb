defmodule Couchdb.Ecto.FetchersTest do
  use ExUnit.Case, async: false
  use TestModelCase
  alias TestRepo.FetchersHelper, as: TestRepo
  alias Couchdb.Ecto.Fetchers

  setup do
    TestRepo |> clear_db!
    %{
      repo: TestRepo,
      db: TestRepo |> Couchdb.Ecto.db_from_repo,
      post: @post,
      posts: @posts,
      grants: @grants
    }
  end


  describe "get" do

    setup %{posts: posts} do
      TestRepo |> insert_docs!(posts)
      TestRepo.insert! %User{_id: "test-user-id0", username: "bob", email: "bob@gmail.com"}
      :ok
    end

    test "get by key" do
      {:ok, u} = Fetchers.get(TestRepo, User, "test-user-id0")
      assert u._id == "test-user-id0"
      assert not is_nil(u._rev)
      assert u.username == "bob"
      assert u.email == "bob@gmail.com"
    end

    test "get by key and preload" do
      pc = TestRepo.insert! %Post{title: "lorem", body: "lorem ipsum", user: %User{_id: "test-user-id1", username: "john", email: "john@gmail.com"}}
      {:ok, pf} = Fetchers.get(TestRepo, Post, pc._id, preload: :user)
      assert pf.title == "lorem"
      assert pf.body == "lorem ipsum"
      assert pf.user._id == "test-user-id1"
      assert pf.user.username == "john"
      assert pf.user.email == "john@gmail.com"
    end

    test "get return nil if not found" do
      {:ok, data} = Fetchers.get(TestRepo, Post, "xpto")
      assert is_nil(data)
    end

  end

  describe "fetch_one and fetch_all" do

    setup %{posts: posts} do
      TestRepo |> create_views!(@schema_design_docs)
      TestRepo |> insert_docs!(posts)
      TestRepo.insert! %User{_id: "test-user-id0", username: "bob", email: "bob@gmail.com"}
      :ok
    end

    test "fetch one returns Ecto model with include_docs" do
      {:ok, %User{} = u} = Fetchers.fetch_one(TestRepo, User, :all_no_doc, key: "test-user-id0", include_docs: true)
      assert u._id == "test-user-id0"
      assert not is_nil(u._rev)
      assert u.username == "bob"
      assert u.email == "bob@gmail.com"
    end

    test "fetch one returns a map" do
      {:ok, u} = Fetchers.fetch_one(TestRepo, User, :all, key: "test-user-id0")
      assert u[:__struct__] == nil
      assert u["_id"] == "test-user-id0"
      assert u["username"] == "bob"
      assert u["email"] == "bob@gmail.com"
    end

    test "fetch one returns nil if not found" do
      assert {:ok, nil} = Fetchers.fetch_one(TestRepo, User, :all, key: "xpto")
    end

    test "fetch one return :many if more than one found" do
      {:ok, :many} = Fetchers.fetch_one(TestRepo, Post, :all)
    end

    test "fetch_all limit" do
      TestRepo.insert! %User{_id: "test-user-id1", username: "bob", email: "bob@gmail.com"}
      {:ok, pf} = Fetchers.fetch_all(TestRepo, User, :all, limit: 1)
      assert [_] = pf
      assert hd(pf)["_id"] == "test-user-id0"
    end

    test "fetch_all descending" do
      TestRepo.insert! %User{_id: "test-user-id1", username: "bob", email: "bob@gmail.com"}
      {:ok, pf} = Fetchers.fetch_all(TestRepo, User, :all, descending: true)
      assert length(pf) == 2
      assert hd(pf)["_id"] == "test-user-id1"
    end

    test "fetch_one limit and descending" do
      TestRepo.insert! %User{_id: "test-user-id1", username: "bob", email: "bob@gmail.com"}
      {:ok, pf} = Fetchers.fetch_one(TestRepo, User, :all, limit: 1, descending: true)
      assert pf["_id"] == "test-user-id1"
    end

    test "fetch all" do
      {:ok, list} = Fetchers.fetch_all(TestRepo, Post, :all)
      assert length(list) == 3
      {:ok, list} = Fetchers.fetch_all(TestRepo, User, :all)
      assert [_] = list
    end

    test "fetch all by keys" do
      {:ok, list} = Fetchers.fetch_all(TestRepo, Post, :all, keys: ["id1", "id2"])
      assert length(list) == 2
      assert (list |> Enum.at(0))["_id"] == "id1"
      assert (list |> Enum.at(1))["_id"] == "id2"
    end

    test "fetch one and all with custom ddoc/view" do
      {:ok, list} = Fetchers.fetch_all(TestRepo, Post, {:Post, :all}, keys: ["id1", "id2"])
      assert length(list) == 2
      assert (list |> Enum.at(0))["_id"] == "id1"
      assert (list |> Enum.at(1))["_id"] == "id2"
    end

    test "fetch_all with group_level 0" do
      {:ok, list} = Fetchers.fetch_all(TestRepo, User, :counts, group_level: 0)
      assert list == [4]
    end

    test "fetch_all with return_keys" do
      {:ok, list} = Fetchers.fetch_all(TestRepo, User, :counts, group_level: 0, return_keys: true)
      assert list == [{nil, 4}]
    end

    test "raise if invalid view name" do
      assert_raise RuntimeError, fn -> Fetchers.fetch_all(TestRepo, Post, :xpto) end
    end

  end

  describe "has_one and has_many" do

    setup(%{posts: posts}) do
      TestRepo |> insert_docs!(posts |> Enum.map(&(&1 |> Map.put(:user_id, "test-user"))))
      TestRepo.insert! %User{_id: "test-user", username: "test", email: "test"}
      :ok
    end

    test "has_one supports cast_assoc" do
      pc = TestRepo.insert! User.changeset_user_data(%User{}, %{_id: "u1", username: "foo", email: "goo", user_data: %{_id: "ud1", extra: "bar"}})
      {:ok, uf} = Fetchers.get(TestRepo, User, "u1")
      {:ok, udf} = Fetchers.get(TestRepo, UserData, "ud1")
      assert pc._id == uf._id
      assert pc.username == uf.username
      assert pc.email == uf.email
      assert udf._id == "ud1"
      assert udf.user_id == pc._id
      assert udf.extra == "bar"
    end

  end

  describe "multiple_fetch_all" do
    setup %{posts: posts} do
      TestRepo |> create_views!(@schema_design_docs)
      TestRepo |> insert_docs!(posts)
      TestRepo.insert! %User{_id: "test-user-id1", type: "User", username: "bob", email: "bob@gmail.com"}
      TestRepo.insert! %User{_id: "test-user-id2", type: "User", username: "alice", email: "alice@gmail.com"}
      TestRepo.insert! %User{_id: "test-user-id3", type: "User", username: "bob", email: "bob@gmail.com"}
      :ok
    end

    test "multiple_fetch_all" do
      {:ok, list} = Fetchers.multiple_fetch_all(TestRepo, User, :all, [%{key: "test-user-id1", include_docs: true}, %{key: "test-user-id2"}])
      a = list |> Enum.at(0) |> Enum.at(0)
      b = list |> Enum.at(1) |> Enum.at(0)
      assert a.__struct__ == User
      assert a._id == "test-user-id1"
      assert a.username == "bob"
      assert a.email == "bob@gmail.com"
      assert b[:__struct__] == nil
      assert b["_id"] == "test-user-id2"
      assert b["username"] == "alice"
      assert b["email"] == "alice@gmail.com"
    end

    test "multiple_fetch_all with group_level 0" do
      {:ok, list} = Fetchers.multiple_fetch_all(TestRepo, User, :counts, [%{group_level: 0}])
      assert list == [[6]]
    end

    test "multiple_fetch_all with return_keys" do
      {:ok, list} = Fetchers.multiple_fetch_all(TestRepo, User, :counts, [%{group_level: 0}], return_keys: true)
      assert list == [[{nil, 6}]]
    end

  end

  describe "find" do

    setup %{posts: posts} do
      TestRepo |> insert_docs!(posts)
      TestRepo.insert! %User{_id: "test-user-id1", type: "User", username: "bob", email: "bob@gmail.com"}
      TestRepo.insert! %User{_id: "test-user-id2", type: "User", username: "alice", email: "alice@gmail.com"}
      TestRepo.insert! %User{_id: "test-user-id3", type: "User", username: "bob", email: "bob@gmail.com"}
      :ok
    end

    test "find" do
      {:ok, %{docs: list}} = Fetchers.find(TestRepo, User, selector: %{username: %{"$eq" => "alice"}})
      assert [%User{} = first | _] = list
      assert first._id == "test-user-id2"
      assert first.email == "alice@gmail.com"
    end

  end

  describe "preload" do

    defmodule D do
      use Ecto.Schema
      @primary_key false
      @foreign_key_type :binary_id
      schema "D" do
        field :_id, :binary_id, autogenerate: true, primary_key: true
        field :_rev, :string, read_after_writes: true, primary_key: true
        field :type, :string, read_after_writes: true
        field :title, :string
      end
      def changeset(struct, params) do
        struct |> Ecto.Changeset.cast(params, [:title])
      end
    end
    defmodule C do
      use Ecto.Schema
      @primary_key false
      @foreign_key_type :binary_id
      schema "C" do
        field :_id, :binary_id, autogenerate: true, primary_key: true
        field :_rev, :string, read_after_writes: true, primary_key: true
        field :type, :string, read_after_writes: true
        field :title, :string
        belongs_to :d, D, references: :_id
      end
      def changeset(struct, params) do
        struct |> Ecto.Changeset.cast(params, [:title]) |> Ecto.Changeset.cast_assoc(:d)
      end
    end
    defmodule B do
      use Ecto.Schema
      @primary_key false
      @foreign_key_type :binary_id
      schema "B" do
        field :_id, :binary_id, autogenerate: true, primary_key: true
        field :_rev, :string, read_after_writes: true, primary_key: true
        field :type, :string, read_after_writes: true
        field :title, :string
        belongs_to :c, C, references: :_id
      end
      def changeset(struct, params) do
        struct |> Ecto.Changeset.cast(params, [:title]) |> Ecto.Changeset.cast_assoc(:c)
      end
    end
    defmodule A do
      use Ecto.Schema
      @primary_key false
      @foreign_key_type :binary_id
      schema "A" do
        field :_id, :binary_id, autogenerate: true, primary_key: true
        field :_rev, :string, read_after_writes: true, primary_key: true
        field :type, :string, read_after_writes: true
        field :title, :string
        belongs_to :b, B, references: :_id
      end
      def changeset(struct, params) do
        struct |> Ecto.Changeset.cast(params, [:title]) |> Ecto.Changeset.cast_assoc(:b)
      end
    end

    setup %{posts: posts} do
      TestRepo |> create_views!(@schema_design_docs)
      TestRepo.insert! %User{_id: "test-user", username: "test", email: "test"}
      TestRepo.insert! %User{_id: "test-user-id0", username: "bob", email: "bob@gmail.com"}
      TestRepo |> insert_docs!(posts |> Enum.map(&(&1 |> Map.put(:user_id, "test-user"))))
      :ok
    end

    test "normalize_preloads" do
      assert Couchdb.Ecto.ResultProcessor.normalize_preloads(:b) == [b: []]
      assert Couchdb.Ecto.ResultProcessor.normalize_preloads([:b]) == [b: []]
      assert Couchdb.Ecto.ResultProcessor.normalize_preloads([b: [c: :d]]) == [b: [c: [d: []]]]
      assert Couchdb.Ecto.ResultProcessor.normalize_preloads([b: [c: [:d]]]) == [b: [c: [d: []]]]
      assert Couchdb.Ecto.ResultProcessor.normalize_preloads([b: [:c, :d]]) == [b: [c: [], d: []]]
      assert Couchdb.Ecto.ResultProcessor.normalize_preloads([b: [c: [:d]]]) == [b: [c: [d: []]]]
      assert Couchdb.Ecto.ResultProcessor.normalize_preloads([b: [c: [:d, :e]]]) == [b: [c: [d: [], e: []]]]
      assert Couchdb.Ecto.ResultProcessor.normalize_preloads([b: [c: [:d, :e]], f: :g]) == [b: [c: [d: [], e: []]], f: [g: []]]
    end

    test "get preload" do
      pc = TestRepo.insert! A.changeset(%A{}, %{title: "a", b: %{title: "b", c: %{title: "c", d: %{title: "d"}}}})
      {:ok, a1} = Fetchers.get(TestRepo, A, pc._id, preload: [b: :c])
      assert a1.title == "a"
      assert a1.b.title == "b"
      assert a1.b.c.title == "c"
      {:ok, a2} = Fetchers.get(TestRepo, A, pc._id, preload: [b: [c: :d]])
      assert a2.title == "a"
      assert a2.b.title == "b"
      assert a2.b.c.title == "c"
      assert a2.b.c.d.title == "d"
    end

    test "get preload missing association" do
      pc = TestRepo.insert! A.changeset(%A{}, %{title: "a"})
      assert not is_nil(Fetchers.get(TestRepo, A, pc._id, preload: :b))
      assert not is_nil(Fetchers.get(TestRepo, A, pc._id, preload: [b: :c]))
    end

    test "fetch_one and preload" do
      pc = TestRepo.insert! %Post{title: "lorem", body: "lorem ipsum", user: %User{_id: "test-user-id1", username: "john", email: "john@gmail.com"}}
      {:ok, pf} = Fetchers.fetch_one(TestRepo, Post, :all, key: pc._id, include_docs: true, preload: :user)
      assert pf.title == "lorem"
      assert pf.body == "lorem ipsum"
      assert pf.user._id == "test-user-id1"
      assert pf.user.username == "john"
      assert pf.user.email == "john@gmail.com"
    end

    test "get and fetch preloading has_one" do
      pc = TestRepo.insert! User.changeset_user_data(%User{}, %{_id: "u1", username: "foo", email: "goo", user_data: %{_id: "ud1", extra: "bar"}})
      {:ok, udf} = Fetchers.get(TestRepo, UserData, "ud1")
      {:ok, uf} = Fetchers.get(TestRepo, User, "u1", preload: :user_data)
      assert pc._id == uf._id
      assert pc.username == uf.username
      assert pc.email == uf.email
      assert udf._id == "ud1"
      assert udf.user_id == pc._id
      assert udf.extra == "bar"
      assert uf.user_data._id == udf._id
      assert uf.user_data.user_id == udf.user_id
      assert uf.user_data.extra == udf.extra
    end

    test "get and fetch preloading has_many" do
      {:ok, pf} = Fetchers.get(TestRepo, User, "test-user", preload: :posts, include_docs: true)
      assert length(pf.posts) == 3
    end

    test "find with preloads" do
      pc = TestRepo.insert! %Post{title: "chibata", body: "lorem ipsum", user: %User{_id: "test-user-id-john", username: "john", email: "john@gmail.com"}}
      {:ok, %{docs: list}} = Fetchers.find(TestRepo, Post, selector: %{title: %{"$eq" => "chibata"}}, preload: :user)
      a = list |> hd
      assert a._id == pc._id
      assert a.title == "chibata"
      assert a.user_id == "test-user-id-john"
      assert not is_nil(a.user)
      assert a.user._id == "test-user-id-john"
    end

  end

  describe "Couchdb.Ecto.TestRepo" do

    test "get" do
      TestRepo.get(Post, "foo")
    end

  end

end
