defmodule Couchdb.Ecto.FetchersTest do
  use Couchdb.Ecto.ModelCase, async: false
  alias Couchdb.Ecto.Fetchers


  setup do
    clear_db!()
    :ok
  end

  describe "get" do

    setup do
      clear_db!()
      insert_docs!(@posts)
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

    test "get nil id" do
      assert {:error, :missing_id} = Fetchers.get(TestRepo, Post, nil)
    end

  end

  describe "get_many" do

    setup do
      clear_db!()
      insert_docs!(@posts)
      :ok
    end

    test "get_many" do
      post1_id = "id1"
      post2_id = "id2"
      assert {:ok, [many_fetched1, many_fetched2]} = Fetchers.get_many(TestRepo, Post, [post1_id, post2_id])
      assert {:ok, fetched1} = Fetchers.get(TestRepo, Post, post1_id)
      assert {:ok, fetched2} = Fetchers.get(TestRepo, Post, post2_id)
      assert many_fetched1 == fetched1
      assert many_fetched2 == fetched2
    end

  end

  describe "one and all" do

    setup do
      create_views!(@schema_design_docs)
      insert_docs!(@posts)
      TestRepo.insert! %User{_id: "test-user-id0", username: "bob", email: "bob@gmail.com"}
      :ok
    end

    test "fetch one returns Ecto model with include_docs" do
      {:ok, %User{} = u} = Fetchers.one(TestRepo, User, :all_no_doc, key: "test-user-id0", include_docs: true)
      assert u._id == "test-user-id0"
      assert not is_nil(u._rev)
      assert u.username == "bob"
      assert u.email == "bob@gmail.com"
    end

    test "fetch one returns a map" do
      {:ok, u} = Fetchers.one(TestRepo, User, :all, key: "test-user-id0")
      assert u[:__struct__] == nil
      assert u["_id"] == "test-user-id0"
      assert u["username"] == "bob"
      assert u["email"] == "bob@gmail.com"
    end

    test "fetch one returns nil if not found" do
      assert {:ok, nil} = Fetchers.one(TestRepo, User, :all, key: "xpto")
    end

    test "fetch one return error if more than one found" do
      {:error, :too_many_results} = Fetchers.one(TestRepo, Post, :all)
    end

    test "fetch_all limit" do
      TestRepo.insert! %User{_id: "test-user-id1", username: "bob", email: "bob@gmail.com"}
      {:ok, pf} = Fetchers.all(TestRepo, User, :all, limit: 1)
      assert [_] = pf
      assert hd(pf)["_id"] == "test-user-id0"
    end

    test "fetch_all descending" do
      TestRepo.insert! %User{_id: "test-user-id1", username: "bob", email: "bob@gmail.com"}
      {:ok, pf} = Fetchers.all(TestRepo, User, :all, descending: true)
      assert length(pf) == 2
      assert hd(pf)["_id"] == "test-user-id1"
    end

    test "fetch_one limit and descending" do
      TestRepo.insert! %User{_id: "test-user-id1", username: "bob", email: "bob@gmail.com"}
      {:ok, pf} = Fetchers.one(TestRepo, User, :all, limit: 1, descending: true)
      assert pf["_id"] == "test-user-id1"
    end

    test "fetch all" do
      {:ok, list} = Fetchers.all(TestRepo, Post, :all)
      assert length(list) == 3
      {:ok, list} = Fetchers.all(TestRepo, User, :all)
      assert [_] = list
    end

    test "fetch all by keys" do
      {:ok, list} = Fetchers.all(TestRepo, Post, :all, keys: ["id1", "id2"])
      assert length(list) == 2
      assert (list |> Enum.at(0))["_id"] == "id1"
      assert (list |> Enum.at(1))["_id"] == "id2"
    end

    test "fetch one and all with custom ddoc/view" do
      {:ok, list} = Fetchers.all(TestRepo, Post, {:Post, :all}, keys: ["id1", "id2"])
      assert length(list) == 2
      assert (list |> Enum.at(0))["_id"] == "id1"
      assert (list |> Enum.at(1))["_id"] == "id2"
    end

    test "fetch_all with group_level 0" do
      {:ok, list} = Fetchers.all(TestRepo, User, :counts, group_level: 0)
      assert list == [4]
    end

    test "fetch_all with return_keys" do
      {:ok, list} = Fetchers.all(TestRepo, Post, :all, return_keys: true)
      assert [{"id1", %{"_id" => "id1"}}, {"id2", %{"_id" => "id2"}}, {"id3", %{"_id" => "id3"}}] = list
      {:ok, list} = Fetchers.all(TestRepo, Post, :all, include_docs: true, return_keys: true)
      assert [{"id1", %Post{_id: "id1"}}, {"id2", %Post{_id: "id2"}}, {"id3", %Post{_id: "id3"}}] = list
      {:ok, list} = Fetchers.all(TestRepo, User, :counts, group_level: 0, return_keys: true)
      assert list == [{nil, 4}]
    end

    test "returns error if invalid view name" do
      assert {:error, :view_not_found} = Fetchers.one(TestRepo, Post, :xpto)
      assert {:error, :view_not_found} = Fetchers.all(TestRepo, Post, :xpto)
    end

  end

  describe "has_one and has_many" do

    setup do
      insert_docs!(@posts |> Enum.map(&(&1 |> Map.put(:user_id, "test-user"))))
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

  describe "multiple_all" do
    setup do
      create_views!(@schema_design_docs)
      insert_docs!(@posts)
      TestRepo.insert! %User{_id: "test-user-id1", type: "User", username: "bob", email: "bob@gmail.com"}
      TestRepo.insert! %User{_id: "test-user-id2", type: "User", username: "alice", email: "alice@gmail.com"}
      TestRepo.insert! %User{_id: "test-user-id3", type: "User", username: "bob", email: "bob@gmail.com"}
      :ok
    end

    test "multiple_all" do
      {:ok, list} = Fetchers.multiple_all(TestRepo, User, :all, [%{key: "test-user-id1", include_docs: true}, %{key: "test-user-id2"}])
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

    test "multiple_all with group_level 0" do
      {:ok, list} = Fetchers.multiple_all(TestRepo, User, :counts, [%{group_level: 0}])
      assert list == [[6]]
    end

    test "multiple_all with return_keys" do
      {:ok, list} = Fetchers.multiple_all(TestRepo, User, :counts, [%{group_level: 0}], [return_keys: true])
      assert list == [[{nil, 6}]]
    end

  end

  describe "find" do

    setup do
      insert_docs!(@posts)
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

    setup do
      create_views!(@schema_design_docs)
      TestRepo.insert! %User{_id: "test-user", username: "test", email: "test"}
      TestRepo.insert! %User{_id: "test-user-id0", username: "bob", email: "bob@gmail.com"}
      insert_docs!(@posts |> Enum.map(&(&1 |> Map.put(:user_id, "test-user"))))
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
      {:ok, pf} = Fetchers.one(TestRepo, Post, :all, key: pc._id, include_docs: true, preload: :user)
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
      {:ok, pf} = Fetchers.get(TestRepo, User, "test-user", preload: :posts)
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

  describe "fetch with schema map" do

    setup do
      create_views!(@schema_design_docs)
      user = TestRepo.insert! %User{_id: "test-user-id1", username: "bob", email: "bob@gmail.com"}
      post = TestRepo.insert! %Post{title: "lorem", body: "lorem ipsum", user: user}
      map_fun = fn %{"type" => type} ->
        case type do
          "User" -> User
          "Post" -> Post
          _ -> nil
        end
      end
      %{user: user, post: post, map_fun: map_fun}
    end

    test "get", %{user: user, post: post, map_fun: map_fun} do
      assert {:ok, %User{} = fetched_user} = Fetchers.get(TestRepo, map_fun, user._id)
      assert fetched_user._id == user._id
      assert fetched_user._rev == user._rev
      assert fetched_user.username == user.username
      assert {:ok, %Post{} = fetched_post} = Fetchers.get(TestRepo, map_fun, post._id)
      assert fetched_post._id == post._id
      assert fetched_post._rev == post._rev
      assert fetched_post.title == post.title
    end

    test "get and preload", %{user: user, post: post, map_fun: map_fun} do
      assert {:ok, %Post{} = fetched_post} = Fetchers.get(TestRepo, map_fun, post._id, preload: :user)
      assert fetched_post._id == post._id
      assert fetched_post._rev == post._rev
      assert fetched_post.title == post.title
      fetched_user = fetched_post.user
      assert fetched_user._id == user._id
      assert fetched_user._rev == user._rev
      assert fetched_user.username == user.username
    end

  end

  describe "fetch return raw" do

    setup do
      clear_db!()
      create_views!(@schema_design_docs)
      TestRepo.insert! %User{_id: "test-user-id0", username: "bob", email: "bob@gmail.com"}
      :ok
    end

    test "get raw" do
      assert {:ok, u} = Fetchers.get(TestRepo, User, "test-user-id0")
      assert {:ok, u_raw} = Fetchers.get(TestRepo, :raw, "test-user-id0")
      assert u_raw["username"] == u.username
    end

    test "all raw" do
      assert {:ok, u} = Fetchers.get(TestRepo, User, "test-user-id0")
      {:ok, [u_raw]} = Fetchers.all(TestRepo, :raw, {"User", :all}, include_docs: true)
      assert u_raw["username"] == u.username
    end

  end

  describe "RepoFetchers" do

    test "get" do
      TestRepo.get(Post, "foo")
    end

  end

end
