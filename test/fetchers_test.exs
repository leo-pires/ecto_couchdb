defmodule Couchdb.Ecto.FetchersTest do
  use ExUnit.Case, async: true
  import TestSupport
  alias TestRepo.FetchersHelper, as: TestRepo
  alias Couchdb.Ecto.Fetchers
  alias Couchdb.Ecto.Attachment

  @post_ddoc_id "Post"
  @post_ddoc_code %{
    views: %{
      all: %{
        map: "function(doc) { if (doc.type === 'Post') emit(doc._id, doc) }"
      },
      by_user_id: %{
        map: "function(doc) { if (doc.type === 'Post' && doc.user_id) emit(doc.user_id, doc) }"
      }
    }
  }
  @user_ddoc_id "User"
  @user_ddoc_code %{
    views: %{
      all: %{
        map: "function(doc) { if (doc.type === 'User') emit(doc._id, doc) }"
      },
      all_no_doc: %{
        map: "function(doc) { if (doc.type === 'User') emit(doc._id, null) }"
      },
      counts: %{
        map: "function(doc) { emit(doc._id, 1) }",
        reduce: "_count"
      }
    }
  }
  @user_data_ddoc_id "UserData"
  @user_data_ddoc_code %{
    views: %{
      all: %{
        map: "function(doc) { if (doc.type === 'UserData') emit(doc.user_id, doc) }"
      },
      by_user_id: %{
        map: "function(doc) { if (doc.type === 'UserData' && doc.user_id) emit(doc.user_id, doc) }"
      }
    }
  }
  @design_docs [
    {@post_ddoc_id, @post_ddoc_code},
    {@user_ddoc_id, @user_ddoc_code},
    {@user_data_ddoc_id, @user_data_ddoc_code}
  ]
  @post %Post{title: "how to write and adapter", body: "Don't know yet"}
  @grants [%Grant{user: "admin", access: "all"}, %Grant{user: "other", access: "read"}]

  setup do
    TestRepo |> clear_db!
    posts =
      for i <- 1..3 do
        %{
          _id: "id#{i}",
          type: "Post",
          title: "t#{i}",
          body: "b#{i}",
          stats: %{visits: i, time: 10 * i},
          grants: [
            %{id: "1", user: "u#{i}.1", access: "a#{i}.1"},
            %{id: "2", user: "u#{i}.2", access: "a#{i}.2"}
          ]
        }
      end
    %{
      repo: TestRepo,
      db: TestRepo |> Couchdb.Ecto.db_from_repo,
      db_props: TestRepo |> Couchdb.Ecto.db_props_for,
      design_docs: @design_docs,
      post: @post,
      posts: posts,
      grants: @grants
    }
  end


  describe "get and fetch" do
    setup %{design_docs: design_docs, posts: posts} do
      TestRepo |> create_views!(design_docs)
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

    test "get as map" do
      {:ok, u} = Fetchers.get(TestRepo, User, "test-user-id0", as_map: true)
      assert u |> Map.get(:_id) == "test-user-id0"
      assert not is_nil(u |> Map.get(:_rev))
      assert u |> Map.get(:username) == "bob"
      assert u |> Map.get(:email) == "bob@gmail.com"
    end

    test "get as raw map" do
      {:ok, u} = Fetchers.get(TestRepo, User, "test-user-id0", as_map: :raw)
      assert u |> Map.get("_id") == "test-user-id0"
      assert not is_nil(u |> Map.get("_rev"))
      assert u |> Map.get("username") == "bob"
      assert u |> Map.get("email") == "bob@gmail.com"
    end

    test "get return nil if not found" do
      {:ok, data} = Fetchers.get(TestRepo, Post, "xpto")
      assert is_nil(data)
    end

    test "fetch one returns struct" do
      {:ok, u} = Fetchers.fetch_one(TestRepo, User, :all, key: "test-user-id0")
      assert u._id == "test-user-id0"
      assert not is_nil(u._rev)
      assert u.username == "bob"
      assert u.email == "bob@gmail.com"
    end

    test "fetch one returns struct with include_docs" do
      {:ok, u} = Fetchers.fetch_one(TestRepo, User, :all_no_doc, key: "test-user-id0", include_docs: true)
      assert u._id == "test-user-id0"
      assert not is_nil(u._rev)
      assert u.username == "bob"
      assert u.email == "bob@gmail.com"
    end

    test "fetch one returns nil if not found" do
      assert {:ok, nil} = Fetchers.fetch_one(TestRepo, User, :all, key: "xpto")
    end

    test "fetch one return :many if more than one found" do
      {:ok, :many} = Fetchers.fetch_one(TestRepo, Post, :all)
    end

    test "fetch_one and preload" do
      pc = TestRepo.insert! %Post{title: "lorem", body: "lorem ipsum", user: %User{_id: "test-user-id1", username: "john", email: "john@gmail.com"}}
      {:ok, pf} = Fetchers.fetch_one(TestRepo, Post, :all, key: pc._id, preload: :user)
      assert pf.title == "lorem"
      assert pf.body == "lorem ipsum"
      assert pf.user._id == "test-user-id1"
      assert pf.user.username == "john"
      assert pf.user.email == "john@gmail.com"
    end

    test "fetch_one and preload with as_map" do
      pc = TestRepo.insert! %Post{title: "lorem", body: "lorem ipsum", user: %User{_id: "test-user-id1", username: "john", email: "john@gmail.com"}}
      {:ok, pf} = Fetchers.fetch_one(TestRepo, Post, :all, key: pc._id, preload: :user, as_map: true)
      assert pf.title == "lorem"
      assert pf.body == "lorem ipsum"
      assert pf.user._id == "test-user-id1"
      assert pf.user.username == "john"
      assert pf.user.email == "john@gmail.com"
    end

    test "fetch_all limit" do
      TestRepo.insert! %User{_id: "test-user-id1", username: "bob", email: "bob@gmail.com"}
      {:ok, pf} = Fetchers.fetch_all(TestRepo, User, :all, limit: 1)
      assert [_] = pf
      assert hd(pf)._id == "test-user-id0"
    end

    test "fetch_all limit with include_docs" do
      TestRepo.insert! %User{_id: "test-user-id1", username: "bob", email: "bob@gmail.com"}
      {:ok, pf} = Fetchers.fetch_all(TestRepo, User, :all_no_doc, include_docs: true, limit: 1)
      assert [_] = pf
      assert hd(pf)._id == "test-user-id0"
    end

    test "fetch_all descending" do
      TestRepo.insert! %User{_id: "test-user-id1", username: "bob", email: "bob@gmail.com"}
      {:ok, pf} = Fetchers.fetch_all(TestRepo, User, :all, descending: true)
      assert length(pf) == 2
      assert hd(pf)._id == "test-user-id1"
    end

    test "fetch_one limit and descending" do
      TestRepo.insert! %User{_id: "test-user-id1", username: "bob", email: "bob@gmail.com"}
      {:ok, pf} = Fetchers.fetch_one(TestRepo, User, :all, limit: 1, descending: true)
      assert pf._id == "test-user-id1"
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
      assert (list |> Enum.at(0))._id == "id1"
      assert (list |> Enum.at(1))._id == "id2"
    end

    test "fetch one with custom ddoc" do
      {:ok, u} = Fetchers.fetch_one(TestRepo, User, {:User, :all}, key: "test-user-id0")
      assert u._id == "test-user-id0"
      assert not is_nil(u._rev)
      assert u.username == "bob"
      assert u.email == "bob@gmail.com"
    end

    test "fetch one and all with custom ddoc" do
      {:ok, list} = Fetchers.fetch_all(TestRepo, Post, {:Post, :all}, keys: ["id1", "id2"])
      assert length(list) == 2
      assert (list |> Enum.at(0))._id == "id1"
      assert (list |> Enum.at(1))._id == "id2"
    end

    test "raise if invalid view name" do
      assert_raise RuntimeError, fn -> Fetchers.fetch_all(TestRepo, Post, :xpto) end
    end

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
  end

  describe "has_one support" do
    setup(%{design_docs: design_docs, posts: posts}) do
      TestRepo |> create_views!(design_docs)
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

  end

  describe "Couchdb.Ecto.TestRepo" do
    test "get" do
      TestRepo.get(Post, "foo")
    end
  end

  describe "direct http calls" do
    setup %{design_docs: design_docs, posts: posts} do
      TestRepo |> create_views!(design_docs)
      TestRepo |> insert_docs!(posts)
      TestRepo.insert! %User{_id: "test-user-id1", type: "User", username: "bob", email: "bob@gmail.com"}
      TestRepo.insert! %User{_id: "test-user-id2", type: "User", username: "alice", email: "alice@gmail.com"}
      TestRepo.insert! %User{_id: "test-user-id3", type: "User", username: "bob", email: "bob@gmail.com"}
      :ok
    end

    test "multiple_fetch_all works for Ecto schema" do
      {:ok, list} = Fetchers.multiple_fetch_all(TestRepo, User, :all, [%{key: "test-user-id1"}, %{key: "test-user-id2"}])
      a = list |> Enum.at(0) |> Enum.at(0)
      b = list |> Enum.at(1) |> Enum.at(0)
      assert a.__struct__ == User
      assert a._id == "test-user-id1"
      assert a.username == "bob"
      assert a.email == "bob@gmail.com"
      assert b.__struct__ == User
      assert b._id == "test-user-id2"
      assert b.username == "alice"
      assert b.email == "alice@gmail.com"
    end

    test "multiple_fetch_all works for map" do
      {:ok, list} = Fetchers.multiple_fetch_all(TestRepo, User, :all, [%{key: "test-user-id1"}, %{key: "test-user-id2"}], as_map: true)
      a = list |> Enum.at(0) |> Enum.at(0)
      b = list |> Enum.at(1) |> Enum.at(0)
      assert is_nil(Map.get(a, :__struct__))
      assert a._id == "test-user-id1"
      assert a.username == "bob"
      assert a.email == "bob@gmail.com"
      assert is_nil(Map.get(b, :__struct__))
      assert b._id == "test-user-id2"
      assert b.username == "alice"
      assert b.email == "alice@gmail.com"
    end

    test "multiple_fetch_all group_level 0" do
      {:ok, list} = Fetchers.multiple_fetch_all(TestRepo, User, :counts, [%{group_level: 0}], as_map: true)
      assert list == [[6]]
    end

    test "multiple_fetch_all with return_keys" do
      {:ok, list} = Fetchers.multiple_fetch_all(TestRepo, User, :counts, [%{group_level: 0}], as_map: true, return_keys: true)
      assert list == [[{nil, 6}]]
    end

    test "find" do
      {:ok, %{docs: list}} = Fetchers.find(TestRepo, User, %{selector: %{username: %{"$eq" => "alice"}}})
      a = list |> hd
      assert a._id == "test-user-id2"
      assert a.email == "alice@gmail.com"
    end

    test "find with preloads" do
      pc = TestRepo.insert! %Post{title: "chibata", body: "lorem ipsum", user: %User{_id: "test-user-id-john", username: "john", email: "john@gmail.com"}}
      {:ok, %{docs: list}} = Fetchers.find(TestRepo, Post, %{selector: %{title: %{"$eq" => "chibata"}}}, preload: :user)
      a = list |> hd
      assert a._id == pc._id
      assert a.title == "chibata"
      assert a.user_id == "test-user-id-john"
      assert not is_nil(a.user)
      assert a.user._id == "test-user-id-john"
    end

    test "find with fields_except" do
      maha = TestRepo.insert! %Post{title: "Mahatma", body: "easter egg", user: %User{_id: "id-mahatma", username: "mahatma", email: "mahatma@gmail.com"}}
      selector =
        %{selector:
          %{title: %{"$eq" => "Mahatma"}},
          fields_except: ["body"]
        }
      {:ok, %{docs: list}} = Fetchers.find(TestRepo, Post, selector, preload: :user)
      a = list |> hd
      assert maha.title == a.title
      assert maha.body == "easter egg"
      assert a.body == nil
    end

  end

end
