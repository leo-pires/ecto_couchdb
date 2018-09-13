defmodule RepoTest do

  use ExUnit.Case, async: true
  import TestSupport
  alias Repo.Couchdb, as: Repo
  alias CouchdbAdapter.Fetchers
  alias CouchdbAdapter.Attachment


  setup do
    Repo |> clear_db!
    db_props = Repo |> CouchdbAdapter.db_props_for
    design_docs = [
      {
        "Post",
        %{
          views: %{
            all: %{
              map: "function(doc) { if (doc.type === 'Post') emit(doc._id, doc) }"
            },
            by_user_id: %{
              map: "function(doc) { if (doc.type === 'Post' && doc.user_id) emit(doc.user_id, doc) }"
            }
          }
        }
      },
      {
        "User",
        %{
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
      },
      {
        "UserData",
        %{
          views: %{
            all: %{
              map: "function(doc) { if (doc.type === 'UserData') emit(doc.user_id, doc) }"
            },
            by_user_id: %{
              map: "function(doc) { if (doc.type === 'UserData' && doc.user_id) emit(doc.user_id, doc) }"
            }
          }
        }
      }
    ]
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
    post =
      %Post{
        title: "how to write and adapter",
        body: "Don't know yet"
      }
    grants =
      [
        %Grant{user: "admin", access: "all"},
        %Grant{user: "other", access: "read"}
      ]
    %{
      db_props: db_props,      
      design_docs: design_docs,
      posts: posts,
      post: post,
      grants: grants
    }
  end

  describe "insert" do
    defp has_id_and_rev?(resource) do
      assert resource._id
      assert resource._rev
    end

    test "generates id/rev", %{post: post} do
      {:ok, result} = Repo.insert(post)
      assert has_id_and_rev?(result)
    end

    test "uses locally generated id", %{post: post} do
      post = struct(post, _id: "FOO")
      {:ok, result} = Repo.insert(post)
      assert has_id_and_rev?(result)
      assert result._id == "FOO"
      assert result.type == "Post"
    end

    test "fails if using the same id twice", %{post: post} do
      post = struct(post, _id: "FOO")
      assert {:ok, _} = Repo.insert(post)
      exception = assert_raise Ecto.ConstraintError, fn -> Repo.insert(post) end
      assert exception.constraint == "Post_id_index"
    end

    test "handles conflicts as changeset errors using unique_constraint", %{post: post} do
      import Ecto.Changeset
      params = Map.from_struct(post)
      changeset =
        cast(%Post{}, %{params | _id: "FOO"}, [:title, :body, :_id])
        |> unique_constraint(:id)
      assert {:ok, _} = Repo.insert(changeset)
      assert {:error, changeset} = Repo.insert(changeset)
      assert changeset.errors[:id] != nil
      assert changeset.errors[:id] == {"has already been taken", []}
    end

    test "supports embeds", %{post: post, grants: grants} do
      post = struct(post, grants: grants)
      {:ok, result} = Repo.insert(post)
      assert has_id_and_rev?(result)
    end

    test "supports embeds without ids", %{post: post} do
      post = struct(post, stats: %Stats{visits: 12, time: 892})
      {:ok, result} = Repo.insert(post)
      assert has_id_and_rev?(result)
    end

    test "generates timestamps", %{post: post} do
      {:ok, inserted} = Repo.insert(post)
      assert not is_nil(inserted.inserted_at)
      assert not is_nil(inserted.updated_at)
    end
  end

  describe "insert_all" do
    setup(%{design_docs: design_docs, posts: posts}) do
      Repo |> create_views!(design_docs)
      posts =
        Enum.map(posts, fn doc ->
          %{doc |
            grants: Enum.map(doc.grants, &struct(Grant, &1)),
            stats: struct(Stats, doc.stats)
          }
        end)
      %{posts: posts}
    end

    test "inserts with generated id/rev", %{db_props: db_props, posts: posts} do
      posts = Enum.map(posts, &Map.drop(&1, [:_id]))
      assert {3, nil} == Repo.insert_all(Post, posts)
      {:ok, %{"rows" => query_result}} = Couchdb.Connector.fetch_all(db_props, "Post", "all")
      assert Enum.count(query_result) == 3
      assert Enum.all?(query_result, fn result ->
        doc = result["value"]
        assert nil != doc
        assert nil != doc["_id"]
        assert nil != doc["_rev"]
      end)
    end

    test "inserts with explicit id", %{db_props: db_props, posts: posts} do
      assert {3, nil} == Repo.insert_all(Post, posts)
      {:ok, %{"rows" => query_result}} = Couchdb.Connector.fetch_all(db_props, "Post", "all")
      assert Enum.count(query_result) == 3
      Enum.each(Enum.zip(query_result, posts), fn {result, post} ->
        doc = result["value"]
        assert nil != doc
        assert post._id == doc["_id"]
        assert nil != doc["_rev"]
        assert post.title == doc["title"]
        assert post.body == doc["body"]
        expected_grants = post.grants |> Enum.map(&(Map.from_struct(&1)))
        doc_grants = doc["grants"] |> atomize_keys!
        assert expected_grants == doc_grants
        expected_stats = Map.from_struct(post.stats)
        doc_stats = doc["stats"] |> atomize_keys!
        assert expected_stats == doc_stats
      end)
    end
  end

  describe "delete" do
    setup %{design_docs: design_docs, posts: posts} do
      Repo |> create_views!(design_docs)
      posts_with_rev = Repo |> insert_docs!(posts)
      %{docs: posts_with_rev}
    end

    test "removes the id", %{db_props: db_props, docs: docs} do
      {deleted_doc, _} = List.pop_at(docs, 1)
      post = struct(Post, _id: deleted_doc._id, _rev: deleted_doc._rev)
      {:ok, deleted_post} = Repo.delete(post)
      assert deleted_post._id == post._id
      # TODO: check what delete should return
      # assert deleted_post._rev > post._rev
      assert {:error, %{"error" => "not_found"}} = Couchdb.Connector.get(db_props, deleted_post._id)
      assert {:ok, _} = Couchdb.Connector.get(db_props, List.first(docs)._id)
    end

    test "succeeds if the id is not found" do
      post = struct(Post, _id: "Not found", _rev: "4-Unknown")
      assert {:ok, _} = Repo.delete(post)
    end

    test "fails with stale if the revision is outdated", %{docs: docs} do
      {deleted_doc, _docs} = List.pop_at(docs, 1)
      assert_raise(Ecto.StaleEntryError,
                   fn ->
                     struct(Post, %{_id: deleted_doc._id, _rev: "0-outdated"})
                     |> Ecto.Changeset.change
                     |> Repo.delete
                   end)
    end

    defmodule Other do
      use Ecto.Schema
      @primary_key false

      schema "posts" do
        field :_id, :binary_id, autogenerate: true, primary_key: true
        field :_rev, :string, read_after_writes: true, primary_key: true
      end
    end

    test "deletes anything on the same database", %{db_props: db_props, docs: docs} do
      to_delete = List.first(docs)
      other = %__MODULE__.Other{_id: to_delete._id, _rev: to_delete._rev}
      {:ok, %{"rows" => query_result}} = Couchdb.Connector.fetch_all(db_props, "Post", "all")
      assert length(query_result) == 3
      assert {:ok, _} = Repo.delete(other)
      {:ok, %{"rows" => query_result}} = Couchdb.Connector.fetch_all(db_props, "Post", "all")
      assert length(query_result) == 2
    end
  end

  describe "update" do
    setup %{design_docs: design_docs, posts: posts} do
      Repo |> create_views!(design_docs)
      Repo |> insert_docs!(posts)
      {:ok, posts} = Repo |> Fetchers.fetch_all(Post, :all)
      %{posts: posts}
    end

    test "changes attributes and _rev", %{db_props: db_props, posts: [post | _]} do
      {:ok, updated_post} =
        post
        |> Ecto.Changeset.change(title: "Changed title")
        |> Ecto.Changeset.put_embed(:stats, %Stats{visits: 1000})
        |> Repo.update
      assert updated_post._rev != post._rev
      assert updated_post.title == "Changed title"
      assert updated_post.stats.visits == 1000
      # check persisted data
      assert {:ok, stored_post} = Couchdb.Connector.get(db_props, post._id)
      assert stored_post["_id"] == updated_post._id
      assert stored_post["_rev"] == updated_post._rev
      # unchanged data is persisted
      assert stored_post["body"] == post.body
    end

    test "works with embeds_many", %{db_props: db_props, posts: [post | _]} do
      new_grants = Enum.take_random(post.grants, 1) |> Enum.map(&%{&1 | access: "new"})
      {:ok, updated_post} =
        post
        |> Ecto.Changeset.change
        |> Ecto.Changeset.put_embed(:grants, new_grants)
        |> Repo.update
      assert length(updated_post.grants) == 1
      assert match?([%Grant{access: "new"}], updated_post.grants)
      # check persisted data
      assert {:ok, stored_post} = Couchdb.Connector.get(db_props, post._id)
      [stored_grant] = stored_post["grants"]
      assert stored_grant["access"] == "new"
    end

    test "works with embeds_many after empty update", %{posts: [post | _]} do
      {:ok, updated_post} =
        post
        |> Ecto.Changeset.change
        |> Ecto.Changeset.put_embed(:grants, [])
        |> Repo.update
      assert updated_post.grants == []
      # check persisted data
      {:ok, post} = Fetchers.get(Repo, Post, post._id)
      assert [] = post.grants
    end

    test "raises Ecto.StaleEntryError if document is not found", %{posts: [post | _]} do
      missing_post = post |> Map.put(:_id, "not found")
      assert_raise(Ecto.StaleEntryError,
                   fn ->
                     missing_post
                     |> Ecto.Changeset.change(title: "Changed title")
                     |> Repo.update
                   end)
    end

    test "raises Ecto.StaleEntryError if the document _rev does not match", %{posts: [post | _]} do
      stale_post = post |> Map.put(:_rev, "not found")
      assert_raise(Ecto.StaleEntryError,
                   fn ->
                     stale_post
                     |> Ecto.Changeset.change(title: "Changed title")
                     |> Repo.update
                   end)
    end

    test "update on_conflict: :replace_all" do
      pc = Post.changeset(%Post{}, %{title: "lorem", body: "lorem ipsum"}) |> Repo.insert!
      assert nil != pc._id
      assert nil != pc._rev
      assert pc.title == "lorem"
      pu1 = Post.changeset(pc, %{title: "ipsum"}) |> Repo.update!
      assert pu1._id == pc._id
      assert pu1._rev > pc._rev
      assert pu1.title == "ipsum"
      pu2 = Post.changeset(pu1, %{title: "foo"}) |> Repo.update!
      assert pu2._id == pu1._id
      assert pu2._rev > pu1._rev
      assert pu2.title == "foo"
      pu3 = Post.changeset(pu1, %{title: "goo"}) |> Repo.update!(on_conflict: :replace_all)
      assert pu3._id == pu2._id
      assert pu3._rev > pu2._rev
      assert pu3.title == "goo"
      {:ok, pf} = Fetchers.get(Repo, Post, pc._id)
      assert pf._id == pu3._id
      assert pf._rev == pu3._rev
      assert pf.title == pu3.title
    end
  end

  describe "insert or update" do
    setup %{design_docs: design_docs} do
      Repo |> create_views!(design_docs)
      :ok
    end

    test "insert or update" do
      pc = Post.changeset(%Post{}, %{title: "lorem", body: "lorem ipsum"}) |> Repo.insert_or_update!
      assert nil != pc._id
      assert nil != pc._rev
      assert "lorem" == pc.title
      pu = Post.changeset(pc, %{title: "ipsum"}) |> Repo.insert_or_update!
      assert nil != pu._id
      assert nil != pu._rev
      assert pu._rev > pc._rev
      assert "ipsum" == pu.title
    end
  end

  describe "get and fetch" do
    setup %{design_docs: design_docs, posts: posts} do
      Repo |> create_views!(design_docs)
      Repo |> insert_docs!(posts)
      Repo.insert! %User{_id: "test-user-id0", username: "bob", email: "bob@gmail.com"}
      :ok
    end

    test "get by key" do
      {:ok, u} = Fetchers.get(Repo, User, "test-user-id0")
      assert u._id == "test-user-id0"
      assert not is_nil(u._rev)
      assert u.username == "bob"
      assert u.email == "bob@gmail.com"
    end

    test "get by key and preload" do
      pc = Repo.insert! %Post{title: "lorem", body: "lorem ipsum", user: %User{_id: "test-user-id1", username: "john", email: "john@gmail.com"}}
      {:ok, pf} = Fetchers.get(Repo, Post, pc._id, preload: :user)
      assert pf.title == "lorem"
      assert pf.body == "lorem ipsum"
      assert pf.user._id == "test-user-id1"
      assert pf.user.username == "john"
      assert pf.user.email == "john@gmail.com"
    end

    test "get as map" do
      {:ok, u} = Fetchers.get(Repo, User, "test-user-id0", as_map: true)
      assert u |> Map.get(:_id) == "test-user-id0"
      assert not is_nil(u |> Map.get(:_rev))
      assert u |> Map.get(:username) == "bob"
      assert u |> Map.get(:email) == "bob@gmail.com"
    end

    test "get as raw map" do
      {:ok, u} = Fetchers.get(Repo, User, "test-user-id0", as_map: :raw)
      assert u |> Map.get("_id") == "test-user-id0"
      assert not is_nil(u |> Map.get("_rev"))
      assert u |> Map.get("username") == "bob"
      assert u |> Map.get("email") == "bob@gmail.com"
    end

    test "get return nil if not found" do
      {:ok, data} = Fetchers.get(Repo, Post, "xpto")
      assert is_nil(data)
    end

    test "fetch one returns struct" do
      {:ok, u} = Fetchers.fetch_one(Repo, User, :all, key: "test-user-id0")
      assert u._id == "test-user-id0"
      assert not is_nil(u._rev)
      assert u.username == "bob"
      assert u.email == "bob@gmail.com"
    end

    test "fetch one returns struct with include_docs" do
      {:ok, u} = Fetchers.fetch_one(Repo, User, :all_no_doc, key: "test-user-id0", include_docs: true)
      assert u._id == "test-user-id0"
      assert not is_nil(u._rev)
      assert u.username == "bob"
      assert u.email == "bob@gmail.com"
    end

    test "fetch one returns nil if not found" do
      assert {:ok, nil} = Fetchers.fetch_one(Repo, User, :all, key: "xpto")
    end

    test "fetch one return :many if more than one found" do
      {:ok, :many} = Fetchers.fetch_one(Repo, Post, :all)
    end

    test "fetch_one and preload" do
      pc = Repo.insert! %Post{title: "lorem", body: "lorem ipsum", user: %User{_id: "test-user-id1", username: "john", email: "john@gmail.com"}}
      {:ok, pf} = Fetchers.fetch_one(Repo, Post, :all, key: pc._id, preload: :user)
      assert pf.title == "lorem"
      assert pf.body == "lorem ipsum"
      assert pf.user._id == "test-user-id1"
      assert pf.user.username == "john"
      assert pf.user.email == "john@gmail.com"
    end

    test "fetch_one and preload with as_map" do
      pc = Repo.insert! %Post{title: "lorem", body: "lorem ipsum", user: %User{_id: "test-user-id1", username: "john", email: "john@gmail.com"}}
      {:ok, pf} = Fetchers.fetch_one(Repo, Post, :all, key: pc._id, preload: :user, as_map: true)
      assert pf.title == "lorem"
      assert pf.body == "lorem ipsum"
      assert pf.user._id == "test-user-id1"
      assert pf.user.username == "john"
      assert pf.user.email == "john@gmail.com"
    end

    test "fetch_all limit" do
      Repo.insert! %User{_id: "test-user-id1", username: "bob", email: "bob@gmail.com"}
      {:ok, pf} = Fetchers.fetch_all(Repo, User, :all, limit: 1)
      assert [_] = pf
      assert hd(pf)._id == "test-user-id0"
    end

    test "fetch_all limit with include_docs" do
      Repo.insert! %User{_id: "test-user-id1", username: "bob", email: "bob@gmail.com"}
      {:ok, pf} = Fetchers.fetch_all(Repo, User, :all_no_doc, include_docs: true, limit: 1)
      assert [_] = pf
      assert hd(pf)._id == "test-user-id0"
    end

    test "fetch_all descending" do
      Repo.insert! %User{_id: "test-user-id1", username: "bob", email: "bob@gmail.com"}
      {:ok, pf} = Fetchers.fetch_all(Repo, User, :all, descending: true)
      assert length(pf) == 2
      assert hd(pf)._id == "test-user-id1"
    end

    test "fetch_one limit and descending" do
      Repo.insert! %User{_id: "test-user-id1", username: "bob", email: "bob@gmail.com"}
      {:ok, pf} = Fetchers.fetch_one(Repo, User, :all, limit: 1, descending: true)
      assert pf._id == "test-user-id1"
    end

    test "fetch all" do
      {:ok, list} = Fetchers.fetch_all(Repo, Post, :all)
      assert length(list) == 3
      {:ok, list} = Fetchers.fetch_all(Repo, User, :all)
      assert [_] = list
    end

    test "fetch all by keys" do
      {:ok, list} = Fetchers.fetch_all(Repo, Post, :all, keys: ["id1", "id2"])
      assert length(list) == 2
      assert (list |> Enum.at(0))._id == "id1"
      assert (list |> Enum.at(1))._id == "id2"
    end

    test "raise if invalid view name" do
      assert_raise RuntimeError, fn -> Fetchers.fetch_all(Repo, Post, :xpto) end
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
      assert CouchdbAdapter.ResultProcessor.normalize_preloads(:b) == [b: []]
      assert CouchdbAdapter.ResultProcessor.normalize_preloads([:b]) == [b: []]
      assert CouchdbAdapter.ResultProcessor.normalize_preloads([b: [c: :d]]) == [b: [c: [d: []]]]
      assert CouchdbAdapter.ResultProcessor.normalize_preloads([b: [c: [:d]]]) == [b: [c: [d: []]]]
      assert CouchdbAdapter.ResultProcessor.normalize_preloads([b: [:c, :d]]) == [b: [c: [], d: []]]
      assert CouchdbAdapter.ResultProcessor.normalize_preloads([b: [c: [:d]]]) == [b: [c: [d: []]]]
      assert CouchdbAdapter.ResultProcessor.normalize_preloads([b: [c: [:d, :e]]]) == [b: [c: [d: [], e: []]]]
      assert CouchdbAdapter.ResultProcessor.normalize_preloads([b: [c: [:d, :e]], f: :g]) == [b: [c: [d: [], e: []]], f: [g: []]]
    end

    test "get preload" do
      pc = Repo.insert! A.changeset(%A{}, %{title: "a", b: %{title: "b", c: %{title: "c", d: %{title: "d"}}}})
      {:ok, a1} = Fetchers.get(Repo, A, pc._id, preload: [b: :c])
      assert a1.title == "a"
      assert a1.b.title == "b"
      assert a1.b.c.title == "c"
      {:ok, a2} = Fetchers.get(Repo, A, pc._id, preload: [b: [c: :d]])
      assert a2.title == "a"
      assert a2.b.title == "b"
      assert a2.b.c.title == "c"
      assert a2.b.c.d.title == "d"
    end

    test "get preload missing association" do
      pc = Repo.insert! A.changeset(%A{}, %{title: "a"})
      assert not is_nil(Fetchers.get(Repo, A, pc._id, preload: :b))
      assert not is_nil(Fetchers.get(Repo, A, pc._id, preload: [b: :c]))
    end
  end

  describe "has_one support" do
    setup(%{design_docs: design_docs, posts: posts}) do
      Repo |> create_views!(design_docs)
      Repo |> insert_docs!(posts |> Enum.map(&(&1 |> Map.put(:user_id, "test-user"))))
      Repo.insert! %User{_id: "test-user", username: "test", email: "test"}
      :ok
    end

    test "has_one supports cast_assoc" do
      pc = Repo.insert! User.changeset_user_data(%User{}, %{_id: "u1", username: "foo", email: "goo", user_data: %{_id: "ud1", extra: "bar"}})
      {:ok, uf} = Fetchers.get(Repo, User, "u1")
      {:ok, udf} = Fetchers.get(Repo, UserData, "ud1")
      assert pc._id == uf._id
      assert pc.username == uf.username
      assert pc.email == uf.email
      assert udf._id == "ud1"
      assert udf.user_id == pc._id
      assert udf.extra == "bar"
    end

    test "get and fetch preloading has_one" do
      pc = Repo.insert! User.changeset_user_data(%User{}, %{_id: "u1", username: "foo", email: "goo", user_data: %{_id: "ud1", extra: "bar"}})
      {:ok, udf} = Fetchers.get(Repo, UserData, "ud1")
      {:ok, uf} = Fetchers.get(Repo, User, "u1", preload: :user_data)
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
      {:ok, pf} = Fetchers.get(Repo, User, "test-user", preload: :posts)
      assert length(pf.posts) == 3
    end

  end

  describe "changeset" do
    setup %{design_docs: design_docs, posts: posts} do
      Repo |> create_views!(design_docs)
      Repo |> insert_docs!(posts |> Enum.map(&(&1 |> Map.put(:user_id, "test-user"))))
      :ok
    end

    test "insert and update from changeset", %{} do
      {:ok, list} = Fetchers.fetch_all(Repo, User, :all)
      assert [] == list
      {:ok, ui} = User.changeset(%User{}, %{_id: "test-user-id", username: "bob", email: "bob@gmail.com"}) |> Repo.insert
      {:ok, list} = Fetchers.fetch_all(Repo, User, :all)
      assert [_] = list
      assert ui._id == "test-user-id"
      assert ui._rev
      assert ui.type == "User"
      {:ok, uq1} = Fetchers.get(Repo, User, "test-user-id")
      assert ui._id == uq1._id
      assert ui._rev == uq1._rev
      assert ui.type == uq1.type
      assert ui.username == uq1.username
      assert ui.email == uq1.email
      assert ui.inserted_at == uq1.inserted_at
      assert ui.updated_at == uq1.updated_at
      {:ok, uu} = User.changeset(uq1, %{username: "silent bob", email: "silent.bob@gmail.com"}) |> Repo.update
      {:ok, list_user} = Fetchers.fetch_all(Repo, User, :all)
      assert [_] = list_user
      {:ok, uq2} = Fetchers.get(Repo, User, "test-user-id")
      assert uu._id == uq1._id
      assert uu._rev != uq1._rev
      assert uu._id == uq2._id
      assert uu._rev == uq2._rev
      assert uu.type == uq2.type
      assert uu.username == uq2.username
      assert uu.email == uq2.email
      assert uu.updated_at == uq2.updated_at
      assert uu.updated_at == uq2.updated_at
      assert uq2.inserted_at == uq1.inserted_at
      assert uq2.updated_at != uq1.inserted_at
    end

    test "cast_assoc" do
      {:ok, list} = Fetchers.fetch_all(Repo, User, :all)
      assert list == []
      {:ok, inserted} = Post.changeset_user(%Post{}, %{title: "lorem", body: "lorem ipsum", user: %{_id: "test-user-id", username: "bob", email: "bob@gmail.com"}}) |> Repo.insert
      assert inserted.user_id == inserted.user._id
      {:ok, list_user} = Fetchers.fetch_all(Repo, User, :all)
      assert [_] = list_user
    end
  end

  describe "integration tests" do
    setup %{design_docs: design_docs, posts: posts} do
      Repo |> create_views!(design_docs)
      Repo |> insert_docs!(posts)
      Repo.insert! %User{_id: "test-user-id0", username: "bob", email: "bob@gmail.com"}
      :ok
    end

    defmodule Foo do
      use Ecto.Schema
      @primary_key false
      @foreign_key_type :binary_id
      schema "Foo" do
        field :_id, :binary_id, autogenerate: true, primary_key: true
        field :_rev, :string, read_after_writes: true, primary_key: true
        field :type, :string, read_after_writes: true
        field :date, :date
        field :time, :time
      end
    end

    test "update from get" do
      {:ok, list_post} = Fetchers.fetch_all(Repo, Post, :all)
      assert length(list_post) == 3
      {:ok, list_user} = Fetchers.fetch_all(Repo, User, :all)
      assert [_] = list_user
      pc = Post.changeset(%Post{}, %{title: "lorem", body: "lorem ipsum", user: %{_id: "test-user-id2", username: "alice", password: "alice@gmail.com"}}) |> Repo.insert!
      {:ok, list_post} = Fetchers.fetch_all(Repo, Post, :all)
      assert length(list_post) == 4
      {:ok, list_user} = Fetchers.fetch_all(Repo, User, :all)
      assert [_] = list_user
      {:ok, pf} = Fetchers.get(Repo, Post, pc._id)
      assert not is_nil(pf)
      Repo.update! Post.changeset(pf, %{title: "new lorem", body: "new lorem ipsum"})
      {:ok, pu} = Fetchers.get(Repo, Post, pc._id)
      assert pu._id == pf._id
      assert pu._rev != pf._rev
      assert pu.title == "new lorem"
      assert pu.body == "new lorem ipsum"
      {:ok, list_post} = Fetchers.fetch_all(Repo, Post, :all)
      assert length(list_post) == 4
      {:ok, list_user} = Fetchers.fetch_all(Repo, User, :all)
      assert [_] = list_user
    end

    test "update including association from get" do
      pc = Post.changeset_user(%Post{}, %{title: "lorem", body: "lorem ipsum", user: %{_id: "test-user-id3", username: "john", email: "john@gmail.com"}}) |> Repo.insert!
      {:ok, list_post} = Fetchers.fetch_all(Repo, Post, :all)
      assert length(list_post) == 4
      {:ok, list_user} = Fetchers.fetch_all(Repo, User, :all)
      assert length(list_user) == 2
      {:ok, pf1} = Fetchers.get(Repo, Post, pc._id, preload: :user)
      assert not is_nil(pf1)
      assert pf1.user_id == pc.user._id
      assert pf1.user_id == pf1.user._id
      assert pf1._rev == pc._rev
      assert pf1.title == "lorem"
      assert pf1.body == "lorem ipsum"
      assert pf1.user._id == "test-user-id3"
      assert pf1.user.username == "john"
      assert pf1.user.email == "john@gmail.com"
      pu = Repo.update! Post.changeset_user(pf1, %{title: "new lorem", body: "new lorem ipsum", user: %{username: "doe", email: "doe@gmail.com"}})
      {:ok, list_post} = Fetchers.fetch_all(Repo, Post, :all)
      assert length(list_post) == 4
      {:ok, list_user} = Fetchers.fetch_all(Repo, User, :all)
      assert length(list_user) == 2
      {:ok, pf2} = Fetchers.get(Repo, Post, pc._id, preload: :user)
      assert pf2.user_id == pc.user._id
      assert pf2.user_id == pf1.user._id
      assert pf2._rev != pf1._rev
      assert pu.user_id == pu.user._id
      assert pu.user_id == pf2.user_id
      assert pu.user._id == pf2.user._id
      assert pu.user._rev == pf2.user._rev
      assert pf2.user_id == pc.user._id
      assert pf2._rev != pc._rev
      assert pf2.title == "new lorem"
      assert pf2.body == "new lorem ipsum"
      {:ok, uf2} = Fetchers.get(Repo, User, pc.user._id)
      assert uf2._id == pf2.user_id
      assert uf2._id == pf2.user._id
      assert uf2._id == pc.user._id
      assert uf2._rev == pu.user._rev
      assert uf2._rev == pf2.user._rev
      assert uf2._rev != pf2._rev
      assert uf2.username == "doe"
      assert uf2.email == "doe@gmail.com"
    end

    test "date and time cast" do
      fooc = Repo.insert! %Foo{date: ~D[1969-07-20], time: ~T[16:20:42]}
      {:ok, foof} = Fetchers.get(Repo, Foo, fooc._id)
      assert fooc.date == foof.date
      assert fooc.time == foof.time
    end
  end

  describe "map as field" do
    defmodule F do
      use Ecto.Schema
      embedded_schema do
        field :t, :string
      end
      def changeset(struct, params) do
        struct |> Ecto.Changeset.cast(params, [:t])
      end
    end
    defmodule E do
      use Ecto.Schema
      @primary_key false
      @foreign_key_type :binary_id
      schema "E" do
        field :_id, :binary_id, autogenerate: true, primary_key: true
        field :_rev, :string, read_after_writes: true, primary_key: true
        field :type, :string, read_after_writes: true
        field :t, :string
        field :u, :string
        field :d, :map
        embeds_one :f, F
      end
      def changeset(struct, params) do
        struct |> Ecto.Changeset.cast(params, [:t, :d]) |> Ecto.Changeset.cast_embed(:f)
      end
    end

    test "map cast" do
      d = %{"a" => "a", "b" => ["b"], "c" => [%{"foo" => 1, "goo" => 2}, %{"foo" => 3, "goo" => 4}], "d" => %{"bar" => 3}}
      {:ok, pc} = E.changeset(%E{}, %{t: "a", u: nil, d: d, f: %{t: nil}}) |> Repo.insert
      {:ok, pf} = Fetchers.get(Repo, E, pc._id)
      assert pf._id == pc._id
      assert pf.t == "a"
      assert is_nil(pf.u)
      assert pf.d == d
      assert is_nil(pf.f.t)
    end

    defmodule G do
      use Ecto.Schema
      @primary_key false
      @foreign_key_type :binary_id
      schema "G" do
        field :_id, :binary_id, autogenerate: true, primary_key: true
        field :_rev, :string, read_after_writes: true, primary_key: true
        field :type, :string, read_after_writes: true
        field :x, {:array, :map}
      end
      def changeset(struct, params) do
        struct |> Ecto.Changeset.cast(params, [:x])
      end
    end
    test "array of map" do
      x = [
        %{"a1" => "a", "b2" => ["b"], "c1" => [%{"foo" => 1, "goo" => 2}, %{"foo" => 3, "goo" => 4}], "d1" => %{"bar" => 3}, "f1" => []},
        %{"a2" => "a", "b2" => ["b"], "c2" => [%{"foo" => 1, "goo" => 2}, %{"foo" => 3, "goo" => 4}], "d2" => %{"bar" => 3}, "f2" => []}
      ]
      {:ok, pc} = G.changeset(%G{}, %{x: x}) |> Repo.insert
      {:ok, pf} = Fetchers.get(Repo, G, pc._id)
      assert pf._id == pc._id
      assert pf.x == x
    end
  end

  describe "direct http calls" do
    setup %{design_docs: design_docs, posts: posts} do
      Repo |> create_views!(design_docs)
      Repo |> insert_docs!(posts)
      Repo.insert! %User{_id: "test-user-id1", type: "User", username: "bob", email: "bob@gmail.com"}
      Repo.insert! %User{_id: "test-user-id2", type: "User", username: "alice", email: "alice@gmail.com"}
      Repo.insert! %User{_id: "test-user-id3", type: "User", username: "bob", email: "bob@gmail.com"}
      :ok
    end

    test "multiple_fetch_all works for Ecto schema" do
      {:ok, list} = Fetchers.multiple_fetch_all(Repo, User, :all, [%{key: "test-user-id1"}, %{key: "test-user-id2"}])
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
      {:ok, list} = Fetchers.multiple_fetch_all(Repo, User, :all, [%{key: "test-user-id1"}, %{key: "test-user-id2"}], as_map: true)
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
      {:ok, list} = Fetchers.multiple_fetch_all(Repo, User, :counts, [%{group_level: 0}], as_map: true)
      assert list == [[6]]
    end

    test "multiple_fetch_all with return_keys" do
      {:ok, list} = Fetchers.multiple_fetch_all(Repo, User, :counts, [%{group_level: 0}], as_map: true, return_keys: true)
      assert list == [[{nil, 6}]]
    end

    test "find" do
      {:ok, %{docs: list}} = Fetchers.find(Repo, User, %{selector: %{username: %{"$eq" => "alice"}}})
      a = list |> hd
      assert a._id == "test-user-id2"
      assert a.email == "alice@gmail.com"
    end

    test "find with preloads" do
      pc = Repo.insert! %Post{title: "chibata", body: "lorem ipsum", user: %User{_id: "test-user-id-john", username: "john", email: "john@gmail.com"}}
      {:ok, %{docs: list}} = Fetchers.find(Repo, Post, %{selector: %{title: %{"$eq" => "chibata"}}}, preload: :user)
      a = list |> hd
      assert a._id == pc._id
      assert a.title == "chibata"
      assert a.user_id == "test-user-id-john"
      assert not is_nil(a.user)
      assert a.user._id == "test-user-id-john"
    end
  end

  describe "CouchdbAdapter.Repo" do
    test "get" do
      Repo.get(Post, "foo")
    end
  end

  describe "Attachments" do
    defmodule TestAttachment do
      use Ecto.Schema
      @primary_key false
      @foreign_key_type :binary_id
      schema "TestAttachment" do
        field :_id, :binary_id, autogenerate: true, primary_key: true
        field :_rev, :string, read_after_writes: true, primary_key: true
        field :type, :string, read_after_writes: true
        field :title, :string
        field :example_attachment, Attachment
      end
      def changeset(struct, params) do
        struct |> Ecto.Changeset.cast(params, [:title, :example_attachment])
      end
    end

    test "integration (fetch, insert, fetch, update, fetch" do
      attachment1 = %{content_type: "application/json", data: %{foo: "goo"}}
      {:ok, ai} = TestAttachment.changeset(%TestAttachment{}, %{title: "foogoo", example_attachment: attachment1}) |> Repo.insert
      assert ai.example_attachment.data == %{foo: "goo"}
      {:ok, aif1} = Fetchers.get(Repo, TestAttachment, ai._id, attachments: true)
      {:ok, aif2} = Fetchers.get(Repo, TestAttachment, ai._id)
      assert aif1._id == ai._id
      assert not is_nil(aif1.example_attachment)
      assert aif1.example_attachment.content_type == "application/json"
      assert aif1.example_attachment.data == %{"foo" => "goo"}
      assert aif1.example_attachment.stub == false
      assert is_nil(aif2.example_attachment)
      attachment2 = %{content_type: "application/json", data: %{bar: "baz"}}
      {:ok, au} = TestAttachment.changeset(ai, %{example_attachment: attachment2}) |> Repo.update
      assert au._id == ai._id
      assert au._rev > ai._rev
      assert au.example_attachment.data == %{bar: "baz"}
      {:ok, auf1} = Fetchers.get(Repo, TestAttachment, au._id, attachments: true)
      assert auf1._id == au._id
      assert auf1._rev == au._rev
      assert not is_nil(auf1.example_attachment)
      assert auf1._rev == au._rev
      assert auf1.example_attachment.data == %{"bar" => "baz"}
      assert auf1.example_attachment.stub == false
      assert auf1.example_attachment.revpos > aif1.example_attachment.revpos
    end

    test "cast" do
      assert {:ok, %Attachment{content_type: "foo", data: "goo"}} == Attachment.cast(%Attachment{content_type: "foo", data: "goo"})
      assert {:ok, %Attachment{content_type: "foo", data: "goo"}} == Attachment.cast(%{content_type: "foo", data: "goo"})
    end

    test "dump" do
      assert {:ok, %{type: :couch_attachment, content_type: "application/json", data: "eyJmb28iOiJnb28ifQ=="}} == Attachment.dump(%Attachment{content_type: "application/json", data: %{foo: "goo"}})
      assert {:ok, %{type: :couch_attachment, content_type: "foogoo", data: "Zm9vZ29v"}} == Attachment.dump(%Attachment{content_type: "foogoo", data: "foogoo"})
    end

    test "load" do
      assert {:ok, %Attachment{content_type: "application/json", data: %{"foo" => "goo"}, length: nil, revpos: 1, digest: "md5-pYBktzMm7KfsL0l/ykX3UA==", stub: false}} == Attachment.load(%{"content_type" => "application/json", "data" => "eyJmb28iOiJnb28ifQ==", "digest" => "md5-pYBktzMm7KfsL0l/ykX3UA==", "revpos" => 1})
      assert {:ok, %Attachment{content_type: "application/json", data: nil, length: 13, revpos: 1, digest: "md5-pYBktzMm7KfsL0l/ykX3UA==", stub: true}} == Attachment.load(%{"content_type" => "application/json", "digest" => "md5-pYBktzMm7KfsL0l/ykX3UA==", "length" => 13, "revpos" => 1, "stub" => true})
    end

  end

end
