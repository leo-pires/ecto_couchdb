defmodule RepoTest do
  #
  # Test for the Ecto.Repository API, delegated to the CouchdbAdapter
  #
  use ExUnit.Case, async: true

  setup_all do
    {:ok, _} = Repo.start_link
    :ok
  end

  setup do
    db = DatabaseCleaner.ensure_clean_db!(Repo)
    design_docs = [%{
                     _id: "_design/Post", language: "javascript",
                     views: %{
                       all: %{
                         map: "function(doc) { if (doc.type === 'Post') emit(doc._id, doc) }"
                       }
                   }}, %{
                     _id: "_design/User", language: "javascript",
                     views: %{
                       all: %{
                         map: "function(doc) { if (doc.type === 'User') emit(doc._id, doc) }"
                       },
                       counts: %{
                         map: "function(doc) { emit(doc._id, 1) }",
                         reduce: "_count"
                       }
                   }}]
    docs = for i <- 1..3, do: %{_id: "id#{i}", title: "t#{i}", body: "b#{i}", type: "Post",
                                stats: %{visits: i, time: 10*i},
                                grants: [%{id: "1", user: "u#{i}.1", access: "a#{i}.1"},
                                         %{id: "2", user: "u#{i}.2", access: "a#{i}.2"}]}
    %{
      db: db,
      post: %Post{title: "how to write and adapter", body: "Don't know yet"},
      grants: [%Grant{user: "admin", access: "all"}, %Grant{user: "other", access: "read"}],
      docs: docs,
      design_docs: design_docs
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
      changeset = cast(%Post{}, %{ params | _id: "FOO"}, [:title, :body, :_id])
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
    setup(%{db: db, docs: docs, design_docs: design_docs}) do
      design_docs |> Enum.each(fn (design_doc) ->
        :couchbeam.save_doc(db, design_doc |> CouchdbAdapter.to_doc)
      end)
      posts = Enum.map(docs, fn(doc) ->
        %{doc |
          grants: Enum.map(doc.grants, &struct(Grant, &1)),
          stats: struct(Stats, doc.stats)
        }
      end)
      %{posts: posts}
    end

    test "inserts with generated id/rev", %{posts: posts, db: db} do
      posts = Enum.map(posts, &Map.drop(&1, [:_id]))
      assert {3, nil} == Repo.insert_all(Post, posts)
      {:ok, query_result} = :couchbeam_view.fetch(db, {"Post", "all"}, [include_docs: true])
      assert Enum.count(query_result) == 3
      assert Enum.all? query_result, fn(result) ->
        doc = :couchbeam_doc.get_value("value", result)
        assert nil != :couchbeam_doc.get_value("_id", doc)
        assert nil != :couchbeam_doc.get_value("_rev", doc)
      end
    end

    test "inserts with explicit id", %{posts: posts, db: db} do
      assert {3, nil} == Repo.insert_all(Post, posts)
      {:ok, query_result} = :couchbeam_view.fetch(db, {"Post", "all"}, [include_docs: posts])
      assert Enum.count(query_result) == 3
      assert Enum.all? Enum.zip(query_result, posts), fn({result, post}) ->
        doc = :couchbeam_doc.get_value("value", result)
        assert post._id == :couchbeam_doc.get_value("_id", doc)
        assert nil != :couchbeam_doc.get_value("_rev", doc)
        assert post.title == :couchbeam_doc.get_value("title", doc)
        assert post.body == :couchbeam_doc.get_value("body", doc)
        expected_grants = Enum.map post.grants, &CouchdbAdapter.to_doc(Map.from_struct(&1))
        assert expected_grants == :couchbeam_doc.get_value("grants", doc)
        assert CouchdbAdapter.to_doc(Map.from_struct(post.stats)) == :couchbeam_doc.get_value("stats", doc)
      end
    end
  end

  describe "delete" do
    setup %{docs: docs, db: db, design_docs: design_docs} do
      design_docs |> Enum.each(fn (design_doc) ->
        :couchbeam.save_doc(db, design_doc |> CouchdbAdapter.to_doc)
      end)
      {:ok, results} = :couchbeam.save_docs(db, Enum.map(docs, fn(doc) ->
        CouchdbAdapter.to_doc(doc)
      end))
      docs_with_rev = results
                      |> Enum.zip(docs)
                      |> Enum.map(fn {res, doc} ->
                           Map.put(doc, :_rev, :couchbeam_doc.get_value("rev", res))
                         end)
      %{docs: docs_with_rev}
    end

    test "removes the id", %{docs: docs, db: db} do
      {deleted_doc, docs} = List.pop_at(docs, 1)
      post = struct(Post, _id: deleted_doc._id, _rev: deleted_doc._rev)
      {:ok, deleted_post} = Repo.delete(post)
      assert deleted_post._id == post._id
      assert deleted_post._rev > post._rev
      assert {:error, :not_found} == :couchbeam.open_doc(db, deleted_post._id)
      assert {:error, :not_found} != :couchbeam.open_doc(db, List.first(docs)._id)
    end

    test "succeeds if the id is not found" do
      post = struct(Post, _id: "Not found", _rev: "4-Unknown")
      assert {:ok, _} = Repo.delete(post)
    end

    test "fails with stale if the revision is outdated", %{docs: docs} do
      import Ecto.Changeset
      {deleted_doc, _docs} = List.pop_at(docs, 1)
      assert_raise(Ecto.StaleEntryError,
                   fn ->
                     struct(Post, %{_id: deleted_doc._id, _rev: "0-outdated"})
                     |> change
                     |> Repo.delete
                   end)
    end

    defmodule Other do
      use Ecto.Schema
      use Couchdb.Design
      @primary_key false

      schema "posts" do
        field :_id, :binary_id, autogenerate: true, primary_key: true
        field :_rev, :string, read_after_writes: true, primary_key: true
      end
    end

    test "deletes anything on the same database", %{db: db, docs: docs} do
      to_delete = List.first(docs)
      other = %__MODULE__.Other{_id: to_delete._id, _rev: to_delete._rev}
      {:ok, query_result} = :couchbeam_view.fetch(db, {"Post", "all"})
      assert length(query_result) == 3
      assert {:ok, _} = Repo.delete(other)
      {:ok, query_result} = :couchbeam_view.fetch(db, {"Post", "all"})
      assert length(query_result) == 2
    end
  end

  describe "update" do
    setup %{docs: docs, db: db} do
      {:ok, results} = :couchbeam.save_docs(db, Enum.map(docs, fn(doc) ->
        CouchdbAdapter.to_doc(doc)
      end))
      docs_with_rev = results
                      |> Enum.zip(docs)
                      |> Enum.map(fn {res, doc} ->
                           Map.put(doc, :_rev, :couchbeam_doc.get_value("rev", res))
                         end)
      posts = Enum.map(docs_with_rev, fn(doc) ->
        struct(Post, %{doc | grants: Enum.map(doc.grants, &struct(Grant, &1)),
                             stats: struct(Stats, doc.stats)})
      end)
      %{posts: posts}
    end

    test "changes attributes and _rev", %{posts: [post | _], db: db} do
      {:ok, updated_post} = post
                            |> Ecto.Changeset.change(title: "Changed title")
                            |> Ecto.Changeset.put_embed(:stats, %Stats{visits: 1000})
                            |> Repo.update
      assert updated_post._rev != post._rev
      assert updated_post.title == "Changed title"
      assert updated_post.stats.visits == 1000
      # check persisted data
      {:ok, stored_post} = :couchbeam.open_doc(db, post._id)
      assert :couchbeam_doc.get_idrev(stored_post) == {updated_post._id, updated_post._rev}
      # unchanged data is persisted
      assert :couchbeam_doc.get_value("body", stored_post) == post.body
    end

    test "works with embeds_many", %{posts: [post | _], db: db} do
      new_grants = Enum.take_random(post.grants, 1) |> Enum.map(&%{&1 | access: "new"})
      {:ok, updated_post} = post
                            |> Ecto.Changeset.change
                            |> Ecto.Changeset.put_embed(:grants, new_grants)
                            |> Repo.update
      assert length(updated_post.grants) == 1
      assert match?([%Grant{access: "new"}], updated_post.grants)
      # check persisted data
      {:ok, stored_post} = :couchbeam.open_doc(db, post._id)
      [stored_grant] = :couchbeam_doc.get_value("grants", stored_post)
      assert :couchbeam_doc.get_value("access", stored_grant) == "new"
    end

    test "works with embeds_many after empty update", %{posts: [post | _]} do
      {:ok, updated_post} = post
                            |> Ecto.Changeset.change
                            |> Ecto.Changeset.put_embed(:grants, [])
                            |> Repo.update
      assert length(updated_post.grants) == 0
      # check persisted data
      post = CouchdbAdapter.get(Repo, Post, post._id)
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
  end

  describe "get and fetch" do
    setup %{docs: docs, db: db, design_docs: design_docs} do
      design_docs |> Enum.each(fn (design_doc) ->
        :couchbeam.save_doc(db, design_doc |> CouchdbAdapter.to_doc)
      end)
      :couchbeam.save_docs(db, Enum.map(docs, fn(doc) ->
        CouchdbAdapter.to_doc(doc)
      end))
      Repo.insert! %User{_id: "test-user-id0", username: "bob", email: "bob@gmail.com"}
      :ok
    end

    test "get by key" do
      u = CouchdbAdapter.get(Repo, User, "test-user-id0")
      assert u._id == "test-user-id0"
      assert not is_nil(u._rev)
      assert u.username == "bob"
      assert u.email == "bob@gmail.com"
    end

    test "get by key and preload" do
      pc = Repo.insert! %Post{title: "lorem", body: "lorem ipsum", user: %User{_id: "test-user-id1", username: "john", email: "john@gmail.com"}}
      pf = CouchdbAdapter.get(Repo, Post, pc._id, preload: :user)
      assert pf.title == "lorem"
      assert pf.body == "lorem ipsum"
      assert pf.user._id == "test-user-id1"
      assert pf.user.username == "john"
      assert pf.user.email == "john@gmail.com"
    end

    test "get return nil if not found" do
      assert is_nil(CouchdbAdapter.get(Repo, Post, "xpto"))
    end

    test "fetch one returns struct" do
      u = CouchdbAdapter.fetch_one(Repo, User, :all, key: "test-user-id0")
      assert u._id == "test-user-id0"
      assert not is_nil(u._rev)
      assert u.username == "bob"
      assert u.email == "bob@gmail.com"
    end

    test "fetch one returns nil if not found" do
      assert is_nil(CouchdbAdapter.fetch_one(Repo, User, :all, key: "xpto"))
    end

    test "fetch one raise if more than one found" do
      assert_raise RuntimeError, fn -> CouchdbAdapter.fetch_one(Repo, Post, :all) end
    end

    test "fetch_one and preload" do
      pc = Repo.insert! %Post{title: "lorem", body: "lorem ipsum", user: %User{_id: "test-user-id1", username: "john", email: "john@gmail.com"}}
      pf = CouchdbAdapter.fetch_one(Repo, Post, :all, key: pc._id, preload: :user)
      assert pf.title == "lorem"
      assert pf.body == "lorem ipsum"
      assert pf.user._id == "test-user-id1"
      assert pf.user.username == "john"
      assert pf.user.email == "john@gmail.com"
    end

    test "fetch_all limit" do
      Repo.insert! %User{_id: "test-user-id1", username: "bob", email: "bob@gmail.com"}
      pf = CouchdbAdapter.fetch_all(Repo, User, :all, limit: 1)
      assert length(pf) == 1
      assert Enum.at(pf, 0)._id == "test-user-id0"
    end

    test "fetch_all descending" do
      Repo.insert! %User{_id: "test-user-id1", username: "bob", email: "bob@gmail.com"}
      pf = CouchdbAdapter.fetch_all(Repo, User, :all, descending: true)
      assert length(pf) == 2
      assert Enum.at(pf, 0)._id == "test-user-id1"
    end

    test "fetch_one limit and descending" do
      Repo.insert! %User{_id: "test-user-id1", username: "bob", email: "bob@gmail.com"}
      pf = CouchdbAdapter.fetch_one(Repo, User, :all, limit: 1, descending: true)
      assert pf._id == "test-user-id1"
    end

    test "fetch all" do
      assert length(CouchdbAdapter.fetch_all(Repo, Post, :all)) == 3
      assert length(CouchdbAdapter.fetch_all(Repo, User, :all)) == 1
    end

    test "raise if invalid view name" do
      assert_raise RuntimeError, fn -> CouchdbAdapter.fetch_all(Repo, Post, :xpto) end
    end

    defmodule D do
      use Ecto.Schema
      use Couchdb.Design
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
      use Couchdb.Design
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
      use Couchdb.Design
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
      use Couchdb.Design
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
      assert CouchdbAdapter.normalize_preloads(:b) == [b: []]
      assert CouchdbAdapter.normalize_preloads([:b]) == [b: []]
      assert CouchdbAdapter.normalize_preloads([b: [c: :d]]) == [b: [c: [d: []]]]
      assert CouchdbAdapter.normalize_preloads([b: [c: [:d]]]) == [b: [c: [d: []]]]
      assert CouchdbAdapter.normalize_preloads([b: [:c, :d]]) == [b: [c: [], d: []]]
      assert CouchdbAdapter.normalize_preloads([b: [c: [:d]]]) == [b: [c: [d: []]]]
      assert CouchdbAdapter.normalize_preloads([b: [c: [:d, :e]]]) == [b: [c: [d: [], e: []]]]
      assert CouchdbAdapter.normalize_preloads([b: [c: [:d, :e]], f: :g]) == [b: [c: [d: [], e: []]], f: [g: []]]
    end

    test "get chaining" do
      pc = Repo.insert! A.changeset(%A{}, %{title: "a", b: %{title: "b", c: %{title: "c", d: %{title: "d"}}}})
      a1 = CouchdbAdapter.get(Repo, A, pc._id, preload: [b: :c])
      assert a1.title == "a"
      assert a1.b.title == "b"
      assert a1.b.c.title == "c"
      a2 = CouchdbAdapter.get(Repo, A, pc._id, preload: [b: [c: :d]])
      assert a2.title == "a"
      assert a2.b.title == "b"
      assert a2.b.c.title == "c"
      assert a2.b.c.d.title == "d"
    end

    test "get preload missing association" do
      pc = Repo.insert! A.changeset(%A{}, %{title: "a"})
      assert not is_nil(CouchdbAdapter.get(Repo, A, pc._id, preload: :b))
      assert not is_nil(CouchdbAdapter.get(Repo, A, pc._id, preload: [b: :c]))
    end
  end

  describe "changeset" do
    setup %{db: db, design_docs: design_docs} do
      design_docs |> Enum.each(fn (design_doc) ->
        :couchbeam.save_doc(db, design_doc |> CouchdbAdapter.to_doc)
      end)
      :ok
    end

    test "insert and update from changeset", %{} do
      assert length(CouchdbAdapter.fetch_all(Repo, User, :all)) == 0
      {:ok, ui} = User.changeset(%User{}, %{_id: "test-user-id", username: "bob", email: "bob@gmail.com"}) |> Repo.insert
      assert length(CouchdbAdapter.fetch_all(Repo, User, :all)) == 1
      assert ui._id == "test-user-id"
      assert ui._rev
      assert ui.type == "User"
      uq1 = CouchdbAdapter.get(Repo, User, "test-user-id")
      assert ui._id == uq1._id
      assert ui._rev == uq1._rev
      assert ui.type == uq1.type
      assert ui.username == uq1.username
      assert ui.email == uq1.email
      assert ui.inserted_at == uq1.inserted_at
      assert ui.updated_at == uq1.updated_at
      {:ok, uu} = User.changeset(uq1, %{username: "silent bob", email: "silent.bob@gmail.com"}) |> Repo.update
      assert length(CouchdbAdapter.fetch_all(Repo, User, :all)) == 1
      uq2 = CouchdbAdapter.get(Repo, User, "test-user-id")
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
      assert length(CouchdbAdapter.fetch_all(Repo, User, :all)) == 0
      {:ok, inserted} = Post.changeset(%Post{}, %{title: "lorem", body: "lorem ipsum", user: %{_id: "test-user-id", username: "bob", email: "bob@gmail.com"}}) |> Repo.insert
      assert inserted.user_id == inserted.user._id
      assert length(CouchdbAdapter.fetch_all(Repo, User, :all)) == 1
    end
  end

  describe "integration tests" do
    setup %{docs: docs, db: db, design_docs: design_docs} do
      design_docs |> Enum.each(fn (design_doc) ->
        :couchbeam.save_doc(db, design_doc |> CouchdbAdapter.to_doc)
      end)
      :couchbeam.save_docs(db, Enum.map(docs, fn(doc) ->
        CouchdbAdapter.to_doc(doc)
      end))
      Repo.insert! %User{_id: "test-user-id0", username: "bob", email: "bob@gmail.com"}
      :ok
    end

    defmodule Foo do
      use Ecto.Schema
      use Couchdb.Design
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
      assert length(CouchdbAdapter.fetch_all(Repo, Post, :all)) == 3
      assert length(CouchdbAdapter.fetch_all(Repo, User, :all)) == 1
      pc = Post.changeset(%Post{}, %{title: "lorem", body: "lorem ipsum", user: %{_id: "test-user-id2", username: "alice", password: "alice@gmail.com"}}) |> Repo.insert!
      assert length(CouchdbAdapter.fetch_all(Repo, Post, :all)) == 4
      assert length(CouchdbAdapter.fetch_all(Repo, User, :all)) == 2
      pf = CouchdbAdapter.get(Repo, Post, pc._id)
      assert not is_nil(pf)
      Repo.update! Post.changeset(pf, %{title: "new lorem", body: "new lorem ipsum"})
      pu = CouchdbAdapter.get(Repo, Post, pc._id)
      assert pu._id == pf._id
      assert pu._rev != pf._rev
      assert pu.title == "new lorem"
      assert pu.body == "new lorem ipsum"
      assert length(CouchdbAdapter.fetch_all(Repo, Post, :all)) == 4
      assert length(CouchdbAdapter.fetch_all(Repo, User, :all)) == 2
    end

    test "update including association from get" do
      pc = Post.changeset(%Post{}, %{title: "lorem", body: "lorem ipsum", user: %{_id: "test-user-id3", username: "john", password: "john@gmail.com"}}) |> Repo.insert!
      assert length(CouchdbAdapter.fetch_all(Repo, Post, :all)) == 4
      assert length(CouchdbAdapter.fetch_all(Repo, User, :all)) == 2
      pf1 = CouchdbAdapter.get(Repo, Post, pc._id, preload: :user)
      assert not is_nil(pf1)
      assert pf1.user_id == pc.user._id
      assert pf1.user_id == pf1.user._id
      assert pf1._rev == pc._rev
      pu = Repo.update! Post.changeset(pf1, %{title: "new lorem", body: "new lorem ipsum", user: %{username: "doe", email: "doe@gmail.com"}})
      assert length(CouchdbAdapter.fetch_all(Repo, Post, :all)) == 4
      assert length(CouchdbAdapter.fetch_all(Repo, User, :all)) == 2
      pf2 = CouchdbAdapter.get(Repo, Post, pc._id, preload: :user)
      assert pu.user_id == pu.user._id
      assert pu.user_id == pf2.user_id
      assert pu.user._id == pf2.user._id
      assert pu.user._rev == pf2.user._rev
      assert pf2.user_id == pc.user._id
      assert pf2._rev != pc._rev
      assert pf2.title == "new lorem"
      assert pf2.body == "new lorem ipsum"
      uf2 = CouchdbAdapter.get(Repo, User, pc.user._id)
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
      foof = CouchdbAdapter.get(Repo, Foo, fooc._id)
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
      pf = CouchdbAdapter.get(Repo, E, pc._id)
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
      pf = CouchdbAdapter.get(Repo, G, pc._id)
      assert pf._id == pc._id
      assert pf.x == x
    end
  end

  describe "direct http calls" do
    setup %{db: db, design_docs: design_docs, docs: docs} do
      design_docs |> Enum.each(fn (design_doc) ->
        :couchbeam.save_doc(db, design_doc |> CouchdbAdapter.to_doc)
      end)
      {:ok, _results} = :couchbeam.save_docs(db, Enum.map(docs, fn(doc) ->
        CouchdbAdapter.to_doc(doc)
      end))
      Repo.insert! %User{_id: "test-user-id1", type: "User", username: "bob", email: "bob@gmail.com"}
      Repo.insert! %User{_id: "test-user-id2", type: "User", username: "alice", email: "alice@gmail.com"}
      Repo.insert! %User{_id: "test-user-id3", type: "User", username: "bob", email: "bob@gmail.com"}
      :ok
    end

    test "multiple_fetch_all works for Ecto schema" do
      {:ok, list} = CouchdbAdapter.multiple_fetch_all(Repo, User, :all, %{queries: [%{key: "test-user-id1"}, %{key: "test-user-id2"}]})
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
      {:ok, list} = CouchdbAdapter.multiple_fetch_all(Repo, User, :all, %{queries: [%{key: "test-user-id1"}, %{key: "test-user-id2"}]}, as_map: true)
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
      {:ok, list} = CouchdbAdapter.multiple_fetch_all(Repo, User, :counts, %{queries: [%{group_level: 0}]}, as_map: true)
      v = list |> Enum.at(0) |> Enum.at(0)
      assert v == 6
    end

    test "multiple_fetch_all with fetch_keys" do
      {:ok, list} = CouchdbAdapter.multiple_fetch_all(Repo, User, :counts, %{queries: [%{group_level: 0}]}, as_map: true, fetch_keys: true)
      assert list == [[nil]]
    end

    test "find" do
      {:ok, list} = CouchdbAdapter.find(Repo, User, %{selector: %{username: %{"$eq" => "alice"}}})
      a = list |> hd
      assert a._id == "test-user-id2"
      assert a.email == "alice@gmail.com"
    end

    test "find with preloads" do
      pc = Repo.insert! %Post{title: "chibata", body: "lorem ipsum", user: %User{_id: "test-user-id-john", username: "john", email: "john@gmail.com"}}
      {:ok, list} = CouchdbAdapter.find(Repo, Post, %{selector: %{title: %{"$eq" => "chibata"}}}, preload: :user)
      a = list |> hd
      assert a._id == pc._id
      assert a.title == "chibata"
      assert a.user_id == "test-user-id-john"
      assert not is_nil(a.user)
      assert a.user._id == "test-user-id-john"
    end
  end
end
