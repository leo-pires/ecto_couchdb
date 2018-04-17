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
    end

    test "fails if using the same id twice", %{post: post} do
      post = struct(post, _id: "FOO")
      assert {:ok, _} = Repo.insert(post)
      exception = assert_raise Ecto.ConstraintError, fn -> Repo.insert(post) end
      assert exception.constraint == "posts_id_index"
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

    test "fails with a check constraint if the revision is outdated", %{docs: docs} do
      import Ecto.Changeset
      {deleted_doc, _docs} = List.pop_at(docs, 1)
      {:error, changeset} = struct(Post, %{_id: deleted_doc._id, _rev: "0-outdated"})
                            |> change
                            |> check_constraint(:_rev, name: "conflict")
                            |> Repo.delete
      assert changeset.errors[:_rev] != nil
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

  describe "all(Schema)" do
    setup %{docs: docs, db: db, design_docs: design_docs} do
      design_docs |> Enum.each(fn (design_doc) ->
        :couchbeam.save_doc(db, design_doc |> CouchdbAdapter.to_doc)
      end)
      :couchbeam.save_docs(db, Enum.map(docs, fn(doc) ->
        CouchdbAdapter.to_doc(doc)
      end))
      :ok
    end

    test "retrieves all Posts as a list", %{docs: docs} do
      results = Repo.all(Post)
      assert length(results) == length(docs)
    end

    test "reads all non-embedded properties", %{docs: docs} do
      # get results indexed by _id to remove database non-determinism
      results = Repo.all(Post) |> Enum.map(fn post -> {post._id, post} end) |> Enum.into(%{})
      # compare values for all keys in the expected against the same-id actuals
      for expected <- docs,
          actual <- [Map.get(results, expected._id)],
          {k, v} <- expected,
          k != :stats and k != :grants,
          do: assert Map.get(actual, k) == v
    end

    test "reads embeds_one properties" do
      # get results indexed by _id to remove database non-determinism
      results = Repo.all(Post) |> Enum.map(fn post -> {post._id, post} end) |> Enum.into(%{})
      assert results["id1"].stats == %Stats{time: 10, visits: 1}
      assert results["id2"].stats == %Stats{time: 20, visits: 2}
    end

    test "reads embeds_many properties" do
      # get results indexed by _id to remove database non-determinism
      results = Repo.all(Post) |> Enum.map(fn post -> {post._id, post} end) |> Enum.into(%{})
      assert results["id1"].grants == [%Grant{user: "u1.1", access: "a1.1", id: "1"},
                                       %Grant{user: "u1.2", access: "a1.2", id: "2"}]
      assert results["id2"].grants == [%Grant{user: "u2.1", access: "a2.1", id: "1"},
                                       %Grant{user: "u2.2", access: "a2.2", id: "2"}]
    end
  end

  describe "all(Ecto.Query)" do
    import Ecto.Query

    setup %{docs: docs, db: db, design_docs: design_docs} do
      design_docs |> Enum.each(fn (design_doc) ->
        :couchbeam.save_doc(db, design_doc |> CouchdbAdapter.to_doc)
      end)
      :couchbeam.save_docs(db, Enum.map(docs, fn(doc) ->
        CouchdbAdapter.to_doc(doc)
      end))
      :ok
    end

    test "Post.all == key" do
      query = from p in Post, where: p.all == "id1"
      results = Repo.all(query)
      assert length(results) == 1
      [result] = results
      assert result._id == "id1"
    end

    test "Post.all in [keys...]" do
      query = from p in Post, where: p.all in ["id1", "id2", "not found"]
      results = Repo.all(query) |> Enum.map(fn post -> {post._id, post} end) |> Enum.into(%{})
      assert length(Map.keys(results)) == 2
    end

    test "Post.all >= key" do
      query = from p in Post, where: p.all >= "id2"
      results = Repo.all(query)
      assert length(results) == 2
      [id2, id3] = results
      assert id2._id == "id2"
      assert id3._id == "id3"
    end

    test "Post.all <= key" do
      query = from p in Post, where: p.all <= "id2"
      results = Repo.all(query)
      assert length(results) == 2
      [id1, id2] = results
      assert id1._id == "id1"
      assert id2._id == "id2"
    end

    test "Post.all >= startkey and <= end_key" do
      query = from p in Post, where: p.all >= "id2" and p.all <= "id2"
      results = Repo.all(query)
      assert length(results) == 1
      [id2] = results
      assert id2._id == "id2"
    end

    test "Post.all in [keys...] and in [other_keys...] intersecs the keys" do
      query = from p in Post, where: p.all in ["id1", "id2"] and p.all in ["id3", "id2"]
      [result] = Repo.all(query)
      assert result._id == "id2"
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

  describe "invalid queries" do
    import Ecto.Query

    test "Multiple >=" do
      assert_raise RuntimeError, ~r/startkey/, fn ->
        Repo.all(from p in Post, where: p.all >= "1" and p.all >= "2")
      end
    end

    test "Multiple <=" do
      assert_raise RuntimeError, ~r/endkey/, fn ->
        Repo.all(from p in Post, where: p.all <= "1" and p.all <= "2")
      end
    end

    test "Multiple ==" do
      assert_raise RuntimeError, ~r/key/,  fn ->
        Repo.all(from p in Post, where: p.all == "1" and p.all == "2")
      end
    end

    test "Post.all > key" do
      assert_raise RuntimeError, ~r/Unsupported expression/, fn ->
        Repo.all(from p in Post, where: p.all > "id2")
      end
    end

    test "delete_all" do
      assert_raise RuntimeError, ~r/Unsupported operation.*delete_all/, fn ->
        Repo.delete_all(Post)
      end

      assert_raise RuntimeError, ~r/Unsupported operation.*delete_all/, fn ->
        Repo.delete_all(from p in Post, where: p.all > "id2")
      end
    end
  end

  describe "changeset" do
    import Ecto.Query

    setup %{db: db, design_docs: design_docs} do
      design_docs |> Enum.each(fn (design_doc) ->
        :couchbeam.save_doc(db, design_doc |> CouchdbAdapter.to_doc)
      end)
      :ok
    end

    test "insert and update from changeset", %{} do
      {:ok, uc} = User.changeset(%User{}, %{_id: "test-user-id", username: "bob", email: "bob@gmail.com"}) |> Repo.insert
      assert uc._id == "test-user-id"
      assert uc._rev
      assert uc.type == "User"
      uq1 = Repo.one(from u in User, where: u.all == "test-user-id")
      assert uc._id == uq1._id
      assert uc._rev == uq1._rev
      assert uc.type == uq1.type
      assert uc.username == uq1.username
      assert uc.email == uq1.email
      assert uc.inserted_at == uq1.inserted_at
      {:ok, uu} = User.changeset(uq1, %{username: "silent bob", email: "silent.bob@gmail.com"}) |> Repo.update
      uq2 = Repo.one(from u in User, where: u.all == "test-user-id")
      assert uu._id == uq1._id
      assert uu._rev != uq1._rev
      assert uu._id == uq2._id
      assert uu._rev == uq2._rev
      assert uu.type == uq2.type
      assert uu.username == uq2.username
      assert uu.email == uq2.email
      assert uu.updated_at == uq2.updated_at
    end

    test "cast_assoc" do
      changeset = Post.changeset(%Post{}, %{title: "lorem", body: "lorem ipsum", user: %{_id: "test-user-id", username: "bob", email: "bob@gmail.com"}})
      {:ok, inserted} = changeset |> Repo.insert
      assert inserted.user_id == inserted.user._id
    end
  end

  describe "integration tests" do
    import Ecto.Query

    setup %{docs: docs, db: db, design_docs: design_docs} do
      design_docs |> Enum.each(fn (design_doc) ->
        :couchbeam.save_doc(db, design_doc |> CouchdbAdapter.to_doc)
      end)
      :couchbeam.save_docs(db, Enum.map(docs, fn(doc) ->
        CouchdbAdapter.to_doc(doc)
      end))
      Repo.insert! %User{_id: "test-user-id", username: "bob", email: "bob@gmail.com"}
      :ok
    end

    test "get return nil if not found" do
      assert is_nil(CouchdbAdapter.get(Repo, Post, :all, "xpto"))
    end

    test "fetch all" do
      assert length(CouchdbAdapter.fetch_all(Repo, Post, :all)) == 3
      assert length(CouchdbAdapter.fetch_all(Repo, User, :all)) == 1
    end

    test "fetch all by key" do
      u = CouchdbAdapter.fetch_all(Repo, User, :all, key: "test-user-id") |> hd
      assert u._id == "test-user-id"
      assert not is_nil(u._rev)
      assert u.username == "bob"
      assert u.email == "bob@gmail.com"
    end

    test "update from fetch all" do
      pc = Post.changeset(%Post{}, %{title: "lorem", body: "lorem ipsum", user: %{_id: "test-user-id2", username: "alice", password: "alice@gmail.com"}}) |> Repo.insert!
      assert length(CouchdbAdapter.fetch_all(Repo, Post, :all)) == 4
      assert length(CouchdbAdapter.fetch_all(Repo, User, :all)) == 2
      pf = CouchdbAdapter.get(Repo, Post, :all, pc._id)
      assert not is_nil(pf)
      Repo.update! Post.changeset(pf, %{title: "new lorem", body: "new lorem ipsum"})
      pu = CouchdbAdapter.get(Repo, Post, :all, pc._id)
      assert pu._id == pf._id
      assert pu._rev != pf._rev
      assert pu.title == "new lorem"
      assert pu.body == "new lorem ipsum"
      assert length(CouchdbAdapter.fetch_all(Repo, Post, :all)) == 4
      assert length(CouchdbAdapter.fetch_all(Repo, User, :all)) == 2
    end

    test "update including association from fetch all" do
      pc = Post.changeset(%Post{}, %{title: "lorem", body: "lorem ipsum", user: %{_id: "test-user-id3", username: "john", password: "john@gmail.com"}}) |> Repo.insert!
      assert length(CouchdbAdapter.fetch_all(Repo, Post, :all)) == 4
      assert length(CouchdbAdapter.fetch_all(Repo, User, :all)) == 2
      pf1 = CouchdbAdapter.get(Repo, Post, :all, pc._id)
      assert not is_nil(pf1)
      assert pf1.user_id == pc.user._id
      assert pf1._rev == pc._rev
      Repo.update! Post.changeset(pf1, %{title: "new lorem", body: "new lorem ipsum", user: %{_id: "test-user-id3", username: "doe", email: "doe@gmail.com"}})
      pf2 = CouchdbAdapter.get(Repo, Post, :all, pc._id)
      assert pf2.user_id == pc.user._id
      assert pf2._rev != pc._rev
      assert pf2.title == "new lorem"
      assert pf2.body == "new lorem ipsum"
      uf2 = CouchdbAdapter.get(Repo, User, :all, pc.user._id)
      assert uf2._id == pc.user._id
      assert uf2._rev != pf2._rev
      assert uf2.username == "doe"
      assert uf2.email == "doe@gmail.com"
      assert length(CouchdbAdapter.fetch_all(Repo, Post, :all)) == 4
      assert length(CouchdbAdapter.fetch_all(Repo, User, :all)) == 2
    end
  end

end
