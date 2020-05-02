defmodule Couchdb.Ecto.RepoTest do
  use Couchdb.Ecto.ModelCase, async: false
  alias Couchdb.Ecto.Fetchers
  alias Couchdb.Ecto.Attachment


  setup do
    clear_db!()
    :ok
  end

  describe "insert" do

    test "generates id/rev" do
      {:ok, result} = TestRepo.insert(@post)
      assert has_id_and_rev?(result)
    end

    test "uses given generated id" do
      post = struct(@post, _id: "FOO")
      {:ok, result} = TestRepo.insert(post)
      assert has_id_and_rev?(result)
      assert result._id == "FOO"
      assert result.type == "Post"
    end

    test "fails if using the same id twice" do
      post = struct(@post, _id: "FOO")
      assert {:ok, _} = TestRepo.insert(post)
      exception = assert_raise Ecto.ConstraintError, fn -> TestRepo.insert(post) end
      assert exception.constraint == "Post_id_index"
    end

    test "handles conflicts as changeset errors using unique_constraint" do
      import Ecto.Changeset
      params = Map.from_struct(@post)
      changeset = cast(%Post{}, %{params | _id: "FOO"}, [:title, :body, :_id]) |> unique_constraint(:id)
      assert {:ok, _} = TestRepo.insert(changeset)
      assert {:error, changeset} = TestRepo.insert(changeset)
      assert changeset.errors[:id] == {"has already been taken", [constraint: :unique, constraint_name: "Post_id_index"]}
    end

    test "supports embeds" do
      post = struct(@post, grants: @grants)
      {:ok, result} = TestRepo.insert(post)
      assert has_id_and_rev?(result)
    end

    test "supports embeds without ids" do
      post = struct(@post, stats: %Stats{visits: 12, time: 892})
      {:ok, result} = TestRepo.insert(post)
      assert has_id_and_rev?(result)
    end

    test "generates timestamps" do
      {:ok, inserted} = TestRepo.insert(@post)
      assert not is_nil(inserted.inserted_at)
      assert not is_nil(inserted.updated_at)
    end
  end

  describe "update" do

    setup do
      create_views!(@schema_design_docs)
      insert_docs!(@posts)
      {:ok, posts} = Fetchers.all(TestRepo, Post, :all, [include_docs: true], [])
      %{posts: posts}
    end

    test "changes attributes and _rev", %{db: db, posts: [post | _]} do
      {:ok, updated_post} =
        post
        |> Ecto.Changeset.change(title: "Changed title")
        |> Ecto.Changeset.put_embed(:stats, %Stats{visits: 1000})
        |> TestRepo.update
      assert updated_post._rev != post._rev
      assert updated_post.title == "Changed title"
      assert updated_post.stats.visits == 1000
      # check persisted data
      assert {:ok, fetched_post} = db |> ICouch.open_doc(post._id)
      assert fetched_post["_id"] == updated_post._id
      assert fetched_post["_rev"] == updated_post._rev
      # unchanged data is still persisted
      assert fetched_post["body"] == post.body
    end

    test "works with embeds_many", %{db: db, posts: [post | _]} do
      new_grants = Enum.take_random(post.grants, 1) |> Enum.map(&%{&1 | access: "new"})
      {:ok, updated_post} =
        post
        |> Ecto.Changeset.change
        |> Ecto.Changeset.put_embed(:grants, new_grants)
        |> TestRepo.update
      assert length(updated_post.grants) == 1
      assert match?([%Grant{access: "new"}], updated_post.grants)
      # check persisted data
      assert {:ok, fetched_post} = db |> ICouch.open_doc(post._id)
      [fetched_grant] = fetched_post["grants"]
      assert fetched_grant["access"] == "new"
    end

    test "works with embeds_many after empty update", %{db: db, posts: [post | _]} do
      {:ok, updated_post} =
        post
        |> Ecto.Changeset.change
        |> Ecto.Changeset.put_embed(:grants, [])
        |> TestRepo.update
      assert updated_post.grants == []
      # check persisted data
      {:ok, fetched_post} = db |> ICouch.open_doc(post._id)
      assert [] = fetched_post["grants"]
    end

    test "raises Ecto.StaleEntryError if document is not found", %{posts: [post | _]} do
      missing_post = post |> Map.put(:_id, "not found")
      assert_raise(Ecto.StaleEntryError,
                   fn ->
                     missing_post
                     |> Ecto.Changeset.change(title: "Changed title")
                     |> TestRepo.update
                   end)
    end

    test "raises Ecto.StaleEntryError if the document _rev does not match", %{posts: [post | _]} do
      stale_post = post |> Map.put(:_rev, "not found")
      assert_raise(Ecto.StaleEntryError,
                   fn ->
                     stale_post
                     |> Ecto.Changeset.change(title: "Changed title")
                     |> TestRepo.update
                   end)
    end

    test "update on_conflict: :replace_all", %{db: db} do
      pc = Post.changeset(%Post{}, %{title: "lorem", body: "lorem ipsum"}) |> TestRepo.insert!
      assert nil != pc._id
      assert nil != pc._rev
      assert pc.title == "lorem"
      pu1 = Post.changeset(pc, %{title: "ipsum"}) |> TestRepo.update!
      assert pu1._id == pc._id
      assert pu1._rev > pc._rev
      assert pu1.title == "ipsum"
      pu2 = Post.changeset(pu1, %{title: "foo"}) |> TestRepo.update!
      assert pu2._id == pu1._id
      assert pu2._rev > pu1._rev
      assert pu2.title == "foo"
      pu3 = Post.changeset(pu1, %{title: "goo"}) |> TestRepo.update!(on_conflict: :replace_all)
      assert pu3._id == pu2._id
      assert pu3._rev > pu2._rev
      assert pu3.title == "goo"
      {:ok, pf} = db |> ICouch.open_doc(pc._id)
      assert pf["_id"] == pu3._id
      assert pf["_rev"] == pu3._rev
      assert pf["title"] == pu3.title
    end
  end

  describe "delete" do

    setup do
      create_views!(@schema_design_docs)
      posts_with_rev = insert_docs!(@posts)
      %{docs: posts_with_rev}
    end

    test "removes the id", %{db: db, docs: docs} do
      {deleted_doc, _} = List.pop_at(docs, 1)
      post = struct(Post, _id: deleted_doc._id, _rev: deleted_doc._rev)
      {:ok, deleted_post} = TestRepo.delete(post)
      assert deleted_post._id == post._id
      assert {:error, :not_found} = db |> ICouch.open_doc(deleted_post._id)
      assert {:ok, _} = db |> ICouch.open_doc(hd(docs)._id)
    end

    test "succeeds if the id is not found" do
      post = struct(Post, _id: "Not found", _rev: "4-Unknown")
      assert {:ok, _} = TestRepo.delete(post)
    end

    test "fails with stale if the revision is outdated", %{docs: docs} do
      {deleted_doc, _docs} = List.pop_at(docs, 1)
      assert_raise(Ecto.StaleEntryError,
                   fn ->
                     struct(Post, %{_id: deleted_doc._id, _rev: "0-outdated"})
                     |> Ecto.Changeset.change
                     |> TestRepo.delete
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

    test "deletes anything on the same database", %{db: db, docs: docs} do
      to_delete = hd(docs)
      other = %__MODULE__.Other{_id: to_delete._id, _rev: to_delete._rev}
      {:ok, %{rows: query_result}} = db |> ICouch.open_view!("Post/all") |> ICouch.View.fetch
      assert length(query_result) == 3
      assert {:ok, _} = TestRepo.delete(other)
      {:ok, %{rows: query_result}} = db |> ICouch.open_view!("Post/all") |> ICouch.View.fetch
      assert length(query_result) == 2
    end
  end

  describe "insert_all" do

    setup do
      create_views!(@schema_design_docs)
      posts =
        Enum.map(@posts, fn doc ->
          %{doc |
            grants: Enum.map(doc.grants, &struct(Grant, &1)),
            stats: struct(Stats, doc.stats)
          }
        end)
      %{posts: posts}
    end

    test "inserts with generated id/rev", %{db: db, posts: posts} do
      posts = Enum.map(posts, &Map.drop(&1, [:_id]))
      assert {3, nil} == TestRepo.insert_all(Post, posts)
      {:ok, %{rows: query_result}} = db |> ICouch.open_view!("Post/all") |> ICouch.View.fetch
      assert Enum.count(query_result) == 3
      assert Enum.all?(query_result, fn result ->
        doc = result["value"]
        assert nil != doc
        assert nil != doc["_id"]
        assert nil != doc["_rev"]
      end)

    end

    test "inserts with explicit id", %{db: db, posts: posts} do
      assert {3, nil} == TestRepo.insert_all(Post, posts)
      {:ok, %{rows: query_result}} = db |> ICouch.open_view!("Post/all") |> ICouch.View.fetch
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

  describe "insert or update" do

    setup do
      create_views!(@schema_design_docs)
      :ok
    end

    test "insert or update" do
      pc = Post.changeset(%Post{}, %{title: "lorem", body: "lorem ipsum"}) |> TestRepo.insert_or_update!
      assert nil != pc._id
      assert nil != pc._rev
      assert "lorem" == pc.title
      pu = Post.changeset(pc, %{title: "ipsum"}) |> TestRepo.insert_or_update!
      assert nil != pu._id
      assert nil != pu._rev
      assert pu._rev > pc._rev
      assert "ipsum" == pu.title
    end
  end

  describe "map and arrays types" do

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
      {:ok, pc} = E.changeset(%E{}, %{t: "a", u: nil, d: d, f: %{t: nil}}) |> TestRepo.insert
      {:ok, pf} = Fetchers.get(TestRepo, E, pc._id, [], [])
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
      {:ok, pc} = G.changeset(%G{}, %{x: x}) |> TestRepo.insert
      {:ok, pf} = TestRepo |> Fetchers.get(G, pc._id, [], [])
      assert pf._id == pc._id
      assert pf.x == x
    end

  end

  describe "dates types" do

    defmodule TestUTCDate do
      use Ecto.Schema
      @primary_key false
      @foreign_key_type :binary_id
      schema "Foo" do
        field :_id, :binary_id, autogenerate: true, primary_key: true
        field :_rev, :string, read_after_writes: true, primary_key: true
        field :type, :string, read_after_writes: true
        field :datetime_usec, :utc_datetime_usec
        field :datetime, :utc_datetime
        field :naive_datetime_usec, :naive_datetime_usec
        field :naive_datetime, :naive_datetime
        field :date, :date
        field :time_usec, :time_usec
        field :time, :time
      end
      def changeset(struct, params) do
        struct |> Ecto.Changeset.cast(params, [:datetime_usec, :datetime, :naive_datetime_usec, :naive_datetime, :date, :time_usec, :time])
      end
    end

    test "support date types", %{db: db} do
      # prepare dates and times
      datetime_usec = DateTime.utc_now
      datetime_truncate = datetime_usec |> DateTime.truncate(:second)
      naive_datetime_usec = NaiveDateTime.utc_now
      naive_datetime_truncate = naive_datetime_usec |> NaiveDateTime.truncate(:second)
      date = Date.utc_today
      time_usec = Time.utc_now
      time_truncate = time_usec |> Time.truncate(:second)
      args = %{
        datetime_usec: datetime_usec, datetime: datetime_usec,
        naive_datetime_usec: naive_datetime_usec, naive_datetime: naive_datetime_usec,
        date: date,
        time_usec: time_usec, time: time_usec
      }
      # insert
      i = TestUTCDate.changeset(%TestUTCDate{}, args) |> TestRepo.insert!
      # check inserted
      assert i._id == i._id
      assert i.datetime_usec == datetime_usec
      assert i.datetime == datetime_truncate
      assert i.naive_datetime_usec == naive_datetime_usec
      assert i.naive_datetime == naive_datetime_truncate
      assert i.date == date
      assert i.time_usec == time_usec
      assert i.time == time_truncate
      # check in db
      assert {:ok, %{fields: f1}} = db |> ICouch.open_doc(i._id)
      assert f1["_id"] == i._id
      assert f1["_rev"] == i._rev
      assert f1["datetime_usec"] == datetime_usec |> DateTime.to_iso8601
      assert f1["datetime"] == datetime_truncate |> DateTime.to_iso8601
      assert f1["naive_datetime_usec"] == naive_datetime_usec |> NaiveDateTime.to_iso8601
      assert f1["naive_datetime"] == naive_datetime_truncate |> NaiveDateTime.to_iso8601
      assert f1["date"] == date |> Date.to_iso8601
      assert f1["time_usec"] == time_usec |> Time.to_iso8601
      assert f1["time"] == time_truncate |> Time.to_iso8601
      # check fetched
      {:ok, f2} = Fetchers.get(TestRepo, TestUTCDate, i._id, [], [])
      assert f2._id == i._id
      assert f2._rev == i._rev
      assert f2.datetime_usec == datetime_usec
      assert f2.datetime == datetime_truncate
      assert f2.naive_datetime_usec == naive_datetime_usec
      assert f2.naive_datetime == naive_datetime_truncate
      assert f2.date == date
      assert f2.time_usec == time_usec
      assert f2.time == time_truncate
    end

  end

  describe "attachments" do

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
        field :other_attachment, Attachment
      end
      def changeset(struct, params) do
        struct |> Ecto.Changeset.cast(params, [:title, :example_attachment, :other_attachment])
      end
    end

    test "integration (fetch, insert, fetch, update, fetch" do
      attachment1 = %{content_type: "application/json", data: %{foo: "goo"}}
      {:ok, ai} = TestAttachment.changeset(%TestAttachment{}, %{title: "foogoo", example_attachment: attachment1}) |> TestRepo.insert
      assert ai.example_attachment.data == %{foo: "goo"}
      # revpos
      {:ok, aif1} = Fetchers.get(TestRepo, TestAttachment, ai._id, [attachments: true], [])
      {:ok, aif2} = Fetchers.get(TestRepo, TestAttachment, ai._id, [], [])
      assert aif1._id == ai._id
      assert aif1._rev == ai._rev
      assert aif2._id == ai._id
      assert aif2._rev == ai._rev
      assert %Attachment{content_type: "application/json", data: %{"foo" => "goo"}} = aif1.example_attachment
      assert %Attachment{content_type: "application/json", data: nil} = aif2.example_attachment
      assert is_nil(aif1.other_attachment)
      assert is_nil(aif2.other_attachment)
      attachment2 = %{content_type: "application/json", data: %{bar: "baz"}}
      {:ok, au} = TestAttachment.changeset(ai, %{example_attachment: attachment2}) |> TestRepo.update
      assert au._id == ai._id
      assert au._rev > ai._rev
      # revpos
      assert au.example_attachment.data == %{bar: "baz"}
      {:ok, auf1} = Fetchers.get(TestRepo, TestAttachment, au._id, [attachments: true], [])
      assert auf1._id == au._id
      assert auf1._rev == au._rev
      assert %Attachment{content_type: "application/json", data: %{"bar" => "baz"}} = auf1.example_attachment
      assert auf1.example_attachment.revpos > aif1.example_attachment.revpos
    end

    test "preserve attachment if stub on update" do
      attachment = %{content_type: "application/json", data: %{foo: "goo"}}
      {:ok, ai} = TestAttachment.changeset(%TestAttachment{}, %{title: "foogoo", example_attachment: attachment}) |> TestRepo.insert
      {:ok, aif} = Fetchers.get(TestRepo, TestAttachment, ai._id, [], [])
      assert aif._id == ai._id
      assert %Attachment{content_type: "application/json", data: nil} = aif.example_attachment
      {:ok, au} = TestAttachment.changeset(ai, %{title: "bar"}) |> TestRepo.update
      assert au._id == ai._id
      assert au._rev > ai._rev
      assert au.title == "bar"
      # revpos
      {:ok, auf} = Fetchers.get(TestRepo, TestAttachment, ai._id, [attachments: true], [])
      assert au._id == ai._id
      assert au._rev > ai._rev
      assert auf._id == au._id
      assert auf._rev == au._rev
      assert auf.title == au.title
      assert %Attachment{content_type: "application/json", data: %{"foo" => "goo"}} = auf.example_attachment
      assert auf.example_attachment.revpos == aif.example_attachment.revpos
    end

    test "remove attachment if nil on update" do
      attachment = %{content_type: "application/json", data: %{foo: "goo"}}
      {:ok, ai} = TestAttachment.changeset(%TestAttachment{}, %{title: "foogoo", example_attachment: attachment}) |> TestRepo.insert
      {:ok, aif} = Fetchers.get(TestRepo, TestAttachment, ai._id, [], [])
      assert aif._id == ai._id
      assert %Attachment{content_type: "application/json", data: nil} = aif.example_attachment
      {:ok, au} = TestAttachment.changeset(ai, %{title: "bar", example_attachment: nil}) |> TestRepo.update
      assert au._id == ai._id
      assert au._rev > ai._rev
      assert au.title == "bar"
      # revpos
      {:ok, auf} = Fetchers.get(TestRepo, TestAttachment, ai._id, [attachments: true], [])
      assert au._id == ai._id
      assert au._rev > ai._rev
      assert auf._id == au._id
      assert auf._rev == au._rev
      assert auf.title == au.title
      assert is_nil(auf.example_attachment)
    end

    test "multiple attachments" do
      attachment1 = %{content_type: "application/json", data: %{"foo" => 1}}
      attachment2 = %{content_type: "foogoo", data: "foogoo"}
      {:ok, ai} = TestAttachment.changeset(%TestAttachment{}, %{title: "foogoo", example_attachment: attachment1, other_attachment: attachment2}) |> TestRepo.insert
      assert not is_nil(ai._id)
      assert not is_nil(ai.example_attachment)
      assert not is_nil(ai.other_attachment)
      assert ai.example_attachment.content_type == "application/json"
      assert ai.example_attachment.data == %{"foo" => 1}
      assert ai.other_attachment.content_type == "foogoo"
      assert ai.other_attachment.data == "foogoo"
      {:ok, af} = Fetchers.get(TestRepo, TestAttachment, ai._id, [attachments: true], [])
      assert af._id == ai._id
      assert af._rev == ai._rev
      assert af.example_attachment.content_type == ai.example_attachment.content_type
      assert af.example_attachment.data == ai.example_attachment.data
      assert af.other_attachment.content_type == ai.other_attachment.content_type
      assert af.other_attachment.data == ai.other_attachment.data
    end

    test "one and all" do
      create_views!([@attachment_doc])
      attachment = %{content_type: "application/json", data: %{foo: "goo"}}
      ai = TestAttachment.changeset(%TestAttachment{}, %{title: "foogoo", example_attachment: attachment}) |> TestRepo.insert!
      # without_doc not returning attachment
      {:ok, fetch_one1} = TestRepo |> Fetchers.one(TestAttachment, :all_without_doc, [key: ai._id, include_docs: true], [])
      {:ok, fetch_all1} = TestRepo |> Fetchers.all(TestAttachment, :all_without_doc, [include_docs: true], [])
      fetch_all1 = fetch_all1 |> hd
      assert fetch_one1._id == ai._id
      assert fetch_one1._rev == ai._rev
      assert %Attachment{content_type: "application/json", data: nil} = fetch_one1.example_attachment
      assert fetch_all1._id == ai._id
      assert fetch_all1._rev == ai._rev
      assert %Attachment{content_type: "application/json", data: nil} = fetch_all1.example_attachment
      # without_doc returning attachment
      {:ok, fetch_one2} = TestRepo |> Fetchers.one(TestAttachment, :all_without_doc, [key: ai._id, include_docs: true, attachments: true], [])
      {:ok, fetch_all2} = TestRepo |> Fetchers.all(TestAttachment, :all_without_doc, [include_docs: true, attachments: true], [])
      fetch_all2 = fetch_all2 |> hd
      assert fetch_one2._id == ai._id
      assert fetch_one2._rev == ai._rev
      assert %Attachment{content_type: "application/json", data: %{"foo" => "goo"}} = fetch_one2.example_attachment
      assert fetch_one2.example_attachment.revpos == 1
      assert fetch_all2._id == ai._id
      assert fetch_all2._rev == ai._rev
      assert %Attachment{content_type: "application/json", data: %{"foo" => "goo"}} = fetch_one2.example_attachment
      assert fetch_all2.example_attachment.revpos == 1
      # with_doc
      {:ok, fetch_one3} = TestRepo |> Fetchers.one(TestAttachment, :all_with_doc, [key: ai._id, include_docs: true, attachments: true], [])
      {:ok, fetch_all3} = TestRepo |> Fetchers.all(TestAttachment, :all_with_doc, [include_docs: true, attachments: true], [])
      fetch_all3 = fetch_all3 |> hd
      assert fetch_one3._id == ai._id
      assert fetch_one3._rev == ai._rev
      assert fetch_one3.title == "foogoo"
      assert %Attachment{content_type: "application/json", data: nil} = fetch_one3.example_attachment
      assert fetch_all3._id == ai._id
      assert fetch_all3._rev == ai._rev
      assert fetch_all3.title == "foogoo"
      assert %Attachment{content_type: "application/json", data: nil} = fetch_all3.example_attachment
    end

    test "cast" do
      assert {:ok, %Attachment{content_type: "foo", data: "goo"}} == Attachment.cast(%Attachment{content_type: "foo", data: "goo"})
      assert {:ok, %Attachment{content_type: "foo", data: "goo"}} == Attachment.cast(%{content_type: "foo", data: "goo"})
    end

    test "dump" do
      assert {:ok, %{content_type: "application/json", data: "eyJmb28iOiJnb28ifQ=="}} == Attachment.dump(%Attachment{content_type: "application/json", data: %{foo: "goo"}})
      assert {:ok, %{content_type: "foogoo", data: "Zm9vZ29v"}} == Attachment.dump(%Attachment{content_type: "foogoo", data: "foogoo"})
    end

    test "load" do
      assert {:ok, %Attachment{content_type: "application/json", data: %{"foo" => "goo"}, revpos: 1}} == Attachment.load(%{content_type: "application/json", data: "{\"foo\":\"goo\"}", revpos: 1})
      assert {:ok, %Attachment{content_type: "application/json", data: nil, revpos: 1}} == Attachment.load(%{content_type: "application/json", data: nil, revpos: 1})
    end

  end

  describe "changeset" do

    setup do
      create_views!(@schema_design_docs)
      insert_docs!(@posts |> Enum.map(&(&1 |> Map.put(:user_id, "test-user"))))
      :ok
    end

    test "insert and update from changeset", %{} do
      {:ok, list} = Fetchers.all(TestRepo, User, :all, [], [])
      assert [] == list
      {:ok, ui} = User.changeset(%User{}, %{_id: "test-user-id", username: "bob", email: "bob@gmail.com"}) |> TestRepo.insert
      {:ok, list} = Fetchers.all(TestRepo, User, :all, [], [])
      assert [_] = list
      assert ui._id == "test-user-id"
      assert ui._rev
      assert ui.type == "User"
      {:ok, uq1} = Fetchers.get(TestRepo, User, "test-user-id", [], [])
      assert ui._id == uq1._id
      assert ui._rev == uq1._rev
      assert ui.type == uq1.type
      assert ui.username == uq1.username
      assert ui.email == uq1.email
      assert ui.inserted_at == uq1.inserted_at
      assert ui.updated_at == uq1.updated_at
      :timer.sleep(1000)
      {:ok, uu} = User.changeset(uq1, %{username: "silent bob", email: "silent.bob@gmail.com"}) |> TestRepo.update
      {:ok, list_user} = Fetchers.all(TestRepo, User, :all, [], [])
      assert [_] = list_user
      {:ok, uq2} = Fetchers.get(TestRepo, User, "test-user-id", [], [])
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
      {:ok, list} = Fetchers.all(TestRepo, User, :all, [], [])
      assert list == []
      {:ok, inserted} = Post.changeset_user(%Post{}, %{title: "lorem", body: "lorem ipsum", user: %{_id: "test-user-id", username: "bob", email: "bob@gmail.com"}}) |> TestRepo.insert
      assert inserted.user_id == inserted.user._id
      {:ok, list_user} = Fetchers.all(TestRepo, User, :all, [], [])
      assert [_] = list_user
    end
  end

  describe "integration tests" do

    setup do
      create_views!(@schema_design_docs)
      insert_docs!(@posts)
      TestRepo.insert! %User{_id: "test-user-id0", username: "bob", email: "bob@gmail.com"}
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
      {:ok, list_post} = Fetchers.all(TestRepo, Post, :all, [], [])
      assert length(list_post) == 3
      {:ok, list_user} = Fetchers.all(TestRepo, User, :all, [], [])
      assert [_] = list_user
      pc = Post.changeset(%Post{}, %{title: "lorem", body: "lorem ipsum", user: %{_id: "test-user-id2", username: "alice", password: "alice@gmail.com"}}) |> TestRepo.insert!
      {:ok, list_post} = Fetchers.all(TestRepo, Post, :all, [], [])
      assert length(list_post) == 4
      {:ok, list_user} = Fetchers.all(TestRepo, User, :all, [], [])
      assert [_] = list_user
      {:ok, pf} = Fetchers.get(TestRepo, Post, pc._id, [], [])
      assert not is_nil(pf)
      TestRepo.update! Post.changeset(pf, %{title: "new lorem", body: "new lorem ipsum"})
      {:ok, pu} = Fetchers.get(TestRepo, Post, pc._id, [], [])
      assert pu._id == pf._id
      assert pu._rev != pf._rev
      assert pu.title == "new lorem"
      assert pu.body == "new lorem ipsum"
      {:ok, list_post} = Fetchers.all(TestRepo, Post, :all, [], [])
      assert length(list_post) == 4
      {:ok, list_user} = Fetchers.all(TestRepo, User, :all, [], [])
      assert [_] = list_user
    end

    test "update including association from get" do
      pc = Post.changeset_user(%Post{}, %{title: "lorem", body: "lorem ipsum", user: %{_id: "test-user-id3", username: "john", email: "john@gmail.com"}}) |> TestRepo.insert!
      {:ok, list_post} = Fetchers.all(TestRepo, Post, :all, [], [])
      assert length(list_post) == 4
      {:ok, list_user} = Fetchers.all(TestRepo, User, :all, [], [])
      assert length(list_user) == 2
      {:ok, pf1} = Fetchers.get(TestRepo, Post, pc._id, [], [preload: :user])
      assert not is_nil(pf1)
      assert pf1.user_id == pc.user._id
      assert pf1.user_id == pf1.user._id
      assert pf1._rev == pc._rev
      assert pf1.title == "lorem"
      assert pf1.body == "lorem ipsum"
      assert pf1.user._id == "test-user-id3"
      assert pf1.user.username == "john"
      assert pf1.user.email == "john@gmail.com"
      pu = TestRepo.update! Post.changeset_user(pf1, %{title: "new lorem", body: "new lorem ipsum", user: %{username: "doe", email: "doe@gmail.com"}})
      {:ok, list_post} = Fetchers.all(TestRepo, Post, :all, [], [])
      assert length(list_post) == 4
      {:ok, list_user} = Fetchers.all(TestRepo, User, :all, [], [])
      assert length(list_user) == 2
      {:ok, pf2} = Fetchers.get(TestRepo, Post, pc._id, [], [preload: :user])
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
      {:ok, uf2} = Fetchers.get(TestRepo, User, pc.user._id, [], [])
      assert uf2._id == pf2.user_id
      assert uf2._id == pf2.user._id
      assert uf2._id == pc.user._id
      assert uf2._rev == pu.user._rev
      assert uf2._rev == pf2.user._rev
      assert uf2._rev != pf2._rev
      assert uf2.username == "doe"
      assert uf2.email == "doe@gmail.com"
    end

  end

end
