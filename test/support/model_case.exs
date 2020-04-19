defmodule Couchdb.Ecto.ModelCase do
  use ExUnit.CaseTemplate

  using do
    quote do

      import ExUnit.Assertions
      import Couchdb.Ecto.Helpers

      @repo TestRepo


      def clear_db!(stack_size \\ 0) do
        if stack_size > 3 do
          raise "Could not clear db!"
        end
        server = @repo |> server_from_repo
        db = @repo |> db_from_repo
        db_name = db.name
        # IO.inspect(server)
        case server |> ICouch.delete_db(db_name) do
          {:error, :not_found} -> :ok
          :ok -> :ok
          _error -> clear_db!(stack_size + 1)
        end
        case server |> ICouch.create_db(db_name) do
          {:ok, _db} -> :ok
          _error -> clear_db!(stack_size + 1)
        end
        :ok
      end

      def create_views!(design_docs) do
        design_docs |> Enum.map(fn {ddoc, code} ->
          @repo |> Couchdb.Ecto.Storage.create_ddoc(ddoc, code)
        end)
      end

      def insert_docs!(docs) do
        {:ok, docs} = @repo |> db_from_repo |> ICouch.save_docs(docs)
        docs |> Enum.map(fn return ->
          %{"ok" => true, "id" => id, "rev" => rev} = return
          %{_id: id, _rev: rev}
        end)
      end

      def has_id_and_rev?(resource) do
        assert resource._id
        assert resource._rev
      end

      def atomize_keys!(list) when is_list(list), do: list |> Enum.map(&(&1 |> atomize_keys!))
      def atomize_keys!(map) when is_map(map), do: map |> Enum.map(fn {k, v} -> {k |> String.to_atom, v} end) |> Map.new

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
      @attachment_ddoc_id "TestAttachment"
      @attachment_data_ddoc_code %{
        views: %{
          all_with_doc: %{
            map: "function(doc) { if (doc.type === 'TestAttachment') emit(doc._id, doc) }"
          },
          all_without_doc: %{
            map: "function(doc) { if (doc.type === 'TestAttachment') emit(doc._id, null) }"
          }
        }
      }
      @schema_design_docs [
        {@post_ddoc_id, @post_ddoc_code},
        {@user_ddoc_id, @user_ddoc_code},
        {@user_data_ddoc_id, @user_data_ddoc_code}
      ]
      @attachment_doc {@attachment_ddoc_id, @attachment_data_ddoc_code }
      @post %Post{title: "how to write and adapter", body: "Don't know yet"}
      @posts [
        %{
          _id: "id1", type: "Post", title: "t1", body: "b1", stats: %{visits: 1, time: 10},
          grants: [%{id: "1", user: "u1.1", access: "a1.1"}, %{id: "2", user: "u1.2", access: "a1.2"}]
        },
        %{
          _id: "id2", type: "Post", title: "t2", body: "b2", stats: %{visits: 2, time: 20},
          grants: [%{id: "1", user: "u2.1", access: "a2.1"}, %{id: "2", user: "u2.2", access: "a2.2"}]
        },
        %{
          _id: "id3", type: "Post", title: "t3", body: "b3", stats: %{visits: 3, time: 30},
          grants: [%{id: "1", user: "u3.1", access: "a3.1"}, %{id: "2", user: "u3.2", access: "a3.2"}]
        }
      ]
      @grants [%Grant{user: "admin", access: "all"}, %Grant{user: "other", access: "read"}]

      @ddoc_doc_id "TestPost"
      @ddoc_doc_id_code %{
        "_id" => @ddoc_doc_id,
        "language" => "javascript",
        "views" => %{
          "all" => %{
            "map" => "function(doc) { if (doc.type === 'Post') emit(doc._id, doc) }"
          }
        }
      }
      @index_code %{
        index: %{
          fields: ["name"]
        },
        ddoc: "TestPostIndex",
        name: "test1"
      }

    end
  end

  setup do
    %{
      repo: TestRepo,
      server: TestRepo |> Couchdb.Ecto.Helpers.server_from_repo,
      db: TestRepo |> Couchdb.Ecto.Helpers.db_from_repo
    }
  end

end
