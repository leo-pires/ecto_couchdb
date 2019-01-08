# TODO: properly exceptions
# TODO: typespec

defmodule CouchdbAdapter.Repo do

  defmacro __using__(_args) do

    quote unquote: false do
      contents = quote do

        def all(schema, view_name, opts \\ []) do
          case CouchdbAdapter.Fetchers.fetch_all(unquote(__MODULE__), schema, view_name, opts) do
            {:ok, data} -> data
            error -> error
          end
        end

        def multiple_all(schema, view_name, queries, opts \\ []) do
          CouchdbAdapter.Fetchers.multiple_fetch_all(unquote(__MODULE__), schema, view_name, queries, opts)
        end

        def get(schema, id, opts \\ []) do
          case CouchdbAdapter.Fetchers.get(unquote(__MODULE__), schema, id, opts) do
            {:ok, data} -> data
            error -> error
          end
        end

        def get!(schema, id, opts \\ []) do
          case CouchdbAdapter.Fetchers.get(unquote(__MODULE__), schema, id, opts) do
            {:ok, nil} -> raise "not found"
            {:ok, data} -> data
            error -> error
          end
        end

        def one(schema, view_name, opts \\ []) do
          case CouchdbAdapter.Fetchers.fetch_one(unquote(__MODULE__), schema, view_name, opts) do
            {:ok, :many} -> raise "too many found"
            {:ok, data} -> data
            error -> error
          end
        end

        def one!(schema, view_name, opts \\ []) do
          case CouchdbAdapter.Fetchers.fetch_one(unquote(__MODULE__), schema, view_name, opts) do
            {:ok, nil} -> raise "not found"
            {:ok, :many} -> raise "too many found"
            {:ok, data} -> data
            error -> error
          end
        end

        def find(schema, query, opts \\ []) do
          CouchdbAdapter.Fetchers.find(unquote(__MODULE__), schema, query, opts)
        end

        def aggregate(a, b, c \\ []), do: unquote(__MODULE__).aggregate(a, b, c)
        def config(), do: unquote(__MODULE__).config()
        def delete(a, b \\ []), do: unquote(__MODULE__).delete(a, b)
        def delete!(a, b \\ []), do: unquote(__MODULE__).delete!(a, b)
        def delete_all(a, b \\ []), do: unquote(__MODULE__).delete_all(a, b)
        def get_by(a, b, c \\ []), do: unquote(__MODULE__).get_by(a, b, c)
        def get_by!(a, b, c \\ []), do: unquote(__MODULE__).get_by!(a, b, c)
        def in_transaction?(), do: unquote(__MODULE__).in_transaction?()
        def insert(a, b \\ []), do: unquote(__MODULE__).insert(a, b)
        def insert!(a, b \\ []), do: unquote(__MODULE__).insert!(a, b)
        def insert_all(a, b, c \\ []), do: unquote(__MODULE__).insert_all(a, b, c)
        def insert_or_update(a, b \\ []), do: unquote(__MODULE__).insert_or_update(a, b)
        def insert_or_update!(a, b \\ []), do: unquote(__MODULE__).insert_or_update!(a, b)
        def load(a, b), do: unquote(__MODULE__).load(a, b)
        def preload(a, b, c \\ []), do: unquote(__MODULE__).preload(a, b, c)
        def rollback(a), do: unquote(__MODULE__).rollback(a)
        def stop(a), do: unquote(__MODULE__).stop(a)
        def stop(a, b), do: unquote(__MODULE__).stop(a, b)
        def stream(a, b \\ []), do: unquote(__MODULE__).stream(a, b)
        def transaction(a, b \\ []), do: unquote(__MODULE__).transaction(a, b)
        def update(a, b \\ []), do: unquote(__MODULE__).update(a, b)
        def update!(a, b \\ []), do: unquote(__MODULE__).update!(a, b)
        def update_all(a, b, c \\ []), do: unquote(__MODULE__).update_all(a, b, c)

      end

      module =
          __MODULE__
          |> Atom.to_string
          |> (Kernel.<> ".Couchdb")
          |> String.to_atom

      Module.create(module, contents, __ENV__)

    end

  end

end
