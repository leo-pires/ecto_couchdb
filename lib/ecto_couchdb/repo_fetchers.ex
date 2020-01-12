defmodule Couchdb.Ecto.RepoFetchers do

  defmacro __using__(_opts) do
    quote do

      def get(schema, id, opts \\ []) do
        case Couchdb.Ecto.Fetchers.get(__MODULE__, schema, id, opts) do
          {:ok, data} -> data
          error -> error
        end
      end

      def get!(schema, id, opts \\ []) do
        case Couchdb.Ecto.Fetchers.get(__MODULE__, schema, id, opts) do
          {:ok, nil} -> raise "not found"
          {:ok, data} -> data
          error -> error
        end
      end

      def one(schema, view_name, opts \\ []) do
        case Couchdb.Ecto.Fetchers.one(__MODULE__, schema, view_name, opts) do
          {:ok, :many} -> raise "too many found"
          {:ok, data} -> data
          error -> error
        end
      end

      def one!(schema, view_name, opts \\ []) do
        case Couchdb.Ecto.Fetchers.one(__MODULE__, schema, view_name, opts) do
          {:ok, nil} -> raise "not found"
          {:ok, :many} -> raise "too many found"
          {:ok, data} -> data
          error -> error
        end
      end

      def all(schema, view_name, opts \\ []) do
        case Couchdb.Ecto.Fetchers.all(__MODULE__, schema, view_name, opts) do
          {:ok, data} -> data
          error -> error
        end
      end

      def multiple_all(schema, view_name, queries, opts \\ []) do
        Couchdb.Ecto.Fetchers.multiple_all(__MODULE__, schema, view_name, queries, opts)
      end

      def find(schema, query, opts \\ []) do
        Couchdb.Ecto.Fetchers.find(__MODULE__, schema, query, opts)
      end

    end
  end

end
