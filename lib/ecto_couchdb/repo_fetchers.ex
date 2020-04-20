# TODO: typespec it
defmodule Couchdb.Ecto.RepoFetchers do

  defmacro __using__(_opts) do
    quote do

      def get(schema_or_map, id, fetch_opts \\ [], processor_opts \\ []) do
        case Couchdb.Ecto.Fetchers.get(__MODULE__, schema_or_map, id, fetch_opts, processor_opts) do
          {:ok, data} -> data
          error -> error
        end
      end

      def get!(schema_or_map, id, fetch_opts \\ [], processor_opts \\ []) do
        case Couchdb.Ecto.Fetchers.get(__MODULE__, schema_or_map, id, fetch_opts, processor_opts) do
          {:ok, nil} -> raise Ecto.NoResultsError
          {:ok, data} -> data
          error -> error
        end
      end

      def one(schema_or_map, ddoc_view, fetch_opts \\ [], processor_opts \\ []) do
        case Couchdb.Ecto.Fetchers.one(__MODULE__, schema_or_map, ddoc_view, fetch_opts, processor_opts) do
          {:ok, :too_many_results} -> raise "too many found"
          {:ok, data} -> data
          error -> error
        end
      end

      def one!(schema_or_map, ddoc_view, fetch_opts \\ [], processor_opts \\ []) do
        case Couchdb.Ecto.Fetchers.one(__MODULE__, schema_or_map, ddoc_view, fetch_opts, processor_opts) do
          {:ok, nil} -> raise Ecto.NoResultsError
          {:ok, :too_many_results} -> raise "too many found"
          {:ok, data} -> data
          error -> error
        end
      end

      def all(schema_or_map, ddoc_view, fetch_opts \\ [], processor_opts \\ []) do
        case Couchdb.Ecto.Fetchers.all(__MODULE__, schema_or_map, ddoc_view, fetch_opts, processor_opts) do
          {:ok, data} -> data
          error -> error
        end
      end

      def multiple_all(schema_or_map, ddoc_view, queries, processor_opts \\ []) do
        Couchdb.Ecto.Fetchers.multiple_all(__MODULE__, schema_or_map, ddoc_view, queries, processor_opts)
      end

      def find(schema_or_map, query, processor_opts \\ []) do
        Couchdb.Ecto.Fetchers.find(__MODULE__, schema_or_map, query, processor_opts)
      end

      def search(schema_or_map, ddoc_view, query, processor_opts \\ []) do
        Couchdb.Ecto.Fetchers.search(__MODULE__, schema_or_map, ddoc_view, query, processor_opts)
      end

    end
  end

end
