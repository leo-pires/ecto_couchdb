defmodule Couchdb.Ecto.RepoFetchers do

  defmacro __using__(_opts) do
    quote do

      alias Couchdb.Ecto.Fetchers


      @spec get(schema_map :: Fetchers.schema_map_fun(), doc_id :: String.t(), fetch_opts :: Fetchers.fetch_options(), processor_opts :: Fetchers.processor_options()) :: Fetchers.doc_result() | nil | {:error, any()}
      def get(schema_or_map, doc_id, fetch_opts \\ [], processor_opts \\ []) do
        case Couchdb.Ecto.Fetchers.get(__MODULE__, schema_or_map, doc_id, fetch_opts, processor_opts) do
          {:ok, data} -> data
          {:error, reason} -> {:error, reason}
        end
      end

      @spec get!(schema_map :: Fetchers.schema_map_fun(), doc_id :: String.t(), fetch_opts :: Fetchers.fetch_options(), processor_opts :: Fetchers.processor_options()) :: Fetchers.doc_result()
      def get!(schema_or_map, doc_id, fetch_opts \\ [], processor_opts \\ []) do
        case Couchdb.Ecto.Fetchers.get(__MODULE__, schema_or_map, doc_id, fetch_opts, processor_opts) do
          {:ok, nil} -> raise Ecto.NoResultsError
          {:ok, data} -> data
          {:error, reason} -> raise "Unknown error #{reason}" # TODO: what kind of error to return instead?
        end
      end

      @spec one(schema_map :: Fetchers.schema_map_fun(), ddoc_view :: Fetchers.ddoc_view(), fetch_opts :: Fetchers.fetch_options(), processor_opts :: Fetchers.processor_options()) :: Fetchers.doc_result() | nil | {:error, :too_many_results | any()}
      def one(schema_or_map, ddoc_view, fetch_opts \\ [], processor_opts \\ []) do
        case Couchdb.Ecto.Fetchers.one(__MODULE__, schema_or_map, ddoc_view, fetch_opts, processor_opts) do
          {:ok, data} -> data
          {:error, :too_many_results} -> {:error, :too_many_results}
          {:error, reason} -> {:error, reason}
        end
      end

      @spec one!(schema_map :: Fetchers.schema_map_fun(), ddoc_view :: Fetchers.ddoc_view(), fetch_opts :: Fetchers.fetch_options(), processor_opts :: Fetchers.processor_options()) :: Fetchers.doc_result()
      def one!(schema_or_map, ddoc_view, fetch_opts \\ [], processor_opts \\ []) do
        case Couchdb.Ecto.Fetchers.one(__MODULE__, schema_or_map, ddoc_view, fetch_opts, processor_opts) do
          {:ok, nil} -> raise Ecto.NoResultsError
          {:ok, data} -> data
          {:error, :too_many_results} -> raise Ecto.MultipleResultsError
          {:error, reason} -> raise "Unknown error #{reason}" # TODO: what kind of error to return instead?
        end
      end

      @spec all(schema_map :: Fetchers.schema_map_fun, ddoc_view :: Fetchers.ddoc_view(), fetch_opts :: Fetchers.fetch_options(), processor_opts :: Fetchers.processor_options()) :: {:ok, list(Fetchers.doc_result())} | {:error, any()}
      def all(schema_or_map, ddoc_view, fetch_opts \\ [], processor_opts \\ []) do
        case Couchdb.Ecto.Fetchers.all(__MODULE__, schema_or_map, ddoc_view, fetch_opts, processor_opts) do
          {:ok, data} -> data
          {:error, reason} -> {:error, reason}
        end
      end

      @spec multiple_all(schema_map :: Fetchers.schema_map_fun(), ddoc_view :: Fetchers.ddoc_view(), queries :: map(), processor_opts :: Fetchers.processor_options()) :: {:ok, list((Fetchers.doc_result()))} | {:error, any()}
      def multiple_all(schema_or_map, ddoc_view, queries, processor_opts \\ []) do
        Couchdb.Ecto.Fetchers.multiple_all(__MODULE__, schema_or_map, ddoc_view, queries, processor_opts)
      end

      @spec find(schema_map :: Fetchers.schema_map_fun(), query :: map(), processor_opts :: Fetchers.processor_options()) :: {:ok, Fetchers.find_result()} | {:error, any()}
      def find(schema_or_map, query, processor_opts \\ []) do
        Couchdb.Ecto.Fetchers.find(__MODULE__, schema_or_map, query, processor_opts)
      end

      @spec search(schema_map :: Fetchers.schema_map_fun(), ddoc_view :: Fetchers.ddoc_view(), query :: map(), processor_opts :: Fetchers.processor_options()) :: {:ok, Fetchers.search_result()} | {:error, any()}
      def search(schema_or_map, ddoc_view, query, processor_opts \\ []) do
        Couchdb.Ecto.Fetchers.search(__MODULE__, schema_or_map, ddoc_view, query, processor_opts)
      end

    end
  end

end
