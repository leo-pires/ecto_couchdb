defmodule Couchdb.Ecto.Fetchers do
  import Couchdb.Ecto.Helpers
  alias Couchdb.Ecto.ResultProcessor

  @type schema_map_fun() :: Ecto.Schema.t() | :raw | (ICouch.Document.t() -> Ecto.Schema.t())
  @type docs_ids_revs() :: [String.t | {String.t, String.t} | {String.t, String.t, String.t} | map()]
  @type ddoc_view() :: {String.t(), String.t()} | String.t()
  @type fetch_options() :: any()
  @type preload_options() :: atom() | [atom()] | [preload: preload_options()]
  @type processor_options() :: [preload: preload_options(), return_keys: boolean()]
  @type doc_result() :: Ecto.Schema.t() | map()
  @type find_result() :: %{docs: list(doc_result()), bookmark: String.t(), warning: any()}
  @type search_result() :: %{docs: list(doc_result()), bookmark: String.t(), total_rows: non_neg_integer()}


  @spec get(repo :: Ecto.Repo.t(), schema_map :: schema_map_fun(), doc_id :: String.t(), fetch_opts :: fetch_options(), processor_opts :: processor_options()) :: {:ok, doc_result() | nil} | {:error, :missing_id | any()}
  def get(_repo, _schema_map, nil, _fetch_opts, _processor_opts) do
    {:error, :missing_id}
  end
  def get(repo, schema_map, doc_id, fetch_opts, processor_opts) do
    case repo |> db_from_repo |> ICouch.open_doc(doc_id, fetch_opts) do
      {:ok, doc} -> {:ok, ResultProcessor.process_result(:get, doc, repo, schema_map, processor_opts)}
      {:error, :not_found} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec get_many(repo :: Ecto.Repo.t(), schema_map :: schema_map_fun(), docs_ids_revs :: docs_ids_revs(), fetch_opts :: fetch_options(), processor_opts :: processor_options()) :: {:ok, list(doc_result() | nil)} | {:error, any()}
  def get_many(repo, schema_map, docs_ids_revs, fetch_opts, processor_opts) do
    case repo |> db_from_repo |> ICouch.open_docs(docs_ids_revs, fetch_opts) do
      {:ok, doc} -> {:ok, ResultProcessor.process_result(:get_many, doc, repo, schema_map, processor_opts)}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec one(repo :: Ecto.Repo.t(), schema_map :: schema_map_fun(), ddoc_view :: ddoc_view(), fetch_opts :: fetch_options(), processor_opts :: processor_options()) :: {:ok, doc_result() | nil} | {:error, :view_not_found | :too_many_results | any()}
  def one(repo, schema_map, ddoc_view, fetch_opts, processor_opts) do
    case all(repo, schema_map, ddoc_view, fetch_opts, processor_opts) do
      {:ok, []} -> {:ok, nil}
      {:ok, [data]} -> {:ok, data}
      {:ok, _} -> {:error, :too_many_results}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec all(repo :: Ecto.Repo.t, schema_map :: schema_map_fun, ddoc_view :: ddoc_view(), fetch_opts :: fetch_options(), processor_opts :: processor_options()) :: {:ok, list(doc_result())} | {:error, :view_not_found | any()}
  def all(repo, schema_map, ddoc_view, fetch_opts, processor_opts) do
    {ddoc, view_name} = split_ddoc_view(schema_map, ddoc_view)
    case repo |> db_from_repo |> view_from_db(ddoc, view_name, fetch_opts) |> ICouch.View.fetch do
      {:ok, view} -> {:ok, ResultProcessor.process_result(:all, view, repo, schema_map, processor_opts)}
      {:error, :not_found} -> {:error, :view_not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec multiple_all(repo :: Ecto.Repo.t(), schema_map :: schema_map_fun(), ddoc_view :: ddoc_view(), queries :: map(), processor_opts :: processor_options()) :: {:ok, list(list(doc_result()))} | {:error, any()}
  def multiple_all(repo, schema_map, ddoc_view, queries, processor_opts) do
    {ddoc, view_name} = split_ddoc_view(schema_map, ddoc_view)
    url = "_design/#{ddoc}/_view/#{view_name}/queries"
    body = %{queries: queries}
    with {:ok, response} <- repo |> db_from_repo |> ICouch.DB.send_req(url, :post, body),
         {:ok, result} <- coerce_multiple_all_response(response)
    do
      {:ok, ResultProcessor.process_result(:multiple_all, result, repo, schema_map, processor_opts)}
    else
      {:error, reason} -> {:error, reason}
    end
  end
  defp coerce_multiple_all_response(%{"results" => raw_results}) do
    raw_results |> Enum.reduce_while([], fn %{"rows" => raw_rows} = raw_result, results_acc ->
      raw_rows |> Enum.reduce_while([], fn
        %{"doc" => raw_doc} = raw_row, rows_acc ->
          case ICouch.Document.from_api(raw_doc) do
            {:ok, doc} -> {:cont, [Map.put(raw_row, "doc", doc) | rows_acc]}
            :error -> {:halt, {:error, :could_not_parse_docs}}
          end
        raw_row, rows_acc ->
          {:cont, [raw_row | rows_acc]}
      end)
      |> case do
        rows when is_list(rows) ->
          rows = rows |> Enum.reverse
          row_wrap =
            case raw_result do
              %{"offset" => offset, "total_rows" => total_rows} -> %{offset: offset, rows: rows, total_rows: total_rows}
              _only_rows -> %{rows: rows}
            end
          {:cont, [row_wrap | results_acc]}
        error ->
          {:halt, error}
      end
    end)
    |> case do
      results when is_list(results) -> {:ok, %{results: results |> Enum.reverse}}
      error -> error
    end
  end

  @spec find(repo :: Ecto.Repo.t(), schema_map :: schema_map_fun(), query :: map(), processor_opts :: processor_options()) :: {:ok, find_result()} | {:error, any()}
  def find(repo, schema_map, query, processor_opts) do
    query_as_map = query |> Enum.into(%{})
    with {:ok, response} <- repo |> db_from_repo |> ICouch.DB.send_req("_find", :post, query_as_map),
         {:ok, result} <- coerce_find_response(response)
    do
      {:ok, ResultProcessor.process_result(:find, result, repo, schema_map, processor_opts)}
    else
      {:error, reason} -> {:error, reason}
    end
  end
  defp coerce_find_response(%{"docs" => raw_docs, "bookmark" => bookmark} = response) do
    raw_docs |> Enum.reduce_while([], fn raw_doc, acc ->
      case ICouch.Document.from_api(raw_doc) do
        {:ok, doc} -> {:cont, [doc | acc]}
        :error -> {:halt, {:error, :could_not_parse_docs}}
      end
    end)
    |> case do
      docs when is_list(docs) ->
        reversed_docs = docs |> Enum.reverse
        docs_wrap =
          case response["warning"] do
            nil -> %{docs: reversed_docs, bookmark: bookmark}
            warning -> %{docs: reversed_docs, bookmark: bookmark, warning: warning}
          end
        {:ok, docs_wrap}
      error ->
        error
    end
  end

  @spec search(repo :: Ecto.Repo.t(), schema_map :: schema_map_fun(), ddoc_view :: ddoc_view(), query :: map(), processor_opts :: processor_options()) :: {:ok, search_result()} | {:error, any()}
  def search(repo, schema_map, ddoc_view, query, processor_opts) do
    {ddoc, view_name} = split_ddoc_view(schema_map, ddoc_view)
    search_endpoint = "_design/#{ddoc}/_search/#{view_name}"
    with {:ok, response} <- repo |> db_from_repo |> ICouch.DB.send_req(search_endpoint, :post, query),
         {:ok, result} <- coerce_search_response(response)
    do
      {:ok, ResultProcessor.process_result(:search, result, repo, schema_map, processor_opts)}
    else
      {:error, reason} -> {:error, reason}
    end
  end
  defp coerce_search_response(%{"rows" => raw_rows, "bookmark" => bookmark, "total_rows" => total_rows}) do
    raw_rows |> Enum.reduce_while([], fn %{"doc" => raw_doc}, acc ->
      case ICouch.Document.from_api(raw_doc) do
        {:ok, doc} -> {:cont, [doc | acc]}
        :error -> {:halt, {:error, :could_not_parse_docs}}
      end
    end)
    |> case do
      docs when is_list(docs) ->
        reversed_docs = docs |> Enum.reverse
        {:ok, %{docs: reversed_docs, bookmark: bookmark, total_rows: total_rows}}
      error ->
        error
    end
  end

end
