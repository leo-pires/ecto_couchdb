defmodule Couchdb.Ecto.Fetchers do
  import Couchdb.Ecto.Helpers
  alias Couchdb.Ecto.ResultProcessor

  @type ddoc_view :: {String.t, String.t} | String.t
  @type fetch_options :: term()
  @type preload_options :: atom() | [atom()] | [preload: preload_options]
  # TODO: refactor return_keys to mimics db return
  @type processor_options :: [preload: preload_options, return_keys: boolean()]


  @spec get(Ecto.Repo.t, Ecto.Schema.t, String.t, fetch_options) :: {:ok, Ecto.Schema.t() | nil} | {:error, term()}
  def get(repo, schema, id, fetch_opts \\ [], processor_opts \\ []) do
    case repo |> db_from_repo |> ICouch.open_doc(id, fetch_opts) do
      {:ok, doc} -> {:ok, ResultProcessor.process_result(:get, doc, repo, schema, processor_opts)}
      {:error, :not_found} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec one(Ecto.Repo.t, Ecto.Schema.t, ddoc_view, fetch_options) :: {:ok, Ecto.Schema.t() | nil} | {:error, term()}
  def one(repo, schema, ddoc_view, fetch_opts \\ [], processor_opts \\ []) do
    case all(repo, schema, ddoc_view, fetch_opts, processor_opts) do
      {:ok, []} -> {:ok, nil}
      {:ok, [data]} -> {:ok, data}
      {:ok, _} -> {:error, :too_many_results}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec all(Ecto.Repo.t, Ecto.Schema.t, ddoc_view, fetch_options) :: {:ok, [Ecto.Schema.t()]} | {:error, term()}
  def all(repo, schema, ddoc_view, fetch_opts \\ [], processor_opts \\ []) do
    {ddoc, view_name} = split_ddoc_view(schema, ddoc_view)
    case repo |> view_from_repo(ddoc, view_name, fetch_opts) |> ICouch.View.fetch do
      {:ok, view} -> {:ok, ResultProcessor.process_result(:all, view, repo, schema, processor_opts)}
      {:error, reason} -> {:error, reason}
    end
  end

  # TODO: fix typespec
  @spec multiple_all(Ecto.Repo.t, Ecto.Schema.t, list(fetch_options), processor_options) :: term()
  def multiple_all(repo, schema, ddoc_view, queries, processor_opts \\ []) do
    {ddoc, view_name} = split_ddoc_view(schema, ddoc_view)
    url = "_design/#{ddoc}/_view/#{view_name}/queries"
    body = %{queries: queries}
    with {:ok, response} <- repo |> db_from_repo |> ICouch.DB.send_req(url, :post, body),
         {:ok, result} <- coerce_multiple_all_response(response)
    do
      {:ok, ResultProcessor.process_result(:multiple_all, result, repo, schema, processor_opts)}
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
            _other -> {:halt, {:error, :could_not_parse_docs}}
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

  @spec find(Ecto.Repo.t, Ecto.Schema.t, fetch_options, processor_options) :: {:ok, %{docs: [Ecto.Schema.t()], bookmark: String.t, warning: term()}} | {:error, term()}
  def find(repo, schema, query, processor_opts \\ []) do
    query_as_map = query |> Enum.into(%{})
    with {:ok, response} <- repo |> db_from_repo |> ICouch.DB.send_req("_find", :post, query_as_map),
         {:ok, result} <- coerce_find_response(response)
    do
      {:ok, ResultProcessor.process_result(:find, result, repo, schema, processor_opts)}
    else
      {:error, reason} -> {:error, reason}
    end
  end
  defp coerce_find_response(%{"docs" => raw_docs, "bookmark" => bookmark} = response) do
    raw_docs |> Enum.reduce_while([], fn raw_doc, acc ->
      case ICouch.Document.from_api(raw_doc) do
        {:ok, doc} -> {:cont, [doc | acc]}
        _other -> {:halt, {:error, :could_not_parse_docs}}
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

  @spec search(Ecto.Repo.t, Ecto.Schema.t, ddoc_view, fetch_options, processor_options) :: {:ok, %{docs: [Ecto.Schema.t()], bookmark: String.t, total_rows: non_neg_integer()}} | {:error, term()}
  def search(repo, schema, ddoc_view, query, processor_opts \\ []) do
    {ddoc, view_name} = split_ddoc_view(schema, ddoc_view)
    search_endpoint = "_design/#{ddoc}/_search/#{view_name}"
    with {:ok, response} <- repo |> db_from_repo |> ICouch.DB.send_req(search_endpoint, :post, query),
         {:ok, result} <- coerce_search_response(response)
    do
      {:ok, ResultProcessor.process_result(:search, result, repo, schema, processor_opts)}
    else
      {:error, reason} -> {:error, reason}
    end
  end
  defp coerce_search_response(%{"rows" => raw_rows, "bookmark" => bookmark, "total_rows" => total_rows}) do
    raw_rows |> Enum.reduce_while([], fn %{"doc" => raw_doc}, acc ->
      case ICouch.Document.from_api(raw_doc) do
        {:ok, doc} -> {:cont, [doc | acc]}
        _other -> {:halt, {:error, :could_not_parse_docs}}
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
