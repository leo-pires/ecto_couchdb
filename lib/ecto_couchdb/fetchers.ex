defmodule Couchdb.Ecto.Fetchers do
  import Couchdb.Ecto.Helpers
  alias Couchdb.Ecto.ResultProcessor

  @type preload :: atom() | [atom()] | keyword(preload)
  @type get_options :: [preload: preload]
  @type fetch_options :: [preload: preload, return_keys: boolean()]
  @type find_options :: [preload: preload]


  @spec get(Ecto.Repo.t, Ecto.Schema.t, String.t, get_options) :: {:ok, Ecto.Schema.t() | nil} | {:error, term()} | no_return()
  def get(repo, schema, id, opts \\ []) do
    {processor_opts, fetch_opts} = opts |> split_fetch_options
    case repo |> db_from_repo |> ICouch.open_doc(id, fetch_opts) do
      {:ok, doc} -> {:ok, ResultProcessor.process_result(:get, doc, repo, schema, processor_opts)}
      {:error, :not_found} -> {:ok, nil}
      {:error, :timeout} -> {:error, :timeout}
      {:error, reason} -> raise "Could not get (#{inspect(reason)})"
    end
  end

  @spec one(Ecto.Repo.t, Ecto.Schema.t, {atom(), atom()} | atom(), fetch_options) :: {:ok, Ecto.Schema.t() | nil} | {:error, term()} | no_return()
  def one(repo, schema, view, opts \\ []) do
    case all(repo, schema, view, opts) do
      {:ok, []} -> {:ok, nil}
      {:ok, [data]} -> {:ok, data}
      {:ok, _} -> {:ok, :many}
    end
  end

  @spec all(Ecto.Repo.t, Ecto.Schema.t, {atom(), atom()} | atom(), fetch_options) :: {:ok, [Ecto.Schema.t()]} | {:error, term()} | no_return()
  def all(repo, schema, view, opts \\ []) do
    {processor_opts, fetch_opts} = opts |> split_fetch_options
    {ddoc, view_name} = split_ddoc_view_name(schema, view)
    case repo |> view_from_repo(ddoc, view_name, fetch_opts) |> ICouch.View.fetch do
      {:ok, view} -> {:ok, ResultProcessor.process_result(:all, view, repo, schema, processor_opts)}
      {:error, :not_found} -> raise "View not found (#{ddoc}, #{view_name})"
      {:error, :timeout} -> {:error, :timeout}
      {:error, reason} -> raise "Could not fetch (#{inspect(reason)})"
    end
  end

  # TODO: write spec
  @spec multiple_all(Ecto.Repo.t, Ecto.Schema.t, {atom(), atom()} | atom(), [map(), ...], fetch_options) :: {:ok, term()}
  def multiple_all(repo, schema, view, queries, processor_opts \\ []) do
    {processor_opts, _} = processor_opts |> split_fetch_options
    {ddoc, view_name} = split_ddoc_view_name(schema, view)
    url = "_design/#{ddoc}/_view/#{view_name}/queries"
    body = %{queries: queries}
    with {:ok, response} <- repo |> db_from_repo |> ICouch.DB.send_req(url, :post, body),
         {:ok, result} <- coerce_multiple_all_response(response)
    do
      {:ok, ResultProcessor.process_result(:multiple_all, result, repo, schema, processor_opts)}
    else
      # TODO: check error for multiple fetch all!
      {:error, %{"error" => "not_found", "reason" => "missing_named_view"}} -> raise "View not found (#{ddoc}, #{view_name})"
      {:error, :timeout} -> {:error, :timeout}
      {:error, reason} -> raise "Could not fetch (#{inspect(reason)})"
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

  @spec find(Ecto.Repo.t, Ecto.Schema.t, find_options) :: term()
  def find(repo, schema, opts \\ []) do
    {processor_opts, fetch_opts} = opts |> split_fetch_options
    query = fetch_opts |> Enum.into(%{})
    with {:ok, response} <- repo |> db_from_repo |> ICouch.DB.send_req("_find", :post, query),
         {:ok, result} <- coerce_find_response(response)
    do
      {:ok, ResultProcessor.process_result(:find, result, repo, schema, processor_opts)}
    else
      error -> error
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
        docs = docs |> Enum.reverse
        docs_wrap =
          case response["warning"] do
            nil -> %{docs: docs, bookmark: bookmark}
            warning -> %{docs: docs, bookmark: bookmark, warning: warning}
          end
        {:ok, docs_wrap}
      error ->
        error
    end
  end

  defp split_fetch_options(opts), do: opts |> Keyword.split([:preload, :return_keys])

  defp split_ddoc_view_name(_, {ddoc, view_name}), do: {ddoc, view_name}
  defp split_ddoc_view_name(schema, view_name), do: {ddoc_name(schema), view_name}

end
