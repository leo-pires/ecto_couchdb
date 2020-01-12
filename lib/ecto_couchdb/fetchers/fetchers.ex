# TODO: create type for raise

defmodule Couchdb.Ecto.Fetchers do

  alias Couchdb.Ecto.ResultProcessor


  @type preload :: atom() | [atom()] | keyword(preload)
  @type get_options :: [preload: preload]
  @type fetch_options :: [preload: preload, return_keys: boolean(), as_map: (boolean() | :raw)]
  @type find_options :: [preload: preload, as_map: boolean()]

  @spec get(Ecto.Repo.t, Ecto.Schema.t, String.t, get_options) :: {:ok, Ecto.Schema.t() | nil} | {:error, term()} | no_return()
  def get(repo, schema, id, opts \\ []) do
    {processor_opts, fetch_opts} = opts |> split_fetch_options
    with {:ok, doc} <- repo |> Couchdb.Ecto.db_from_repo |> ICouch.open_doc(id, fetch_opts)
    do
      {:ok, ResultProcessor.process_result(:get, doc, repo, schema, processor_opts)}
    else
      {:error, :not_found} -> {:ok, nil}
      {:error, :timeout} -> {:error, :timeout}
      {:error, reason} -> raise "Could not get (#{inspect(reason)})"
    end
  end

  @spec fetch_one(Ecto.Repo.t, Ecto.Schema.t, {atom(), atom()} | atom(), fetch_options) :: {:ok, Ecto.Schema.t() | nil} | {:error, term()} | no_return()
  def fetch_one(repo, schema, view, opts \\ []) do
    case fetch_all(repo, schema, view, opts) do
      {:ok, []} -> {:ok, nil}
      {:ok, [data]} -> {:ok, data}
      {:ok, _} -> {:ok, :many}
    end
  end

  @spec fetch_all(Ecto.Repo.t, Ecto.Schema.t, {atom(), atom()} | atom(), fetch_options) :: {:ok, [Ecto.Schema.t()]} | {:error, term()} | no_return()
  def fetch_all(repo, schema, view, opts \\ []) do
    {processor_opts, fetch_opts} = opts |> split_fetch_options
    {ddoc, view_name} = split_ddoc_view_name(schema, view)
    case repo |> Couchdb.Ecto.view_from_repo(ddoc, view_name, fetch_opts) |> ICouch.View.fetch do
      {:ok, view} -> {:ok, ResultProcessor.process_result(:fetch_all, view, repo, schema, processor_opts)}
      {:error, :not_found} -> raise "View not found (#{ddoc}, #{view_name})"
      {:error, :timeout} -> {:error, :timeout}
      {:error, reason} -> raise "Could not fetch (#{inspect(reason)})"
    end
  end

  # TODO: write spec
  @spec multiple_fetch_all(Ecto.Repo.t, Ecto.Schema.t, {atom(), atom()} | atom(), [map(), ...], fetch_options) :: {:ok, term()}
  def multiple_fetch_all(repo, schema, view, queries, opts \\ []) do
    {processor_opts, _} = opts |> split_fetch_options
    {ddoc, view_name} = split_ddoc_view_name(schema, view)
    with db_props <- Couchdb.Ecto.db_props_for(repo),
         {:ok, data} <- Couchdb.Connector.fetch_all(db_props, ddoc, view_name, queries)
    do
      {:ok, ResultProcessor.process_result(:multiple_fetch_all, data, repo, schema, processor_opts)}
    else
      # TODO: check error for multiple fetch all!
      {:error, %{"error" => "not_found", "reason" => "missing_named_view"}} -> raise "View not found (#{ddoc}, #{view_name})"
      {:error, :timeout} -> {:error, :timeout}
      {:error, reason} -> raise "Could not fetch (#{inspect(reason)})"
    end
  end

  @spec find(Ecto.Repo.t, Ecto.Schema.t, map(), find_options) :: term()
  def find(repo, schema, query, opts \\ []) do
    {processor_opts, _} = opts |> split_fetch_options
    with db_props <- Couchdb.Ecto.db_props_for(repo),
         {:ok, query} <- fields_for_query(schema, query),
         {:ok, data} <- Couchdb.Connector.find(db_props, query)
    do
      {:ok, ResultProcessor.process_result(:find, data, repo, schema, processor_opts)}
    else
      # TODO: check error for find!
      error -> error
    end
  end

  defp fields_for_query(_, %{fields: _, fields_except: _} = _query) do
    raise "Cannot use both fields and fields_except"
  end
  defp fields_for_query(schema, %{fields_except: fields_except} = query) do
    fields = schema.__schema__(:fields) |> Enum.map(&(Atom.to_string(&1)))
    query =
      query
      |> Map.put(:fields, fields -- fields_except)
      |> Map.drop([:fields_except])
    {:ok, query}
  end
  defp fields_for_query(_, query), do: {:ok, query}

  defp split_fetch_options(opts), do: opts |> Keyword.split([:preload, :as_map, :return_keys])

  defp split_ddoc_view_name(_, {ddoc, view_name}), do: {ddoc, view_name}
  defp split_ddoc_view_name(schema, view_name), do: {Couchdb.Ecto.ddoc_name(schema), view_name}

end
