# TODO: create type for raise

defmodule CouchdbAdapter.Fetchers do

  alias CouchdbAdapter.ResultProcessor


  @type preload :: atom() | [atom()] | keyword(preload)
  @type get_options :: [preload: preload]
  @type fetch_options :: [preload: preload, return_keys: boolean(), as_map: (boolean() | :raw)]
  @type find_options :: [preload: preload, as_map: boolean()]

  @spec get(Ecto.Repo.t, Ecto.Schema.t, String.t) :: Ecto.Schema.t() | nil | no_return()
  def get(repo, schema, id) do
    get(repo, schema, id, [])
  end

  @spec get(Ecto.Repo.t, Ecto.Schema.t, String.t, get_options) :: Ecto.Schema.t() | nil | no_return()
  def get(repo, schema, id, opts) do
    {processor_opts, _} = opts |> split_fetch_options
    with db_props <- CouchdbAdapter.db_props_for(repo),
         {:ok, data} <- Couchdb.Connector.get(db_props, id)
    do
      ResultProcessor.process_result(:get, data, repo, schema, processor_opts)
    else
      {:error, %{"error" => "not_found"}} -> nil
      {:error, reason} -> raise "Could not get (#{inspect(reason)})"
    end
  end

  @spec fetch_one(Ecto.Repo.t, Ecto.Schema.t, atom(), fetch_options) :: term()
  def fetch_one(repo, schema, view_name, opts \\ []) do
    case fetch_all(repo, schema, view_name, opts) do
      {:error, error} -> {:error, error}
      data when length(data) == 1 -> hd(data)
      data when length(data) == 0 -> nil
      _ -> raise "Fetch returning more than one value"
    end
  end

  @spec fetch_all(Ecto.Repo.t, Ecto.Schema.t, atom(), fetch_options) :: term()
  def fetch_all(repo, schema, view_name, opts \\ []) do
    {processor_opts, query} = opts |> split_fetch_options
    query = query |> Enum.into(%{})
    ddoc = CouchdbAdapter.db_name(schema)
    with db_props <- CouchdbAdapter.db_props_for(repo),
         {:ok, data} <- Couchdb.Connector.fetch_all(db_props, ddoc, view_name, query)
    do
      ResultProcessor.process_result(:fetch_all, data, repo, schema, processor_opts)
    else
      {:error, %{"error" => "not_found"}} -> raise "View not found (#{ddoc}, #{view_name})"
      {:error, reason} -> raise "Error while fetching (#{inspect(reason)})"
    end
  end

  @spec multiple_fetch_all(Ecto.Repo.t, Ecto.Schema.t, atom(), [map(), ...], fetch_options) :: {:ok, term()}
  def multiple_fetch_all(repo, schema, view_name, queries, opts \\ []) do
    {processor_opts, _} = opts |> split_fetch_options
    ddoc = CouchdbAdapter.db_name(schema)
    with db_props <- CouchdbAdapter.db_props_for(repo),
         {:ok, data} <- Couchdb.Connector.fetch_all(db_props, ddoc, view_name, queries)
    do
      ResultProcessor.process_result(:multiple_fetch_all, data, repo, schema, processor_opts)
    else
      # TODO: check error for multiple fetch all!
      {:error, %{"error" => "not_found", "reason" => "missing_named_view"}} -> raise "View not found (#{ddoc}, #{view_name})"
      {:error, reason} -> raise "Error while fetching (#{inspect(reason)})"
    end
  end

  @spec find(Ecto.Repo.t, Ecto.Schema.t, map(), find_options) :: term()
  def find(repo, schema, query, opts \\ []) do
    {processor_opts, _} = opts |> split_fetch_options
    with db_props <- CouchdbAdapter.db_props_for(repo),
         {:ok, data} <- Couchdb.Connector.find(db_props, query)
    do
      {:ok, ResultProcessor.process_result(:find, data, repo, schema, processor_opts)}
    else
      # TODO: check error for find!
      error -> error
    end
  end

  defp split_fetch_options(opts), do: opts |> Keyword.split([:preload, :as_map, :return_keys])

end
