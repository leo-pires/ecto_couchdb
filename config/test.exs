use Mix.Config

config :couchdb_adapter, Repo,
  adapter: CouchdbAdapter,
  protocol: "http",
  hostname: "localhost",
  port: 5984,
  username: "admin",
  password: "admin",
  database: "ecto_couchdb_test",
  pool_size: 5,
  pool_timeout: 2000
