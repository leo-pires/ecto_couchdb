use Mix.Config

config :ecto_couchdb, Repo,
  adapter: Couchdb.Ecto,
  protocol: "http",
  hostname: "localhost",
  port: 5984,
  username: "admin",
  password: "admin",
  database: "ecto_couchdb_test",
  pool_size: 5,
  pool_timeout: 2000
