use Mix.Config

config :couchdb_adapter, Repo,
  adapter: CouchdbAdapter,
  protocol: "https",
  hostname: "192.168.25.52",
  port: 6984,
  username: "admin",
  password: "qqpK3VZ4LvkWqsbw",
  database: "ecto_couchdb_test",
  pool_size: 5,
  pool_timeout: 2000
