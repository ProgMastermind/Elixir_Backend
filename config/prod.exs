import Config

config :logger, level: :info

config :elixircache,
  server_port: {:system, "PORT"}
