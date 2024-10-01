import Config

config :logger, level: :info

config :redis,
  server_port: {:system, "PORT"}
