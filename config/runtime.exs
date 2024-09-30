import Config

config :redis, port: String.to_integer(System.get_env("PORT") || "4000")
