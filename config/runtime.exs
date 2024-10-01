import Config

config :redis, port: String.to_integer(System.get_env("PORT") || "4000")
config :redis, websocket_port: String.to_integer(System.get_env("WS_PORT") || "3001")
config :logger, level: :info

if config_env() == :prod do

  host = System.get_env("PHX_HOST" || "elixircache.gigalixir.com")
  port = String.to_integer(System.get_env("PORT") || "4000")

  config :redis,
      http:[
        ip: {0,0,0,0,0,0,0,0},
          port: port
      ]
end
