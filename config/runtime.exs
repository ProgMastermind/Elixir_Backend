import Config

# config :redis, port: String.to_integer(System.get_env("PORT") || "4000")
# config :redis, websocket_port: String.to_integer(System.get_env("WS_PORT") || "3001")
# config :logger, level: :info
if config_env() == :prod do
  port = System.get_env("PORT") || "4000"
  config :redis, server_port: String.to_integer(port)
end
