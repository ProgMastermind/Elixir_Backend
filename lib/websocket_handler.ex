defmodule WebSocketHandler do
  @behaviour :cowboy_websocket
  require Logger

  def init(req, [type]) do
    {:cowboy_websocket, req, %{type: type}}
  end

  def websocket_init(state) do
    Registry.register(Registry.WebSockets, :all, {})
    # port = if state.type == :master, do: 6379, else: start_slave()
    port = if state.type == :master, do: 4000, else: start_slave()

    case :gen_tcp.connect(~c"localhost", port, [:binary, active: false]) do
      {:ok, socket} ->
        Logger.info("Connected to #{state.type} on port #{port}")
        {:ok, Map.put(state, :socket, socket)}

      {:error, reason} ->
        Logger.error("Failed to connect to #{state.type}: #{inspect(reason)}")
        {:stop, reason}
    end
  end

  def broadcast_update(message, target \\ :all) do
    Registry.dispatch(Registry.WebSockets, :all, fn entries ->
      for {pid, _} <- entries do
        send(pid, {:broadcast, message, target})
      end
    end)
  end

  # def websocket_handle({:text, message}, %{type: type} = state) do
  #   case Jason.decode(message) do
  #     {:ok, %{"type" => "COMMAND", "command" => command}} ->
  #       result =
  #         if type == :slave and is_write_command?(command) do
  #           "Error: Cannot write to slave"
  #         else
  #           send_command(state.socket, command)
  #         end

  #       {:reply,
  #        {:text, Jason.encode!(%{type: "COMMAND_RESULT", result: result, connection: type})},
  #        state}

  #     _ ->
  #       {:reply, {:text, Jason.encode!(%{error: "Invalid JSON"})}, state}
  #   end
  # end
  #
  def websocket_handle({:text, message}, %{type: type} = state) do
    case Jason.decode(message) do
      {:ok, %{"type" => "COMMAND", "command" => command}} ->
        result =
          if type == :slave and is_write_command?(command) do
            "Error: Cannot write to slave"
          else
            send_command(state.socket, command)
          end

        {:reply,
         {:text, Jason.encode!(%{type: "COMMAND_RESULT", result: result, connection: type})},
         state}

      {:ok, %{"type" => "reset_server"}} ->
        Logger.info("Triggering the reset functionality")
        Server.reset_all_states()
        broadcast_update("Server state has been reset", :all)

        {:ok, state}

      _ ->
        {:reply, {:text, Jason.encode!(%{error: "Invalid JSON"})}, state}
    end
  end

  defp is_write_command?(command) do
    write_commands = ["SET", "DEL", "INCR", "LPUSH", "RPUSH", "SADD", "ZADD"]
    String.upcase(List.first(String.split(command))) in write_commands
  end

  def websocket_info({:broadcast, message, target}, state) do
    if target == :all or target == state.type do
      {:reply,
       {:text, Jason.encode!(%{type: "BROADCAST", message: message, connection: state.type})},
       state}
    else
      {:ok, state}
    end
  end

  #

  defp send_command(socket, command) do
    parts = String.split(command)
    packed_command = Server.Protocol.pack(parts) |> IO.iodata_to_binary()

    case :gen_tcp.send(socket, packed_command) do
      :ok ->
        case :gen_tcp.recv(socket, 0) do
          {:ok, data} -> data
          {:error, reason} -> "Error receiving: #{inspect(reason)}"
        end

      {:error, reason} ->
        "Error sending: #{inspect(reason)}"
    end
  end

  defp start_slave do
    # Call your existing function to start a slave
    case Server.start_and_connect_slave() do
      {:ok, port} -> port
      {:error, _} -> raise "Failed to start slave"
    end
  end

  def terminate(_reason, _req, %{socket: socket}) do
    :gen_tcp.close(socket)
    :ok
  end
end
