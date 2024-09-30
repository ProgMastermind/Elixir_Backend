defmodule Server.Store do
  use Agent
  require Logger

  def start_link(_args \\ []) do
    Logger.info("Starting Server.Store")
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  def update(key, value, ttl \\ nil) do
    Logger.info(
      "Updating Server.Store - Key: #{key}, Value: #{inspect(value)}, TTL: #{inspect(ttl)}"
    )

    expiry =
      case ttl do
        nil ->
          nil

        ttl when is_integer(ttl) and ttl > 0 ->
          :os.system_time(:millisecond) + ttl

        _ ->
          nil
      end

    Logger.info(
      "Storing in Server.Store - Key: #{key}, Value: #{inspect(value)}, Expiry: #{inspect(expiry)}"
    )

    Agent.update(__MODULE__, fn state ->
      new_state = Map.put(state, key, {value, expiry})
      Logger.info("Updated state: #{inspect(new_state)}")
      new_state
    end)
  end

  def get_value_or_false(key) do
    Logger.info("Getting value from Server.Store - Key: #{key}")

    result =
      Agent.get(__MODULE__, fn state ->
        case Map.get(state, key) do
          nil ->
            Logger.info("Key not found in Server.Store: #{key}")
            {:error, :not_found}

          {value, nil} ->
            Logger.info("Retrieved from Server.Store - Key: #{key}, Value: #{inspect(value)}")
            {:ok, value}

          {value, expiry} ->
            current_time = :os.system_time(:millisecond)

            if expiry > current_time do
              Logger.info("Retrieved from Server.Store - Key: #{key}, Value: #{inspect(value)}")
              {:ok, value}
            else
              Logger.info("Key expired in Server.Store: #{key}")
              {:error, :expired}
            end
        end
      end)

    Logger.info("Get result for key: #{key} - #{inspect(result)}")
    result
  end

  def clear_all do
    Agent.update(__MODULE__, fn _ -> %{} end)
  end
end
