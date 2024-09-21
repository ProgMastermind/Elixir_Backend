defmodule Server.RdbStore do
  use GenServer
  require Logger

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def init(_) do
    {:ok, %{}}
  end

  def load_rdb(file_path) do
    GenServer.call(__MODULE__, {:load_rdb, file_path})
  end

  def get_keys do
    GenServer.call(__MODULE__, :get_keys)
  end

  def get_state do
    GenServer.call(__MODULE__, :get_state)
  end

  def handle_call({:load_rdb, file_path}, _from, _state) do
    new_state = case File.read(file_path) do
      {:ok, content} ->
        Server.Rdb.parse_rdb_file(content)
      {:error, reason} ->
        Logger.warning("Could not read RDB file: #{reason}")
        %{}
    end
    {:reply, :ok, new_state}
  end

  def handle_call(:get_keys, _from, state) do
    keys = Map.keys(state)
    {:reply, keys, state}
  end

  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end
end
