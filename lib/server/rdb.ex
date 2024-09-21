defmodule Server.Rdb do
  require Logger

  defmodule Error do
    defexception [:message]
  end

  @aux <<0xFA>>
  @resize_db <<0xFB>>
  @expire_time_ms <<0xFC>>
  @expire_time <<0xFD>>
  @select_db <<0xFE>>
  @eof <<0xFF>>

  def parse_rdb_file(data) do
    data
    |> parse_header()
    |> parse_section()
  end

  defp parse_header(data) do
    case data do
      <<"REDIS", _version::binary-size(4), rest::binary>> -> rest
      _ -> raise(Error, message: "RDB file does not start with \"REDIS\" magic string")
    end
  end

  defp parse_section(data, kv_store \\ %{}) do
    case data do
      <<@aux, rest::binary>> ->
        {_, rest} = parse_string(rest)
        {_, rest} = parse_string(rest)
        parse_section(rest, kv_store)
      <<@select_db, _byte, rest::binary>> ->
        parse_section(rest, kv_store)
      <<@resize_db, rest::binary>> ->
        {_, rest} = parse_length(rest)
        {_, rest} = parse_length(rest)
        parse_section(rest, kv_store)
      <<@expire_time, rest::binary>> ->
        {expires_at, rest} = parse_time(rest, :second)
        {key, value, rest} = parse_key_value_pair(rest)
        parse_section(rest, Map.put(kv_store, key, {value, expires_at}))
      <<@expire_time_ms, rest::binary>> ->
        {expires_at, rest} = parse_time(rest, :millisecond)
        {key, value, rest} = parse_key_value_pair(rest)
        parse_section(rest, Map.put(kv_store, key, {value, expires_at}))
      <<@eof, _rest::binary>> ->
        kv_store

      data ->
        {key, value, rest} = parse_key_value_pair(data)
        parse_section(rest, Map.put(kv_store, key, value))
    end
  end

  def parse_length(data) do
    case data do
      <<0b00::2, length::6, rest::binary>> ->
        {length, rest}
      <<0b01::2, length::14, rest::binary>> ->
        {length, rest}
      <<0b10::2, _::6, length::32, rest::binary>> ->
        {length, rest}
      <<0b11::2, format::6, rest::binary>> ->
        length =
          case format do
            0 -> 1
            1 -> 2
            2 -> 4
            _ -> raise(Error, message: "Special format length encoding not yet implemented")
          end
        {length, rest}
    end
  end

  defp parse_string(data) do
    {length, rest} = parse_length(data)
    <<string::binary-size(length), rest::binary>> = rest
    {string, rest}
  end

  defp parse_key_value_pair(data) do
    <<value_type, rest::binary>> = data
    {key, rest} = parse_string(rest)
    {value, rest} =
      case value_type do
        0 -> parse_string(rest)
        _ -> raise(Error, message: "Value type not yet implemented: #{inspect(value_type)}")
      end
    {key, value, rest}
  end

  defp parse_time(data, unit) do
    {timestamp, rest} =
      case unit do
        :second ->
          <<timestamp::little-integer-size(32), rest::binary>> = data
          {timestamp, rest}
        :millisecond ->
          <<timestamp::little-integer-size(64), rest::binary>> = data
          {timestamp, rest}
      end
    case DateTime.from_unix(timestamp, unit) do
      {:ok, timestamp} -> {timestamp, rest}
      {:error, reason} -> raise(Error, "Could not parse timestamp: #{reason}")
    end
  end
end
