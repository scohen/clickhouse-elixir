defmodule Clickhouse.Connection do
  defstruct conn: nil, hello: nil, client_info: nil, buffer: <<>>

  alias Clickhouse.{ClientInfo, Messages, Protocol}

  require Protocol.Server
  use DBConnection

  def checkin({:ok, state}) do
    {:ok, state}
  end

  def checkout(state) do
    {:ok, state}
  end

  def connect(opts) do
    host = opts |> Keyword.get(:host, "localhost") |> String.to_charlist()
    port = Keyword.get(opts, :port, 9000)
    database = Keyword.get(opts, :database, "sereno_dev")
    username = Keyword.get(opts, :username, "")
    password = Keyword.get(opts, :password, "")

    with {:ok, conn_pid} <-
           :gen_tcp.connect(host, port, active: false, mode: :binary, nodelay: true),
         {:ok, state} <- do_handshake(conn_pid, database, username, password) do
      {:ok, state}
    else
      error ->
        error
    end
  end

  def disconnect(_err, state) do
    case state do
      %{conn: nil} ->
        :ok

      %{conn: conn} ->
        :gen_tcp.close(conn)
    end

    :ok
  end

  def handle_begin(_opts, state) do
    IO.puts("begin")
    {:ok, state}
  end

  def handle_close(_query, _opts, state) do
    IO.puts("close")
    {:ok, nil, state}
  end

  def handle_commit(_opts, state) do
    IO.puts("commit")
    {:ok, state}
  end

  def handle_deallocate(_query, _cursor, _opts, state) do
    IO.puts("deallocate")
    {:ok, state}
  end

  def handle_declare(_query, _params, _opts, state) do
    IO.puts("declare")
    {:ok, state}
  end

  def stream_decider(%Clickhouse.Queries.Select{}) do
    fn
      <<Protocol.Server.data(), _::binary>> = data when byte_size(data) >= 5 ->
        :binary.part(data, {byte_size(data), -5}) == <<255, 0, 0, 0, 5>>

      <<Protocol.Server.end_of_stream()>> ->
        true

      <<Protocol.Server.exception(), _::bits>> = data ->
        case Messages.Server.decode(data) do
          {:ok, _, _} -> true
          _ -> false
        end

      _ ->
        false
    end
  end

  def stream_decider(%Clickhouse.Queries.Insert{}) do
    fn data ->
      case Messages.Server.decode(data) do
        {:ok, _, _} ->
          true

        _ ->
          false
      end
    end
  end

  def stream_decider(_) do
    fn
      <<Protocol.Server.end_of_stream()>> ->
        true

      <<Protocol.Server.exception(), _::bits>> = data ->
        case Messages.Server.decode(data) do
          {:ok, _, _} -> true
          _ -> false
        end

      _ ->
        false
    end
  end

  def handle_execute(query, _params, _opts, %{conn: conn} = state) do
    alias Clickhouse.Block

    query_packet =
      Messages.Client.Query.encode(
        query.statement,
        state.client_info,
        nil,
        Clickhouse.Protocol.Compression.disabled()
      )

    block_packet = Messages.Client.Data.encode(Block.new())

    :ok = :gen_tcp.send(conn, [query_packet, block_packet])

    stream_ended? = stream_decider(query)

    init_fn = fn ->
      {:ok, bytes} = receive_until(conn, stream_ended?, state.buffer)
      bytes
    end

    parse = fn bytes_to_parse ->
      case Messages.Server.decode(bytes_to_parse) do
        {:ok, message, remainder} ->
          {[message], remainder}

        {:error, :incomplete} ->
          {:halt, bytes_to_parse}
      end
    end

    close = fn _ -> nil end

    {time, response} =
      :timer.tc(fn ->
        Stream.resource(init_fn, parse, close)
        |> Enum.to_list()
        |> to_response()
      end)

    IO.puts("Took #{time / 1000}ms")

    case response do
      {:error, exception} ->
        {:error, exception, state}

      :ok ->
        {:ok, query, :ok, state}

      {:ok, rows} ->
        {:ok, query, rows, state}
    end
  end

  def handle_fetch(_query, _cursor, _opts, state) do
    IO.puts("fetch")
    {:ok, nil, state}
  end

  def handle_prepare(query, _opts, state) do
    IO.puts("prepare")
    {:ok, query, state}
  end

  def handle_rollback(_opts, _state) do
    IO.puts("rollback")
  end

  def handle_status(_opts, state) do
    IO.puts("status")
    {:ok, state}
  end

  def handle_info(message, state) do
    IO.inspect(message, label: "handle_info")
    {:noreply, state}
  end

  def ping(%{conn: conn} = state) do
    IO.puts("PING")

    with :ok <- :gen_tcp.send(conn, Messages.Client.Ping.encode()),
         {:ok, message, rest} <- receive_packet(conn, &Messages.Server.decode/1, state.buffer) do
      IO.inspect(message)
      {:ok, %{state | buffer: rest}}
    end
  end

  defp do_handshake(conn, database, username, password) do
    hello_packet = Messages.Client.Hello.encode({1, 0, 0}, database, username, password)

    with :ok <- :gen_tcp.send(conn, hello_packet),
         {:ok, %Messages.Server.Hello{} = hello, buffer} <-
           receive_packet(conn, &Messages.Server.decode/1, <<>>) do
      state = %__MODULE__{conn: conn, hello: hello, buffer: buffer, client_info: ClientInfo.new()}
      {:ok, state}
    end
  end

  def receive_until(conn, decider_fn, acc) do
    IO.inspect(acc, label: "Data", limit: :infinity)

    if decider_fn.(acc) do
      {:ok, acc}
    else
      {:ok, data} = :gen_tcp.recv(conn, 0)
      receive_until(conn, decider_fn, acc <> data)
    end
  end

  defp receive_packet(conn, decode_fn, acc) do
    case decode_fn.(acc) do
      {:ok, _message, _remainder} = success ->
        success

      {:error, :incomplete} ->
        case :gen_tcp.recv(conn, 0) do
          {:ok, data} ->
            all_data = acc <> data

            case decode_fn.(all_data) do
              {:ok, _message, _buffer} = success ->
                success

              {:error, :incomplete} ->
                receive_packet(conn, decode_fn, all_data)
            end

          {:error, :timeout} = err ->
            err
        end
    end
  end

  defp to_response([%Messages.Server.EndOfStream{}]) do
    :ok
  end

  defp to_response([%Messages.Server.Exception{} = ex]) do
    {:error, ex}
  end

  defp to_response([]) do
    {:ok, %{rows: [], row_count: 0, columns: []}}
  end

  defp to_response(messages) do
    metadata = Enum.find(messages, &match?(%{row_count: 0}, &1))

    rows =
      for %Messages.Server.Data{data: data, row_count: row_count} <- messages,
          row_count > 0 do
        data
        |> Enum.chunk_every(row_count)
        |> Enum.zip()
      end
      |> List.flatten()

    row_count =
      for %Messages.Server.Data{row_count: count} <- messages do
        count
      end
      |> Enum.sum()

    columns = Enum.map(metadata.column_meta, &elem(&1, 0))

    {:ok, %{rows: rows, row_count: row_count, columns: columns}}
  end
end
