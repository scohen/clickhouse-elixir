defmodule Clickhouse.Connection do
  defstruct conn: nil, hello: nil, client_info: nil, buffer: <<>>

  alias Clickhouse.ClientInfo
  alias Clickhouse.Messages

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

    init_fn = fn ->
      state.buffer
    end

    receive_packet_fn = fn unprocessed_bytes ->
      IO.inspect(unprocessed_bytes, label: "remainder")

      case receive_packet(conn, &Messages.Server.decode/1, unprocessed_bytes) do
        {:ok, %Messages.Server.EndOfStream{}, rest} ->
          {:halt, rest}

        {:ok, message, rest} ->
          IO.inspect(message, label: "emitting")
          {[message], rest}

        {:error, :incomplete} ->
          IO.inspect("incomplete")
          {[], unprocessed_bytes}

        {:error, :timeout} ->
          IO.inspect("timeout")
          {:halt, unprocessed_bytes}
      end
    end

    close_fn = fn _ -> nil end

    stream = Stream.resource(init_fn, receive_packet_fn, close_fn)

    messages =
      Enum.to_list(stream)
      |> IO.inspect(label: "messages")

    {:ok, query, nil, state}
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

  defp receive_packet(conn, decode_fn, acc) do
    case decode_fn.(acc) do
      {:ok, message, remainder} = success ->
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
end
