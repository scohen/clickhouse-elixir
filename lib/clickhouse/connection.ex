defmodule Clickhouse.Connection do
  defstruct conn: nil, hello: nil, client_info: nil, buffer: <<>>, decoded_packets: []

  alias Clickhouse.{
    ClientInfo,
    Messages,
    Protocol,
    Queries.Insert,
    Queries.Select,
    Queries.Modify
  }

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

  def handle_execute(%Insert{} = insert, params, _opts, %{conn: conn} = state) do
    alias Clickhouse.Block
    IO.puts("insert")

    query_packet =
      Messages.Client.Query.encode(
        insert.statement,
        state.client_info,
        nil,
        Clickhouse.Protocol.Compression.disabled()
      )

    IO.inspect(params, label: "params")

    block_packet = Messages.Client.Data.encode(Block.new())

    :ok = :gen_tcp.send(conn, [query_packet, block_packet])

    {:ok, sample_block, new_state} = receive_packet(state)

    data_iodata =
      Block.new()
      |> Map.put(:data, params)
      |> Messages.Client.Data.encode()
      |> IO.inspect()

    :ok = :gen_tcp.send(conn, data_iodata)
    {:ok, [], new_state} = packet_stream(new_state)

    IO.inspect(sample_block, label: "Insert")

    {:ok, insert, :ok, new_state}
  end

  def handle_execute(query, _params, _opts, %{conn: conn} = state) do
    alias Clickhouse.Block

    IO.puts("exec")

    query_packet =
      Messages.Client.Query.encode(
        query.statement,
        state.client_info,
        nil,
        Clickhouse.Protocol.Compression.disabled()
      )

    block_packet = Messages.Client.Data.encode(Block.new())

    :ok = :gen_tcp.send(conn, [query_packet, block_packet])

    {:ok, packets, new_state} = packet_stream(state)

    IO.inspect(packets, label: "stream packets")

    case packets do
      [] ->
        {:ok, query, :ok, new_state}

      messages ->
        case to_response(messages) do
          {:ok, response} ->
            {:ok, query, response, new_state}

          {:error, _} = err ->
            {:ok, query, err, new_state}
        end
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
         {:ok, message, new_state} <- receive_packet(state) do
      IO.inspect(message)
      {:ok, new_state}
    end
  end

  defp do_handshake(conn, database, username, password) do
    hello_packet = Messages.Client.Hello.encode({1, 0, 0}, database, username, password)
    state = %__MODULE__{conn: conn, buffer: <<>>}

    with :ok <- :gen_tcp.send(conn, hello_packet),
         {:ok, [%Messages.Server.Hello{} = hello], new_state} <- packet_stream(state) do
      state = %{new_state | hello: hello, client_info: ClientInfo.new()}
      {:ok, state}
    else
      other ->
        IO.inspect(other, "Failed")
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

  @valid_packets [
    Messages.Server.Data,
    Messages.Server.Progress
  ]
  def packet_stream(state) do
    packet_stream(:cont, state, [])
  end

  def packet_stream(:halt, new_state, packets) do
    {:ok, Enum.reverse(packets), new_state}
  end

  def packet_stream(:cont, state, accum) do
    case receive_packet(state) do
      {:ok, %packet_module{} = packet, new_state} when packet_module in @valid_packets ->
        IO.puts("Adding #{inspect(packet)} to the stream")
        packet_stream(:cont, new_state, [packet | accum])

      {:ok, %Messages.Server.Exception{} = packet, new_state} ->
        packet_stream(:halt, new_state, [packet | accum])

      {:ok, %Messages.Server.EndOfStream{}, new_state} ->
        packet_stream(:halt, new_state, accum)

      {:ok, %Messages.Server.Hello{} = packet, new_state} ->
        packet_stream(:halt, new_state, [packet | accum])

      {:ok, _ignored_packet, new_state} ->
        packet_stream(:cont, new_state, accum)
    end
  end

  defp receive_packet(%{decoded_packets: [], conn: conn, buffer: buffer} = state) do
    case Messages.Server.decode(buffer) do
      {:ok, %packet_module{} = packet, remainder} ->
        IO.puts("Got it #{packet_module}")
        {:ok, packet, %{state | buffer: remainder}}

      {:error, :incomplete} ->
        {:ok, data} = :gen_tcp.recv(conn, 0)
        IO.puts("incomplete")
        receive_packet(%{state | buffer: buffer <> data})
    end
  end

  # defp receive_packet(%{decoded_packets: [packet | rest]} = state) do
  #   {:ok, packet, %{state | decoded_packets: rest}}
  # end

  # defp receive_packet(conn, decode_fn, acc) do
  #   case decode_fn.(acc) do
  #     {:ok, _message, _remainder} = success ->
  #       success

  #     {:error, :incomplete} ->
  #       case :gen_tcp.recv(conn, 0) do
  #         {:ok, data} ->
  #           all_data = acc <> data

  #           case decode_fn.(all_data) do
  #             {:ok, _message, _buffer} = success ->
  #               success

  #             {:error, :incomplete} ->
  #               IO.inspect(acc, label: "incomplete")
  #               receive_packet(conn, decode_fn, all_data)
  #           end

  #         {:error, :timeout} = err ->
  #           err
  #       end
  #   end
  # end

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
