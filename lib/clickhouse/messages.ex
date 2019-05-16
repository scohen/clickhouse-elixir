defmodule Clickhouse.Messages do
  defmodule Client do
    defmodule Hello do
      alias Clickhouse.Binary
      alias Clickhouse.Protocol
      require Protocol.Client

      @client_revision 54406

      def encode({major, minor, _patch}, database_name, username, password) do
        [
          Binary.encode(:varint, Protocol.Client.hello()),
          Binary.encode(:string, "Elixir Client"),
          Binary.encode(:varint, 18),
          Binary.encode(:varint, 10),
          Binary.encode(:varint, @client_revision),
          Binary.encode(:string, database_name),
          Binary.encode(:string, username),
          Binary.encode(:string, password)
        ]
      end
    end

    defmodule Ping do
      alias Clickhouse.Binary
      alias Clickhouse.Protocol
      require Protocol.Client

      def encode() do
        [Binary.encode(:varint, Protocol.Client.ping())]
      end
    end

    defmodule Query do
      alias Clickhouse.ClientInfo
      alias Clickhouse.Binary
      alias Clickhouse.Protocol
      require Protocol.Client

      def encode(query, client_info, settings, compression, query_id \\ "") do
        [
          Binary.encode(:varint, Protocol.Client.query()),
          Binary.encode(:string, query_id),
          ClientInfo.to_iodata(client_info, ClientInfo.initial_query()),
          # settings
          Binary.encode(:string, ""),
          # 2 is query processing state complete
          Binary.encode(:varint, 2),
          Binary.encode(:varint, compression),
          Binary.encode(:string, query)
        ]
      end
    end

    defmodule Data do
      alias Clickhouse.{Binary, Block, Protocol}
      require Protocol.Client

      def encode(block, table_name \\ "") do
        [
          Binary.encode(:varint, Protocol.Client.data()),
          Binary.encode(:string, table_name),
          Block.to_iodata(block)
        ]
      end
    end
  end

  defmodule Server do
    defmodule Hello do
      use Clickhouse.PacketParser,
        server_name: :string,
        server_version_major: :varint,
        server_version_minor: :varint,
        server_revision: :varint,
        server_timezone: :string,
        server_display_name: :string,
        server_version_patch: :varint
    end

    defmodule Exception do
      use Clickhouse.PacketParser,
        code: :i32,
        name: :string,
        message: :string,
        stack_trace: :string,
        has_nested: :boolean
    end

    defmodule Progress do
      use Clickhouse.PacketParser,
        rows: :varint,
        bytes: :varint,
        total_rows: :varint
    end

    defmodule ProfileInfo do
      use Clickhouse.PacketParser,
        rows: :varint,
        blocks: :varint,
        bytes: :varint,
        applied_limit: :boolean,
        rows_before_limit: :varint,
        calculated_rows_before_limit: :boolean
    end

    defmodule Data do
      @compile {:bin_opt_info, true}
      alias Clickhouse.Binary
      use Bitwise

      defstruct data: [],
                block_info: nil,
                row_count: 0,
                column_count: 0,
                column_meta: %{}

      @type_map %{
        "String" => :string,
        "Date" => :date,
        "DateTime" => :datetime,
        "Float32" => :f32,
        "Float64" => :f64,
        "Int8" => :i8,
        "Int16" => :i16,
        "Int32" => :i32,
        "Int64" => :i64,
        "UInt8" => :u8,
        "UInt16" => :u16,
        "UInt32" => :u32,
        "UInt64" => :u64
      }

      def decode(<<rest::binary>>) do
        {:ok, _temporary_table, rest} = Binary.decode(rest, :string)
        rest = strip_block_info(rest)
        {:ok, column_count, rest} = Binary.decode(rest, :varint)
        {:ok, row_count, rest} = Binary.decode(rest, :varint)
        {:ok, data, rest} = decode_columns(rest, row_count, column_count, [])
      end

      defp strip_block_info(<<0, rest::binary>>), do: rest
      defp strip_block_info(<<1, _::size(8), rest::binary>>), do: strip_block_info(rest)
      defp strip_block_info(<<2, _::size(32), rest::binary>>), do: strip_block_info(rest)

      defp decode_columns(rest, _row_count, 0, data), do: {:ok, Enum.reverse(data), rest}

      defp decode_columns(rest, row_count, n, data) do
        {:ok, name, rest} = Binary.decode(rest, :string)
        {:ok, type, rest} = Binary.decode(rest, :string)
        {:ok, values, rest} = read_column(type, row_count, rest, [])
        decode_columns(rest, row_count, n - 1, [{name, type, values} | data])
      end

      defp read_column(_type, 0, rest, values), do: {:ok, Enum.reverse(values), rest}

      defp read_column(type, n, rest, values) do
        {:ok, string, rest} = Binary.decode(rest, @type_map[type])
        read_column(type, n - 1, rest, [string | values])
      end
    end

    defmodule Pong do
      defstruct [:value]
    end

    defmodule EndOfStream do
      defstruct value: true
    end

    alias Clickhouse.Messages
    require Clickhouse.Protocol.Server
    import Clickhouse.Protocol.Server

    def decode(<<hello(), rest::binary>>) do
      Messages.Server.Hello.decode(rest)
    end

    def decode(<<exception(), rest::binary>>) do
      Messages.Server.Exception.decode(rest)
    end

    def decode(<<pong(), rest::binary>>) do
      {:ok, %Messages.Server.Pong{}, rest}
    end

    def decode(<<profile_info(), rest::binary>>) do
      IO.puts("PROF INFO")
      Messages.Server.ProfileInfo.decode(rest)
    end

    def decode(<<data(), rest::binary>>) do
      Messages.Server.Data.decode(rest)
    end

    def decode(<<end_of_stream(), rest::binary>>) do
      IO.puts("ENDO OF STREAM")
      {:ok, %Messages.Server.EndOfStream{}, rest}
    end

    def decode(<<progress(), rest::binary>>) do
      Messages.Server.Progress.decode(rest)
    end

    def decode(_), do: {:error, :incomplete}
  end
end
