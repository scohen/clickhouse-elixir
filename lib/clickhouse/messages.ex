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
      alias Clickhouse.Settings
      require Protocol.Client

      def encode(query, client_info, settings, compression, query_id \\ "") do
        [
          Binary.encode(:varint, Protocol.Client.query()),
          Binary.encode(:string, query_id),
          ClientInfo.to_iodata(client_info, ClientInfo.initial_query()),
          Settings.to_iodata(settings),
          # 2 is query processing state complete
          Binary.encode(:varint, 2),
          Binary.encode(:varint, compression),
          Binary.encode(:string, query)
        ]
      end
    end
  end

  defmodule Server do
    defmodule Hello do
      @compile {:bin_opt_info, true}
      use Clickhouse.PacketParser,
        server_name: :string,
        server_version_major: :varint,
        server_version_minor: :varint,
        server_revision: :varint,
        server_timezone: :string,      # >= 54058
        server_display_name: :string,  # >= 54372
        server_version_patch: :varint  # >= 54401
    end

    defmodule Exception do
      @compile {:bin_opt_info, true}
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

    defmodule Pong do
      defstruct [:value]
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

    def decode(_), do: {:error, :incomplete}
  end
end
