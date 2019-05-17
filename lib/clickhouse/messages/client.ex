defmodule Clickhouse.Messages.Client do
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
