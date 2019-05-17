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
      alias Clickhouse.BlockInfo
      alias Clickhouse.Binary

      use Bitwise
      use Clickhouse.ColumnMacros

      defstruct data: [],
                block_info: nil,
                row_count: 0,
                column_count: 0,
                column_meta: %{}

      def decode(<<rest::binary>>) do
        {:ok, x, rest} = Binary.decode(rest, :string)
        decode_block_info_field(rest, [])
      end

      def decode_block_info_field(<<0::size(1), 1::size(7), rest::binary>>, accum) do
        decode_is_overflow(rest, accum)
      end

      def decode_block_info_field(<<0::size(1), 2::size(7), rest::binary>>, accum) do
        decode_bucket_num(rest, accum)
      end

      def decode_block_info_field(<<_unused::size(8), rest::binary>>, accum) do
        decode_column_count(rest, 0, 0, fix_up_block_info(accum))
      end

      def decode_bucket_num(<<bucket_num::little-signed-size(32), rest::binary>>, accum) do
        decode_block_info_field(rest, [{:bucket_num, bucket_num} | accum])
      end

      def decode_is_overflow(<<0::little-unsigned-size(8), rest::binary>>, accum) do
        decode_block_info_field(rest, [{:is_overflow, false} | accum])
      end

      def decode_is_overflow(<<1::little-unsigned-size(8), rest::binary>>, accum) do
        decode_block_info_field(rest, [{:is_overflow, true} | accum])
      end

      # Data decoding

      def decode_column_count(
            <<0::size(1), chunk::size(7), rest::binary>>,
            count,
            shift,
            %__MODULE__{} = data
          ) do
        count = count ||| chunk <<< shift
        decode_row_count(rest, 0, 0, %{data | column_count: count})
      end

      def decode_column_count(
            <<1::size(1), chunk::size(7), rest::binary>>,
            count,
            shift,
            %__MODULE__{} = data
          ) do
        count = count ||| chunk <<< shift
        decode_column_count(rest, count, shift + 7, data)
      end

      def decode_row_count(
            <<0::size(1), chunk::size(7), rest::binary>>,
            count,
            shift,
            %__MODULE__{} = data
          ) do
        count = count ||| chunk <<< shift

        decode_columns(rest, %{data | row_count: count})
      end

      def decode_row_count(
            <<1::size(1), chunk::size(7), rest::binary>>,
            count,
            shift,
            data
          ) do
        count = count ||| chunk <<< shift

        decode_row_count(rest, count, shift + 7, data)
      end

      defp fix_up_block_info(accum) do
        block_info =
          case Enum.sort(accum) do
            [] ->
              BlockInfo.new()

            [bucket_num: bucket_num] ->
              %{BlockInfo.new() | bucket_num: bucket_num}

            [is_overflow: is_overflow] ->
              %{BlockInfo.new() | is_overflow: is_overflow}

            [bucket_num: bucket_num, is_overflow: is_overflow] ->
              %{BlockInfo.new() | bucket_num: bucket_num, is_overflow: is_overflow}
          end

        %__MODULE__{block_info: block_info}
      end

      defp decode_columns(<<rest::binary>>, %{column_count: column_count} = accum) do
        decode_column(rest, column_count, [], [], [], accum)
      end

      defp decode_column(<<rest::binary>>, 0, data, names, types, accum) do
        data = Enum.reverse(data)
        names = Enum.reverse(names)
        types = Enum.reverse(types)

        column_meta =
          names
          |> Enum.zip(types)

        accum = %{accum | data: data, column_meta: column_meta}
        {:ok, accum, rest}
      end

      defp decode_column(<<rest::binary>>, remaining_columns, data, names, types, accum) do
        decode_column_name(rest, remaining_columns, data, names, types, accum)
      end

      defp decode_column_name(
             <<0::size(1), length::size(7), column_name::binary-size(length), rest::binary>>,
             column_count,
             data,
             names,
             types,
             accum
           ) do
        decode_column_type(
          rest,
          column_count,
          data,
          [column_name | names],
          types,
          accum
        )
      end

      defp decode_column_type(
             <<0::size(1), length::size(7), type_name::binary-size(length), rest::binary>>,
             column_count,
             data,
             names,
             types,
             accum
           ) do
        types = [type_name | types]

        case accum do
          %{row_count: 0} ->
            decode_column(rest, column_count - 1, data, names, [type_name | types], accum)

          %{row_count: row_count} ->
            data = [[] | data]

            case type_name do
              "Int64" ->
                decode_i64_columns(rest, column_count, row_count, data, names, types, accum)

              "Int32" ->
                decode_i32_columns(rest, column_count, row_count, data, names, types, accum)

              "Int16" ->
                decode_i16_columns(rest, column_count, row_count, data, names, types, accum)

              "Int8" ->
                decode_i8_columns(rest, column_count, row_count, data, names, types, accum)

              "UInt64" ->
                decode_u64_columns(rest, column_count, row_count, data, names, types, accum)

              "UInt32" ->
                decode_u32_columns(rest, column_count, row_count, data, names, types, accum)

              "UInt16" ->
                decode_u16_columns(rest, column_count, row_count, data, names, types, accum)

              "UInt8" ->
                decode_u8_columns(rest, column_count, row_count, data, names, types, accum)

              "Float64" ->
                decode_f64_columns(rest, column_count, row_count, data, names, types, accum)

              "Float32" ->
                decode_f32_columns(rest, column_count, row_count, data, names, types, accum)

              "Float16" ->
                decode_f16_columns(rest, column_count, row_count, data, names, types, accum)

              "String" ->
                decode_string_columns(rest, column_count, row_count, data, names, types, accum)

              "Date" ->
                decode_date_columns(rest, column_count, row_count, data, names, types, accum)

              "DateTime" ->
                decode_datetime_columns(rest, column_count, row_count, data, names, types, accum)

              "" ->
                decode_column(rest, column_count - 1, [nil | data], names, types, accum)
            end
        end
      end

      defp decode_column_type(_, _, _, _, _, _) do
        {:error, :incomplete}
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
      Messages.Server.ProfileInfo.decode(rest)
    end

    def decode(<<data(), rest::binary>>) do
      Messages.Server.Data.decode(rest)
    end

    def decode(<<end_of_stream(), rest::binary>>) do
      {:ok, %Messages.Server.EndOfStream{}, rest}
    end

    def decode(<<progress(), rest::binary>>) do
      Messages.Server.Progress.decode(rest)
    end

    def decode(_), do: {:error, :incomplete}
  end
end
