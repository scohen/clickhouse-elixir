defmodule Clickhouse.Protocol do
  defmodule Client do
    defmacro hello, do: 0

    defmacro query, do: 1

    defmacro data, do: 2

    defmacro cancel, do: 3

    defmacro ping, do: 4

    defmacro table_status, do: 5
  end

  defmodule Server do
    defmacro hello, do: 0

    # A block of data (compressed or not).
    defmacro data, do: 1

    # The exception during query execution.
    defmacro exception, do: 2

    # Query execution progress: rows read, bytes read.
    defmacro progress, do: 3

    # Ping response
    defmacro pong, do: 4

    # All packets were transmitted
    defmacro end_of_stream, do: 5

    # Packet with profiling info.
    defmacro profile_info, do: 6

    # A block with totals (compressed or not).
    defmacro totals, do: 7

    # A block with minimums and maximums (compressed or not).
    defmacro extremes, do: 8

    # A response to TablesStatus request.
    defmacro tables_status_response, do: 9

    # System logs of the query execution
    defmacro log, do: 10
  end
end
