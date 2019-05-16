defmodule Clickhouse.ColumnMacros do
  defmacro __using__(_) do
    quote do
      defp decode_i64_columns(<<rest::binary>>, column_count, 0, data, names, types, accum) do
        decode_column(rest, column_count - 1, data, names, types, accum)
      end

      defp decode_i64_columns(
             <<decoded::little-signed-size(64), rest::binary>>,
             column_count,
             row_count,
             data,
             names,
             types,
             accum
           ) do
        decode_i64_columns(
          rest,
          column_count,
          row_count - 1,
          [decoded | data],
          names,
          types,
          accum
        )
      end
    end
  end
end
