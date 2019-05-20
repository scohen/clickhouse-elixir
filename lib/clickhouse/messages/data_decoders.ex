defmodule Clickhouse.Messages.DataDecoders do
  defmacro __using__(_) do
    integer_decoders = [
      {:i64, 64, quote(do: decoded :: little - signed - size(64))},
      {:i32, 32, quote(do: decoded :: little - signed - size(32))},
      {:i16, 16, quote(do: decoded :: little - signed - size(16))},
      {:i8, 8, quote(do: decoded :: little - signed - size(8))},
      {:u64, 64, quote(do: decoded :: little - unsigned - size(64))},
      {:u32, 32, quote(do: decoded :: little - unsigned - size(32))},
      {:u16, 16, quote(do: decoded :: little - unsigned - size(16))},
      {:u8, 8, quote(do: decoded :: little - unsigned - size(8))},
      {:f64, 64, quote(do: decoded :: little - signed - size(64))},
      {:f32, 32, quote(do: decoded :: little - signed - size(32))},
      {:f16, 16, quote(do: decoded :: little - signed - size(16))},
      {:uf64, 64, quote(do: decoded :: little - unsigned - size(64))},
      {:uf32, 32, quote(do: decoded :: little - unsigned - size(32))},
      {:uf16, 16, quote(do: decoded :: little - unsigned - size(16))}
    ]

    integer_decoders =
      for {name, width, binary_match} <- integer_decoders do
        fn_name = :"decode_#{name}_columns"
        nullable_fn_name = :"decode_nullable_#{name}_columns"

        quote do
          defp unquote(nullable_fn_name)(
                 <<rest::binary>>,
                 column_count,
                 [],
                 data,
                 names,
                 types,
                 accum
               ) do
            decode_column(rest, column_count - 1, data, names, types, accum)
          end

          defp unquote(nullable_fn_name)(
                 <<_::size(unquote(width)), rest::binary>>,
                 column_count,
                 [true | nulls],
                 data,
                 names,
                 types,
                 accum
               ) do
            unquote(nullable_fn_name)(
              rest,
              column_count,
              nulls,
              [nil | data],
              names,
              types,
              accum
            )
          end

          defp unquote(nullable_fn_name)(
                 <<unquote(binary_match), rest::binary>>,
                 column_count,
                 [false | nulls],
                 data,
                 names,
                 types,
                 accum
               ) do
            unquote(nullable_fn_name)(
              rest,
              column_count,
              nulls,
              [decoded | data],
              names,
              types,
              accum
            )
          end

          defp unquote(fn_name)(<<rest::binary>>, column_count, 0, data, names, types, accum) do
            decode_column(rest, column_count - 1, data, names, types, accum)
          end

          defp unquote(fn_name)(
                 <<unquote(binary_match), rest::binary>>,
                 column_count,
                 row_count,
                 data,
                 names,
                 types,
                 accum
               ) do
            unquote(fn_name)(
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

    quote do
      unquote_splicing(integer_decoders)

      defp decode_nullable_date_columns(
             <<rest::binary>>,
             column_count,
             [],
             data,
             names,
             types,
             accum
           ) do
        decode_column(rest, column_count - 1, data, names, types, accum)
      end

      defp decode_nullable_date_columns(
             <<_::size(16), rest::binary>>,
             column_count,
             [true | nulls],
             data,
             names,
             types,
             accum
           ) do
        decode_nullable_date_columns(rest, column_count, nulls, [nil | data], names, types, accum)
      end

      defp decode_nullable_date_columns(
             <<days_since_epoch::little-unsigned-size(16), rest::binary>>,
             column_count,
             [false | nulls],
             data,
             names,
             types,
             accum
           ) do
        {:ok, date} = Date.new(1970, 01, 01)

        date = Date.add(date, days_since_epoch)
        data = [date | data]

        decode_nullable_date_columns(rest, column_count, nulls, data, names, types, accum)
      end

      defp decode_nullable_datetime_columns(
             <<rest::binary>>,
             column_count,
             [],
             data,
             names,
             types,
             accum
           ) do
        decode_column(rest, column_count - 1, data, names, types, accum)
      end

      defp decode_nullable_datetime_columns(
             <<_::size(32), rest::binary>>,
             column_count,
             [true | nulls],
             data,
             names,
             types,
             accum
           ) do
        decode_nullable_datetime_columns(
          rest,
          column_count,
          nulls,
          [nil | data],
          types,
          names,
          accum
        )
      end

      defp decode_nullable_datetime_columns(
             <<seconds_since_epoch::little-unsigned-size(32), rest::binary>>,
             column_count,
             [false | nulls],
             data,
             names,
             types,
             accum
           ) do
        {:ok, date_time} = NaiveDateTime.new(1970, 1, 1, 0, 0, 0)
        date_time = NaiveDateTime.add(date_time, seconds_since_epoch)

        decode_nullable_datetime_columns(
          rest,
          column_count,
          nulls,
          [date_time | data],
          types,
          names,
          accum
        )
      end

      defp decode_nullable_string_columns(
             <<rest::binary>>,
             column_count,
             [],
             data,
             names,
             types,
             accum
           ) do
        decode_column(rest, column_count - 1, data, names, types, accum)
      end

      defp decode_nullable_string_columns(
             <<rest::binary>>,
             column_count,
             row_count,
             data,
             names,
             types,
             accum
           ) do
        decode_nullable_string_length_column(
          rest,
          0,
          0,
          column_count,
          row_count,
          data,
          names,
          types,
          accum
        )
      end

      defp decode_nullable_string_length_column(
             <<0::size(1), chunk::size(7), rest::binary>>,
             length,
             shift,
             column_count,
             row_count,
             data,
             names,
             types,
             accum
           ) do
        length = length ||| chunk <<< shift

        decode_nullable_string_value_column(
          rest,
          length,
          column_count,
          row_count,
          data,
          names,
          types,
          accum
        )
      end

      defp decode_nullable_string_length_column(
             <<1::size(1), chunk::size(7), rest::binary>>,
             length,
             shift,
             column_count,
             row_count,
             data,
             names,
             types,
             accum
           ) do
        length = length ||| chunk <<< shift

        decode_nullable_string_length_column(
          rest,
          length,
          shift + 7,
          column_count,
          row_count,
          data,
          names,
          types,
          accum
        )
      end

      defp decode_nullable_string_value_column(
             <<rest::binary>>,
             length,
             column_count,
             [true | nulls],
             data,
             names,
             types,
             accum
           ) do
        <<string_value::binary-size(length), rest::binary>> = rest

        decode_nullable_string_columns(
          rest,
          column_count,
          nulls,
          [nil | data],
          names,
          types,
          accum
        )
      end

      defp decode_nullable_string_value_column(
             <<rest::binary>>,
             length,
             column_count,
             [false | nulls],
             data,
             names,
             types,
             accum
           ) do
        <<string_value::binary-size(length), rest::binary>> = rest

        decode_nullable_string_columns(
          rest,
          column_count,
          nulls,
          [string_value | data],
          names,
          types,
          accum
        )
      end

      defp decode_date_columns(<<rest::binary>>, column_count, 0, data, names, types, accum) do
        decode_column(rest, column_count - 1, data, names, types, accum)
      end

      defp decode_date_columns(
             <<days_since_epoch::little-unsigned-size(16), rest::binary>>,
             column_count,
             row_count,
             data,
             names,
             types,
             accum
           ) do
        {:ok, date} = Date.new(1970, 01, 01)

        date = Date.add(date, days_since_epoch)
        data = [date | data]
        decode_date_columns(rest, column_count, row_count - 1, data, names, types, accum)
      end

      defp decode_datetime_columns(<<rest::binary>>, column_count, 0, data, names, types, accum) do
        decode_column(rest, column_count - 1, data, names, types, accum)
      end

      defp decode_datetime_columns(
             <<seconds_since_epoch::little-unsigned-size(32), rest::binary>>,
             column_count,
             row_count,
             data,
             names,
             types,
             accum
           ) do
        {:ok, date_time} = NaiveDateTime.new(1970, 1, 1, 0, 0, 0)
        date_time = NaiveDateTime.add(date_time, seconds_since_epoch)

        decode_datetime_columns(
          rest,
          column_count,
          row_count - 1,
          [date_time | data],
          types,
          names,
          accum
        )
      end

      defp decode_string_columns(<<rest::binary>>, column_count, 0, data, names, types, accum) do
        decode_column(rest, column_count - 1, data, names, types, accum)
      end

      defp decode_string_columns(<<rest::binary>>, column_count, 0, data, names, types, accum) do
        decode_column(rest, column_count - 1, data, names, types, accum)
      end

      defp decode_string_columns(
             <<rest::binary>>,
             column_count,
             row_count,
             data,
             names,
             types,
             accum
           ) do
        decode_string_length_column(
          rest,
          0,
          0,
          column_count,
          row_count,
          data,
          names,
          types,
          accum
        )
      end

      defp decode_string_length_column(
             <<0::size(1), chunk::size(7), rest::binary>>,
             length,
             shift,
             column_count,
             row_count,
             data,
             names,
             types,
             accum
           ) do
        length = length ||| chunk <<< shift

        decode_string_value_column(
          rest,
          length,
          column_count,
          row_count,
          data,
          names,
          types,
          accum
        )
      end

      defp decode_string_length_column(
             <<1::size(1), chunk::size(7), rest::binary>>,
             length,
             shift,
             column_count,
             row_count,
             data,
             names,
             types,
             accum
           ) do
        length = length ||| chunk <<< shift

        decode_string_length_column(
          rest,
          length,
          shift + 7,
          column_count,
          row_count,
          data,
          names,
          types,
          accum
        )
      end

      defp decode_string_value_column(
             <<rest::binary>>,
             length,
             column_count,
             row_count,
             data,
             names,
             types,
             accum
           ) do
        <<string_value::binary-size(length), rest::binary>> = rest

        decode_string_columns(
          rest,
          column_count,
          row_count - 1,
          [string_value | data],
          names,
          types,
          accum
        )
      end
    end
  end
end
