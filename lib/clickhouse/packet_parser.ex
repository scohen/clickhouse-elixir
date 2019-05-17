defmodule Clickhouse.PacketParser do
  defmacro __using__(fields) do
    decoders =
      for [current, next] <- Enum.chunk_every(fields, 2, 1, [{:result, :struct}]) do
        build_decoder(current, next)
      end
      |> List.flatten()

    [field_spec | _] = fields

    names = for {name, _} <- Enum.reverse(fields), do: name

    quote do
      use Bitwise

      defstruct unquote(names)

      def decode(message) when is_binary(message) do
        unquote(decoder_name(field_spec))(message, [])
      end

      defp decode_result_struct(<<rest::binary>>, accum) do
        values = Enum.zip(unquote(names), accum)
        decoded = struct(__MODULE__, values)

        {:ok, decoded, rest}
      end

      unquote_splicing(decoders)
    end
  end

  def build_decoder({_, :varint} = spec, next_spec) do
    current_name = decoder_name(spec)
    next_function_name = decoder_name(next_spec)

    quote do
      defp unquote(current_name)(<<rest::bits>>, accum) do
        unquote(current_name)(rest, 0, 0, accum)
      end

      defp unquote(current_name)(
             <<0::size(1), chunk::size(7), rest::bits>>,
             result,
             shift,
             accum
           ) do
        result = result ||| chunk <<< shift
        unquote(next_function_name)(rest, [result | accum])
      end

      defp unquote(current_name)(
             <<1::size(1), chunk::size(7), rest::bits>>,
             result,
             shift,
             accum
           ) do
        result = result ||| chunk <<< shift
        unquote(current_name)(rest, result, shift + 7, accum)
      end

      defp unquote(current_name)(_, _) do
        {:error, :incomplete}
      end
    end
  end

  def build_decoder({name, :string} = spec, next_spec) do
    current_name = decoder_name(spec)
    length_decoder_name = :"decode_#{name}_length_varint"
    value_decoder_name = :"decode_#{name}_body"
    next_function_name = decoder_name(next_spec)

    quote do
      defp unquote(current_name)(<<rest::bits>>, accum) do
        unquote(length_decoder_name)(rest, 0, 0, accum)
      end

      defp unquote(length_decoder_name)(
             <<0::size(1), chunk::size(7), rest::bits>>,
             result,
             shift,
             accum
           ) do
        result = result ||| chunk <<< shift
        unquote(value_decoder_name)(rest, result, accum)
      end

      defp unquote(length_decoder_name)(
             <<1::size(1), chunk::size(7), rest::bits>>,
             result,
             shift,
             accum
           ) do
        result = result ||| chunk <<< shift
        unquote(length_decoder_name)(rest, result, shift + 7, accum)
      end

      defp unquote(value_decoder_name)(binary, length, accum) do
        case binary do
          <<string_value::binary-size(length), rest::bits>> ->
            unquote(next_function_name)(rest, [string_value | accum])

          _ ->
            {:error, :incomplete}
        end
      end

      defp unquote(current_name)(_, _) do
        {:error, :incomplete}
      end
    end
  end

  def build_decoder({_, :i64} = spec, next_spec) do
    current_name = decoder_name(spec)
    next_function_name = decoder_name(next_spec)

    quote do
      defp unquote(current_name)(<<decoded::little-signed-size(64), rest::binary>>, accum) do
        unquote(next_function_name)(rest, [decoded | accum])
      end

      defp unquote(current_name)(_, _) do
        {:error, :incomplete}
      end
    end
  end

  def build_decoder({_, :i32} = spec, next_spec) do
    current_name = decoder_name(spec)
    next_function_name = decoder_name(next_spec)

    quote do
      defp unquote(current_name)(<<decoded::little-signed-size(32), rest::binary>>, accum) do
        unquote(next_function_name)(rest, [decoded | accum])
      end

      defp unquote(current_name)(_, _) do
        {:error, :incomplete}
      end
    end
  end

  def build_decoder({_, :i16} = spec, next_spec) do
    current_name = decoder_name(spec)
    next_function_name = decoder_name(next_spec)

    quote do
      defp unquote(current_name)(<<decoded::little-signed-size(16), rest::binary>>, accum) do
        unquote(next_function_name)(rest, [decoded | accum])
      end

      defp unquote(current_name)(_, _) do
        {:error, :incomplete}
      end
    end
  end

  def build_decoder({_, :i8} = spec, next_spec) do
    current_name = decoder_name(spec)
    next_function_name = decoder_name(next_spec)

    quote do
      defp unquote(current_name)(<<decoded::little-signed-size(8), rest::binary>>, accum) do
        unquote(next_function_name)(rest, [decoded | accum])
      end

      defp unquote(current_name)(_, _) do
        {:error, :incomplete}
      end
    end
  end

  def build_decoder({_, :u64} = spec, next_spec) do
    current_name = decoder_name(spec)
    next_function_name = decoder_name(next_spec)

    quote do
      defp unquote(current_name)(<<decoded::little-unsigned-size(64), rest::binary>>, accum) do
        unquote(next_function_name)(rest, [decoded | accum])
      end

      defp unquote(current_name)(_, _) do
        {:error, :incomplete}
      end
    end
  end

  def build_decoder({_, :u32} = spec, next_spec) do
    current_name = decoder_name(spec)
    next_function_name = decoder_name(next_spec)

    quote do
      defp unquote(current_name)(<<decoded::little-unsigned-size(32), rest::binary>>, accum) do
        unquote(next_function_name)(rest, [decoded | accum])
      end

      defp unquote(current_name)(_, _) do
        {:error, :incomplete}
      end
    end
  end

  def build_decoder({_, :u16} = spec, next_spec) do
    current_name = decoder_name(spec)
    next_function_name = decoder_name(next_spec)

    quote do
      defp unquote(current_name)(<<decoded::little-unsigned-size(16), rest::binary>>, accum) do
        unquote(next_function_name)(rest, [decoded | accum])
      end

      defp unquote(current_name)(_, _) do
        {:error, :incomplete}
      end
    end
  end

  def build_decoder({_, :u8} = spec, next_spec) do
    current_name = decoder_name(spec)
    next_function_name = decoder_name(next_spec)

    quote do
      defp unquote(current_name)(<<decoded::little-unsigned-size(8), rest::binary>>, accum) do
        unquote(next_function_name)(rest, [decoded | accum])
      end

      defp unquote(current_name)(_, _) do
        {:error, :incomplete}
      end
    end
  end

  def build_decoder({_, :f64} = spec, next_spec) do
    current_name = decoder_name(spec)
    next_function_name = decoder_name(next_spec)

    quote do
      defp unquote(current_name)(<<decoded::little-signed-float-size(64), rest::binary>>, accum) do
        unquote(next_function_name)(rest, [decoded | accum])
      end

      defp unquote(current_name)(_, _) do
        {:error, :incomplete}
      end
    end
  end

  def build_decoder({_, :f32} = spec, next_spec) do
    current_name = decoder_name(spec)
    next_function_name = decoder_name(next_spec)

    quote do
      defp unquote(current_name)(<<decoded::little-signed-float-size(32), rest::binary>>, accum) do
        unquote(next_function_name)(rest, [decoded | accum])

        defp unquote(current_name)(_, _) do
          {:error, :incomplete}
        end
      end
    end
  end

  def build_decoder({_, :date} = spec, next_spec) do
    current_name = decoder_name(spec)
    next_function_name = decoder_name(next_spec)

    quote do
      defp unquote(current_name)(
             <<days_since_epoch::little-unsigned-size(16), rest::binary>>,
             accum
           ) do
        {:ok, date} = Date.new(1970, 01, 01)
        date = Date.add(date, days_since_epoch)
        unquote(next_function_name)(rest, [date | accum])
      end

      defp unquote(current_name)(_, _) do
        {:error, :incomplete}
      end
    end
  end

  def build_decoder({_, :datetime} = spec, next_spec) do
    current_name = decoder_name(spec)
    next_function_name = decoder_name(next_spec)

    quote do
      defp unquote(current_name)(
             <<seconds_since_epoch::little-unsigned-size(32), rest::binary>>,
             accum
           ) do
        {:ok, date_time} = NaiveDateTime.new(1970, 1, 1, 0, 0, 0)
        date_time = NaiveDateTime.add(date_time, seconds_since_epoch)

        unquote(next_function_name)(rest, [date_time | accum])
      end

      defp unquote(current_name)(_, _) do
        {:error, :incomplete}
      end
    end
  end

  def build_decoder({_, :boolean} = spec, next_spec) do
    current_name = decoder_name(spec)

    next_function_name = decoder_name(next_spec)

    quote do
      defp unquote(current_name)(<<1::little-unsigned-size(8), rest::binary>>, accum) do
        unquote(next_function_name)(rest, [true | accum])
      end

      defp unquote(current_name)(<<0::little-unsigned-size(8), rest::binary>>, accum) do
        unquote(next_function_name)(rest, [false | accum])
      end

      defp unquote(current_name)(_, _) do
        {:error, :incomplete}
      end
    end
  end

  defp decoder_name({name, type}) do
    :"decode_#{name}_#{type}"
  end
end
