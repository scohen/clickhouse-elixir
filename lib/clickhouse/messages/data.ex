defmodule Clickhouse.Messages.Server.Data do
  #  @compile {:bin_opt_info, true}
  alias Clickhouse.{Binary, BlockInfo, Messages.DataDecoders}

  use Bitwise
  use DataDecoders

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
         %{row_count: 0} = accum
       ) do
    decode_column(rest, column_count - 1, data, names, [type_name | types], accum)
  end

  defp decode_nullability(<<rest::binary>>, 0, nullability) do
    {rest, Enum.reverse(nullability)}
  end

  defp decode_nullability(<<1, rest::binary>>, row_count, nullability) do
    decode_nullability(rest, row_count - 1, [true | nullability])
  end

  defp decode_nullability(<<0, rest::binary>>, row_count, nullability) do
    decode_nullability(rest, row_count - 1, [false | nullability])
  end

  @decode_fns [
    {"Int64", :i64},
    {"Int32", :i32},
    {"Int16", :i16},
    {"Int8", :i8},
    {"UInt64", :u64},
    {"UInt32", :u32},
    {"UInt16", :u16},
    {"UInt8", :u8},
    {"Float64", :f64},
    {"Float32", :f32},
    {"Float16", :f16},
    {"String", :string},
    {"Date", :date},
    {"DateTime", :datetime}
  ]

  # We build the decoders with a macro because if we use a case statement to switch between them,
  # we don't get binary optimizations.

  for {ch_name, local_name} <- @decode_fns do
    decoder_name = :"decode_#{local_name}_columns"
    null_ch_name = "Nullable(#{ch_name})"
    null_decoder_name = :"decode_nullable_#{local_name}_columns"

    defp decode_column_type(
           <<_length::size(8), unquote(ch_name), rest::binary>>,
           column_count,
           data,
           names,
           types,
           accum
         ) do
      types = [unquote(ch_name) | types]

      unquote(decoder_name)(rest, column_count, accum.row_count, data, names, types, accum)
    end

    defp decode_column_type(
           <<_length::size(8), unquote(null_ch_name), rest::binary>>,
           column_count,
           data,
           names,
           types,
           accum
         ) do
      types = [unquote(ch_name) | types]

      {rest, null_map} = decode_nullability(rest, accum.row_count, [])
      unquote(null_decoder_name)(rest, column_count, null_map, data, names, types, accum)
    end
  end
end
