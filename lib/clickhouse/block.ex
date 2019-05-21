defmodule Clickhouse.BlockInfo do
  defstruct is_overflow: false, bucket_num: -1
  alias Clickhouse.Binary

  def new() do
    %__MODULE__{}
  end

  def to_iodata(%__MODULE__{is_overflow: is_overflow, bucket_num: bucket_num}) do
    [
      Binary.encode(:varint, 1),
      Binary.encode(:boolean, is_overflow),
      Binary.encode(:varint, 2),
      Binary.encode(:i32, bucket_num),
      Binary.encode(:varint, 0)
    ]
  end
end

defmodule Clickhouse.Block do
  defstruct info: nil, data: [], row_count: 0, column_count: 0, column_names: [], column_types: []
  alias Clickhouse.BlockInfo
  alias Clickhouse.Binary

  def new do
    %__MODULE__{info: BlockInfo.new()}
  end

  def to_iodata(%__MODULE__{info: info, data: data} = block, column_descriptions) do
    row_data = Enum.zip(data)

    data_iolist =
      for {{col_name, col_type}, col_data} <- Enum.zip(column_descriptions, row_data) do
        [
          Binary.encode(:string, col_name),
          Binary.encode(:string, col_type),
          colum_data_iolist(col_type, col_data)
        ]
      end

    [
      BlockInfo.to_iodata(info),
      Binary.encode(:varint, block.column_count),
      Binary.encode(:varint, block.row_count),
      data_iolist
    ]
  end

  def to_iodata(%__MODULE__{info: info} = block) do
    [
      BlockInfo.to_iodata(info),
      Binary.encode(:varint, block.column_count),
      Binary.encode(:varint, block.row_count)
    ]
  end

  defp colum_data_iolist(column_type, column_data) do
    {column_type, nullable?} = decompose_type(column_type)

    null_map =
      if nullable? do
        null_map(column_data)
      else
        []
      end

    column_data =
      for datum <- column_data do
        to_iodata(column_type, datum, nullable?)
      end

    [null_map, column_data]
  end

  defp decompose_type("Nullable(" <> col_type) do
    {String.replace_suffix(col_type, ")", ""), true}
  end

  defp decompose_type(col_type) do
    {col_type, false}
  end

  defp null_map(column_data) do
    for item <- column_data do
      if is_nil(item) do
        Binary.encode(:u8, 1)
      else
        Binary.encode(:u8, 0)
      end
    end
  end

  defp to_native(raw_type) do
    case raw_type do
      "Int64" -> :i64
      "Int32" -> :i32
      "Int16" -> :i16
      "Int8" -> :i8
      "UInt64" -> :u64
      "UInt32" -> :u32
      "UInt16" -> :u16
      "UInt8" -> :u8
      "Float64" -> :f64
      "Float32" -> :f32
      "Float16" -> :f16
      "Float8" -> :f8
      "String" -> :string
      "Date" -> :date
      "DateTime" -> :datetime
    end
  end

  defp to_iodata(format, nullable?, value) when is_bitstring(format) do
    format
    |> to_native()
    |> to_iodata(nullable?, value)
  end

  defp to_iodata(format, true, nil) do
    to_iodata(format, true, empty_value(format))
  end

  defp to_iodata(format, _nullable?, value) do
    Binary.encode(format, value)
  end

  defp empty_value(format) when format in ~w(i64 i32 i16 i8 u64 u32 u16 u8 date datetime)a do
    0
  end

  defp empty_value(format) when format in ~w(:f64, :f32, :f16, :f8) do
    0.0
  end

  defp empty_value(:string) do
    ""
  end
end
