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

  def to_iodata(%__MODULE__{info: info} = block) do
    [
      BlockInfo.to_iodata(info),
      Binary.encode(:varint, block.column_count),
      Binary.encode(:varint, block.row_count)
    ]
  end
end
