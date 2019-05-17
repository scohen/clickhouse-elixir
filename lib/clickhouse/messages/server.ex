defmodule Clickhouse.Messages.Server do
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
