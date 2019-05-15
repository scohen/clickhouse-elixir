defmodule Clickhouse.ClientInfo do
  defstruct [
    :client_hostname,
    :client_name,
    :initial_address,
    :initial_query_id,
    :initial_user,
    :interface,
    :os_user,
    :patch,
    :quota_key,
    :revision,
    :server_revision,
    :version_major,
    :version_minor
  ]

  @query_kind_no_query 0
  @query_kind_initial_query 1
  @query_kind_secondary_query 2

  @interface_tcp 1
  @interface_http 2

  alias Clickhouse.Binary

  def new() do
    {:ok, hostname} = :inet.gethostname()

    %__MODULE__{
      client_hostname: List.to_string(hostname),
      client_name: "Clickhouse Elixir",
      initial_address: "0.0.0.0",
      initial_query_id: "",
      initial_user: "",
      interface: @interface_tcp,
      os_user: System.get_env("USER"),
      patch: 3,
      quota_key: "",
      revision: 54416,
      version_major: 19,
      version_minor: 4
    }
  end

  def initial_query do
    @query_kind_initial_query
  end

  def no_query do
    @query_kind_no_query
  end

  def secondary_query do
    @query_kind_secondary_query
  end

  defguard is_empty(kind) when kind == @query_kind_no_query

  def to_iodata(%__MODULE__{} = info, query_kind) when is_empty(query_kind) do
    [
      Binary.encode(:u8, query_kind)
    ]
  end

  def to_iodata(%__MODULE__{} = info, query_kind) do
    [
      Binary.encode(:u8, query_kind),
      Binary.encode(:string, info.initial_user),
      Binary.encode(:string, info.initial_query_id),
      Binary.encode(:string, info.initial_address),
      Binary.encode(:u8, info.interface),
      Binary.encode(:string, info.os_user),
      Binary.encode(:string, info.client_hostname),
      Binary.encode(:string, info.client_name),
      Binary.encode(:varint, info.version_major),
      Binary.encode(:varint, info.version_minor),
      Binary.encode(:varint, info.revision),
      Binary.encode(:string, info.quota_key),
      Binary.encode(:varint, info.patch)
    ]
  end
end
