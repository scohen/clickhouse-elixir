defmodule Clickhouse do
  @moduledoc """
  Documentation for Clickhouse.
  """

  def start_link(opts \\ []) do
    opts = Keyword.put(opts, :show_sensitive_data_on_connection_error, true)
    DBConnection.start_link(Clickhouse.Connection, opts)
  end

  def query(conn, query, params \\ [], opts \\ []) do
    DBConnection.prepare_execute(conn, query, params, opts)
  end
end
