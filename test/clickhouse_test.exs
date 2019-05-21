defmodule ClickhouseTest do
  use ExUnit.Case
  import Clickhouse

  setup_all do
    {:ok, client} = Clickhouse.start_link(database: "", username: "default")

    {:ok, client: client}
  end

  setup %{client: client} do
    {:ok, _, _} = query(client, "CREATE DATABASE test")

    ddl = """
    CREATE TABLE IF NOT EXISTS
    test.simple_select (
      name String
    ) ENGINE = Memory
    """

    {:ok, _, _} = query(client, ddl)

    on_exit(fn ->
      nil
      #      query(client, "DROP DATABASE test")
    end)
  end

  test "it should allow you to query", %{client: client} do
    result = query(client, "select * from test.simple_select")
    IO.inspect(result)
    {:ok, _, %{rows: []}} = result
  end

  test "you should be able to insert", %{client: client} do
    IO.puts("A")
    {:ok, _, _} = query(client, "INSERT INTO test.simple_select (name) VALUES ('stinky')")
    IO.puts("B")
    {:ok, _, %{rows: row}} = query(client, "select * from test.simple_select")
  end
end
