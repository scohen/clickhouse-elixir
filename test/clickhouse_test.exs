defmodule ClickhouseTest do
  use ExUnit.Case

  setup do
    {:ok, client} = Clickhouse.start_link(database: "sereno_dev", username: "default")
    {:ok, client: client}
  end

  test "it should allow you to query", %{client: client} do
    assert {:ok, _, []} = Clickhouse.query(client, "select id, event_name from events")
  end
end
