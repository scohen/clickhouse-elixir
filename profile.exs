{:ok, client} = Clickhouse.start_link(database: "sereno_dev", username: "default")
Clickhouse.query(client, "select id, event_name from events")
