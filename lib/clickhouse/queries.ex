defmodule Clickhouse.Queries do
  defmodule Insert do
    defstruct statement: nil, params: [], opts: nil

    def new(query, opts) do
      %__MODULE__{statement: query, opts: opts}
    end
  end

  defmodule Select do
    defstruct statement: nil, params: [], opts: nil

    def new(query, opts) do
      %__MODULE__{statement: query, opts: opts}
    end
  end

  defmodule Modify do
    defstruct statement: nil, params: [], opts: nil

    def new(query, opts) do
      %__MODULE__{statement: query, opts: opts}
    end
  end
end

defimpl DBConnection.Query, for: BitString do
  @select_re ~r/^\s*select/i
  @insert_re ~r/^\s*insert/i

  def parse(query, opts) do
    query_module =
      if Regex.match?(@select_re, query) do
        Clickhouse.Queries.Select
      else
        if Regex.match?(@insert_re, query) do
          Clickhouse.Queries.Insert
        else
          Clickhouse.Queries.Modify
        end
      end

    query_module.new(query, opts)
  end

  def prepare(_, _), do: {:error, :not_preparable}
  def encode(_, _, _), do: {:error, :not_encodeable}
  def describe(_, _), do: {:error, :indescribable}
  def decode(_, result, _), do: result
end

defimpl DBConnection.Query, for: Clickhouse.Queries.Select do
  def parse(query, _opts) do
    query
  end

  def prepare(_, _), do: {:error, :not_preparable}
  def encode(_, params, _), do: params
  def describe(query, _), do: query
  def decode(_query, result, _), do: result
end

defimpl DBConnection.Query, for: Clickhouse.Queries.Insert do
  def parse(query, _opts) do
    query
  end

  def prepare(_, _), do: {:error, :not_preparable}
  def encode(_query, params, _opts), do: params
  def describe(query, _), do: query
  def decode(_query, result, _), do: result
end

defimpl DBConnection.Query, for: Clickhouse.Queries.Modify do
  def parse(query, _opts) do
    query
  end

  def prepare(_, _), do: {:error, :not_preparable}
  def encode(_, params, _), do: params
  def describe(query, _), do: query
  def decode(_query, result, _), do: result
end
