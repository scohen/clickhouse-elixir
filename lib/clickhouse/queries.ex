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
end

defimpl DBConnection.Query, for: BitString do
  @select_re ~r/select/i

  def parse(query, opts) do
    query_module =
      if Regex.match?(@select_re, query) do
        Clickhouse.Queries.Select
      else
        Clickhouse.Queries.Insert
      end

    query_module.new(query, opts)
  end

  def prepare(_, _), do: {:error, :not_preparable}
  def encode(_, _, _), do: {:error, :not_encodeable}
  def describe(_, _), do: {:error, :indescribable}
  def decode(_, result, _), do: result
end

defimpl DBConnection.Query, for: Clickhouse.Queries.Select do
  def parse(query, opts) do
    query
  end

  def prepare(_, _), do: {:error, :not_preparable}
  def encode(_, _, _), do: {:error, :not_encodeable}
  def describe(query, _), do: query
  def decode(_query, result, _), do: result
end
