defmodule Platform.Infrastructure.Cache.EtsAdapter do
  @moduledoc """
  ETS-backed in-process cache. ~1μs reads, no network hop.

  Used for: permission cache, tenant settings, feature flags,
            notification unread counts.

  TTL is enforced lazily on read — no background sweep needed for MVP.
  """
  @behaviour Platform.Infrastructure.Cache.Behaviour

  use GenServer

  @table :platform_ets_cache

  def start_link(_opts), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  @impl GenServer
  def init(_) do
    :ets.new(@table, [:named_table, :public, read_concurrency: true, write_concurrency: true])
    {:ok, %{}}
  end

  @impl Platform.Infrastructure.Cache.Behaviour
  def get(key) do
    case :ets.lookup(@table, key) do
      [{^key, value, :infinity}] ->
        {:ok, value}

      [{^key, value, expires_at}] ->
        if System.monotonic_time(:second) < expires_at do
          {:ok, value}
        else
          :ets.delete(@table, key)
          {:error, :not_found}
        end

      [] ->
        {:error, :not_found}
    end
  end

  @impl Platform.Infrastructure.Cache.Behaviour
  def put(key, value, ttl_seconds \\ 300) do
    expires_at =
      if ttl_seconds == :infinity,
        do: :infinity,
        else: System.monotonic_time(:second) + ttl_seconds

    :ets.insert(@table, {key, value, expires_at})
    :ok
  end

  @impl Platform.Infrastructure.Cache.Behaviour
  def delete(key) do
    :ets.delete(@table, key)
    :ok
  end

  @impl Platform.Infrastructure.Cache.Behaviour
  def exists?(key) do
    match?({:ok, _}, get(key))
  end

  @impl Platform.Infrastructure.Cache.Behaviour
  def flush_namespace(namespace) do
    # Match spec: delete all keys starting with "namespace:"
    ms = [
      {{:"$1", :_, :_}, [{:is_binary, :"$1"}],
       [
         {:orelse, {:==, :"$1", namespace},
          {:andalso, {:>, {:byte_size, :"$1"}, byte_size(namespace) + 1},
           {:==, {:binary_part, :"$1", 0, byte_size(namespace) + 1}, <<namespace::binary, ":">>}}}
       ]}
    ]

    :ets.select_delete(@table, ms)
    :ok
  end

  def table_size, do: :ets.info(@table, :size)
end
