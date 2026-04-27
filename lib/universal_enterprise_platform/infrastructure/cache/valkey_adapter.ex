defmodule UniversalEnterprisePlatform.Infrastructure.Cache.ValkeyAdapter do
  @moduledoc """
  Valkey (Redis-compatible) distributed cache via Redix connection pool.
  Cross-node shared state, rate limiting, distributed locks.

  Pool: :valkey_1 .. :valkey_N (started by UniversalEnterprisePlatform.Application).
  """
  @behaviour UniversalEnterprisePlatform.Infrastructure.Cache.Behaviour

  defp conn do
    pool_size = Application.get_env(:universal_enterprise_platform, :valkey_pool_size, 5)
    :"valkey_#{:rand.uniform(pool_size)}"
  end

  @impl UniversalEnterprisePlatform.Infrastructure.Cache.Behaviour
  def get(key) do
    case Redix.command(conn(), ["GET", key]) do
      {:ok, nil} -> {:error, :not_found}
      {:ok, value} -> {:ok, Jason.decode!(value)}
      {:error, err} -> {:error, err}
    end
  end

  @impl UniversalEnterprisePlatform.Infrastructure.Cache.Behaviour
  def put(key, value, ttl_seconds \\ 300) do
    encoded = Jason.encode!(value)

    case Redix.command(conn(), ["SETEX", key, ttl_seconds, encoded]) do
      {:ok, "OK"} -> :ok
      {:error, err} -> {:error, err}
    end
  end

  @impl UniversalEnterprisePlatform.Infrastructure.Cache.Behaviour
  def delete(key) do
    case Redix.command(conn(), ["DEL", key]) do
      {:ok, _} -> :ok
      {:error, err} -> {:error, err}
    end
  end

  @impl UniversalEnterprisePlatform.Infrastructure.Cache.Behaviour
  def exists?(key) do
    case Redix.command(conn(), ["EXISTS", key]) do
      {:ok, 1} -> true
      _ -> false
    end
  end

  @impl UniversalEnterprisePlatform.Infrastructure.Cache.Behaviour
  def flush_namespace(namespace) do
    with {:ok, keys} <- Redix.command(conn(), ["KEYS", "#{namespace}:*"]),
         true <- keys != [] do
      Redix.command(conn(), ["DEL" | keys])
    end

    :ok
  end

  @doc "Publish a message to a Valkey channel (cross-node cache invalidation)."
  def publish(channel, message) do
    Redix.command(conn(), ["PUBLISH", channel, Jason.encode!(message)])
  end

  @doc "Returns {:ok, latency_ms} or {:error, reason}."
  def health_check do
    t = System.monotonic_time(:millisecond)

    case Redix.command(conn(), ["PING"]) do
      {:ok, "PONG"} -> {:ok, System.monotonic_time(:millisecond) - t}
      {:error, err} -> {:error, err}
    end
  end
end
