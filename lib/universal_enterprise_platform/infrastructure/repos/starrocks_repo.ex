defmodule UniversalEnterprisePlatform.Infrastructure.Repos.StarRocksRepo do
  @moduledoc """
  StarRocks analytical database — MySQL wire protocol on port 9030.

  STATUS: Configured, NOT started in development by default.

  To test:    make test-starrocks
  To enable:  config :universal_enterprise_platform, :starrocks_enabled, true
              (also starts the repo in the supervision tree)

  All functions degrade gracefully when disabled — QueryRouter
  falls back to Core.Repo for analytical queries automatically.
  """
  use Ecto.Repo,
    otp_app: :universal_enterprise_platform,
    adapter: Ecto.Adapters.MyXQL

  def enabled?, do: Application.get_env(:universal_enterprise_platform, :starrocks_enabled, true)

  @doc """
  Connection health check. Use to verify before enabling in production.
  Returns {:ok, info_map} | {:disabled, reason} | {:error, reason}
  """
  def health_check do
    if enabled?() do
      case query("SELECT CURRENT_TIMESTAMP, version()") do
        {:ok, %{rows: [[ts, version]]}} ->
          {:ok, %{connected_at: ts, version: version}}

        {:error, reason} ->
          {:error, inspect(reason)}
      end
    else
      {:disabled,
       "StarRocks not started. Set config :universal_enterprise_platform, :starrocks_enabled, true"}
    end
  end

  @doc """
  Run an analytical SQL query. Returns {:error, :disabled} if not enabled —
  callers should fall back to Core.Repo for development.
  """
  def analytical_query(sql, params \\ []) do
    if enabled?() do
      query(sql, params)
    else
      {:error, :starrocks_disabled}
    end
  end
end
