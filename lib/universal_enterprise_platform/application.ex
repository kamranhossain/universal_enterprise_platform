defmodule UniversalEnterprisePlatform.Application do
  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    Logger.info("[UniversalEnterprisePlatform] Starting — env=#{Mix.env()}")

    children =
      base_children()
      |> maybe_add(
        UniversalEnterprisePlatform.Clients.TigerBeetleClient,
        Application.get_env(:universal_enterprise_platform, :tigerbeetle_enabled, true)
      )
      |> maybe_add(
        UniversalEnterprisePlatform.Clients.StarRocksRepo,
        Application.get_env(:universal_enterprise_platform, :starrocks_enabled, false)
      )

    opts = [strategy: :one_for_one, name: Platform.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp base_children do
    [
      # ── L1 Infrastructure ──────────────────────────────────────

      # Cache (ETS must start first — synchronous, never fails)
      Platform.Infrastructure.Cache.EtsAdapter,

      # Databases
      Platform.Repo,
      Platform.TimescaleRepo,

      # Valkey connection pool
      valkey_pool_spec(),

      # PubSub — before Oban and anything that broadcasts
      {Phoenix.PubSub, name: Platform.PubSub},

      # HTTP client pool (Swoosh email, Meilisearch, Flink health)
      {Finch, name: Platform.Finch},

      # ── L2 Kernel ──────────────────────────────────────────────

      # Background jobs — after repos, before web
      # {Oban, PlatformKernel.ObanConfig.config()},

      # Telemetry — start early so metrics capture boot events
      Platform.Infrastructure.Telemetry,

      # ── L6 Web ─────────────────────────────────────────────────
      PlatformWeb.Endpoint
    ]
  end

  defp maybe_add(children, child, true) do
    Logger.info("[Platform] + #{inspect(child)}")
    children ++ [child]
  end

  defp maybe_add(children, child, false) do
    Logger.info("[Platform] - #{inspect(child)} (disabled)")
    children
  end

  defp valkey_pool_spec do
    url =
      Application.get_env(:universal_enterprise_platform, :valkey_url, "redis://localhost:6379")

    pool_size = Application.get_env(:universal_enterprise_platform, :valkey_pool_size, 5)

    children =
      for i <- 1..pool_size do
        Supervisor.child_spec(
          {Redix, {url, [name: :"valkey_#{i}"]}},
          id: :"valkey_#{i}"
        )
      end

    %{
      id: :valkey_pool,
      start:
        {Supervisor, :start_link, [children, [strategy: :one_for_one, name: Platform.ValkeyPool]]},
      type: :supervisor
    }
  end
end
