defmodule UniversalEnterprisePlatform.Application do
  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    Logger.info("[UniversalEnterprisePlatform] Starting — env=#{Mix.env()}")

    children =
      base_children()
      |> maybe_add(
        UniversalEnterprisePlatform.Infrastructure.Clients.TigerBeetleClient,
        Application.get_env(:universal_enterprise_platform, :tigerbeetle_enabled, true)
      )
      |> maybe_add(
        UniversalEnterprisePlatform.Infrastructure.Clients.StarRocksRepo,
        Application.get_env(:universal_enterprise_platform, :starrocks_enabled, false)
      )

    opts = [strategy: :one_for_one, name: UniversalEnterprisePlatform.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp base_children do
    [
      # ── L1 Infrastructure ──────────────────────────────────────────────────
      UniversalEnterprisePlatform.Infrastructure.Cache.EtsAdapter,
      UniversalEnterprisePlatform.Infrastructure.Repos.Repo,
      UniversalEnterprisePlatform.Infrastructure.Repos.TimescaleRepo,
      valkey_pool_spec(),
      {Phoenix.PubSub, name: Platform.PubSub},
      {Finch, name: UniversalEnterprisePlatform.Finch},
      # ── L6 Web ─────────────────────────────────────────────────────────────
      UniversalEnterprisePlatformWeb.Endpoint
    ]
  end

  # Logs once per child, not twice (enabled + disabled)
  defp maybe_add(children, child, enabled?) do
    tag = if enabled?, do: "+", else: "-"
    suffix = if enabled?, do: "", else: " (disabled)"
    Logger.info("[UniversalEnterprisePlatform] #{tag} #{inspect(child)}#{suffix}")
    if enabled?, do: children ++ [child], else: children
  end

  defp valkey_pool_spec do
    url =
      Application.get_env(:universal_enterprise_platform, :valkey_url, "redis://localhost:6379")

    pool_size = Application.get_env(:universal_enterprise_platform, :valkey_pool_size, 5)

    children =
      for i <- 1..pool_size do
        Supervisor.child_spec({Redix, {url, [name: :"valkey_#{i}"]}}, id: :"valkey_#{i}")
      end

    %{
      id: :valkey_pool,
      start:
        {Supervisor, :start_link,
         [children, [strategy: :one_for_one, name: UniversalEnterprisePlatform.ValkeyPool]]},
      type: :supervisor
    }
  end
end
