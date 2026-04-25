defmodule UniversalEnterprisePlatform.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    pool_size = Application.get_env(:core, :valkey_pool_size, 5)

    valkey_children =
      for i <- 1..pool_size do
        {Redix, name: :"valkey_#{i}", host: "127.0.0.1", port: 6379}
      end

    children =
      [
        UniversalEnterprisePlatformWeb.Telemetry,
        UniversalEnterprisePlatform.Infrastructure.Repos.Repo
      ] ++
        valkey_children ++
        [
          {DNSCluster,
           query:
             Application.get_env(
               :universal_enterprise_platform,
               :dns_cluster_query
             ) || :ignore},
          {Phoenix.PubSub, name: UniversalEnterprisePlatform.PubSub},
          UniversalEnterprisePlatformWeb.Endpoint
        ]

    opts = [strategy: :one_for_one, name: UniversalEnterprisePlatform.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    UniversalEnterprisePlatformWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
