defmodule UniversalEnterprisePlatformWeb.HealthController do
  use UniversalEnterprisePlatformWeb, :controller

  alias UniversalEnterprisePlatform.Infrastructure.Health.Checker

  def index(conn, _params) do
    health = Checker.check_all()
    status = if health.status == :ok, do: 200, else: 503
    conn |> put_status(status) |> json(health)
  end
end
