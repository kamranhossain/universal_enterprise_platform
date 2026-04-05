defmodule UniversalEnterprisePlatform.Infrastructure.Repos.Repo do
  use Ecto.Repo,
    otp_app: :universal_enterprise_platform,
    adapter: Ecto.Adapters.Postgres
end
