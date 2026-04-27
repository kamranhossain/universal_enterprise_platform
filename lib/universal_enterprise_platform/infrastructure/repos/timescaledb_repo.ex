defmodule UniversalEnterprisePlatform.Infrastructure.Repos.TimescaleRepo do
  use Ecto.Repo,
    otp_app: :universal_enterprise_platform,
    adapter: Ecto.Adapters.Postgres

  @doc """
  Verify the TimescaleDB extension is installed and working.
  Called by Health.Checker on startup and /health endpoint.
  """
  def health_check do
    case query("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'") do
      {:ok, %{rows: [[version]]}} ->
        {:ok, "TimescaleDB #{version}"}

      {:ok, %{rows: []}} ->
        {:error, "TimescaleDB extension not installed — run scripts/init_db.sql"}

      {:error, reason} ->
        {:error, inspect(reason)}
    end
  end

  @doc """
  Create a hypertable from an existing table.
  Call from migrations after CREATE TABLE.

  Example:
    TimescaleRepo.create_hypertable("gps_points", "recorded_at")
  """
  def create_hypertable(table, time_column, opts \\ []) do
    chunk_interval = Keyword.get(opts, :chunk_interval, "7 days")

    query!(
      """
        SELECT create_hypertable(
          $1, $2,
          chunk_time_interval => INTERVAL $3,
          if_not_exists => TRUE
        )
      """,
      [table, time_column, chunk_interval]
    )
  end

  @doc """
  Set retention policy on a hypertable.
  Example: set_retention("audit_logs", "90 days")
  """
  def set_retention(table, interval) do
    query!(
      "SELECT add_retention_policy($1, INTERVAL $2, if_not_exists => TRUE)",
      [table, interval]
    )
  end
end
