defmodule UniversalEnterprisePlatform.Infrastructure.Flink.CdcConfig do
  @moduledoc """
  Flink CDC — connection management and job definitions.

  STATUS: Configured, NOT active by default.
  Flink runs as a separate Docker process (--profile cdc).

  To test connection:  make test-flink
  To enable:          config :core, :flink_enabled, true

  CDC job SQL files live in flink/jobs/ — they are ready to submit
  once PostgreSQL WAL replication slots and StarRocks are configured.
  """

  require Logger

  def enabled?, do: Application.get_env(:core, :flink_enabled, false)

  defp rest_url, do: Application.get_env(:core, :flink_rest_url, "http://localhost:8081")

  @doc """
  Check if Flink JobManager REST API is reachable.
  Works regardless of :flink_enabled flag — useful for infra validation.
  """
  def health_check do
    case Req.get("#{rest_url()}/v1/overview", receive_timeout: 3_000) do
      {:ok, %Req.Response{status: 200, body: body}} ->
        {:ok,
         %{
           version: body["flink-version"],
           jobs_running: body["jobs-running"],
           jobs_finished: body["jobs-finished"],
           taskmanagers: body["taskmanagers"],
           slots_available: body["slots-available"]
         }}

      {:ok, %Req.Response{status: status}} ->
        {:error, "Flink REST returned HTTP #{status}"}

      {:error, %{reason: reason}} ->
        {:error, "Cannot reach Flink at #{rest_url()} — #{inspect(reason)}"}
    end
  end

  @doc "List all currently submitted Flink jobs."
  def list_jobs do
    case Req.get("#{rest_url()}/v1/jobs") do
      {:ok, %Req.Response{status: 200, body: %{"jobs" => jobs}}} -> {:ok, jobs}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  CDC job definitions. Each maps to a SQL file in flink/jobs/.
  Used for documentation, health reporting, and future job submission.
  """
  def job_definitions do
    [
      %{
        id: :cdc_financial,
        name: "CDC Financial",
        sql_file: "flink/jobs/cdc_financial.sql",
        source_tables: ~w[journal_lines invoices payments],
        sink_tables: ~w[fact_transactions fact_invoices fact_payments],
        status: :not_submitted
      },
      %{
        id: :cdc_operational,
        name: "CDC Operational",
        sql_file: "flink/jobs/cdc_operational.sql",
        source_tables: ~w[visits orders work_orders],
        sink_tables: ~w[fact_visits fact_orders fact_work_orders],
        status: :not_submitted
      },
      %{
        id: :cdc_hr_procurement,
        name: "CDC HR & Procurement",
        sql_file: "flink/jobs/cdc_hr_procurement.sql",
        source_tables: ~w[employees leave_requests purchase_orders],
        sink_tables: ~w[dim_employees fact_leave fact_purchase_orders],
        status: :not_submitted
      }
    ]
  end
end
