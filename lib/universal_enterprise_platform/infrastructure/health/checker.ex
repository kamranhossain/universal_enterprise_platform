defmodule UniversalEnterprisePlatform.Infrastructure.Health.Checker do
  @moduledoc "Aggregates health of all infrastructure. Drives /health endpoint."

  alias UniversalEnterprisePlatform.Infrastructure.{Repo, TimescaleRepo, StarRocksRepo}
  alias UniversalEnterprisePlatform.Infrastructure.Cache.{EtsAdapter, ValkeyAdapter}
  alias UniversalEnterprisePlatform.Infrastructure.Clients.{TigerBeetleClient, SpiceDBClient}
  alias UniversalEnterprisePlatform.Infrastructure.Flink.CdcConfig

  def check_all do
    components = %{
      postgresql: check_postgresql(),
      timescaledb: check_timescaledb(),
      postgis: check_postgis(),
      pgvector: check_pgvector(),
      valkey: check_valkey(),
      ets: check_ets(),
      tigerbeetle: check_tigerbeetle(),
      spicedb: check_spicedb(),
      starrocks: check_starrocks(),
      flink_cdc: check_flink()
    }

    # Critical = system cannot function without these
    critical = [:postgresql, :timescaledb, :valkey, :ets, :tigerbeetle, :spicedb]

    overall =
      if Enum.all?(critical, &(components[&1].status in [:ok, :disabled])),
        do: :ok,
        else: :degraded

    %{
      timestamp: DateTime.utc_now(),
      status: overall,
      components: components
    }
  end

  defp check_postgresql do
    case Repo.query("SELECT version()") do
      {:ok, %{rows: [[v]]}} ->
        %{status: :ok, message: String.slice(v, 0, 45)}

      {:error, e} ->
        %{status: :error, message: inspect(e)}
    end
  rescue
    e -> err(e)
  end

  defp check_timescaledb do
    case TimescaleRepo.health_check() do
      {:ok, v} -> %{status: :ok, message: v}
      {:error, e} -> %{status: :error, message: e}
    end
  rescue
    e -> err(e)
  end

  defp check_postgis do
    case Repo.query("SELECT default_version FROM pg_available_extensions WHERE name = 'postgis'") do
      {:ok, %{rows: [[v]]}} -> %{status: :ok, message: "PostGIS #{v}"}
      _ -> %{status: :error, message: "PostGIS extension not found"}
    end
  rescue
    e -> err(e)
  end

  defp check_pgvector do
    case Repo.query("SELECT default_version FROM pg_available_extensions WHERE name = 'vector'") do
      {:ok, %{rows: [[v]]}} -> %{status: :ok, message: "pgvector #{v}"}
      _ -> %{status: :error, message: "pgvector extension not found"}
    end
  rescue
    e -> err(e)
  end

  defp check_valkey do
    case ValkeyAdapter.health_check() do
      {:ok, ms} -> %{status: :ok, message: "#{ms}ms"}
      {:error, e} -> %{status: :error, message: inspect(e)}
    end
  rescue
    e -> err(e)
  end

  defp check_ets do
    %{status: :ok, message: "#{EtsAdapter.table_size()} entries"}
  rescue
    e -> err(e)
  end

  defp check_tigerbeetle do
    if Application.get_env(:platform, :tigerbeetle_enabled, true) do
      case TigerBeetleClient.health_check() do
        {:ok, info} -> %{status: :ok, active: true, message: inspect(info)}
        {:error, e} -> %{status: :error, active: true, message: inspect(e)}
      end
    else
      %{status: :disabled, active: false, message: "TigerBeetle disabled"}
    end
  rescue
    e -> err(e)
  end

  defp check_spicedb do
    case SpiceDBClient.health_check() do
      {:ok, info} ->
        %{status: :ok, active: true, message: "schema loaded (#{info.schema_length} chars)"}

      {:error, e} ->
        %{status: :error, active: false, message: e}
    end
  rescue
    e -> err(e)
  end

  defp check_starrocks do
    case StarRocksRepo.health_check() do
      {:ok, info} -> %{status: :ok, active: true, message: inspect(info)}
      {:disabled, msg} -> %{status: :disabled, active: false, message: msg}
      {:error, e} -> %{status: :error, active: false, message: inspect(e)}
    end
  rescue
    e -> err(e)
  end

  defp check_flink do
    case CdcConfig.health_check() do
      {:ok, info} ->
        %{status: :ok, active: CdcConfig.enabled?(), message: "Flink #{info.version}"}

      {:error, r} ->
        status = if CdcConfig.enabled?(), do: :error, else: :disabled
        %{status: status, active: false, message: r}
    end
  rescue
    e -> err(e)
  end

  defp err(e), do: %{status: :error, message: Exception.message(e)}
end
