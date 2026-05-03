defmodule UniversalEnterprisePlatform.Infrastructure.Health.Checker do
  @moduledoc "Aggregates health of all infrastructure. Drives /health endpoint."

  alias UniversalEnterprisePlatform.Infrastructure.Repos.{Repo, TimescaleRepo, StarRocksRepo}
  alias UniversalEnterprisePlatform.Infrastructure.Cache.{EtsAdapter, ValkeyAdapter}
  alias UniversalEnterprisePlatform.Infrastructure.Clients.{TigerBeetleClient, SpiceDBClient}
  alias UniversalEnterprisePlatform.Infrastructure.Flink.CdcConfig
  alias UniversalEnterprisePlatform.Infrastructure.Messaging.{KafkaClient, MqttClient}
  alias UniversalEnterprisePlatform.Infrastructure.Search.MeilisearchAdapter

  def check_all do
    components = %{
      postgresql: check_postgresql(),
      timescaledb: check_timescaledb(),
      postgis: check_postgis(),
      pg_textsearch: check_pg_textsearch(),
      pgvector: check_pgvector(),
      meilisearch: check_meilisearch(),
      kafka: check_kafka(),
      valkey: check_valkey(),
      ets: check_ets(),
      tigerbeetle: check_tigerbeetle(),
      spicedb: check_spicedb(),
      starrocks: check_starrocks(),
      flink_cdc: check_flink()
    }

    # Critical = system cannot function without these
    critical = [
      :postgresql,
      :timescaledb,
      :valkey,
      :ets,
      :tigerbeetle,
      :spicedb,
      :meilisearch,
      :kafka
    ]

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

  defp check_pg_textsearch do
    case Repo.query(
           "SELECT default_version FROM pg_available_extensions WHERE name = 'pg_textsearch'"
         ) do
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

  defp check_mqtt do
    mqtt_module =
      Application.get_env(
        :universal_enterprise_platform,
        :mqtt_adapter,
        MqttClient
      )

    case Process.whereis(mqtt_module) do
      nil ->
        # Try direct TCP check if GenServer not started
        tcp_check_mqtt()

      _pid ->
        case mqtt_module.health_check() do
          {:ok, %{status: :connected} = info} ->
            host = Map.get(info, :host, "localhost")
            port = Map.get(info, :port, 1883)
            subs = Map.get(info, :subscriptions, 0)
            ok("Connected to #{host}:#{port}, #{subs} subscriptions")

          {:ok, %{adapter: :mock}} ->
            %{active: false, status: "ok", message: "MQTT mock adapter"}

          {:error, :disconnected} ->
            warn("MQTT disconnected — broker unreachable or reconnecting")

          {:error, reason} ->
            error_result(inspect(reason))
        end
    end
  end

  defp tcp_check_mqtt do
    config = Application.get_env(:universal_enterprise_platform, :mqtt, [])
    host = config[:host] || "localhost"
    port = config[:port] || 1883

    case :gen_tcp.connect(to_charlist(host), port, [:binary, active: false], 3_000) do
      {:ok, sock} ->
        :gen_tcp.close(sock)
        warn("Broker reachable at #{host}:#{port} but client not started yet")

      {:error, reason} ->
        %{
          active: false,
          status: "disabled",
          message: "MQTT broker not reachable at #{host}:#{port} — #{reason}"
        }
    end
  end

  # ── Kafka (KRaft) ──────────────────────────────────────────────

  defp check_kafka do
    kafka_module =
      Application.get_env(
        :universal_enterprise_platform,
        :kafka_adapter,
        KafkaClient
      )

    case kafka_module.health_check() do
      {:ok, %{status: :connected, mode: mode, brokers: broker_count, topics: topic_count}} ->
        ok("#{mode} cluster — #{broker_count} broker(s), #{topic_count} topic(s)")

      {:ok, %{status: :reachable, mode: mode, host: host, port: port}} ->
        ok("#{mode} broker reachable at #{host}:#{port}")

      {:ok, %{adapter: :mock}} ->
        %{active: false, status: "ok", message: "Kafka mock adapter"}

      {:error, reason} ->
        config = Application.get_env(:universal_enterprise_platform, :kafka, [])
        brokers = Keyword.get(config, :brokers, [{"localhost", 9092}])
        {host, port} = List.first(brokers) || {"localhost", 9092}

        case :gen_tcp.connect(to_charlist(host), port, [], 3_000) do
          {:ok, sock} ->
            :gen_tcp.close(sock)

            warn(
              "Kafka reachable at #{host}:#{port} but metadata fetch failed — #{inspect(reason)}"
            )

          {:error, _} ->
            %{
              active: false,
              status: "disabled",
              message:
                "Kafka not reachable at #{host}:#{port}. Start with: brew services start kafka"
            }
        end
    end
  end

  # ── MeiliSearch ────────────────────────────────────────────────

  defp check_meilisearch do
    t0 = System.monotonic_time(:millisecond)

    case MeilisearchAdapter.readiness_check() do
      {:ok, _msg} ->
        latency = System.monotonic_time(:millisecond) - t0

        {:ok, "Meilisearch ready (#{latency}ms)"}

      {:error, :auth_failed} ->
        %{
          active: false,
          status: "error",
          message: "Meilisearch auth failed (invalid API key)"
        }

      {:error, :unreachable} ->
        %{
          active: false,
          status: "error",
          message: "Meilisearch not reachable (systemd service likely down)"
        }

      {:error, :timeout} ->
        %{
          active: false,
          status: "error",
          message: "Meilisearch timeout"
        }

      {:error, reason} ->
        %{
          active: false,
          status: "error",
          message: "Meilisearch error: #{inspect(reason)}"
        }
    end
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
    if Application.get_env(:universal_enterprise_platform, :tigerbeetle_enabled, true) do
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
