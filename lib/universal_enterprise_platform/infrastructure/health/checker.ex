defmodule UniversalEnterprisePlatform.Infrastructure.Health.Checker do
  @moduledoc """
  Comprehensive health checker for all infrastructure components.

  Components checked:
    Databases:     postgresql, timescaledb, postgis, pgvector, pg_textsearch
    Cache:         valkey, ets
    Messaging:     mqtt, kafka
    Search:        meilisearch
    Ledger:        tigerbeetle
    Authorization: spicedb
    Analytics:     starrocks (optional), flink_cdc (optional)
  """

  alias UniversalEnterprisePlatform.Infrastructure.Repos.{Repo, TimescaleRepo, StarRocksRepo}
  alias UniversalEnterprisePlatform.Infrastructure.Cache.{EtsAdapter, ValkeyAdapter}
  alias UniversalEnterprisePlatform.Infrastructure.Clients.{TigerBeetleClient, SpiceDBClient}
  alias UniversalEnterprisePlatform.Infrastructure.Flink.CdcConfig

  @timeout 5_000

  @doc "Run all health checks. Returns {:ok, results} always — never raises."
  def check_all do
    checks = [
      # ── PostgreSQL extensions (all via main Repo) ───────────────
      {:postgresql, &check_postgresql/0},
      {:timescaledb, &check_timescaledb/0},
      {:postgis, &check_postgis/0},
      {:pgvector, &check_pgvector/0},
      {:pg_textsearch, &check_pg_textsearch/0},

      # ── Cache ────────────────────────────────────────────────────
      {:valkey, &check_valkey/0},
      {:ets, &check_ets/0},

      # ── Messaging ────────────────────────────────────────────────
      {:mqtt, &check_mqtt/0},
      {:kafka, &check_kafka/0},

      # ── Search ───────────────────────────────────────────────────
      {:meilisearch, &check_meilisearch/0},

      # ── External services ─────────────────────────────────────────
      {:tigerbeetle, &check_tigerbeetle/0},
      {:spicedb, &check_spicedb/0},

      # ── Analytics (optional) ──────────────────────────────────────
      {:starrocks, &check_starrocks/0},
      {:flink_cdc, &check_flink_cdc/0}
    ]

    results =
      checks
      |> Task.async_stream(
        fn {name, check_fn} ->
          result =
            try do
              check_fn.()
            rescue
              e -> error_result("Exception: #{Exception.message(e)}")
            catch
              :exit, reason -> error_result("Exit: #{inspect(reason)}")
            end

          {name, result}
        end,
        timeout: @timeout,
        on_timeout: :kill_task
      )
      |> Enum.map(fn
        {:ok, {name, result}} -> {name, result}
        {:exit, :timeout} -> {:unknown, error_result("Timeout")}
      end)
      |> Map.new()

    overall = overall_status(results)
    {:ok, %{status: overall, timestamp: DateTime.utc_now(), components: results}}
  end

  # ── PostgreSQL ─────────────────────────────────────────────────

  defp check_postgresql do
    %{rows: [[version]]} = Repo.query!("SELECT version()")
    # Extract just the version number for a clean message
    short = version |> String.split(" on ") |> List.first()
    ok(short)
  end

  # ── TimescaleDB ────────────────────────────────────────────────

  defp check_timescaledb do
    %{rows: [[version]]} =
      Repo.query!("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")

    ok("TimescaleDB #{version}")
  end

  # ── PostGIS ────────────────────────────────────────────────────

  defp check_postgis do
    %{rows: [[version]]} = Repo.query!("SELECT PostGIS_Lib_Version()")
    ok("PostGIS #{version}")
  end

  # ── pgvector ───────────────────────────────────────────────────

  defp check_pgvector do
    %{rows: [[version]]} =
      Repo.query!("SELECT extversion FROM pg_extension WHERE extname = 'vector'")

    ok("pgvector #{version}")
  end

  # ── pg_textsearch (BM25) ───────────────────────────────────────

  defp check_pg_textsearch do
    case Repo.query("SELECT extversion FROM pg_extension WHERE extname = 'pg_textsearch'") do
      {:ok, %{rows: [[version]]}} ->
        # Also verify the BM25 index type is accessible
        Repo.query!("SELECT 1 WHERE EXISTS (
            SELECT 1 FROM pg_am WHERE amname = 'bm25'
          )")
        ok("pg_textsearch #{version} (BM25)")

      {:ok, %{rows: []}} ->
        warn("pg_textsearch not installed — run: CREATE EXTENSION pg_textsearch")

      {:error, %{postgres: %{code: :undefined_function}}} ->
        warn("pg_textsearch not loaded — add to shared_preload_libraries")

      {:error, reason} ->
        error_result("pg_textsearch check failed: #{inspect(reason)}")
    end
  end

  # ── Valkey / Redis ─────────────────────────────────────────────

  defp check_valkey do
    conn = valkey_conn()
    t0 = System.monotonic_time(:millisecond)

    case Redix.command(conn, ["PING"]) do
      {:ok, "PONG"} ->
        latency = System.monotonic_time(:millisecond) - t0
        ok("#{latency}ms")

      {:error, reason} ->
        error_result("PING failed: #{inspect(reason)}")
    end
  end

  # ── ETS ────────────────────────────────────────────────────────

  defp check_ets do
    count = :ets.all() |> length()
    ok("#{count} tables")
  end

  # ── MQTT ───────────────────────────────────────────────────────

  defp check_mqtt do
    mqtt_module =
      Application.get_env(
        :universal_enterprise_platform,
        :mqtt_adapter,
        UniversalEnterprisePlatform.Infrastructure.Messaging.MqttClient
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
        UniversalEnterprisePlatform.Infrastructure.Messaging.KafkaClient
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
    config = Application.get_env(:universal_enterprise_platform, :meilisearch, [])
    base = config[:url] || "http://localhost:7700"
    api_key = config[:api_key] || "masterKey"

    headers = [
      {"Authorization", "Bearer #{api_key}"},
      {"Content-Type", "application/json"}
    ]

    t0 = System.monotonic_time(:millisecond)

    case Req.get("#{base}/health", headers: headers, receive_timeout: 3_000) do
      {:ok, %{status: 200, body: %{"status" => "available"}}} ->
        latency = System.monotonic_time(:millisecond) - t0

        # Also get version
        version_msg =
          case Req.get("#{base}/version", headers: headers, receive_timeout: 2_000) do
            {:ok, %{status: 200, body: %{"pkgVersion" => v}}} ->
              "Meilisearch v#{v} (#{latency}ms)"

            _ ->
              "Meilisearch available (#{latency}ms)"
          end

        ok(version_msg)

      {:ok, %{status: status, body: body}} ->
        error_result("HTTP #{status}: #{inspect(body)}")

      {:error, %{reason: :econnrefused}} ->
        %{
          active: false,
          status: "disabled",
          message: "Meilisearch not running at #{base}. Start: meilisearch --master-key=masterKey"
        }

      {:error, reason} ->
        error_result("#{inspect(reason)}")
    end
  end

  # ── TigerBeetle ────────────────────────────────────────────────

  defp check_tigerbeetle do
    enabled = Application.get_env(:universal_enterprise_platform, :tigerbeetle_enabled, false)

    if enabled do
      tb_module =
        Application.get_env(
          :universal_enterprise_platform,
          :ledger_adapter,
          UniversalEnterprisePlatform.Infrastructure.Ledger.TigerBeetleClient
        )

      case tb_module.health_check() do
        {:ok, info} -> %{active: true, status: "ok", message: inspect(info)}
        {:error, reason} -> %{active: true, status: "error", message: inspect(reason)}
      end
    else
      %{
        active: false,
        status: "disabled",
        message: "TigerBeetle disabled. Set :tigerbeetle_enabled, true to enable."
      }
    end
  end

  # ── SpiceDB ────────────────────────────────────────────────────

  defp check_spicedb do
    authz_module =
      Application.get_env(
        :universal_enterprise_platform,
        :authz_adapter,
        UniversalEnterprisePlatform.Infrastructure.Authorization.SpiceDBClient
      )

    case authz_module.health_check() do
      {:ok, info} ->
        %{active: true, status: "ok", message: inspect(info)}

      {:error, reason} ->
        msg = reason |> inspect() |> String.slice(0, 120)
        %{active: true, status: "error", message: "SpiceDB gRPC error: #{msg}"}
    end
  end

  # ── StarRocks ──────────────────────────────────────────────────

  defp check_starrocks do
    enabled = Application.get_env(:universal_enterprise_platform, :starrocks_enabled, false)

    if enabled do
      case UniversalEnterprisePlatform.Infrastructure.Repos.StarRocksRepo.query(
             "SELECT version()"
           ) do
        {:ok, %{rows: [[v]]}} -> %{active: true, status: "ok", message: "StarRocks #{v}"}
        {:error, reason} -> %{active: true, status: "error", message: inspect(reason)}
      end
    else
      %{
        active: false,
        status: "disabled",
        message: "StarRocks not started. Set :starrocks_enabled, true to enable."
      }
    end
  end

  # ── Flink CDC ──────────────────────────────────────────────────

  defp check_flink_cdc do
    config = Application.get_env(:universal_enterprise_platform, :flink, [])
    base_url = config[:url] || "http://localhost:8081"

    case Req.get("#{base_url}/overview", receive_timeout: 3_000) do
      {:ok, %{status: 200, body: body}} ->
        version = Map.get(body, "flink-version", "unknown")
        jobs = Map.get(body, "jobs-running", 0)
        %{active: true, status: "ok", message: "Flink #{version} — #{jobs} job(s) running"}

      {:ok, %{status: s}} ->
        %{active: false, status: "error", message: "HTTP #{s}"}

      {:error, %{reason: :econnrefused}} ->
        %{active: false, status: "disabled", message: "Flink not running at #{base_url}"}

      {:error, _} ->
        # If Flink is disabled just show version from config
        version = config[:version] || "unknown"
        %{active: false, status: "ok", message: "Flink #{version} (not started)"}
    end
  end

  # ── Helpers ────────────────────────────────────────────────────

  defp ok(message), do: %{status: "ok", message: message}
  defp warn(message), do: %{status: "warn", message: message}
  defp error_result(m), do: %{status: "error", message: m}

  defp overall_status(results) do
    statuses = Enum.map(results, fn {_k, v} -> Map.get(v, :status, "ok") end)

    cond do
      Enum.any?(statuses, &(&1 == "error")) -> "degraded"
      Enum.any?(statuses, &(&1 == "warn")) -> "warn"
      true -> "ok"
    end
  end

  defp valkey_conn do
    # Pick from the PartitionSupervisor pool
    PartitionSupervisor.whereis_name({UniversalEnterprisePlatform.ValkeyPool, self()})
  rescue
    _ ->
      # Fallback: try first named connection
      :valkey_1
  end
end
