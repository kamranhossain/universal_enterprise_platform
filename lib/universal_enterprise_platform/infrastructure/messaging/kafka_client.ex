defmodule UniversalEnterprisePlatform.Infrastructure.Messaging.KafkaClient do
  @moduledoc """
  Kafka client backed by brod.
  Works with both KRaft (no ZooKeeper) and classic Kafka.
  The client doesn't know or care — it connects directly to brokers.

  KRaft note: brokers handle metadata internally.
  No ZooKeeper address needed. Just the broker list.

  Topics (defined in config):
    platform.events  — domain events (replaces PubSub for cross-node)
    platform.gps     — high-volume GPS telemetry
    platform.audit   — immutable audit stream
    platform.webhooks— outbound webhook queue

  Usage:
    KafkaClient.produce("platform.events", tenant_id, Jason.encode!(event))
    KafkaClient.produce_batch("platform.gps", tenant_id, points)
  """

  @behaviour UniversalEnterprisePlatform.Infrastructure.Messaging.KafkaBehaviour

  use GenServer
  require Logger

  @client_name :platform_kafka

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # ── Public API ────────────────────────────────────────────────

  @impl true
  def produce(topic, key, value, opts \\ []) do
    partition = Keyword.get(opts, :partition, :hash)
    headers = Keyword.get(opts, :headers, [])

    partition_id =
      case partition do
        :hash -> hash_partition(key, topic)
        n when is_integer(n) -> n
      end

    msg = %{value: value, key: key, headers: headers, ts: :os.system_time(:millisecond)}

    case :brod.produce_sync(@client_name, topic, partition_id, key, msg) do
      :ok -> :ok
      {:ok, _offset} -> :ok
      {:error, reason} -> {:error, reason}
    end
  rescue
    e -> {:error, Exception.message(e)}
  end

  @impl true
  def produce_batch(topic, key, messages) when is_list(messages) do
    partition_id = hash_partition(key, topic)

    batch =
      Enum.map(messages, fn msg ->
        %{value: msg, key: key, ts: :os.system_time(:millisecond)}
      end)

    case :brod.produce_sync_offset(@client_name, topic, partition_id, key, batch) do
      {:ok, _offset} -> :ok
      {:error, reason} -> {:error, reason}
    end
  rescue
    e -> {:error, Exception.message(e)}
  end

  @impl true
  def health_check do
    config = Application.get_env(:universal_enterprise_platform, :kafka, [])
    brokers = Keyword.get(config, :brokers, [{"localhost", 9092}])

    case :brod.get_metadata(@client_name, :all) do
      {:ok, metadata} ->
        broker_count = length(metadata.brokers)
        topic_count = length(metadata.topics)

        # Detect KRaft vs classic by checking controller info
        # KRaft clusters have controller_id set; classic uses ZooKeeper
        mode =
          if Map.get(metadata, :controller_id) != nil,
            do: "KRaft",
            else: "Classic"

        {:ok,
         %{
           status: :connected,
           mode: mode,
           brokers: broker_count,
           topics: topic_count,
           broker_list: Enum.map(brokers, fn {h, p} -> "#{h}:#{p}" end)
         }}

      {:error, reason} ->
        {:error, inspect(reason)}
    end
  rescue
    _ ->
      # Try TCP connection as fallback health indicator
      config = Application.get_env(:universal_enterprise_platform, :kafka, [])
      brokers = Keyword.get(config, :brokers, [{"localhost", 9092}])
      {host, port} = List.first(brokers) || {"localhost", 9092}

      case :gen_tcp.connect(to_charlist(host), port, [], 3_000) do
        {:ok, sock} ->
          :gen_tcp.close(sock)
          {:ok, %{status: :reachable, mode: "KRaft", host: host, port: port}}

        {:error, reason} ->
          {:error, "Cannot reach broker #{host}:#{port} — #{inspect(reason)}"}
      end
  end

  # ── GenServer ─────────────────────────────────────────────────

  @impl GenServer
  def init(_opts) do
    config = Application.get_env(:universal_enterprise_platform, :kafka, [])
    brokers = Keyword.get(config, :brokers, [{"localhost", 9092}])

    # brod client config
    client_config = [
      auto_start_producers: true,
      allow_topic_auto_creation: false,
      default_producer_config: [
        # wait for all in-sync replicas
        required_acks: -1,
        ack_timeout: 10_000,
        compression: :snappy
      ]
    ]

    case :brod.start_client(brokers, @client_name, client_config) do
      :ok ->
        Logger.info("[Kafka] brod client started — brokers: #{inspect(brokers)}")
        ensure_topics_exist(config)
        {:ok, %{status: :connected, config: config}}

      {:error, {:already_started, _}} ->
        {:ok, %{status: :connected, config: config}}

      {:error, reason} ->
        Logger.warning("[Kafka] Failed to start brod client: #{inspect(reason)}")
        # Start anyway — brod will retry internally
        {:ok, %{status: :degraded, config: config, error: reason}}
    end
  end

  # ── Private ───────────────────────────────────────────────────

  defp hash_partition(key, topic) do
    # Consistent hashing by key ensures same key always goes to same partition
    # Required for ordered processing per tenant/entity
    case :brod.get_partitions_count(@client_name, topic) do
      {:ok, count} when count > 0 ->
        :erlang.phash2(key) |> rem(count)

      _ ->
        0
    end
  end

  defp ensure_topics_exist(config) do
    topics = Keyword.get(config, :topics, [])

    Enum.each(topics, fn topic ->
      case :brod.get_partitions_count(@client_name, topic) do
        {:ok, _} ->
          :ok

        {:error, _} ->
          Logger.warning("[Kafka] Topic '#{topic}' not found. Create it manually:")

          Logger.warning(
            "  kafka-topics --create --bootstrap-server localhost:9092 --topic #{topic} --partitions 3 --replication-factor 1"
          )
      end
    end)
  end
end
