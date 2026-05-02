defmodule UniversalEnterprisePlatform.Infrastructure.Messaging.MqttClient do
  @moduledoc """
  MQTT client backed by emqtt (EMQX Erlang client).
  Handles connect/reconnect, publish, subscribe.
  Topics follow the pattern: platform/{tenant_id}/{resource}/{event}

  Used for:
    - IoT device telemetry ingestion (GPS, OEE sensors)
    - Field agent mobile app real-time events
    - Third-party system integrations

  For internal BEAM events use Phoenix.PubSub.
  MQTT is the gateway for external/IoT clients.
  """

  @behaviour UniversalEnterprisePlatform.Infrastructure.Messaging.MqttBehaviour

  use GenServer
  require Logger

  @reconnect_delay 5_000
  @qos_at_least_once 1

  defstruct [:client, :config, :status, :subscriptions]

  # ── Public API ────────────────────────────────────────────────

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def publish(topic, payload, opts \\ []) do
    GenServer.call(__MODULE__, {:publish, topic, payload, opts})
  end

  @impl true
  def subscribe(topic, qos \\ @qos_at_least_once) do
    GenServer.call(__MODULE__, {:subscribe, topic, qos})
  end

  @impl true
  def unsubscribe(topic) do
    GenServer.call(__MODULE__, {:unsubscribe, topic})
  end

  @impl true
  def health_check do
    GenServer.call(__MODULE__, :health_check, 5_000)
  rescue
    _ -> {:error, :mqtt_unavailable}
  end

  def connected? do
    GenServer.call(__MODULE__, :connected?, 3_000)
  rescue
    _ -> false
  end

  # Helpers for topic building
  def topic(tenant_id, resource, event),
    do: "platform/#{tenant_id}/#{resource}/#{event}"

  def gps_topic(tenant_id),
    do: "platform/#{tenant_id}/gps/location"

  def oee_topic(tenant_id, work_centre),
    do: "platform/#{tenant_id}/oee/#{work_centre}"

  def command_topic(tenant_id, device_id),
    do: "platform/#{tenant_id}/cmd/#{device_id}"

  # ── GenServer callbacks ───────────────────────────────────────

  @impl GenServer
  def init(_opts) do
    config = Application.get_env(:universal_enterprise_platform, :mqtt, [])

    state = %__MODULE__{
      client: nil,
      config: config,
      status: :disconnected,
      subscriptions: []
    }

    # Connect asynchronously so supervisor doesn't block
    send(self(), :connect)
    {:ok, state}
  end

  @impl GenServer
  def handle_info(:connect, state) do
    case do_connect(state.config) do
      {:ok, client} ->
        Logger.info("[MQTT] Connected to #{state.config[:host]}:#{state.config[:port]}")
        {:noreply, %{state | client: client, status: :connected}}

      {:error, reason} ->
        Logger.warning(
          "[MQTT] Connection failed: #{inspect(reason)}. Retrying in #{@reconnect_delay}ms"
        )

        Process.send_after(self(), :connect, @reconnect_delay)
        {:noreply, %{state | status: :disconnected}}
    end
  end

  def handle_info({:disconnected, reason, _props}, state) do
    Logger.warning("[MQTT] Disconnected: #{inspect(reason)}. Reconnecting...")
    Process.send_after(self(), :connect, @reconnect_delay)
    {:noreply, %{state | status: :disconnected, client: nil}}
  end

  def handle_info({:publish, %{topic: topic, payload: payload, qos: _qos}}, state) do
    # Incoming message — forward to Phoenix.PubSub for BEAM consumers
    Phoenix.PubSub.broadcast(
      UniversalEnterprisePlatform.PubSub,
      "mqtt:#{topic}",
      {:mqtt_message, topic, payload}
    )

    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl GenServer
  def handle_call({:publish, topic, payload, opts}, _from, %{status: :connected} = state) do
    qos = Keyword.get(opts, :qos, @qos_at_least_once)
    retain = Keyword.get(opts, :retain, false)

    result = :emqtt.publish(state.client, topic, payload, [{:qos, qos}, {:retain, retain}])
    {:reply, result, state}
  end

  def handle_call({:publish, _topic, _payload, _opts}, _from, state) do
    {:reply, {:error, :not_connected}, state}
  end

  def handle_call({:subscribe, topic, qos}, _from, %{status: :connected} = state) do
    result = :emqtt.subscribe(state.client, {topic, qos})
    subs = [{topic, qos} | state.subscriptions] |> Enum.uniq()
    {:reply, result, %{state | subscriptions: subs}}
  end

  def handle_call({:subscribe, _topic, _qos}, _from, state) do
    {:reply, {:error, :not_connected}, state}
  end

  def handle_call({:unsubscribe, topic}, _from, state) do
    if state.status == :connected do
      :emqtt.unsubscribe(state.client, topic)
    end

    subs = Enum.reject(state.subscriptions, fn {t, _} -> t == topic end)
    {:reply, :ok, %{state | subscriptions: subs}}
  end

  def handle_call(:health_check, _from, state) do
    result =
      case state.status do
        :connected ->
          host = state.config[:host] || "localhost"
          port = state.config[:port] || 1883

          {:ok,
           %{
             status: :connected,
             host: host,
             port: port,
             subscriptions: length(state.subscriptions)
           }}

        :disconnected ->
          {:error, :disconnected}
      end

    {:reply, result, state}
  end

  def handle_call(:connected?, _from, state) do
    {:reply, state.status == :connected, state}
  end

  # ── Private ───────────────────────────────────────────────────

  defp do_connect(config) do
    host = config[:host] || "localhost"
    port = config[:port] || 1883
    client_id = to_string(config[:client_id] || "platform_#{:os.system_time(:millisecond)}")

    opts = [
      {:host, to_charlist(host)},
      {:port, port},
      {:clientid, client_id},
      {:clean_start, Keyword.get(config, :clean_session, true)},
      {:keepalive, Keyword.get(config, :keepalive, 60)},
      {:owner, self()}
    ]

    opts =
      if config[:username] do
        opts ++
          [
            {:username, to_charlist(config[:username])},
            {:password, to_charlist(config[:password] || "")}
          ]
      else
        opts
      end

    case :emqtt.start_link(opts) do
      {:ok, client} ->
        case :emqtt.connect(client) do
          {:ok, _connack} ->
            {:ok, client}

          {:error, reason} ->
            :emqtt.stop(client)
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end
end
