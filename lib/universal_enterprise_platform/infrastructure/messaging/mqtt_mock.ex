defmodule UniversalEnterprisePlatform.Infrastructure.Messaging.MqttMock do
  @behaviour UniversalEnterprisePlatform.Infrastructure.Messaging.MqttBehaviour
  use GenServer

  def start_link(_), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)
  def init(_), do: {:ok, %{messages: []}}

  @impl true
  def publish(topic, payload, _opts) do
    GenServer.cast(__MODULE__, {:publish, topic, payload})
    :ok
  end

  @impl true
  def subscribe(_topic, _qos), do: :ok

  @impl true
  def unsubscribe(_topic), do: :ok

  @impl true
  def health_check, do: {:ok, %{status: :connected, adapter: :mock}}

  def published_messages do
    GenServer.call(__MODULE__, :messages)
  end

  def reset, do: GenServer.cast(__MODULE__, :reset)

  def handle_cast({:publish, topic, payload}, state) do
    {:noreply, %{state | messages: [{topic, payload} | state.messages]}}
  end

  def handle_cast(:reset, state), do: {:noreply, %{state | messages: []}}
  def handle_call(:messages, _from, state), do: {:reply, Enum.reverse(state.messages), state}
end
