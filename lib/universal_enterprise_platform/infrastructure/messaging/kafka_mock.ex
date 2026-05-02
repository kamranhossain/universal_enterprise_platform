defmodule UniversalEnterprisePlatform.Infrastructure.Messaging.KafkaMock do
  @behaviour UniversalEnterprisePlatform.Infrastructure.Messaging.KafkaBehaviour
  use GenServer

  def start_link(_), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)
  def init(_), do: {:ok, %{messages: []}}

  @impl true
  def produce(topic, key, value, _opts) do
    GenServer.cast(__MODULE__, {:produce, topic, key, value})
    :ok
  end

  @impl true
  def produce_batch(topic, key, messages) do
    Enum.each(messages, &GenServer.cast(__MODULE__, {:produce, topic, key, &1}))
    :ok
  end

  @impl true
  def health_check, do: {:ok, %{status: :connected, adapter: :mock, mode: "KRaft"}}

  def messages_for(topic) do
    GenServer.call(__MODULE__, {:messages_for, topic})
  end

  def reset, do: GenServer.cast(__MODULE__, :reset)

  def handle_cast({:produce, topic, key, value}, state) do
    {:noreply, %{state | messages: [{topic, key, value} | state.messages]}}
  end

  def handle_cast(:reset, state), do: {:noreply, %{state | messages: []}}

  def handle_call({:messages_for, topic}, _from, state) do
    msgs =
      state.messages
      |> Enum.filter(fn {t, _, _} -> t == topic end)
      |> Enum.reverse()

    {:reply, msgs, state}
  end
end
