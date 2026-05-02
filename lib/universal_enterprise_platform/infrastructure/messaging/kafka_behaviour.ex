defmodule UniversalEnterprisePlatform.Infrastructure.Messaging.KafkaBehaviour do
  @callback produce(topic :: String.t(), key :: binary(), value :: binary(), opts :: keyword()) ::
              :ok | {:error, term()}

  @callback produce_batch(topic :: String.t(), key :: binary(), messages :: [binary()]) ::
              :ok | {:error, term()}

  @callback health_check() :: {:ok, map()} | {:error, term()}

  def impl do
    Application.get_env(
      :universal_enterprise_platform,
      :kafka_adapter,
      UniversalEnterprisePlatform.Infrastructure.Messaging.KafkaClient
    )
  end
end
