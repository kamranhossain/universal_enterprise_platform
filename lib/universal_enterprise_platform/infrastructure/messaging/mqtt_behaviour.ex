defmodule UniversalEnterprisePlatform.Infrastructure.Messaging.MqttBehaviour do
  @callback publish(topic :: String.t(), payload :: binary(), opts :: keyword()) ::
              :ok | {:error, term()}

  @callback subscribe(topic :: String.t(), qos :: 0 | 1 | 2) ::
              :ok | {:error, term()}

  @callback unsubscribe(topic :: String.t()) :: :ok

  @callback health_check() :: {:ok, map()} | {:error, term()}

  def impl do
    Application.get_env(
      :universal_enterprise_platform,
      :mqtt_adapter,
      UniversalEnterprisePlatform.Infrastructure.Messaging.MqttClient
    )
  end
end
