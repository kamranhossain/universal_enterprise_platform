defmodule Platform.Infrastructure.Cache.Behaviour do
  @callback get(key :: String.t()) ::
              {:ok, any()} | {:error, :not_found} | {:error, any()}

  @callback put(key :: String.t(), value :: any(), ttl_seconds :: pos_integer()) ::
              :ok | {:error, any()}

  @callback delete(key :: String.t()) :: :ok | {:error, any()}

  @callback exists?(key :: String.t()) :: boolean()

  @callback flush_namespace(namespace :: String.t()) :: :ok | {:error, any()}
end
