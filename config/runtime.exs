# config/runtime.exs
import Config

# Enable the Phoenix server in releases when PHX_SERVER is set.
# Use System.get_env/1 instead of fetch_env!/1 so it does not crash
# when PHX_SERVER is not defined.
if System.get_env("PHX_SERVER") do
  config :universal_enterprise_platform,
         UniversalEnterprisePlatformWeb.Endpoint,
         server: true
end

# Configure HTTP port for all environments (default: 4000)
config :universal_enterprise_platform,
       UniversalEnterprisePlatformWeb.Endpoint,
       http: [port: String.to_integer(System.get_env("PORT", "4000"))]

if config_env() == :prod do
  # ---------------------------------------------------------------------------
  # Database Configuration
  # ---------------------------------------------------------------------------
  database_url =
    System.get_env("DATABASE_URL") ||
      raise """
      environment variable DATABASE_URL is missing.
      Example:
        ecto://USER:PASS@HOST/DATABASE
      """

  maybe_ipv6 =
    if System.get_env("ECTO_IPV6") in ~w(true 1),
      do: [:inet6],
      else: []

  config :universal_enterprise_platform,
         UniversalEnterprisePlatform.Repo,
         url: database_url,
         pool_size: String.to_integer(System.get_env("POOL_SIZE", "10")),
         socket_options: maybe_ipv6

  # ssl: true

  # ---------------------------------------------------------------------------
  # Secret Key Base
  # ---------------------------------------------------------------------------
  secret_key_base =
    System.get_env("SECRET_KEY_BASE") ||
      raise """
      environment variable SECRET_KEY_BASE is missing.
      Generate one with:
        mix phx.gen.secret
      """

  # ---------------------------------------------------------------------------
  # Endpoint Configuration
  # ---------------------------------------------------------------------------
  host = System.get_env("PHX_HOST", "example.com")

  config :universal_enterprise_platform,
         :dns_cluster_query,
         System.get_env("DNS_CLUSTER_QUERY")

  config :universal_enterprise_platform,
         UniversalEnterprisePlatformWeb.Endpoint,
         url: [host: host, port: 443, scheme: "https"],
         http: [
           # Bind to all IPv4 interfaces.
           # Use {:local, "/path/to/socket"} if deploying behind a Unix socket.
           ip: {0, 0, 0, 0}
         ],
         secret_key_base: secret_key_base

  # ---------------------------------------------------------------------------
  # Meilisearch Configuration
  # ---------------------------------------------------------------------------
  config :universal_enterprise_platform,
         :meilisearch,
         url: System.get_env("MEILI_URL", "http://127.0.0.1:7700"),
         api_key: System.get_env("MEILI_API_KEY")

  # ---------------------------------------------------------------------------
  # Optional Mailer Configuration (example)
  # ---------------------------------------------------------------------------
  # config :universal_enterprise_platform, UniversalEnterprisePlatform.Mailer,
  #   adapter: Swoosh.Adapters.Mailgun,
  #   api_key: System.get_env("MAILGUN_API_KEY"),
  #   domain: System.get_env("MAILGUN_DOMAIN")
end
