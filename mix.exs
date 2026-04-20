defmodule UniversalEnterprisePlatform.MixProject do
  use Mix.Project

  def project do
    [
      app: :universal_enterprise_platform,
      version: "0.1.0",
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps(),
      compilers: [:phoenix_live_view] ++ Mix.compilers(),
      listeners: [Phoenix.CodeReloader]
    ]
  end

  # Configuration for the OTP application.
  #
  # Type `mix help compile.app` for more information.
  def application do
    [
      mod: {UniversalEnterprisePlatform.Application, []},
      extra_applications: [:logger, :runtime_tools]
    ]
  end

  def cli do
    [
      preferred_envs: [precommit: :test]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Specifies your project dependencies.
  #
  # Type `mix help deps` for examples and options.
  defp deps do
    [
      # ── Data layer ─────────────────────────────────────────────
      {:ecto_sql, "~> 3.13"},
      {:phoenix_ecto, "~> 4.7"},
      {:postgrex, "~> 0.22.0"},
      # StarRocks MySQL wire
      {:myxql, "~> 0.8.1"},
      {:pgvector, "~> 0.3.1"},

      # ── Financial ledger ────────────────────────────────────────
      {:tigerbeetlex, "~> 0.16.78"},

      # ── Authorization ───────────────────────────────────────────
      # SpiceDB gRPC client
      {:authzed, "~> 1.6"},
      # gRPC transport for authzed
      {:grpc, "~> 0.10.0"},

      # ── Cache ───────────────────────────────────────
      {:redix, "~> 1.5"},

      # ── Messaging / Streaming ────────────────────────────────────────────────
      {:broadway, "~> 1.2"},
      # BroadwayKafka producer
      {:broadway_kafka, "~> 0.4.4"},
      # Kafka client (used by broadway_kafka)
      {:brod, "~> 4.5"},
      # LiveDashboard inspecting Broadway pipelines
      {:broadway_dashboard, "~> 0.4.1"},
      {:phoenix_pubsub, "~> 2.2"},

      # UUID v7
      {:uniq, "~> 0.6.2"},
      # Up-to-date CA certificate store
      {:castore, "~> 1.0"},

      # ── Web ────────────────────────────────────────────────────
      {:phoenix, "~> 1.8"},
      {:phoenix_live_view, "~> 1.1"},
      {:phoenix_live_reload, "~> 1.6", only: :dev},
      {:phoenix_html_helpers, "~> 1.0"},
      {:bandit, "~> 1.10"},
      {:plug_cowboy, "~> 2.8"},
      {:phoenix_html, "~> 4.3"},
      {:phoenix_live_dashboard, "~> 0.8.7"},
      {:lazy_html, "~> 0.1.11", only: :test},
      {:esbuild, "~> 0.10.0", runtime: Mix.env() == :dev},
      {:tailwind, "~> 0.4.1", runtime: Mix.env() == :dev},
      {:heroicons,
       github: "tailwindlabs/heroicons",
       tag: "v2.2.0",
       sparse: "optimized",
       app: false,
       compile: false,
       depth: 1},

      # ── Auth ────────────────────────────────────────────────────
      {:bcrypt_elixir, "~> 3.3"},
      {:joken, "~> 2.6"},

      # ── HTTP / utilities ────────────────────────────────────────
      {:req, "~> 0.5.17"},
      {:jason, "~> 1.4"},
      {:nimble_options, "~> 1.1"},
      {:telemetry, "~> 1.4"},
      {:telemetry_metrics, "~> 1.1"},
      {:telemetry_poller, "~> 1.3"},

      # ── Rust NIFs ────────────────────────────────────────────────────────────
      {:rustler, "~> 0.37"},
      {:rustler_precompiled, "~> 0.8.4", runtime: false},

      # ── Background Jobs ─────────────────────────────────────────────────────
      {:oban, "~> 2.20"},
      # Web UI for Oban (optional)
      {:oban_web, "~> 2.11"},
      # Cron-like job scheduler for Elixir
      {:quantum, "~> 3.5"},
      # Elixir library for parsing, writing, and calculating Cron format strings.
      {:crontab, "~> 1.2"},

      # ── Internationalization ──────────────────────────────────────────────────
      {:gettext, "~> 1.0"},

      # ── Email ──────────────────────────────────────────────────
      {:swoosh, "~> 1.25"},
      # ── Cluster ──────────────────────────────────────────────────

      {:dns_cluster, "~> 0.2.0"},

      # ── Boundary — compile-time layer enforcement ───────────────
      {:boundary, "~> 0.10.4"},

      # ── Dev/test ───────────────────────────────────────────────
      {:mix_test_watch, "~> 1.4", only: :dev, runtime: false},
      {:ex_machina, "~> 2.8", only: :test},
      {:faker, "~> 0.18.0", only: :test},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:sobelow, "~> 0.14.1", only: [:dev, :test], runtime: false},
      {:mix_audit, "~> 2.1", only: [:dev, :test], runtime: false}
    ]
  end

  # Aliases are shortcuts or tasks specific to the current project.
  # For example, to install project dependencies and perform other setup tasks, run:
  #
  #     $ mix setup
  #
  # See the documentation for `Mix` for more info on aliases.
  defp aliases do
    [
      setup: ["deps.get", "ecto.setup", "assets.setup", "assets.build"],
      "ecto.setup": ["ecto.create", "ecto.migrate", "run priv/repo/seeds.exs"],
      "ecto.reset": ["ecto.drop", "ecto.setup"],
      test: ["ecto.create --quiet", "ecto.migrate --quiet", "test"],
      "assets.setup": ["tailwind.install --if-missing", "esbuild.install --if-missing"],
      "assets.build": [
        "compile",
        "tailwind universal_enterprise_platform",
        "esbuild universal_enterprise_platform"
      ],
      "assets.deploy": [
        "tailwind universal_enterprise_platform --minify",
        "esbuild universal_enterprise_platform --minify",
        "phx.digest"
      ],
      precommit: ["compile --warnings-as-errors", "deps.unlock --unused", "format", "test"]
    ]
  end
end
