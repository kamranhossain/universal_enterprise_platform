defmodule Platform do
  @moduledoc """
  L1 — Infrastructure layer.

  May be called by any layer above it.
  Calls nothing above itself — no kernel, no domain, no web.
  """
  use Boundary,
    # depends on nothing internal
    deps: [],
    exports: [
      Repo,
      TimescaleRepo,
      StarRocksRepo,
      Infrastructure.TenantContext,
      Infrastructure.QueryRouter,
      Infrastructure.Cache.Behaviour,
      Infrastructure.Cache.EtsAdapter,
      Infrastructure.Cache.ValkeyAdapter,
      Infrastructure.Flink.CdcConfig,
      Infrastructure.Health.Checker
    ]
end
