defmodule UniversalEnterprisePlatformWeb.Router do
  use UniversalEnterprisePlatformWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {UniversalEnterprisePlatformWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
  end

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/", UniversalEnterprisePlatformWeb do
    pipe_through :browser

    get "/", PageController, :home
    get "/health", HealthController, :index
  end

  # Other scopes may use custom stacks.
  # ── API v1 ────────────────────────────────────────────────────
  scope "/api/v1", PlatformWeb.API.V1 do
    pipe_through :api
    # Feature routes added per layer as you build them
  end

  # ── Browser / LiveView ────────────────────────────────────────
  scope "/", PlatformWeb do
    pipe_through :browser
    live "/", DashboardLive, :index
  end

  # Enable LiveDashboard and Swoosh mailbox preview in development
  if Application.compile_env(:universal_enterprise_platform, :dev_routes) do
    # If you want to use the LiveDashboard in production, you should put
    # it behind authentication and allow only admins to access it.
    # If your application does not have an admins-only section yet,
    # you can use Plug.BasicAuth to set up some basic authentication
    # as long as you are also using SSL (which you should anyway).
    import Phoenix.LiveDashboard.Router

    scope "/dev" do
      pipe_through :browser

      live_dashboard "/dashboard", metrics: UniversalEnterprisePlatformWeb.Telemetry
      forward "/mailbox", Plug.Swoosh.MailboxPreview
    end
  end
end
