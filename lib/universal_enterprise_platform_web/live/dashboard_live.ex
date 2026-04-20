defmodule PlatformWeb.DashboardLive do
  use PlatformWeb, :live_view

  @impl true
  def mount(_params, _session, socket) do
    {:ok, assign(socket, :page_title, "Dashboard")}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div class="p-8">
      <h1 class="text-2xl font-medium">Platform MVP</h1>
      <p class="mt-2 text-gray-500">Infrastructure is running.</p>
      <.link href="/health" class="mt-4 inline-block text-blue-600 underline">
        /health — check all connections
      </.link>
    </div>
    """
  end
end
