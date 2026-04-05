defmodule UniversalEnterprisePlatformWeb.PageController do
  use UniversalEnterprisePlatformWeb, :controller

  def home(conn, _params) do
    render(conn, :home)
  end
end
