defmodule Platform.Infrastructure.Search.MeilisearchAdapter do
  @moduledoc """
  Production-grade Meilisearch adapter.

  Features:
  - Runtime config (no compile_env)
  - Safe auth handling
  - Strong error handling
  - Retry + timeout
  - Tenant-aware indexing
  - Health + readiness checks
  - Just now in check

  Index strategy:
    "#{tenant_short}_#{resource}"
  """

  require Logger

  # ─────────────────────────────────────────────────────────────
  # Config
  # ─────────────────────────────────────────────────────────────

  defp config do
    Application.get_env(:platform, :meilisearch, [])
  end

  defp base_url do
    config()[:url] || "http://127.0.0.1:7700"
  end

  defp api_key do
    config()[:api_key]
  end

  defp headers(include_auth \\ true) do
    base = [{"Content-Type", "application/json"}]

    if include_auth and api_key() do
      [{"Authorization", "Bearer #{api_key()}"} | base]
    else
      base
    end
  end

  defp req_opts(headers) do
    [
      headers: headers,
      receive_timeout: 5_000,
      retry: :transient,
      max_retries: 2
    ]
  end

  # ─────────────────────────────────────────────────────────────
  # Index naming
  # ─────────────────────────────────────────────────────────────

  defp index_name(tenant_id, resource) do
    short =
      tenant_id
      |> String.replace("-", "")
      |> String.slice(0, 8)

    "#{short}_#{resource}"
  end

  # ─────────────────────────────────────────────────────────────
  # Public API
  # ─────────────────────────────────────────────────────────────

  # ── Indexing

  def index_document(tenant_id, resource, doc) do
    index = index_name(tenant_id, resource)
    post("/indexes/#{index}/documents", [doc])
  end

  def index_documents(tenant_id, resource, docs) when is_list(docs) do
    index = index_name(tenant_id, resource)
    post("/indexes/#{index}/documents", docs)
  end

  def delete_document(tenant_id, resource, id) do
    index = index_name(tenant_id, resource)
    delete("/indexes/#{index}/documents/#{id}")
  end

  # ── Search

  def search(tenant_id, resource, query, opts \\ []) do
    index = index_name(tenant_id, resource)

    body =
      %{
        q: query,
        limit: Keyword.get(opts, :limit, 20),
        offset: Keyword.get(opts, :offset, 0)
      }
      |> maybe_put(:filter, opts[:filter])
      |> maybe_put(:sort, opts[:sort])

    with {:ok, %{"hits" => hits}} <- post("/indexes/#{index}/search", body) do
      {:ok, hits}
    end
  end

  # ── Index lifecycle

  def create_tenant_indexes(tenant_id) do
    resources = ~w[customers products employees work_orders invoices knowledge]

    Enum.reduce_while(resources, :ok, fn resource, _ ->
      index = index_name(tenant_id, resource)

      with {:ok, _} <- create_index_if_missing(index),
           {:ok, _} <- configure_index(index, resource) do
        {:cont, :ok}
      else
        err ->
          Logger.error("Meilisearch index setup failed: #{inspect(err)}")
          {:halt, err}
      end
    end)
  end

  # ─────────────────────────────────────────────────────────────
  # Health & Readiness
  # ─────────────────────────────────────────────────────────────

  def health_check do
    case Req.get("#{base_url()}/health", receive_timeout: 2_000) do
      {:ok, %{status: 200, body: %{"status" => "available"}}} ->
        {:ok, "Meilisearch alive"}

      {:error, %{reason: :econnrefused}} ->
        {:error, :unreachable}

      {:error, %{reason: :timeout}} ->
        {:error, :timeout}

      {:ok, %{status: s}} ->
        {:error, "HTTP #{s}"}

      {:error, e} ->
        {:error, e}
    end
  end

  # stronger check (auth + DB working)

  def readiness_check do
    case get("/indexes") do
      {:ok, _} -> {:ok, "Meilisearch ready"}
      {:error, {401, _}} -> {:error, :auth_failed}
      err -> err
    end
  end

  # ─────────────────────────────────────────────────────────────
  # Internal helpers
  # ─────────────────────────────────────────────────────────────

  defp create_index_if_missing(index) do
    case post("/indexes", %{uid: index, primaryKey: "id"}) do
      {:ok, _} -> {:ok, :created}
      {:error, {409, _}} -> {:ok, :exists}
      err -> err
    end
  end

  defp configure_index(index, "customers") do
    patch("/indexes/#{index}/settings", %{
      searchableAttributes: ["name", "email", "phone", "code"],
      filterableAttributes: ["status", "tenant_id", "territory_id"],
      sortableAttributes: ["name", "inserted_at"]
    })
  end

  defp configure_index(index, "products") do
    patch("/indexes/#{index}/settings", %{
      searchableAttributes: ["name", "sku", "description"],
      filterableAttributes: ["status", "category_id"],
      sortableAttributes: ["name", "price"]
    })
  end

  defp configure_index(index, _resource) do
    patch("/indexes/#{index}/settings", %{
      filterableAttributes: ["status", "tenant_id"],
      sortableAttributes: ["inserted_at"]
    })
  end

  # ─────────────────────────────────────────────────────────────
  # HTTP layer
  # ─────────────────────────────────────────────────────────────

  defp get(path, auth \\ true) do
    request(:get, path, nil, auth)
  end

  defp post(path, body) do
    request(:post, path, body, true)
  end

  defp patch(path, body) do
    request(:patch, path, body, true)
  end

  defp delete(path) do
    request(:delete, path, nil, true)
  end

  defp request(method, path, body, auth?) do
    url = "#{base_url()}#{path}"
    opts = req_opts(headers(auth?))

    response =
      case method do
        :get -> Req.get(url, opts)
        :post -> Req.post(url, Keyword.put(opts, :json, body))
        :patch -> Req.patch(url, Keyword.put(opts, :json, body))
        :delete -> Req.delete(url, opts)
      end

    normalize_response(response)
  end

  defp normalize_response({:ok, %{status: s, body: b}}) when s in [200, 201, 202] do
    {:ok, b}
  end

  defp normalize_response({:ok, %{status: s, body: b}}) do
    {:error, {s, b}}
  end

  defp normalize_response({:error, e}) do
    {:error, e}
  end

  defp maybe_put(map, _k, nil), do: map
  defp maybe_put(map, k, v), do: Map.put(map, k, v)
end
