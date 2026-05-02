defmodule Platform.Infrastructure.Search.MeilisearchAdapter do
  @moduledoc """
  Meilisearch adapter — per-tenant indexes, typo tolerance, instant search.

  Index naming: "{tenant_id}_{resource}" e.g. "019f_customers"
  Tenant isolation is achieved by separate indexes per tenant.

  Deferred for MVP if Meilisearch is not running — all functions
  return {:error, :search_unavailable} gracefully.
  """

  @base_url Application.compile_env(:platform, :meilisearch_url, "http://localhost:7700")
  @api_key Application.compile_env(:platform, :meilisearch_key, "masterKey")

  defp headers,
    do: [
      {"Content-Type", "application/json"},
      {"Authorization", "Bearer #{@api_key}"}
    ]

  defp index_name(tenant_id, resource) do
    short = String.replace(tenant_id, "-", "") |> String.slice(0, 8)
    "#{short}_#{resource}"
  end

  # ── Indexing ────────────────────────────────────────────────────

  def index_document(tenant_id, resource, document) do
    index = index_name(tenant_id, resource)
    post("/indexes/#{index}/documents", [document])
  end

  def index_documents(tenant_id, resource, documents) when is_list(documents) do
    index = index_name(tenant_id, resource)
    post("/indexes/#{index}/documents", documents)
  end

  def delete_document(tenant_id, resource, id) do
    index = index_name(tenant_id, resource)
    delete("/indexes/#{index}/documents/#{id}")
  end

  # ── Search ──────────────────────────────────────────────────────

  @doc """
  Search within a tenant's resource index.

  opts:
    limit:    max results (default 20)
    offset:   pagination offset
    filter:   Meilisearch filter string e.g. "status = 'active'"
    sort:     list of sort strings e.g. ["name:asc"]
  """
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

    case post("/indexes/#{index}/search", body) do
      {:ok, %{"hits" => hits}} -> {:ok, hits}
      {:error, _} = err -> err
    end
  end

  # ── Index management ────────────────────────────────────────────

  def create_tenant_indexes(tenant_id) do
    resources = ~w[customers products employees work_orders invoices knowledge]

    Enum.each(resources, fn resource ->
      index = index_name(tenant_id, resource)
      post("/indexes", %{uid: index, primaryKey: "id"})
      configure_index(index, resource)
    end)

    :ok
  end

  defp configure_index(index, "customers") do
    patch("/indexes/#{index}/settings", %{
      searchableAttributes: ["name", "email", "phone", "code"],
      filterableAttributes: ["status", "tenant_id", "territory_id"],
      sortableAttributes: ["name", "inserted_at"],
      typoTolerance: %{enabled: true, minWordSizeForTypos: %{oneTypo: 4, twoTypos: 8}}
    })
  end

  defp configure_index(index, "products") do
    patch("/indexes/#{index}/settings", %{
      searchableAttributes: ["name", "sku", "description", "category"],
      filterableAttributes: ["status", "category_id"],
      sortableAttributes: ["name", "price", "inserted_at"]
    })
  end

  defp configure_index(index, _resource) do
    patch("/indexes/#{index}/settings", %{
      filterableAttributes: ["status", "tenant_id"],
      sortableAttributes: ["inserted_at"]
    })
  end

  # ── Health ──────────────────────────────────────────────────────

  def health_check do
    case get("/health") do
      {:ok, %{"status" => "available"}} -> {:ok, "Meilisearch available"}
      {:ok, body} -> {:error, "Unexpected: #{inspect(body)}"}
      {:error, _} -> {:error, :search_unavailable}
    end
  end

  # ── HTTP helpers ─────────────────────────────────────────────────

  defp get(path) do
    case Req.get("#{@base_url}#{path}", headers: headers()) do
      {:ok, %{status: 200, body: body}} -> {:ok, body}
      {:ok, %{status: s, body: b}} -> {:error, {s, b}}
      {:error, e} -> {:error, e}
    end
  end

  defp post(path, body) do
    case Req.post("#{@base_url}#{path}", json: body, headers: headers()) do
      {:ok, %{status: s, body: b}} when s in [200, 201, 202] -> {:ok, b}
      {:ok, %{status: s, body: b}} -> {:error, {s, b}}
      {:error, e} -> {:error, e}
    end
  end

  defp patch(path, body) do
    case Req.patch("#{@base_url}#{path}", json: body, headers: headers()) do
      {:ok, %{status: s, body: b}} when s in [200, 202] -> {:ok, b}
      {:ok, %{status: s, body: b}} -> {:error, {s, b}}
      {:error, e} -> {:error, e}
    end
  end

  defp delete(path) do
    case Req.delete("#{@base_url}#{path}", headers: headers()) do
      {:ok, _} -> :ok
      {:error, e} -> {:error, e}
    end
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, val), do: Map.put(map, key, val)
end
