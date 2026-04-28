defmodule UniversalEnterprisePlatform.Infrastructure.Clients.SpiceDBClient do
  @moduledoc """
  SpiceDB authorization client — Google Zanzibar model.

  Replaces: custom RBAC, ABAC, PolicyEngine, UnifiedAuthorizer.
  Every permission check in the system goes through check_permission/3.

  The three operations you'll use 90% of the time:
    check_permission/3  — "can user X do action Y on resource Z?"
    write_relationship/3 — "grant user X role Y on resource Z"
    delete_relationship/3 — "revoke user X role Y on resource Z"

  SpiceDB uses a consistency token (ZedToken) to prevent
  the "New Enemy Problem" — always pass the zed_token returned
  from write operations back to subsequent checks.

  Object type format:  "organization", "work_order", "invoice"
  Object ID format:    UUID strings
  Permission format:   "view", "edit", "approve", "manage"
  Subject type format: "user"
  """

  alias Authzed.Api.V1.{
    Client,
    GRPCUtil,
    CheckPermissionRequest,
    WriteRelationshipsRequest,
    DeleteRelationshipsRequest,
    ObjectReference,
    SubjectReference,
    Relationship,
    RelationshipUpdate,
    RelationshipFilter
  }

  @consistency_full_consistency %Authzed.Api.V1.Consistency{
    requirement: {:fully_consistent, true}
  }

  # ── Connection ────────────────────────────────────────────────

  def client do
    endpoint =
      Application.get_env(:universal_enterprise_platform, :spicedb_endpoint, "localhost:50051")

    token = Application.get_env(:universal_enterprise_platform, :spicedb_token, "local_dev_key")

    Client.new(endpoint, GRPCUtil.insecure_bearer_auth_token(token))
  end

  def health_check do
    c = client()
    # Check if we can connect by writing a no-op schema validation
    case Authzed.Api.V1.SchemaService.Stub.read_schema(
           c.channel,
           %Authzed.Api.V1.ReadSchemaRequest{},
           metadata: c.metadata
         ) do
      {:ok, resp} ->
        {:ok,
         %{
           schema_length: String.length(resp.schema_text),
           endpoint: Application.get_env(:universal_enterprise_platform, :spicedb_endpoint)
         }}

      {:error, %GRPC.RPCError{} = err} ->
        {:error, "SpiceDB gRPC error: #{err.message}"}

      {:error, reason} ->
        {:error, inspect(reason)}
    end
  rescue
    e -> {:error, Exception.message(e)}
  end

  # ── Permission check ─────────────────────────────────────────

  @doc """
  Check if a user has a permission on an object.

  Returns :allowed | :denied | {:error, reason}

  Examples:
    check_permission("user", user_id, "edit", "work_order", work_order_id)
    check_permission("user", user_id, "approve", "invoice", invoice_id)
    check_permission("user", user_id, "manage", "organization", org_id)
  """
  @spec check_permission(
          subject_type :: String.t(),
          subject_id :: String.t(),
          permission :: String.t(),
          resource_type :: String.t(),
          resource_id :: String.t()
        ) :: :allowed | :denied | {:error, term()}
  def check_permission(subject_type, subject_id, permission, resource_type, resource_id) do
    c = client()

    request =
      CheckPermissionRequest.new(
        consistency: @consistency_full_consistency,
        resource: object_ref(resource_type, resource_id),
        permission: permission,
        subject: subject_ref(subject_type, subject_id)
      )

    case Authzed.Api.V1.PermissionsService.Stub.check_permission(
           c.channel,
           request,
           metadata: c.metadata
         ) do
      {:ok, %{permissionship: :PERMISSIONSHIP_HAS_PERMISSION}} -> :allowed
      {:ok, _} -> :denied
      {:error, reason} -> {:error, reason}
    end
  rescue
    e -> {:error, Exception.message(e)}
  end

  @doc "Shorthand — returns boolean. Use when you don't need :error distinction."
  def can?(subject_type, subject_id, permission, resource_type, resource_id) do
    check_permission(subject_type, subject_id, permission, resource_type, resource_id) ==
      :allowed
  end

  # ── Relationship writes ───────────────────────────────────────

  @doc """
  Grant a relationship.

  Examples:
    write_relationship("organization", org_id, "admin", "user", user_id)
    write_relationship("organization", org_id, "member", "user", user_id)
    write_relationship("work_order", wo_id, "assigned_to", "user", user_id)
  """
  @spec write_relationship(
          resource_type :: String.t(),
          resource_id :: String.t(),
          relation :: String.t(),
          subject_type :: String.t(),
          subject_id :: String.t()
        ) :: {:ok, map()} | {:error, term()}
  def write_relationship(resource_type, resource_id, relation, subject_type, subject_id) do
    c = client()

    update =
      RelationshipUpdate.new(
        operation: :OPERATION_TOUCH,
        relationship:
          Relationship.new(
            resource: object_ref(resource_type, resource_id),
            relation: relation,
            subject: subject_ref(subject_type, subject_id)
          )
      )

    request = WriteRelationshipsRequest.new(updates: [update])

    case Authzed.Api.V1.PermissionsService.Stub.write_relationships(
           c.channel,
           request,
           metadata: c.metadata
         ) do
      {:ok, resp} -> {:ok, %{zed_token: resp.written_at}}
      {:error, r} -> {:error, r}
    end
  rescue
    e -> {:error, Exception.message(e)}
  end

  @doc "Revoke a relationship."
  @spec delete_relationship(
          resource_type :: String.t(),
          resource_id :: String.t(),
          relation :: String.t(),
          subject_type :: String.t(),
          subject_id :: String.t()
        ) :: :ok | {:error, term()}
  def delete_relationship(resource_type, resource_id, relation, subject_type, subject_id) do
    c = client()

    update =
      RelationshipUpdate.new(
        operation: :OPERATION_DELETE,
        relationship:
          Relationship.new(
            resource: object_ref(resource_type, resource_id),
            relation: relation,
            subject: subject_ref(subject_type, subject_id)
          )
      )

    case Authzed.Api.V1.PermissionsService.Stub.write_relationships(
           c.channel,
           WriteRelationshipsRequest.new(updates: [update]),
           metadata: c.metadata
         ) do
      {:ok, _} -> :ok
      {:error, r} -> {:error, r}
    end
  rescue
    e -> {:error, Exception.message(e)}
  end

  # ── Private ───────────────────────────────────────────────────

  defp object_ref(type, id) do
    ObjectReference.new(object_type: type, object_id: id)
  end

  defp subject_ref(type, id) do
    SubjectReference.new(object: ObjectReference.new(object_type: type, object_id: id))
  end
end
