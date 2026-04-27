defmodule UniversalEnterprisePlatform.Infrastructure.Clients.TigerBeetleClient do
  @moduledoc """
  TigerBeetle financial ledger client.

  TigerBeetle owns: accounts, transfers (the double-entry ledger).
  PostgreSQL owns: invoices, payment records, PO references, everything else.

  Key design constraints:
    - All IDs are uint128 binaries (<<n::128>>). Use `uuid_to_id/1` to convert
      UUID strings. Use `TigerBeetlex.ID.from_int/1` for integer IDs.
    - Amounts are integers only — no floats. Use smallest currency unit
      (cents for USD/GBP, paise for INR, fils for AED).
    - Transfers are immutable. No updates or deletes.

  Ledger values (ISO 4217 numeric codes):
    1 = USD, 2 = EUR, 3 = GBP, 44 = BDT, 356 = INR, 682 = SAR
  """

  require Logger

  alias TigerBeetlex.{Account, AccountFlags, Transfer, TransferFlags}

  @conn_name :tigerbeetle

  # ── child_spec — called by the supervisor when passed as a bare module ──────

  def child_spec(_opts) do
    config = config!()

    %{
      id: __MODULE__,
      # Delegate start to TigerBeetlex.Connection directly — no wrapper GenServer
      start:
        {TigerBeetlex.Connection, :start_link,
         [
           [
             name: @conn_name,
             cluster_id: TigerBeetlex.ID.from_int(config[:cluster_id]),
             addresses: config[:addresses]
           ]
         ]},
      type: :worker,
      restart: :permanent,
      shutdown: 5_000
    }
  end

  # ── Accounts ────────────────────────────────────────────────────────────────

  @doc """
  Create accounts in TigerBeetle.
  Called once per Chart of Accounts entry, not per transaction.

  Required keys per account map:
    :id     — 16-byte binary (use `uuid_to_id/1` or `TigerBeetlex.ID.from_int/1`)
    :ledger — ISO 4217 numeric (1=USD, 356=INR, etc.)
    :code   — account type code (your Chart of Accounts numbering)

  Optional:
    :flags         — %AccountFlags{}  (default: no flags)
    :user_data_128 — pack tenant_id here for audit trail (default: 0)
    :user_data_64  — (default: 0)
    :user_data_32  — (default: 0)
  """
  @spec create_accounts([map()]) :: :ok | {:error, term()}
  def create_accounts(accounts) when is_list(accounts) do
    tb_accounts = Enum.map(accounts, &to_tigerbeetle_account/1)

    case TigerBeetlex.Connection.create_accounts(@conn_name, tb_accounts) do
      {:ok, []} -> :ok
      {:ok, errors} -> {:error, errors}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Record a double-entry transfer.

  Required keys per transfer map:
    :id               — 16-byte binary, idempotency key
    :debit_account_id — 16-byte binary
    :credit_account_id — 16-byte binary
    :ledger           — must match both account ledgers
    :amount           — positive integer, smallest currency unit
    :code             — transfer type (1=payment, 2=refund, etc.)

  Optional:
    :flags         — %TransferFlags{}  (default: no flags)
    :user_data_128 — pack tenant_id here for audit trail
    :user_data_64
    :user_data_32
  """
  @spec create_transfers([map()]) :: :ok | {:error, term()}
  def create_transfers(transfers) when is_list(transfers) do
    tb_transfers = Enum.map(transfers, &to_tigerbeetle_transfer/1)

    case TigerBeetlex.Connection.create_transfers(@conn_name, tb_transfers) do
      {:ok, []} -> :ok
      {:ok, errors} -> {:error, errors}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Look up accounts by ID list (16-byte binaries)."
  @spec lookup_accounts([binary()]) :: {:ok, [Account.t()]} | {:error, term()}
  def lookup_accounts(ids) when is_list(ids) do
    TigerBeetlex.Connection.lookup_accounts(@conn_name, ids)
  end

  @doc "Look up a single account. Returns {:error, :not_found} when missing."
  @spec lookup_account(binary()) ::
          {:ok, Account.t()} | {:error, :not_found} | {:error, term()}
  def lookup_account(id) do
    case lookup_accounts([id]) do
      {:ok, [account]} -> {:ok, account}
      {:ok, []} -> {:error, :not_found}
      {:error, _} = err -> err
    end
  end

  @doc "Look up transfers by ID list (16-byte binaries)."
  @spec lookup_transfers([binary()]) :: {:ok, [Transfer.t()]} | {:error, term()}
  def lookup_transfers(ids) when is_list(ids) do
    TigerBeetlex.Connection.lookup_transfers(@conn_name, ids)
  end

  @doc "Check TigerBeetle connection health."
  @spec health_check() :: {:ok, map()} | {:error, term()}
  def health_check do
    # Use a well-known non-zero ID that won't exist — an empty result is still a
    # successful round-trip, which is all we need to confirm liveness.
    probe_id = TigerBeetlex.ID.from_int(0xDEADBEEF)

    case TigerBeetlex.Connection.lookup_accounts(@conn_name, [probe_id]) do
      {:ok, _} ->
        config = config!()

        {:ok,
         %{addresses: config[:addresses], cluster_id: config[:cluster_id], status: "connected"}}

      {:error, reason} ->
        {:error, inspect(reason)}
    end
  end

  # ── UUID ↔ TigerBeetle ID helpers ──────────────────────────────────────────

  @doc """
  Convert a UUID string to a 16-byte binary suitable for TigerBeetle IDs.
  UUIDs are 128-bit — they map perfectly to TigerBeetle's uint128.

      iex> TigerBeetleClient.uuid_to_id("550e8400-e29b-41d4-a716-446655440000")
      <<85, 14, 132, 0, 226, 155, 65, 212, 167, 22, 68, 102, 85, 64, 0, 0>>
  """
  @spec uuid_to_id(String.t()) :: binary()
  def uuid_to_id(uuid) do
    uuid
    |> String.replace("-", "")
    |> Base.decode16!(case: :mixed)
  end

  @doc "Convert a 16-byte TigerBeetle ID back to a UUID string."
  @spec id_to_uuid(binary()) :: String.t()
  def id_to_uuid(<<a::32, b::16, c::16, d::16, e::48>>) do
    :io_lib.format(
      "~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b",
      [a, b, c, d, e]
    )
    |> to_string()
  end

  # ── Private mappers ─────────────────────────────────────────────────────────

  defp to_tigerbeetle_account(params) do
    %Account{
      id: Map.fetch!(params, :id),
      ledger: Map.fetch!(params, :ledger),
      code: Map.fetch!(params, :code),
      flags: Map.get(params, :flags, %AccountFlags{}),
      user_data_128: Map.get(params, :user_data_128, 0),
      user_data_64: Map.get(params, :user_data_64, 0),
      user_data_32: Map.get(params, :user_data_32, 0)
    }
  end

  defp to_tigerbeetle_transfer(params) do
    %Transfer{
      id: Map.fetch!(params, :id),
      debit_account_id: Map.fetch!(params, :debit_account_id),
      credit_account_id: Map.fetch!(params, :credit_account_id),
      ledger: Map.fetch!(params, :ledger),
      amount: Map.fetch!(params, :amount),
      code: Map.fetch!(params, :code),
      flags: Map.get(params, :flags, %TransferFlags{}),
      user_data_128: Map.get(params, :user_data_128, 0),
      user_data_64: Map.get(params, :user_data_64, 0),
      user_data_32: Map.get(params, :user_data_32, 0)
    }
  end

  defp config! do
    case Application.get_env(:universal_enterprise_platform, :tigerbeetle) do
      nil ->
        raise """
        [TigerBeetleClient] Missing config. Add to your config files:

            config :universal_enterprise_platform, :tigerbeetle,
              cluster_id: 0,
              addresses: ["127.0.0.1:3001"]
        """

      config ->
        config
    end
  end
end
