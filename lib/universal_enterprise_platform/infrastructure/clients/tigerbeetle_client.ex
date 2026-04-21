defmodule UniversalEnterprisePlatform.Infrastructure.Clients.TigerBeetleClient do
  @moduledoc """
  TigerBeetle financial ledger client.

  TigerBeetle owns: accounts, transfers (the double-entry ledger).
  PostgreSQL owns: invoices, payment records, PO references, everything else.

  Key design constraints:
    - All IDs are uint128. We map UUID tenant/account IDs to uint128
      using the first 16 bytes of the binary UUID.
    - Amounts are integers only — no floats. Use smallest currency unit
      (cents for USD/GBP, paise for INR, fils for AED).
    - Transfers are immutable. No updates or deletes.
    - Each tenant gets a dedicated ledger ID (derived from tenant UUID).

  Account ID encoding:
    We pack (tenant_id, account_type, sequence) into uint128 using
    a deterministic mapping stored in PostgreSQL. This lets us
    reconstruct account IDs without round-tripping to TigerBeetle.

  Ledger values (use consistent values across the cluster):
    1 = USD, 2 = EUR, 3 = GBP, 44 = BDT, 356 = INR, 682 = SAR
    Use ISO 4217 numeric codes as ledger IDs.
  """

  use GenServer
  require Logger

  @port Application.compile_env(:platform, :tigerbeetle_port, 3001)
  @cluster_id 0

  alias TigerBeetlex.{Account, AccountFlags, Transfer, TransferFlags}

  # ── Client API ─────────────────────────────────────────────────

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Create accounts in TigerBeetle.
  Called once per Chart of Accounts entry, not per transaction.

  account params:
    id         — uint128 (use uuid_to_uint128/1 helper)
    ledger     — ISO 4217 numeric (1=USD, 356=INR, etc.)
    code       — account type code (your Chart of Accounts numbering)
    flags      — TigerBeetle account flags (see TigerBeetlex.AccountFlags)
  """
  @spec create_accounts([map()]) :: {:ok, [map()]} | {:error, term()}
  def create_accounts(accounts) do
    GenServer.call(__MODULE__, {:create_accounts, accounts}, 10_000)
  end

  @doc """
  Record a double-entry transfer.

  transfer params:
    id              — uint128, idempotency key (use uuid_to_uint128/1)
    debit_account_id  — uint128
    credit_account_id — uint128
    ledger          — must match account ledger
    amount          — positive integer, smallest currency unit
    code            — your transfer type code (1=payment, 2=refund, etc.)
    user_data_128   — pack tenant_id here for audit trail
  """
  @spec create_transfers([map()]) :: {:ok, [map()]} | {:error, term()}
  def create_transfers(transfers) do
    GenServer.call(__MODULE__, {:create_transfers, transfers}, 10_000)
  end

  @doc "Look up accounts by ID list."
  @spec lookup_accounts([non_neg_integer()]) :: {:ok, [map()]} | {:error, term()}
  def lookup_accounts(ids) do
    GenServer.call(__MODULE__, {:lookup_accounts, ids}, 5_000)
  end

  @doc "Check TigerBeetle connection health."
  @spec health_check() :: {:ok, map()} | {:error, term()}
  def health_check do
    GenServer.call(__MODULE__, :health_check, 3_000)
  end

  # ── UUID ↔ uint128 helpers ────────────────────────────────────

  @doc """
  Convert a UUID string to a uint128 integer for TigerBeetle.
  UUIDs are 128-bit — they map perfectly to TigerBeetle IDs.
  """
  @spec uuid_to_uint128(String.t()) :: non_neg_integer()
  def uuid_to_uint128(uuid) do
    uuid
    |> String.replace("-", "")
    |> Base.decode16!(case: :mixed)
    |> :binary.decode_unsigned(:big)
  end

  @doc "Convert uint128 back to UUID string."
  @spec uint128_to_uuid(non_neg_integer()) :: String.t()
  def uint128_to_uuid(n) do
    <<a::32, b::16, c::16, d::16, e::48>> = <<n::128>>

    :io_lib.format(
      "~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b",
      [a, b, c, d, e]
    )
    |> to_string()
  end

  # ── GenServer ─────────────────────────────────────────────────

  @impl GenServer
  def init(_opts) do
    address = Application.get_env(:platform, :tigerbeetle_address, "3001")
    Logger.info("[TigerBeetle] Connecting to #{address}")

    case TigerBeetlex.connect(@cluster_id, [address]) do
      {:ok, client} ->
        Logger.info("[TigerBeetle] Connected")
        {:ok, %{client: client, address: address}}

      {:error, reason} ->
        Logger.error("[TigerBeetle] Connection failed: #{inspect(reason)}")
        {:stop, {:connection_failed, reason}}
    end
  end

  @impl GenServer
  def handle_call({:create_accounts, accounts}, _from, %{client: client} = state) do
    tb_accounts = Enum.map(accounts, &to_tigerbeetle_account/1)
    result = TigerBeetlex.create_accounts(client, tb_accounts)
    {:reply, result, state}
  end

  @impl GenServer
  def handle_call({:create_transfers, transfers}, _from, %{client: client} = state) do
    tb_transfers = Enum.map(transfers, &to_tigerbeetle_transfer/1)
    result = TigerBeetlex.create_transfers(client, tb_transfers)
    {:reply, result, state}
  end

  @impl GenServer
  def handle_call({:lookup_accounts, ids}, _from, %{client: client} = state) do
    result = TigerBeetlex.lookup_accounts(client, ids)
    {:reply, result, state}
  end

  @impl GenServer
  def handle_call(:health_check, _from, %{client: client, address: address} = state) do
    # Attempt a lookup of a non-existent account — if it responds, we're healthy
    case TigerBeetlex.lookup_accounts(client, [0]) do
      {:ok, _} -> {:reply, {:ok, %{address: address, status: "connected"}}, state}
      {:error, e} -> {:reply, {:error, inspect(e)}, state}
    end
  end

  # ── Private mappers ───────────────────────────────────────────

  defp to_tigerbeetle_account(params) do
    %Account{
      id: params.id,
      ledger: params.ledger,
      code: params.code,
      flags: Map.get(params, :flags, %AccountFlags{}),
      user_data_128: Map.get(params, :user_data_128, 0),
      user_data_64: Map.get(params, :user_data_64, 0),
      user_data_32: Map.get(params, :user_data_32, 0)
    }
  end

  defp to_tigerbeetle_transfer(params) do
    %Transfer{
      id: params.id,
      debit_account_id: params.debit_account_id,
      credit_account_id: params.credit_account_id,
      ledger: params.ledger,
      amount: params.amount,
      code: params.code,
      flags: Map.get(params, :flags, %TransferFlags{}),
      user_data_128: Map.get(params, :user_data_128, 0),
      user_data_64: Map.get(params, :user_data_64, 0),
      user_data_32: Map.get(params, :user_data_32, 0)
    }
  end
end
