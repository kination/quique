defmodule Snaq.Topic.Supervisor do
  use DynamicSupervisor

  def start_link(_opts) do
    DynamicSupervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(:ok) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc "Starts a named topic process under this supervisor (idempotent)."
  @spec ensure_topic(String.t()) :: :ok | {:error, term()}
  def ensure_topic(name) do
    case DynamicSupervisor.start_child(__MODULE__, {Snaq.Topic.Server, name}) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      error -> error
    end
  end
end
