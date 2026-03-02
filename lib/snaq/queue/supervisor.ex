defmodule Snaq.Queue.Supervisor do
  use DynamicSupervisor

  def start_link(_opts) do
    DynamicSupervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(:ok) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  # Idempotent: safe to call even if queue already exists
  def ensure_queue(name) do
    case DynamicSupervisor.start_child(__MODULE__, {Snaq.Queue.Server, name}) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      error -> error
    end
  end
end
