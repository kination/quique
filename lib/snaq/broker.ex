defmodule Snaq.Broker do
  alias Snaq.Queue

  @doc "Pushes `data` onto the named queue, auto-creating the queue if it does not exist."
  @spec push(String.t(), binary()) :: :ok
  def push(queue_name, data) do
    :ok = Queue.Supervisor.ensure_queue(queue_name)
    Queue.Server.push(queue_name, data)
    :ok
  end

  @doc "Non-blocking dequeue from the named queue, auto-creating if needed. Returns `{:ok, data}` or `:empty`."
  @spec pop(String.t()) :: {:ok, binary()} | :empty
  def pop(queue_name) do
    :ok = Queue.Supervisor.ensure_queue(queue_name)
    Queue.Server.pop(queue_name)
  end

  @doc "Blocking dequeue with `timeout_ms` wait, auto-creating queue if needed. Returns `{:ok, data}` or `:empty`."
  @spec pop_wait(String.t(), non_neg_integer()) :: {:ok, binary()} | :empty
  def pop_wait(queue_name, timeout_ms) do
    :ok = Queue.Supervisor.ensure_queue(queue_name)
    Queue.Server.pop_wait(queue_name, timeout_ms)
  end
end
