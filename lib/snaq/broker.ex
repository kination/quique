defmodule Snaq.Broker do
  alias Snaq.Queue

  def push(queue_name, data) do
    :ok = Queue.Supervisor.ensure_queue(queue_name)
    Queue.Server.push(queue_name, data)
    :ok
  end

  def pop(queue_name) do
    :ok = Queue.Supervisor.ensure_queue(queue_name)
    Queue.Server.pop(queue_name)
  end

  def pop_wait(queue_name, timeout_ms) do
    :ok = Queue.Supervisor.ensure_queue(queue_name)
    Queue.Server.pop_wait(queue_name, timeout_ms)
  end
end
