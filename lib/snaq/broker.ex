defmodule Snaq.Broker do
  alias Snaq.{Queue, Topic}

  @doc "Produces `data` to `topic_name`, auto-creating topic + same-name queue + binding if missing."
  @spec produce(String.t(), binary()) :: :ok
  def produce(topic_name, data) do
    :ok = ensure_topic(topic_name)
    :ok = ensure_queue(topic_name)
    :ok = ensure_binding(topic_name, topic_name)
    Topic.Server.fanout(topic_name, data)
  end

  @doc "Explicitly creates a topic (idempotent)."
  @spec create_topic(String.t()) :: :ok
  def create_topic(name), do: ensure_topic(name)

  @doc "Explicitly creates a queue (idempotent)."
  @spec create_queue(String.t()) :: :ok
  def create_queue(name), do: ensure_queue(name)

  @doc "Binds `queue_name` to `topic_name`, auto-creating both if needed."
  @spec bind(String.t(), String.t()) :: :ok
  def bind(topic_name, queue_name) do
    :ok = ensure_topic(topic_name)
    :ok = ensure_queue(queue_name)
    Topic.Server.bind(topic_name, queue_name)
  end

  @doc "Non-blocking dequeue from `queue_name`. Returns `{:ok, data}` or `:empty`."
  @spec pop(String.t()) :: {:ok, binary()} | :empty
  def pop(queue_name) do
    :ok = ensure_queue(queue_name)
    Queue.Server.pop(queue_name)
  end

  @doc "Blocking dequeue from `queue_name` with `timeout_ms` wait."
  @spec pop_wait(String.t(), non_neg_integer()) :: {:ok, binary()} | :empty
  def pop_wait(queue_name, timeout_ms) do
    :ok = ensure_queue(queue_name)
    Queue.Server.pop_wait(queue_name, timeout_ms)
  end

  defp ensure_topic(name), do: Topic.Supervisor.ensure_topic(name)
  defp ensure_queue(name), do: Queue.Supervisor.ensure_queue(name)

  defp ensure_binding(topic_name, queue_name) do
    Topic.Server.bind(topic_name, queue_name)
  end
end
