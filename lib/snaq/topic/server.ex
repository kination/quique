defmodule Snaq.Topic.Server do
  use GenServer

  # state: %{name: String.t(), bound_queues: [String.t()]}

  @doc "Starts a named topic GenServer registered via Registry."
  @spec start_link(String.t()) :: GenServer.on_start()
  def start_link(name) do
    GenServer.start_link(__MODULE__, name, name: via(name))
  end

  def child_spec(name) do
    %{id: {__MODULE__, name}, start: {__MODULE__, :start_link, [name]}}
  end

  @doc "Returns the list of queue names bound to this topic."
  @spec bound_queues(String.t()) :: [String.t()]
  def bound_queues(name) do
    GenServer.call(via(name), :bound_queues)
  end

  @doc "Binds a queue to this topic (idempotent)."
  @spec bind(String.t(), String.t()) :: :ok
  def bind(topic_name, queue_name) do
    GenServer.call(via(topic_name), {:bind, queue_name})
  end

  @doc "Pushes data to every queue bound to this topic."
  @spec fanout(String.t(), binary()) :: :ok
  def fanout(topic_name, data) do
    GenServer.call(via(topic_name), {:fanout, data})
  end

  # Callbacks

  def init(name) do
    {:ok, %{name: name, bound_queues: []}}
  end

  def handle_call(:bound_queues, _from, state) do
    {:reply, state.bound_queues, state}
  end

  def handle_call({:bind, queue_name}, _from, state) do
    if queue_name in state.bound_queues do
      {:reply, :ok, state}
    else
      {:reply, :ok, %{state | bound_queues: [queue_name | state.bound_queues]}}
    end
  end

  def handle_call({:fanout, data}, _from, state) do
    Enum.each(state.bound_queues, fn q_name ->
      Snaq.Queue.Server.push(q_name, data)
    end)

    {:reply, :ok, state}
  end

  defp via(name), do: {:via, Registry, {Snaq.TopicRegistry, name}}
end
