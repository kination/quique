defmodule Snaq.Queue.Server do
  use GenServer
  require Logger

  # state: %{name, messages: :queue.new(), waiting: [{from, timer_ref}]}

  @doc "Starts a named queue GenServer registered via Registry."
  @spec start_link(String.t()) :: GenServer.on_start()
  def start_link(name) do
    GenServer.start_link(__MODULE__, name, name: via(name))
  end

  @doc "Enqueues `data` into the named queue (fire-and-forget)."
  @spec push(String.t(), binary()) :: :ok
  def push(name, data) do
    GenServer.cast(via(name), {:push, data})
  end

  @doc "Non-blocking dequeue. Returns `{:ok, data}` or `:empty`."
  @spec pop(String.t()) :: {:ok, binary()} | :empty
  def pop(name) do
    GenServer.call(via(name), :pop)
  end

  @doc "Blocks until a message arrives or `timeout_ms` elapses. Returns `{:ok, data}` or `:empty`."
  @spec pop_wait(String.t(), non_neg_integer()) :: {:ok, binary()} | :empty
  def pop_wait(name, timeout_ms) do
    GenServer.call(via(name), {:pop_wait, timeout_ms}, timeout_ms + 2000)
  end

  # Callbacks

  def init(name) do
    Logger.debug("Queue started: #{name}")
    {:ok, %{name: name, messages: :queue.new(), waiting: []}}
  end

  # push: if someone is waiting, reply immediately; otherwise enqueue
  def handle_cast({:push, data}, state) do
    case state.waiting do
      [{from, timer_ref} | rest] ->
        Process.cancel_timer(timer_ref)
        GenServer.reply(from, {:ok, data})
        {:noreply, %{state | waiting: rest}}

      [] ->
        {:noreply, %{state | messages: :queue.in(data, state.messages)}}
    end
  end

  # pop: non-blocking, returns :empty immediately if queue is empty
  def handle_call(:pop, _from, state) do
    case :queue.out(state.messages) do
      {{:value, msg}, q} -> {:reply, {:ok, msg}, %{state | messages: q}}
      {:empty, _} -> {:reply, :empty, state}
    end
  end

  # pop_wait: defer reply, store caller; timer will fire :empty on timeout
  def handle_call({:pop_wait, timeout_ms}, from, state) do
    case :queue.out(state.messages) do
      {{:value, msg}, q} ->
        {:reply, {:ok, msg}, %{state | messages: q}}

      {:empty, _} ->
        timer = Process.send_after(self(), {:timeout, from}, timeout_ms)
        {:noreply, %{state | waiting: [{from, timer} | state.waiting]}}
    end
  end

  def handle_info({:timeout, from}, state) do
    GenServer.reply(from, :empty)
    waiting = Enum.reject(state.waiting, fn {f, _} -> f == from end)
    {:noreply, %{state | waiting: waiting}}
  end

  defp via(name), do: {:via, Registry, {Snaq.QueueRegistry, name}}
end
