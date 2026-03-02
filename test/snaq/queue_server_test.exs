defmodule Snaq.Queue.ServerTest do
  use ExUnit.Case, async: true

  alias Snaq.Queue.Server

  # Each test starts its own isolated queue server under a unique name.
  setup do
    name = "test-queue-#{:erlang.unique_integer([:positive])}"
    {:ok, _pid} = start_supervised({Server, name})
    %{name: name}
  end

  test "push then pop returns the message", %{name: name} do
    Server.push(name, "hello")
    assert Server.pop(name) == {:ok, "hello"}
  end

  test "pop on empty queue returns :empty", %{name: name} do
    assert Server.pop(name) == :empty
  end

  test "pop_wait returns immediately when message already queued", %{name: name} do
    Server.push(name, "immediate")
    assert Server.pop_wait(name, 5000) == {:ok, "immediate"}
  end

  test "pop_wait returns :empty after timeout when no message arrives", %{name: name} do
    assert Server.pop_wait(name, 100) == :empty
  end

  test "pop_wait unblocked by a concurrent push", %{name: name} do
    parent = self()

    Task.start(fn ->
      result = Server.pop_wait(name, 3000)
      send(parent, {:got, result})
    end)

    # Give the task time to register as a waiter before we push.
    Process.sleep(50)
    Server.push(name, "wake-up")

    assert_receive {:got, {:ok, "wake-up"}}, 2000
  end
end
