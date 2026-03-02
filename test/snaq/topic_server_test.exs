defmodule Snaq.Topic.ServerTest do
  use ExUnit.Case, async: true

  alias Snaq.Topic.Server

  setup do
    name = "topic-#{:erlang.unique_integer([:positive])}"
    {:ok, _pid} = start_supervised({Server, name})
    %{name: name}
  end

  test "new topic has no bound queues", %{name: name} do
    assert Server.bound_queues(name) == []
  end

  test "bind adds a queue name", %{name: name} do
    :ok = Server.bind(name, "q1")
    assert Server.bound_queues(name) == ["q1"]
  end

  test "bind is idempotent", %{name: name} do
    :ok = Server.bind(name, "q1")
    :ok = Server.bind(name, "q1")
    assert Server.bound_queues(name) == ["q1"]
  end

  test "fanout pushes data to all bound queues", %{name: name} do
    # Start real queue processes for the queues we'll bind
    q1 = "fanout-q1-#{:erlang.unique_integer([:positive])}"
    q2 = "fanout-q2-#{:erlang.unique_integer([:positive])}"
    {:ok, _} = start_supervised({Snaq.Queue.Server, q1})
    {:ok, _} = start_supervised({Snaq.Queue.Server, q2})

    :ok = Server.bind(name, q1)
    :ok = Server.bind(name, q2)
    :ok = Server.fanout(name, "hello")

    assert Snaq.Queue.Server.pop(q1) == {:ok, "hello"}
    assert Snaq.Queue.Server.pop(q2) == {:ok, "hello"}
  end
end
