defmodule Snaq.Topic.SupervisorTest do
  use ExUnit.Case, async: false
  @moduletag :skip

  test "ensure_topic starts a new topic process" do
    name = "ts-test-#{:erlang.unique_integer([:positive])}"
    assert :ok = Snaq.Topic.Supervisor.ensure_topic(name)
    assert is_pid(GenServer.whereis({:via, Registry, {Snaq.TopicRegistry, name}}))
  end

  test "ensure_topic is idempotent" do
    name = "ts-idem-#{:erlang.unique_integer([:positive])}"
    assert :ok = Snaq.Topic.Supervisor.ensure_topic(name)
    assert :ok = Snaq.Topic.Supervisor.ensure_topic(name)
  end
end
