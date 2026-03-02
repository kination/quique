defmodule Snaq.BrokerTest do
  use ExUnit.Case, async: false

  alias Snaq.Broker

  test "produce auto-creates topic+queue+binding and delivers message" do
    topic = "broker-prod-#{:erlang.unique_integer([:positive])}"
    assert :ok = Broker.produce(topic, "msg1")
    assert Broker.pop(topic) == {:ok, "msg1"}
  end

  test "produce fans out to multiple bound queues" do
    topic = "broker-fanout-#{:erlang.unique_integer([:positive])}"
    q2 = "broker-q2-#{:erlang.unique_integer([:positive])}"
    :ok = Broker.create_topic(topic)
    :ok = Broker.create_queue(topic)
    :ok = Broker.create_queue(q2)
    :ok = Broker.bind(topic, topic)
    :ok = Broker.bind(topic, q2)

    :ok = Broker.produce(topic, "fan")

    assert Broker.pop(topic) == {:ok, "fan"}
    assert Broker.pop(q2) == {:ok, "fan"}
  end

  test "create_topic is idempotent" do
    topic = "broker-ct-#{:erlang.unique_integer([:positive])}"
    assert :ok = Broker.create_topic(topic)
    assert :ok = Broker.create_topic(topic)
  end

  test "create_queue is idempotent" do
    q = "broker-cq-#{:erlang.unique_integer([:positive])}"
    assert :ok = Broker.create_queue(q)
    assert :ok = Broker.create_queue(q)
  end
end
