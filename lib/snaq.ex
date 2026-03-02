defmodule Snaq do
  defdelegate push(queue_name, data), to: Snaq.Broker
  defdelegate pop(queue_name), to: Snaq.Broker
  defdelegate pop_wait(queue_name, timeout_ms), to: Snaq.Broker
end
