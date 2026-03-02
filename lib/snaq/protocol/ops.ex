defmodule Snaq.Protocol.Ops do
  # Operation codes
  defmacro create_topic, do: 0x01
  defmacro produce, do: 0x02
  defmacro consume, do: 0x03
  defmacro create_queue, do: 0x06
  defmacro bind_queue, do: 0x07

  # Status codes
  defmacro ok, do: 0
  defmacro empty, do: 11
  defmacro topic_exists, do: 12
  defmacro not_found, do: 13
  defmacro bad_request, do: 400
  defmacro server_error, do: 500
end
