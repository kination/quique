defmodule Snaq.Protocol.Ops do
  # Operation codes
  defmacro produce, do: 0x02
  defmacro consume, do: 0x03

  # Status codes
  defmacro ok, do: 0
  defmacro empty, do: 11
  defmacro bad_request, do: 400
  defmacro server_error, do: 500
end
