defmodule Snaq.CLI do
  @default_host "127.0.0.1"
  @default_port 7001

  # Request header: <<version::8, op::8, stream_id::32, body_len::32>> = 10 bytes
  # Response header: <<stream_id::32, status::16, body_len::16>> = 8 bytes

  def main(args) do
    {opts, argv, _} =
      OptionParser.parse(args,
        strict: [
          topic: :string,
          queue: :string,
          data: :string,
          timeout: :integer,
          host: :string,
          port: :integer
        ]
      )

    case argv do
      ["produce" | _] -> run_produce(opts)
      ["consume" | _] -> run_consume(opts)
      ["create-topic" | _] -> run_create_topic(opts)
      ["create-queue" | _] -> run_create_queue(opts)
      ["bind-queue" | _] -> run_bind_queue(opts)
      _ -> usage()
    end
  end

  defp run_produce(opts) do
    topic = fetch!(opts, :topic, "produce")
    data = fetch!(opts, :data, "produce")

    with_socket(opts, fn socket ->
      body = put_str(topic) <> put_bytes(data)
      send_request(socket, 0x02, body)

      case recv_response(socket, 5000) do
        {:ok, %{status: 0}} -> IO.puts("OK")
        {:ok, %{status: s}} -> IO.puts("Error: status #{s}")
        {:error, r} -> IO.puts("Error: #{inspect(r)}")
      end
    end)
  end

  defp run_consume(opts) do
    queue = fetch!(opts, :queue, "consume")
    timeout_ms = Keyword.get(opts, :timeout, 0)
    recv_timeout = if timeout_ms > 0, do: timeout_ms + 5000, else: 5000

    with_socket(opts, fn socket ->
      body = put_str(queue) <> put_u32(timeout_ms)
      send_request(socket, 0x03, body)

      case recv_response(socket, recv_timeout) do
        {:ok, %{status: 0, body: b}} ->
          <<len::32, data::binary-size(len)>> = b
          IO.puts(data)

        {:ok, %{status: 11}} ->
          IO.puts("(empty)")

        {:ok, %{status: s}} ->
          IO.puts("Error: status #{s}")

        {:error, r} ->
          IO.puts("Error: #{inspect(r)}")
      end
    end)
  end

  defp run_create_topic(opts) do
    topic = fetch!(opts, :topic, "create-topic")

    with_socket(opts, fn socket ->
      body = put_str(topic)
      send_request(socket, 0x01, body)

      case recv_response(socket, 5000) do
        {:ok, %{status: 0}} -> IO.puts("OK")
        {:ok, %{status: s}} -> IO.puts("Error: status #{s}")
        {:error, r} -> IO.puts("Error: #{inspect(r)}")
      end
    end)
  end

  defp run_create_queue(opts) do
    queue = fetch!(opts, :queue, "create-queue")

    with_socket(opts, fn socket ->
      body = put_str(queue)
      send_request(socket, 0x06, body)

      case recv_response(socket, 5000) do
        {:ok, %{status: 0}} -> IO.puts("OK")
        {:ok, %{status: s}} -> IO.puts("Error: status #{s}")
        {:error, r} -> IO.puts("Error: #{inspect(r)}")
      end
    end)
  end

  defp run_bind_queue(opts) do
    topic = fetch!(opts, :topic, "bind-queue")
    queue = fetch!(opts, :queue, "bind-queue")

    with_socket(opts, fn socket ->
      body = put_str(topic) <> put_str(queue)
      send_request(socket, 0x07, body)

      case recv_response(socket, 5000) do
        {:ok, %{status: 0}} -> IO.puts("OK")
        {:ok, %{status: s}} -> IO.puts("Error: status #{s}")
        {:error, r} -> IO.puts("Error: #{inspect(r)}")
      end
    end)
  end

  defp with_socket(opts, fun) do
    host = Keyword.get(opts, :host, @default_host) |> String.to_charlist()
    port = Keyword.get(opts, :port, @default_port)

    case :gen_tcp.connect(host, port, [:binary, packet: :raw, active: false]) do
      {:ok, socket} ->
        fun.(socket)
        :gen_tcp.close(socket)

      {:error, reason} ->
        IO.puts("Connection failed (#{@default_host}:#{port}): #{inspect(reason)}")
        System.halt(1)
    end
  end

  defp send_request(socket, op, body) do
    header = <<1::8, op::8, 1::32, byte_size(body)::32>>
    :gen_tcp.send(socket, header <> body)
  end

  defp recv_response(socket, timeout) do
    case :gen_tcp.recv(socket, 8, timeout) do
      {:ok, <<sid::32, status::16, body_len::16>>} ->
        body =
          if body_len > 0 do
            {:ok, b} = :gen_tcp.recv(socket, body_len, timeout)
            b
          else
            <<>>
          end

        {:ok, %{stream_id: sid, status: status, body: body}}

      {:error, _} = err ->
        err
    end
  end

  defp put_str(s), do: <<byte_size(s)::16, s::binary>>
  defp put_bytes(b) when is_binary(b), do: <<byte_size(b)::32, b::binary>>
  defp put_u32(v), do: <<v::32>>

  defp fetch!(opts, key, cmd) do
    case Keyword.fetch(opts, key) do
      {:ok, v} -> v
      :error -> IO.puts("Missing --#{key} for #{cmd}") && System.halt(1)
    end
  end

  defp usage do
    IO.puts("""
    Usage:
      snaq-cli produce --topic <name> --data <data>
      snaq-cli consume --queue <name> [--timeout <ms>]
      snaq-cli create-topic --topic <name>
      snaq-cli create-queue --queue <name>
      snaq-cli bind-queue --topic <name> --queue <name>

    Options:
      --host <host>   Server host (default: 127.0.0.1)
      --port <port>   Server port (default: 7001)
    """)
  end
end
