defmodule Snaq.TCP.Handler do
  require Logger

  alias Snaq.Protocol.{Codec, Ops}
  require Ops
  alias Snaq.Broker

  def handle(socket) do
    case :gen_tcp.recv(socket, Codec.req_header_size()) do
      {:ok, header_bin} ->
        with {:ok, %{op: op, stream_id: sid, body_len: body_len}} <-
               Codec.decode_req_header(header_bin),
             {:ok, body} <- recv_body(socket, body_len) do
          response = dispatch(op, sid, body)
          :gen_tcp.send(socket, response)
          handle(socket)
        else
          {:error, reason} ->
            Logger.warning("Protocol error: #{inspect(reason)}")
            :gen_tcp.close(socket)
        end

      {:error, :closed} ->
        :ok

      {:error, reason} ->
        Logger.warning("Socket recv error: #{inspect(reason)}")
        :ok
    end
  end

  # CreateTopic: body = put_str(topic_name)
  defp dispatch(op, sid, body) when op == Ops.create_topic() do
    {topic_name, _} = Codec.get_str(body)

    case Broker.create_topic(topic_name) do
      :ok -> Codec.encode_response(sid, Ops.ok())
      _ -> Codec.encode_response(sid, Ops.server_error())
    end
  end

  # Produce: body = put_str(topic_name) <> put_bytes(data)
  defp dispatch(op, sid, body) when op == Ops.produce() do
    {topic_name, rest} = Codec.get_str(body)
    {data, _} = Codec.get_bytes(rest)

    case Broker.produce(topic_name, data) do
      :ok -> Codec.encode_response(sid, Ops.ok())
      _ -> Codec.encode_response(sid, Ops.server_error())
    end
  end

  # Consume: body = put_str(queue_name) <> put_u32(timeout_ms)
  defp dispatch(op, sid, body) when op == Ops.consume() do
    {queue_name, rest} = Codec.get_str(body)
    {timeout_ms, _} = Codec.get_u32(rest)

    result =
      if timeout_ms == 0,
        do: Broker.pop(queue_name),
        else: Broker.pop_wait(queue_name, timeout_ms)

    case result do
      {:ok, data} -> Codec.encode_response(sid, Ops.ok(), Codec.put_bytes(data))
      :empty -> Codec.encode_response(sid, Ops.empty())
      _ -> Codec.encode_response(sid, Ops.server_error())
    end
  end

  # CreateQueue: body = put_str(queue_name)
  defp dispatch(op, sid, body) when op == Ops.create_queue() do
    {queue_name, _} = Codec.get_str(body)

    case Broker.create_queue(queue_name) do
      :ok -> Codec.encode_response(sid, Ops.ok())
      _ -> Codec.encode_response(sid, Ops.server_error())
    end
  end

  # BindQueue: body = put_str(topic_name) <> put_str(queue_name)
  defp dispatch(op, sid, body) when op == Ops.bind_queue() do
    {topic_name, rest} = Codec.get_str(body)
    {queue_name, _} = Codec.get_str(rest)

    case Broker.bind(topic_name, queue_name) do
      :ok -> Codec.encode_response(sid, Ops.ok())
      _ -> Codec.encode_response(sid, Ops.server_error())
    end
  end

  defp dispatch(_op, sid, _body) do
    Codec.encode_response(sid, Ops.bad_request())
  end

  defp recv_body(_socket, 0), do: {:ok, <<>>}
  defp recv_body(socket, len), do: :gen_tcp.recv(socket, len)
end
