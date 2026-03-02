defmodule Snaq.Protocol.Codec do
  # Request:  <<version::8, op::8, stream_id::32, body_len::32>> = 10 bytes
  # Response: <<stream_id::32, status::16, body_len::16>>         =  8 bytes

  @req_header_size 10

  def req_header_size, do: @req_header_size

  def decode_req_header(<<_version::8, op::8, stream_id::32, body_len::32>>) do
    {:ok, %{op: op, stream_id: stream_id, body_len: body_len}}
  end

  def decode_req_header(_), do: {:error, :invalid_header}

  def encode_response(stream_id, status, body \\ <<>>) do
    <<stream_id::32, status::16, byte_size(body)::16, body::binary>>
  end

  # TLV encode helpers — each returns just its encoded segment
  def put_str(s), do: <<byte_size(s)::16, s::binary>>
  def put_bytes(b), do: <<byte_size(b)::32, b::binary>>
  def put_u32(v), do: <<v::32>>

  # TLV decode helpers — return {value, rest}
  def get_str(<<len::16, str::binary-size(len), rest::binary>>), do: {str, rest}
  def get_bytes(<<len::32, data::binary-size(len), rest::binary>>), do: {data, rest}
  def get_u32(<<val::32, rest::binary>>), do: {val, rest}
end
