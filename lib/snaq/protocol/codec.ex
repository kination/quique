defmodule Snaq.Protocol.Codec do
  # Request:  <<version::8, op::8, stream_id::32, body_len::32>> = 10 bytes
  # Response: <<stream_id::32, status::16, body_len::16>>         =  8 bytes

  @req_header_size 10

  @doc "Returns the fixed request header size in bytes (10)."
  @spec req_header_size() :: 10
  def req_header_size, do: @req_header_size

  @doc "Decodes a 10-byte request header binary. Returns `{:ok, map}` or `{:error, :invalid_header}`."
  @spec decode_req_header(binary()) ::
          {:ok, %{op: byte(), stream_id: non_neg_integer(), body_len: non_neg_integer()}}
          | {:error, :invalid_header}
  def decode_req_header(<<_version::8, op::8, stream_id::32, body_len::32>>) do
    {:ok, %{op: op, stream_id: stream_id, body_len: body_len}}
  end

  def decode_req_header(_), do: {:error, :invalid_header}

  @doc "Encodes a response frame: stream_id + status code + optional body."
  @spec encode_response(non_neg_integer(), non_neg_integer(), binary()) :: binary()
  def encode_response(stream_id, status, body \\ <<>>) do
    <<stream_id::32, status::16, byte_size(body)::16, body::binary>>
  end

  @doc "TLV-encodes a string as `<<len::16, utf8_bytes>>`."
  @spec put_str(String.t()) :: binary()
  def put_str(s), do: <<byte_size(s)::16, s::binary>>

  @doc "TLV-encodes raw bytes as `<<len::32, bytes>>`."
  @spec put_bytes(binary()) :: binary()
  def put_bytes(b), do: <<byte_size(b)::32, b::binary>>

  @doc "Encodes a uint32 as a 4-byte big-endian binary."
  @spec put_u32(non_neg_integer()) :: binary()
  def put_u32(v), do: <<v::32>>

  @doc "Reads a TLV string from the head of a binary. Returns `{string, rest}`."
  @spec get_str(binary()) :: {String.t(), binary()}
  def get_str(<<len::16, str::binary-size(len), rest::binary>>), do: {str, rest}

  @doc "Reads TLV bytes from the head of a binary. Returns `{data, rest}`."
  @spec get_bytes(binary()) :: {binary(), binary()}
  def get_bytes(<<len::32, data::binary-size(len), rest::binary>>), do: {data, rest}

  @doc "Reads a uint32 from the head of a binary. Returns `{value, rest}`."
  @spec get_u32(binary()) :: {non_neg_integer(), binary()}
  def get_u32(<<val::32, rest::binary>>), do: {val, rest}
end
