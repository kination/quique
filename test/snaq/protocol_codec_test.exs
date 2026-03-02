defmodule Snaq.Protocol.CodecTest do
  use ExUnit.Case, async: true

  alias Snaq.Protocol.Codec

  describe "decode_req_header/1" do
    test "valid 10-byte header returns {:ok, map}" do
      # version=1, op=2, stream_id=42, body_len=100
      header = <<1::8, 2::8, 42::32, 100::32>>
      assert Codec.decode_req_header(header) == {:ok, %{op: 2, stream_id: 42, body_len: 100}}
    end

    test "wrong-length binary returns {:error, :invalid_header}" do
      assert Codec.decode_req_header(<<1, 2, 3>>) == {:error, :invalid_header}
    end

    test "empty binary returns {:error, :invalid_header}" do
      assert Codec.decode_req_header(<<>>) == {:error, :invalid_header}
    end
  end

  describe "encode_response/3" do
    test "encodes stream_id, status, and body" do
      result = Codec.encode_response(7, 0, "hi")
      assert result == <<7::32, 0::16, 2::16, "hi"::binary>>
    end

    test "defaults to empty body" do
      result = Codec.encode_response(1, 200)
      assert result == <<1::32, 200::16, 0::16>>
    end
  end

  describe "put_str / get_str roundtrip" do
    test "encodes and decodes a string" do
      encoded = Codec.put_str("hello")
      assert {str, <<>>} = Codec.get_str(encoded)
      assert str == "hello"
    end

    test "preserves trailing bytes" do
      encoded = Codec.put_str("ab") <> <<99>>
      assert {"ab", <<99>>} = Codec.get_str(encoded)
    end
  end

  describe "put_bytes / get_bytes roundtrip" do
    test "encodes and decodes raw bytes" do
      data = <<1, 2, 3, 4>>
      encoded = Codec.put_bytes(data)
      assert {^data, <<>>} = Codec.get_bytes(encoded)
    end
  end

  describe "put_u32 / get_u32 roundtrip" do
    test "encodes and decodes a uint32" do
      encoded = Codec.put_u32(0xDEADBEEF)
      assert {0xDEADBEEF, <<>>} = Codec.get_u32(encoded)
    end

    test "preserves trailing bytes" do
      encoded = Codec.put_u32(1) <> <<255>>
      assert {1, <<255>>} = Codec.get_u32(encoded)
    end
  end
end
