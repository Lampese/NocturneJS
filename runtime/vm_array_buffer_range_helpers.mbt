///|
fn ensure_array_buffer_range(
  buf_len : Int,
  offset : Int,
  len : Int,
) -> Unit raise {
  let end = offset + len
  if offset < 0 || len < 0 || end < offset || end > buf_len {
    let _ = throw_range_error("read/write array buffer overflow")

  }
}

///|
fn byte_from_value(value : Value) -> Byte {
  match value {
    Number(num) => Int::to_byte(to_int32(num))
    _ => 0
  }
}

///|
fn array_bytes_from_value(
  value : Value,
  offset : Int,
  len : Int,
) -> Array[Byte] raise {
  let buf = require_array_buffer_data(value)
  let buf_len = buf.bytes.length()
  ensure_array_buffer_range(buf_len, offset, len)
  let bytes : Array[Byte] = []
  let mut i = 0
  while i < len {
    bytes.push(buf.bytes[offset + i])
    i = i + 1
  }
  bytes
}

///|
fn write_bytes_to_array(
  value : Value,
  offset : Int,
  bytes : Array[Byte],
) -> Int raise {
  let buf = require_array_buffer_data(value)
  let buf_len = buf.bytes.length()
  ensure_array_buffer_range(buf_len, offset, bytes.length())
  let mut i = 0
  while i < bytes.length() {
    buf.bytes[offset + i] = bytes[i]
    i = i + 1
  }
  bytes.length()
}
