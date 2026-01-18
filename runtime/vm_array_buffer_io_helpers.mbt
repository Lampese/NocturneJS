///|
fn byte_to_uint(value : Byte) -> UInt {
  Int::reinterpret_as_uint(Byte::to_int(value))
}

///|
fn buffer_read_u16_le(buf : ArrayBufferData, index : Int) -> Int {
  let b0 = Byte::to_int(buf.bytes[index])
  let b1 = Byte::to_int(buf.bytes[index + 1])
  b0 + (b1 << 8)
}

///|
fn buffer_read_u16_be(buf : ArrayBufferData, index : Int) -> Int {
  let b0 = Byte::to_int(buf.bytes[index])
  let b1 = Byte::to_int(buf.bytes[index + 1])
  (b0 << 8) + b1
}

///|
fn buffer_read_u32_le(buf : ArrayBufferData, index : Int) -> UInt {
  let b0 = byte_to_uint(buf.bytes[index])
  let b1 = byte_to_uint(buf.bytes[index + 1])
  let b2 = byte_to_uint(buf.bytes[index + 2])
  let b3 = byte_to_uint(buf.bytes[index + 3])
  let v0 = b0
  let v1 = b1 << 8
  let v2 = b2 << 16
  let v3 = b3 << 24
  UInt::lor(v0, UInt::lor(v1, UInt::lor(v2, v3)))
}

///|
fn buffer_read_u32_be(buf : ArrayBufferData, index : Int) -> UInt {
  let b0 = byte_to_uint(buf.bytes[index])
  let b1 = byte_to_uint(buf.bytes[index + 1])
  let b2 = byte_to_uint(buf.bytes[index + 2])
  let b3 = byte_to_uint(buf.bytes[index + 3])
  let v0 = b0 << 24
  let v1 = b1 << 16
  let v2 = b2 << 8
  let v3 = b3
  UInt::lor(v0, UInt::lor(v1, UInt::lor(v2, v3)))
}

///|
fn buffer_read_u64_le(buf : ArrayBufferData, index : Int) -> UInt64 {
  let b0 = UInt64::extend_uint(byte_to_uint(buf.bytes[index]))
  let b1 = UInt64::extend_uint(byte_to_uint(buf.bytes[index + 1])) << 8
  let b2 = UInt64::extend_uint(byte_to_uint(buf.bytes[index + 2])) << 16
  let b3 = UInt64::extend_uint(byte_to_uint(buf.bytes[index + 3])) << 24
  let b4 = UInt64::extend_uint(byte_to_uint(buf.bytes[index + 4])) << 32
  let b5 = UInt64::extend_uint(byte_to_uint(buf.bytes[index + 5])) << 40
  let b6 = UInt64::extend_uint(byte_to_uint(buf.bytes[index + 6])) << 48
  let b7 = UInt64::extend_uint(byte_to_uint(buf.bytes[index + 7])) << 56
  let v0 = UInt64::lor(b0, b1)
  let v1 = UInt64::lor(b2, b3)
  let v2 = UInt64::lor(b4, b5)
  let v3 = UInt64::lor(b6, b7)
  UInt64::lor(UInt64::lor(v0, v1), UInt64::lor(v2, v3))
}

///|
fn buffer_read_u64_be(buf : ArrayBufferData, index : Int) -> UInt64 {
  let b0 = UInt64::extend_uint(byte_to_uint(buf.bytes[index])) << 56
  let b1 = UInt64::extend_uint(byte_to_uint(buf.bytes[index + 1])) << 48
  let b2 = UInt64::extend_uint(byte_to_uint(buf.bytes[index + 2])) << 40
  let b3 = UInt64::extend_uint(byte_to_uint(buf.bytes[index + 3])) << 32
  let b4 = UInt64::extend_uint(byte_to_uint(buf.bytes[index + 4])) << 24
  let b5 = UInt64::extend_uint(byte_to_uint(buf.bytes[index + 5])) << 16
  let b6 = UInt64::extend_uint(byte_to_uint(buf.bytes[index + 6])) << 8
  let b7 = UInt64::extend_uint(byte_to_uint(buf.bytes[index + 7]))
  let v0 = UInt64::lor(b0, b1)
  let v1 = UInt64::lor(b2, b3)
  let v2 = UInt64::lor(b4, b5)
  let v3 = UInt64::lor(b6, b7)
  UInt64::lor(UInt64::lor(v0, v1), UInt64::lor(v2, v3))
}

///|
fn buffer_write_u16_le(buf : ArrayBufferData, index : Int, value : Int) -> Unit {
  let low = Int::to_byte(value & 0xff)
  let high = Int::to_byte((value >> 8) & 0xff)
  buf.bytes[index] = low
  buf.bytes[index + 1] = high
}

///|
fn buffer_write_u16_be(buf : ArrayBufferData, index : Int, value : Int) -> Unit {
  let high = Int::to_byte((value >> 8) & 0xff)
  let low = Int::to_byte(value & 0xff)
  buf.bytes[index] = high
  buf.bytes[index + 1] = low
}

///|
fn buffer_write_u32_le(
  buf : ArrayBufferData,
  index : Int,
  value : UInt,
) -> Unit {
  let b0 = UInt::reinterpret_as_int(UInt::land(value, 0xffU))
  let b1 = UInt::reinterpret_as_int(UInt::land(value >> 8, 0xffU))
  let b2 = UInt::reinterpret_as_int(UInt::land(value >> 16, 0xffU))
  let b3 = UInt::reinterpret_as_int(UInt::land(value >> 24, 0xffU))
  buf.bytes[index] = Int::to_byte(b0)
  buf.bytes[index + 1] = Int::to_byte(b1)
  buf.bytes[index + 2] = Int::to_byte(b2)
  buf.bytes[index + 3] = Int::to_byte(b3)
}

///|
fn buffer_write_u32_be(
  buf : ArrayBufferData,
  index : Int,
  value : UInt,
) -> Unit {
  let b0 = UInt::reinterpret_as_int(UInt::land(value >> 24, 0xffU))
  let b1 = UInt::reinterpret_as_int(UInt::land(value >> 16, 0xffU))
  let b2 = UInt::reinterpret_as_int(UInt::land(value >> 8, 0xffU))
  let b3 = UInt::reinterpret_as_int(UInt::land(value, 0xffU))
  buf.bytes[index] = Int::to_byte(b0)
  buf.bytes[index + 1] = Int::to_byte(b1)
  buf.bytes[index + 2] = Int::to_byte(b2)
  buf.bytes[index + 3] = Int::to_byte(b3)
}

///|
fn buffer_write_u64_le(
  buf : ArrayBufferData,
  index : Int,
  value : UInt64,
) -> Unit {
  let bytes = UInt64::to_le_bytes(value)
  buf.bytes[index] = bytes[0]
  buf.bytes[index + 1] = bytes[1]
  buf.bytes[index + 2] = bytes[2]
  buf.bytes[index + 3] = bytes[3]
  buf.bytes[index + 4] = bytes[4]
  buf.bytes[index + 5] = bytes[5]
  buf.bytes[index + 6] = bytes[6]
  buf.bytes[index + 7] = bytes[7]
}

///|
fn buffer_write_u64_be(
  buf : ArrayBufferData,
  index : Int,
  value : UInt64,
) -> Unit {
  let bytes = UInt64::to_le_bytes(value)
  buf.bytes[index] = bytes[7]
  buf.bytes[index + 1] = bytes[6]
  buf.bytes[index + 2] = bytes[5]
  buf.bytes[index + 3] = bytes[4]
  buf.bytes[index + 4] = bytes[3]
  buf.bytes[index + 5] = bytes[2]
  buf.bytes[index + 6] = bytes[1]
  buf.bytes[index + 7] = bytes[0]
}

///|
fn clamp_uint8(value : Double) -> Int {
  if Double::is_nan(value) {
    return 0
  }
  if value <= 0.0 {
    return 0
  }
  if value >= 255.0 {
    return 255
  }
  let floor_value = Double::floor(value)
  let int_floor = Int64::to_int(Double::to_int64(floor_value))
  let diff = value - floor_value
  if diff > 0.5 {
    return int_floor + 1
  }
  if diff < 0.5 {
    return int_floor
  }
  if Int::land(int_floor, 1) == 1 {
    int_floor + 1
  } else {
    int_floor
  }
}

///|
fn float32_from_bytes_le(buf : ArrayBufferData, index : Int) -> Double {
  let bits = buffer_read_u32_le(buf, index)
  let f = Float::reinterpret_from_uint(bits)
  Float::to_double(f)
}

///|
fn float32_to_bytes_le(value : Double) -> Bytes {
  let f = Float::from_double(value)
  f.to_le_bytes()
}

///|
fn float64_from_bytes_le(buf : ArrayBufferData, index : Int) -> Double {
  let bits = buffer_read_u64_le(buf, index)
  UInt64::reinterpret_as_double(bits)
}

///|
fn float64_to_bytes_le(value : Double) -> Bytes {
  let bits = Double::reinterpret_as_uint64(value)
  UInt64::to_le_bytes(bits)
}

///|
let float16_scale : Double = @math.scalbn(1.0, 1008)

///|
fn float16_bits_to_double(bits : UInt) -> Double {
  let b = UInt::to_uint64(bits)
  let mut v1 = b & 0x7fffUL
  if v1 >= 0x7c00UL {
    v1 = v1 + 0x1f8000UL
  }
  let sign = b >> 15
  let d_bits = (sign << 63) | (v1 << (52 - 10))
  let d = UInt64::reinterpret_as_double(d_bits)
  d * float16_scale
}

///|
fn double_to_float16_bits(value : Double) -> UInt {
  let mut a = Double::reinterpret_as_uint64(value)
  let sgn = a >> 63
  a = a & 0x7fffffffffffffffUL
  let mut v = 0UL
  if a > 0x7ff0000000000000UL {
    v = 0x7c01UL
  } else if a < 0x3f10000000000000UL {
    if a <= 0x3e60000000000000UL {
      v = 0UL
    } else {
      let shift = 1051 - UInt64::to_int(a >> 52)
      a = (1UL << 52) | (a & ((1UL << 52) - 1UL))
      let addend = ((a >> shift) & 1UL) + ((1UL << (shift - 1)) - 1UL)
      v = (a + addend) >> shift
    }
  } else {
    a = a - 0x3f00000000000000UL
    let addend = ((a >> (52 - 10)) & 1UL) + ((1UL << (52 - 11)) - 1UL)
    v = (a + addend) >> (52 - 10)
    if v > 0x7c00UL {
      v = 0x7c00UL
    }
  }
  UInt64::to_uint(v | (sgn << 15))
}

///|
priv enum DataViewValueKind {
  Int8
  Uint8
  Int16
  Uint16
  Int32
  Uint32
  BigInt64
  BigUint64
  Float16
  Float32
  Float64
}

///|
fn dataview_value_size(kind : DataViewValueKind) -> Int {
  match kind {
    DataViewValueKind::Int8 => 1
    DataViewValueKind::Uint8 => 1
    DataViewValueKind::Int16 => 2
    DataViewValueKind::Uint16 => 2
    DataViewValueKind::Int32 => 4
    DataViewValueKind::Uint32 => 4
    DataViewValueKind::BigInt64 => 8
    DataViewValueKind::BigUint64 => 8
    DataViewValueKind::Float16 => 2
    DataViewValueKind::Float32 => 4
    DataViewValueKind::Float64 => 8
  }
}

///|
fn dataview_get_value(
  data : DataViewData,
  args : Array[Value],
  kind : DataViewValueKind,
) -> Value raise {
  let pos64 = if args.is_empty() { 0L } else { to_index_int64(args[0]) }
  let little_endian = args.length() > 1 && is_truthy(args[1])
  let size = dataview_value_size(kind)
  match data.buffer.array_buffer_data {
    Some(buf) => {
      if buf.detached {
        return throw_type_error("ArrayBuffer is detached")
      }
      let len = dataview_effective_length(data)
      let end = pos64 + Int64::from_int(size)
      if end > Int64::from_int(len) {
        return throw_range_error("out of bound")
      }
      if dataview_is_oob(data) {
        return throw_type_error("ArrayBuffer is detached or resized")
      }
      let pos = Int64::to_int(pos64)
      let byte_index = data.byte_offset + pos
      if byte_index + size > buf.bytes.length() {
        return throw_type_error("ArrayBuffer is detached or resized")
      }
      match kind {
        DataViewValueKind::Int8 => {
          let raw = Byte::to_int(buf.bytes[byte_index])
          let value = if raw >= 128 { raw - 256 } else { raw }
          Number(Double::from_int(value))
        }
        DataViewValueKind::Uint8 => {
          let value = Byte::to_int(buf.bytes[byte_index])
          Number(Double::from_int(value))
        }
        DataViewValueKind::Int16 => {
          let raw = if little_endian {
            buffer_read_u16_le(buf, byte_index)
          } else {
            buffer_read_u16_be(buf, byte_index)
          }
          let value = if raw >= 0x8000 { raw - 0x10000 } else { raw }
          Number(Double::from_int(value))
        }
        DataViewValueKind::Uint16 => {
          let value = if little_endian {
            buffer_read_u16_le(buf, byte_index)
          } else {
            buffer_read_u16_be(buf, byte_index)
          }
          Number(Double::from_int(value))
        }
        DataViewValueKind::Int32 => {
          let raw = if little_endian {
            buffer_read_u32_le(buf, byte_index)
          } else {
            buffer_read_u32_be(buf, byte_index)
          }
          Number(Double::from_int(UInt::reinterpret_as_int(raw)))
        }
        DataViewValueKind::Uint32 => {
          let value = if little_endian {
            buffer_read_u32_le(buf, byte_index)
          } else {
            buffer_read_u32_be(buf, byte_index)
          }
          Number(UInt::to_double(value))
        }
        DataViewValueKind::BigInt64 => {
          let bits = if little_endian {
            buffer_read_u64_le(buf, byte_index)
          } else {
            buffer_read_u64_be(buf, byte_index)
          }
          let signed = UInt64::reinterpret_as_int64(bits)
          BigInt(@bigint.BigInt::from_int64(signed))
        }
        DataViewValueKind::BigUint64 => {
          let bits = if little_endian {
            buffer_read_u64_le(buf, byte_index)
          } else {
            buffer_read_u64_be(buf, byte_index)
          }
          BigInt(@bigint.BigInt::from_uint64(bits))
        }
        DataViewValueKind::Float16 => {
          let raw = if little_endian {
            buffer_read_u16_le(buf, byte_index)
          } else {
            buffer_read_u16_be(buf, byte_index)
          }
          Number(float16_bits_to_double(Int::reinterpret_as_uint(raw)))
        }
        DataViewValueKind::Float32 => {
          let bits = if little_endian {
            buffer_read_u32_le(buf, byte_index)
          } else {
            buffer_read_u32_be(buf, byte_index)
          }
          let f = Float::reinterpret_from_uint(bits)
          Number(Float::to_double(f))
        }
        DataViewValueKind::Float64 => {
          let bits = if little_endian {
            buffer_read_u64_le(buf, byte_index)
          } else {
            buffer_read_u64_be(buf, byte_index)
          }
          Number(UInt64::reinterpret_as_double(bits))
        }
      }
    }
    None => throw_type_error("ArrayBuffer is detached")
  }
}

///|
fn buffer_write_bytes(
  buf : ArrayBufferData,
  index : Int,
  bytes : Bytes,
  reverse : Bool,
) -> Unit {
  let len = bytes.length()
  let mut i = 0
  while i < len {
    let src = if reverse { len - 1 - i } else { i }
    buf.bytes[index + i] = bytes[src]
    i = i + 1
  }
}

///|
fn dataview_set_value(
  data : DataViewData,
  args : Array[Value],
  kind : DataViewValueKind,
) -> Value raise {
  let pos64 = if args.is_empty() { 0L } else { to_index_int64(args[0]) }
  let value_arg = if args.length() > 1 { args[1] } else { Undefined }
  let little_endian = args.length() > 2 && is_truthy(args[2])
  let size = dataview_value_size(kind)
  let mut byte_value : Byte? = None
  let mut u16_value : Int? = None
  let mut u32_value : UInt? = None
  let mut u64_value : UInt64? = None
  let mut bytes_value : Bytes? = None
  match kind {
    DataViewValueKind::Int8 | DataViewValueKind::Uint8 => {
      let bits = to_uint32(to_number(value_arg))
      byte_value = Some(Int::to_byte(UInt::reinterpret_as_int(bits & 0xffU)))
    }
    DataViewValueKind::Int16 | DataViewValueKind::Uint16 => {
      let bits = to_uint32(to_number(value_arg))
      u16_value = Some(UInt::reinterpret_as_int(UInt::land(bits, 0xffffU)))
    }
    DataViewValueKind::Int32 | DataViewValueKind::Uint32 =>
      u32_value = Some(to_uint32(to_number(value_arg)))
    DataViewValueKind::BigInt64 =>
      u64_value = Some(bigint_to_uint64_bits(value_arg, true))
    DataViewValueKind::BigUint64 =>
      u64_value = Some(bigint_to_uint64_bits(value_arg, false))
    DataViewValueKind::Float16 => {
      let num = to_number(value_arg)
      let bits = double_to_float16_bits(num)
      u16_value = Some(UInt::reinterpret_as_int(bits))
    }
    DataViewValueKind::Float32 => {
      let num = to_number(value_arg)
      bytes_value = Some(float32_to_bytes_le(num))
    }
    DataViewValueKind::Float64 => {
      let num = to_number(value_arg)
      bytes_value = Some(float64_to_bytes_le(num))
    }
  }
  match data.buffer.array_buffer_data {
    Some(buf) => {
      if buf.detached {
        return throw_type_error("ArrayBuffer is detached")
      }
      let len = dataview_effective_length(data)
      let end = pos64 + Int64::from_int(size)
      if end > Int64::from_int(len) {
        return throw_range_error("out of bound")
      }
      if dataview_is_oob(data) {
        return throw_type_error("ArrayBuffer is detached or resized")
      }
      let pos = Int64::to_int(pos64)
      let byte_index = data.byte_offset + pos
      if byte_index + size > buf.bytes.length() {
        return throw_type_error("ArrayBuffer is detached or resized")
      }
      match byte_value {
        Some(byte) => buf.bytes[byte_index] = byte
        None =>
          match u16_value {
            Some(u16) =>
              if little_endian {
                buffer_write_u16_le(buf, byte_index, u16)
              } else {
                buffer_write_u16_be(buf, byte_index, u16)
              }
            None =>
              match u32_value {
                Some(u32) =>
                  if little_endian {
                    buffer_write_u32_le(buf, byte_index, u32)
                  } else {
                    buffer_write_u32_be(buf, byte_index, u32)
                  }
                None =>
                  match u64_value {
                    Some(u64) =>
                      if little_endian {
                        buffer_write_u64_le(buf, byte_index, u64)
                      } else {
                        buffer_write_u64_be(buf, byte_index, u64)
                      }
                    None =>
                      match bytes_value {
                        Some(bytes) =>
                          buffer_write_bytes(
                            buf,
                            byte_index,
                            bytes,
                            !little_endian,
                          )
                        None => ()
                      }
                  }
              }
          }
      }
      Undefined
    }
    None => throw_type_error("ArrayBuffer is detached")
  }
}
