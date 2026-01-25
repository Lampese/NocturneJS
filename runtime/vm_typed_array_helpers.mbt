///|
fn typed_array_bytes_per_element(kind : TypedArrayKind) -> Int {
  match kind {
    TypedArrayKind::Uint8 => 1
    TypedArrayKind::Int8 => 1
    TypedArrayKind::Uint8Clamped => 1
    TypedArrayKind::Uint16 => 2
    TypedArrayKind::Int16 => 2
    TypedArrayKind::Float16 => 2
    TypedArrayKind::Uint32 => 4
    TypedArrayKind::Int32 => 4
    TypedArrayKind::BigInt64 => 8
    TypedArrayKind::BigUint64 => 8
    TypedArrayKind::Float32 => 4
    TypedArrayKind::Float64 => 8
  }
}

///|
fn typed_array_constructor_name(kind : TypedArrayKind) -> String {
  match kind {
    TypedArrayKind::Uint8 => "Uint8Array"
    TypedArrayKind::Int8 => "Int8Array"
    TypedArrayKind::Uint8Clamped => "Uint8ClampedArray"
    TypedArrayKind::Uint16 => "Uint16Array"
    TypedArrayKind::Int16 => "Int16Array"
    TypedArrayKind::Uint32 => "Uint32Array"
    TypedArrayKind::Int32 => "Int32Array"
    TypedArrayKind::BigInt64 => "BigInt64Array"
    TypedArrayKind::BigUint64 => "BigUint64Array"
    TypedArrayKind::Float16 => "Float16Array"
    TypedArrayKind::Float32 => "Float32Array"
    TypedArrayKind::Float64 => "Float64Array"
  }
}

///|
fn new_typed_array_view(
  kind : TypedArrayKind,
  buffer : ObjectValue,
  byte_offset : Int,
  length : Int,
  length_tracking : Bool,
  proto? : Value? = None,
) -> Value {
  let elements : Array[Value?] = []
  let mut i = 0
  while i < length {
    elements.push(None)
    i = i + 1
  }
  let props = props_new()
  let bpe = typed_array_bytes_per_element(kind)
  let proto_value = match proto {
    Some(value) => Some(value)
    None =>
      match value_from_object(typed_array_proto_for_kind(kind)) {
        Some(value) => Some(value)
        None => value_from_object(array_proto())
      }
  }
  let arr = register_array_value(ArrayValue::{
    id: alloc_id(),
    elements,
    proto: proto_value,
    props,
    typed_array_data: Some(TypedArrayData::{
      kind,
      buffer,
      byte_offset,
      length,
      length_tracking,
      bytes_per_element: bpe,
    }),
    extensible: true,
  })
  rc_incref_optional_value(proto_value)
  rc_incref_value(Object(buffer))
  Array(arr)
}

///|
fn new_typed_array_value(
  kind : TypedArrayKind,
  length : Int,
  proto? : Value? = None,
  buffer_proto? : Value? = None,
) -> Value {
  let bpe = typed_array_bytes_per_element(kind)
  let byte_length = length * bpe
  let buffer_value = new_array_buffer_value_with_options(
    byte_length,
    byte_length,
    false,
    proto=buffer_proto,
  )
  match buffer_value {
    Object(buffer_obj) =>
      new_typed_array_view(kind, buffer_obj, 0, length, false, proto~)
    _ =>
      new_typed_array_view(
        kind,
        new_object_struct(None),
        0,
        length,
        false,
        proto~,
      )
  }
}

///|
fn new_typed_array_from_array(
  kind : TypedArrayKind,
  arr : ArrayValue,
  proto : Value?,
  buffer_proto : Value?,
) -> Value raise {
  let source_value = Array(arr)
  let length = match arr.typed_array_data {
    Some(src_data) =>
      if typed_array_is_oob(src_data) {
        let _ = throw_type_error("ArrayBuffer is detached")
        0
      } else {
        typed_array_effective_length(src_data)
      }
    None => array_like_length(source_value)
  }
  let value = new_typed_array_value(kind, length, proto~, buffer_proto~)
  match value {
    Array(target) =>
      match target.typed_array_data {
        Some(data) => {
          let mut i = 0
          while i < length {
            let element = match arr.typed_array_data {
              Some(src_data) => typed_array_get_index(src_data, i)
              None => property_get(source_value, Int::to_string(i))
            }
            let _ = typed_array_set_index(data, i, element)
            i = i + 1
          }
        }
        None => ()
      }
    _ => ()
  }
  value
}

///|
fn new_typed_array_from_buffer(
  kind : TypedArrayKind,
  buffer_value : Value,
  byte_offset_value : Value,
  length_value : Value?,
  target_value : Value,
) -> Value raise {
  let (buffer_obj, _) = require_array_buffer(Some(buffer_value))
  let offset64 = to_index_int64(byte_offset_value)
  let buf = match buffer_obj.array_buffer_data {
    Some(data) => data
    None => return throw_type_error("not an ArrayBuffer")
  }
  if buf.detached {
    return throw_type_error("ArrayBuffer is detached")
  }
  let bpe = typed_array_bytes_per_element(kind)
  let bpe64 = Int64::from_int(bpe)
  let mut max_len64 = Int64::from_int(array_buffer_data_length(buf))
  if offset64 > max_len64 || Int64::mod(offset64, bpe64) != 0L {
    return throw_range_error("invalid offset")
  }
  let mut track_rab = false
  let len64 = match length_value {
    None => {
      track_rab = buf.resizable
      if !track_rab && Int64::mod(max_len64, bpe64) != 0L {
        return throw_range_error("invalid length")
      }
      (max_len64 - offset64) / bpe64
    }
    Some(value) =>
      if value is Undefined {
        track_rab = buf.resizable
        if !track_rab && Int64::mod(max_len64, bpe64) != 0L {
          return throw_range_error("invalid length")
        }
        (max_len64 - offset64) / bpe64
      } else {
        let next_len64 = to_index_int64(value)
        let next_buf = match buffer_obj.array_buffer_data {
          Some(data) => data
          None => return throw_type_error("not an ArrayBuffer")
        }
        if next_buf.detached {
          return throw_type_error("ArrayBuffer is detached")
        }
        max_len64 = Int64::from_int(array_buffer_data_length(next_buf))
        if offset64 > max_len64 {
          return throw_range_error("invalid length")
        }
        let available = max_len64 - offset64
        if next_len64 > available / bpe64 {
          return throw_range_error("invalid length")
        }
        next_len64
      }
  }
  let max_int = 2147483647
  if len64 > Int64::from_int(max_int) {
    return throw_range_error("invalid array buffer length")
  }
  let offset = Int64::to_int(offset64)
  let len = Int64::to_int(len64)
  let proto = typed_array_proto_from_target(kind, target_value)
  new_typed_array_view(kind, buffer_obj, offset, len, track_rab, proto~)
}

///|
fn typed_array_proto_from_target(
  kind : TypedArrayKind,
  target_value : Value,
) -> Value? raise {
  match target_value {
    Undefined =>
      match value_from_object(typed_array_proto_for_kind(kind)) {
        Some(value) => Some(value)
        None => value_from_object(array_proto())
      }
    _ => {
      let realm_env = function_realm_env_checked(target_value)
      let default_proto = match
        value_from_object(typed_array_proto_for_kind_env(realm_env, kind)) {
        Some(value) => Some(value)
        None =>
          match value_from_object(typed_array_proto_for_kind(kind)) {
            Some(value) => Some(value)
            None => value_from_object(array_proto())
          }
      }
      constructor_proto_from_target(target_value, default_proto)
    }
  }
}

///|
fn typed_array_buffer_proto_from_target(target_value : Value) -> Value? raise {
  match target_value {
    Undefined =>
      match current_env() {
        Some(env) =>
          match value_from_object(array_buffer_proto_for_env(env)) {
            Some(value) => Some(value)
            None => value_from_object(array_buffer_proto())
          }
        None => value_from_object(array_buffer_proto())
      }
    _ => {
      let realm_env = function_realm_env_checked(target_value)
      match value_from_object(array_buffer_proto_for_env(realm_env)) {
        Some(value) => Some(value)
        None => value_from_object(array_buffer_proto())
      }
    }
  }
}

///|
fn new_typed_array_from_args(
  kind : TypedArrayKind,
  args : Array[Value],
  target_value : Value,
) -> Value raise {
  let buffer_proto = typed_array_buffer_proto_from_target(target_value)
  if args.is_empty() {
    let proto = typed_array_proto_from_target(kind, target_value)
    return new_typed_array_value(kind, 0, proto~, buffer_proto~)
  }
  match args[0] {
    Array(arr) => {
      let proto = typed_array_proto_from_target(kind, target_value)
      match arr.typed_array_data {
        Some(_) => new_typed_array_from_array(kind, arr, proto, buffer_proto)
        None => {
          let source = array_from_value(Undefined, [Array(arr)])
          match source {
            Array(source_arr) =>
              new_typed_array_from_array(kind, source_arr, proto, buffer_proto)
            _ => new_typed_array_value(kind, 0, proto~, buffer_proto~)
          }
        }
      }
    }
    Object(obj) =>
      match obj.array_buffer_data {
        Some(_) => {
          let offset_value = if args.length() > 1 { args[1] } else { Undefined }
          let length_value = if args.length() > 2 {
            Some(args[2])
          } else {
            None
          }
          new_typed_array_from_buffer(
            kind,
            Object(obj),
            offset_value,
            length_value,
            target_value,
          )
        }
        None => {
          let proto = typed_array_proto_from_target(kind, target_value)
          let source = array_from_value(Undefined, [Object(obj)])
          match source {
            Array(source_arr) =>
              new_typed_array_from_array(kind, source_arr, proto, buffer_proto)
            _ => new_typed_array_value(kind, 0, proto~, buffer_proto~)
          }
        }
      }
    _ if is_object_like(args[0]) => {
      let proto = typed_array_proto_from_target(kind, target_value)
      let source = array_from_value(Undefined, [args[0]])
      match source {
        Array(source_arr) =>
          new_typed_array_from_array(kind, source_arr, proto, buffer_proto)
        _ => new_typed_array_value(kind, 0, proto~, buffer_proto~)
      }
    }
    _ => {
      let len64 = to_index_int64(args[0])
      let bpe = typed_array_bytes_per_element(kind)
      let max_int = 2147483647
      let max_len = max_int / bpe
      if len64 > Int64::from_int(max_len) {
        return throw_range_error("invalid array buffer length")
      }
      let len = Int64::to_int(len64)
      let proto = typed_array_proto_from_target(kind, target_value)
      new_typed_array_value(kind, len, proto~, buffer_proto~)
    }
  }
}

///|
fn typed_array_get_index(data : TypedArrayData, index : Int) -> Value {
  let len = typed_array_effective_length(data)
  if index < 0 || index >= len {
    return Undefined
  }
  match data.buffer.array_buffer_data {
    Some(buf) =>
      if buf.detached {
        Undefined
      } else {
        let byte_index = data.byte_offset + index * data.bytes_per_element
        if byte_index + data.bytes_per_element > buf.bytes.length() {
          return Undefined
        }
        match data.kind {
          TypedArrayKind::Uint8 => {
            let value = Byte::to_int(buf.bytes[byte_index])
            Number(Double::from_int(value))
          }
          TypedArrayKind::Int8 => {
            let raw = Byte::to_int(buf.bytes[byte_index])
            let value = if raw >= 128 { raw - 256 } else { raw }
            Number(Double::from_int(value))
          }
          TypedArrayKind::Uint8Clamped => {
            let value = Byte::to_int(buf.bytes[byte_index])
            Number(Double::from_int(value))
          }
          TypedArrayKind::Uint16 => {
            let value = buffer_read_u16_le(buf, byte_index)
            Number(Double::from_int(value))
          }
          TypedArrayKind::Int16 => {
            let raw = buffer_read_u16_le(buf, byte_index)
            let value = if raw >= 0x8000 { raw - 0x10000 } else { raw }
            Number(Double::from_int(value))
          }
          TypedArrayKind::Uint32 => {
            let value = buffer_read_u32_le(buf, byte_index)
            Number(UInt::to_double(value))
          }
          TypedArrayKind::Int32 => {
            let value = buffer_read_u32_le(buf, byte_index)
            Number(Double::from_int(UInt::reinterpret_as_int(value)))
          }
          TypedArrayKind::BigInt64 => {
            let bits = buffer_read_u64_le(buf, byte_index)
            let signed = UInt64::reinterpret_as_int64(bits)
            BigInt(@bigint.BigInt::from_int64(signed))
          }
          TypedArrayKind::BigUint64 => {
            let bits = buffer_read_u64_le(buf, byte_index)
            BigInt(@bigint.BigInt::from_uint64(bits))
          }
          TypedArrayKind::Float32 =>
            Number(float32_from_bytes_le(buf, byte_index))
          TypedArrayKind::Float64 =>
            Number(float64_from_bytes_le(buf, byte_index))
          TypedArrayKind::Float16 => {
            let raw = buffer_read_u16_le(buf, byte_index)
            Number(float16_bits_to_double(Int::reinterpret_as_uint(raw)))
          }
        }
      }
    None => Undefined
  }
}

///|

///|
fn typed_array_set_index(
  data : TypedArrayData,
  index : Int,
  value : Value,
) -> Bool raise {
  if index < 0 {
    return false
  }
  match data.kind {
    TypedArrayKind::BigInt64 => {
      let len = typed_array_effective_length(data)
      if index >= len {
        return true
      }
      match data.buffer.array_buffer_data {
        Some(buf) =>
          if buf.detached {
            true
          } else {
            let byte_index = data.byte_offset + index * data.bytes_per_element
            if byte_index + data.bytes_per_element > buf.bytes.length() {
              return true
            }
            let bits = bigint_to_uint64_bits(value, true)
            buffer_write_u64_le(buf, byte_index, bits)
            true
          }
        None => true
      }
    }
    TypedArrayKind::BigUint64 => {
      let len = typed_array_effective_length(data)
      if index >= len {
        return true
      }
      match data.buffer.array_buffer_data {
        Some(buf) =>
          if buf.detached {
            true
          } else {
            let byte_index = data.byte_offset + index * data.bytes_per_element
            if byte_index + data.bytes_per_element > buf.bytes.length() {
              return true
            }
            let bits = bigint_to_uint64_bits(value, false)
            buffer_write_u64_le(buf, byte_index, bits)
            true
          }
        None => true
      }
    }
    _ => {
      let num = to_number(value)
      let len = typed_array_effective_length(data)
      if index >= len {
        return true
      }
      match data.buffer.array_buffer_data {
        Some(buf) =>
          if buf.detached {
            true
          } else {
            let byte_index = data.byte_offset + index * data.bytes_per_element
            if byte_index + data.bytes_per_element > buf.bytes.length() {
              return true
            }
            match data.kind {
              TypedArrayKind::Uint8 => {
                let masked = UInt::land(to_uint32(num), 0xffU)
                buf.bytes[byte_index] = Int::to_byte(
                  UInt::reinterpret_as_int(masked),
                )
              }
              TypedArrayKind::Int8 => {
                let masked = UInt::land(to_uint32(num), 0xffU)
                buf.bytes[byte_index] = Int::to_byte(
                  UInt::reinterpret_as_int(masked),
                )
              }
              TypedArrayKind::Uint8Clamped => {
                let clamped = clamp_uint8(num)
                buf.bytes[byte_index] = Int::to_byte(clamped)
              }
              TypedArrayKind::Uint16 => {
                let masked = UInt::land(to_uint32(num), 0xffffU)
                buffer_write_u16_le(
                  buf,
                  byte_index,
                  UInt::reinterpret_as_int(masked),
                )
              }
              TypedArrayKind::Int16 => {
                let masked = UInt::land(to_uint32(num), 0xffffU)
                buffer_write_u16_le(
                  buf,
                  byte_index,
                  UInt::reinterpret_as_int(masked),
                )
              }
              TypedArrayKind::Uint32 => {
                let masked = to_uint32(num)
                buffer_write_u32_le(buf, byte_index, masked)
              }
              TypedArrayKind::Int32 => {
                let signed = to_int32(num)
                let bits = Int::reinterpret_as_uint(signed)
                buffer_write_u32_le(buf, byte_index, bits)
              }
              TypedArrayKind::Float32 => {
                let bytes = float32_to_bytes_le(num)
                buf.bytes[byte_index] = bytes[0]
                buf.bytes[byte_index + 1] = bytes[1]
                buf.bytes[byte_index + 2] = bytes[2]
                buf.bytes[byte_index + 3] = bytes[3]
              }
              TypedArrayKind::Float64 => {
                let bytes = float64_to_bytes_le(num)
                buf.bytes[byte_index] = bytes[0]
                buf.bytes[byte_index + 1] = bytes[1]
                buf.bytes[byte_index + 2] = bytes[2]
                buf.bytes[byte_index + 3] = bytes[3]
                buf.bytes[byte_index + 4] = bytes[4]
                buf.bytes[byte_index + 5] = bytes[5]
                buf.bytes[byte_index + 6] = bytes[6]
                buf.bytes[byte_index + 7] = bytes[7]
              }
              TypedArrayKind::Float16 => {
                let bits = double_to_float16_bits(num)
                let low = UInt::reinterpret_as_int(UInt::land(bits, 0xffU))
                let high = UInt::reinterpret_as_int(
                  UInt::land(bits >> 8, 0xffU),
                )
                buf.bytes[byte_index] = Int::to_byte(low)
                buf.bytes[byte_index + 1] = Int::to_byte(high)
              }
              TypedArrayKind::BigInt64 => ()
              TypedArrayKind::BigUint64 => ()
            }
            true
          }
        None => false
      }
    }
  }
}

///|
fn require_typed_array(value : Value?) -> (ArrayValue, TypedArrayData) raise {
  match value {
    Some(Array(arr)) =>
      match arr.typed_array_data {
        Some(data) => (arr, data)
        None => {
          let _ = throw_type_error("not a typed array")
          (
            arr,
            TypedArrayData::{
              kind: TypedArrayKind::Uint8,
              buffer: new_object_struct(None),
              byte_offset: 0,
              length: 0,
              length_tracking: false,
              bytes_per_element: 1,
            },
          )
        }
      }
    _ => {
      let _ = throw_type_error("not a typed array")
      let fallback = register_array_value(ArrayValue::{
        id: alloc_id(),
        elements: [],
        proto: value_from_object(array_proto()),
        props: props_new(),
        typed_array_data: None,
        extensible: true,
      })
      rc_incref_optional_value(fallback.proto)
      (
        fallback,
        TypedArrayData::{
          kind: TypedArrayKind::Uint8,
          buffer: new_object_struct(None),
          byte_offset: 0,
          length: 0,
          length_tracking: false,
          bytes_per_element: 1,
        },
      )
    }
  }
}

///|
fn typed_array_kind_from_value(value : Value) -> TypedArrayKind? {
  match value {
    Array(arr) =>
      match arr.typed_array_data {
        Some(data) => Some(data.kind)
        None => None
      }
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => typed_array_kind_from_value(data.target)
        None => None
      }
    _ => None
  }
}

///|
fn require_typed_array_checked(
  value : Value?,
) -> (ArrayValue, TypedArrayData) raise {
  let (arr, data) = require_typed_array(value)
  if typed_array_is_oob(data) {
    let _ = throw_type_error("ArrayBuffer is detached")

  }
  (arr, data)
}
