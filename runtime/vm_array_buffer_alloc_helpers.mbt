///|
fn require_array_buffer_array(value : Value) -> ArrayValue raise {
  match value {
    Array(arr) => arr
    _ => {
      let _ = throw_type_error("invalid buffer")
      match new_array_value([]) {
        Array(arr) => arr
        _ => fail("invalid array buffer")
      }
    }
  }
}

///|
fn new_array_buffer_value_with_options(
  byte_length : Int,
  max_byte_length : Int,
  resizable : Bool,
  proto? : Value? = None,
) -> Value {
  let proto_value = match proto {
    Some(value) => Some(value)
    None => value_from_object(array_buffer_proto())
  }
  let value = new_object_value_with_proto(proto_value)
  match value {
    Object(obj) => {
      let bytes = Array::make(byte_length, Int::to_byte(0))
      obj.array_buffer_data = Some(ArrayBufferData::{
        bytes,
        detached: false,
        shared: false,
        max_byte_length,
        resizable,
      })
    }
    _ => ()
  }
  value
}

///|
fn new_array_buffer_value(byte_length : Int) -> Value {
  new_array_buffer_value_with_options(byte_length, byte_length, false)
}

///|
fn new_shared_array_buffer_value_with_options(
  byte_length : Int,
  max_byte_length : Int,
  resizable : Bool,
  proto : Value?,
) -> Value {
  let proto_value = match proto {
    Some(value) => Some(value)
    None => value_from_object(shared_array_buffer_proto())
  }
  let value = new_object_value_with_proto(proto_value)
  match value {
    Object(obj) => {
      let bytes = Array::make(byte_length, Int::to_byte(0))
      obj.array_buffer_data = Some(ArrayBufferData::{
        bytes,
        detached: false,
        shared: true,
        max_byte_length,
        resizable,
      })
    }
    _ => ()
  }
  value
}

///|
fn new_dataview_value_with_proto(
  buffer : ObjectValue,
  byte_offset : Int,
  byte_length : Int,
  length_tracking : Bool,
  proto : Value?,
) -> Value {
  let value = new_object_value_with_proto(proto)
  match value {
    Object(obj) =>
      obj.dataview_data = Some(DataViewData::{
        buffer,
        byte_offset,
        byte_length,
        length_tracking,
      })
    _ => ()
  }
  value
}

///|
fn new_dataview_from_args(
  args : Array[Value],
  target_value : Value,
) -> Value raise {
  let buffer_value = if args.is_empty() { Undefined } else { args[0] }
  let (buffer_obj, data) = require_array_buffer(Some(buffer_value))
  let offset64 = if args.length() > 1 { to_index_int64(args[1]) } else { 0L }
  if data.detached {
    return throw_type_error("ArrayBuffer is detached")
  }
  let max_len = array_buffer_data_length(data)
  if offset64 > Int64::from_int(max_len) {
    return throw_range_error("invalid byteOffset")
  }
  let offset = Int64::to_int(offset64)
  let mut len = max_len - offset
  let mut length_tracking = false
  if args.length() > 2 && !(args[2] is Undefined) {
    let len64 = to_index_int64(args[2])
    if len64 > Int64::from_int(len) {
      return throw_range_error("invalid byteLength")
    }
    len = Int64::to_int(len64)
  } else {
    length_tracking = data.resizable
  }
  let proto = constructor_proto_from_target(
    target_value,
    {
      let realm_env = function_realm_env_checked(target_value)
      match value_from_object(dataview_proto_for_env(realm_env)) {
        Some(value) => Some(value)
        None => value_from_object(dataview_proto())
      }
    },
  )
  match buffer_obj.array_buffer_data {
    Some(updated) => {
      if updated.detached {
        return throw_type_error("ArrayBuffer is detached")
      }
      let current_len = array_buffer_data_length(updated)
      if offset > current_len {
        return throw_range_error("invalid byteOffset or byteLength")
      }
      if length_tracking {
        len = current_len - offset
      } else if offset + len > current_len {
        return throw_range_error("invalid byteOffset or byteLength")
      }
    }
    None => return throw_type_error("ArrayBuffer is detached")
  }
  new_dataview_value_with_proto(buffer_obj, offset, len, length_tracking, proto)
}
