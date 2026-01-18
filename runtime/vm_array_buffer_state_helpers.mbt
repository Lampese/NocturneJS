///|
fn require_array_buffer_data(value : Value) -> ArrayBufferData raise {
  let (_, data) = require_array_buffer(Some(value))
  if data.detached {
    let _ = throw_type_error("ArrayBuffer is detached")
    ArrayBufferData::{
      bytes: [],
      detached: true,
      shared: false,
      max_byte_length: 0,
      resizable: false,
    }
  } else {
    data
  }
}

///|
fn detach_array_buffer(value : Value) -> Unit {
  match value {
    Object(obj) =>
      match obj.array_buffer_data {
        Some(data) =>
          if !data.detached && !data.shared {
            data.detached = true
            data.bytes = []
            obj.array_buffer_data = Some(data)
          }
        None => ()
      }
    _ => ()
  }
}

///|
fn array_buffer_byte_length(value : Value) -> Int raise {
  let data = require_array_buffer_data(value)
  data.bytes.length()
}

///|
fn array_buffer_byte_length_value(this_value : Value?) -> Value raise {
  let (_, data) = require_non_shared_array_buffer(this_value)
  Number(Double::from_int(array_buffer_data_length(data)))
}

///|
fn array_buffer_max_byte_length_value(this_value : Value?) -> Value raise {
  let (_, data) = require_non_shared_array_buffer(this_value)
  let len = array_buffer_data_length(data)
  let max_len = if data.resizable { data.max_byte_length } else { len }
  Number(Double::from_int(max_len))
}

///|
fn array_buffer_resizable_value(this_value : Value?) -> Value raise {
  let (_, data) = require_non_shared_array_buffer(this_value)
  Bool(data.resizable)
}

///|
fn array_buffer_detached_value(this_value : Value?) -> Value raise {
  let (_, data) = require_non_shared_array_buffer(this_value)
  Bool(data.detached)
}

///|
fn shared_array_buffer_byte_length_value(this_value : Value?) -> Value raise {
  let (_, data) = require_shared_array_buffer(this_value)
  Number(Double::from_int(array_buffer_data_length(data)))
}

///|
fn shared_array_buffer_max_byte_length_value(
  this_value : Value?,
) -> Value raise {
  let (_, data) = require_shared_array_buffer(this_value)
  let len = array_buffer_data_length(data)
  let max_len = if data.resizable { data.max_byte_length } else { len }
  Number(Double::from_int(max_len))
}

///|
fn shared_array_buffer_growable_value(this_value : Value?) -> Value raise {
  let (_, data) = require_shared_array_buffer(this_value)
  Bool(data.resizable)
}

///|
fn array_buffer_resize_value(
  obj : ObjectValue,
  data : ArrayBufferData,
  new_len : Int,
) -> Value raise {
  if data.detached {
    return throw_type_error("ArrayBuffer is detached")
  }
  if !data.resizable {
    return throw_type_error("array buffer is not resizable")
  }
  if new_len < 0 || new_len > data.max_byte_length {
    return throw_range_error("invalid array buffer length")
  }
  let current = data.bytes.length()
  if new_len < current {
    data.bytes.truncate(new_len)
  } else if new_len > current {
    while data.bytes.length() < new_len {
      data.bytes.push(Int::to_byte(0))
    }
  }
  obj.array_buffer_data = Some(data)
  Undefined
}

///|
fn shared_array_buffer_grow_value(
  obj : ObjectValue,
  data : ArrayBufferData,
  new_len : Int,
) -> Value raise {
  if data.detached {
    return throw_type_error("ArrayBuffer is detached")
  }
  if !data.resizable {
    return throw_type_error("array buffer is not resizable")
  }
  if new_len < 0 || new_len > data.max_byte_length {
    return throw_range_error("invalid array buffer length")
  }
  let current = data.bytes.length()
  if new_len < current {
    return throw_range_error("invalid array buffer length")
  }
  while data.bytes.length() < new_len {
    data.bytes.push(Int::to_byte(0))
  }
  obj.array_buffer_data = Some(data)
  Undefined
}

///|
fn array_buffer_transfer_value(
  this_value : Value?,
  args : Array[Value],
  to_fixed_length : Bool,
) -> Value raise {
  let (obj, data) = require_non_shared_array_buffer(this_value)
  if data.detached {
    return throw_type_error("ArrayBuffer is detached")
  }
  let new_len64 = if args.is_empty() || args[0] is Undefined {
    Int64::from_int(data.bytes.length())
  } else {
    to_index_int64(args[0])
  }
  let max_int = 2147483647
  if new_len64 > Int64::from_int(max_int) {
    return throw_range_error("invalid array buffer length")
  }
  let new_len = Int64::to_int(new_len64)
  if !to_fixed_length && data.resizable {
    if new_len > data.max_byte_length {
      return throw_type_error("invalid array buffer length")
    }
  }
  let new_max_len = if !to_fixed_length && data.resizable {
    data.max_byte_length
  } else {
    new_len
  }
  let new_resizable = !to_fixed_length && data.resizable
  let new_buffer = new_array_buffer_value_with_options(
    new_len, new_max_len, new_resizable,
  )
  match new_buffer {
    Object(new_obj) =>
      match new_obj.array_buffer_data {
        Some(new_data) => {
          let mut i = 0
          let copy_len = if data.bytes.length() < new_len {
            data.bytes.length()
          } else {
            new_len
          }
          while i < copy_len {
            new_data.bytes[i] = data.bytes[i]
            i = i + 1
          }
          new_obj.array_buffer_data = Some(new_data)
        }
        None => ()
      }
    _ => ()
  }
  data.detached = true
  data.bytes = []
  obj.array_buffer_data = Some(data)
  new_buffer
}

///|
fn array_buffer_slice_value(
  this_value : Value,
  args : Array[Value],
  shared : Bool,
) -> Value raise {
  let (_, data) = if shared {
    require_shared_array_buffer(Some(this_value))
  } else {
    require_non_shared_array_buffer(Some(this_value))
  }
  if data.detached {
    return throw_type_error("ArrayBuffer is detached")
  }
  let len = data.bytes.length()
  let mut start = 0
  if args.length() > 0 {
    start = to_int64_clamp(args[0], 0, len, len)
  }
  let mut end = len
  if args.length() > 1 && !(args[1] is Undefined) {
    end = to_int64_clamp(args[1], 0, len, len)
  }
  let new_len = if end > start { end - start } else { 0 }
  let ctor = property_get(this_value, "constructor")
  let mut species = ctor
  if is_object_like(ctor) {
    match symbol_species_key() {
      Some(key) => {
        let next = property_get(ctor, key)
        species = match next {
          Null => Undefined
          _ => next
        }
      }
      None => ()
    }
  }
  let realm_env = match function_realm_env(ctor) {
    Some(env) => env
    None =>
      match get_proto_of_value_checked(this_value) {
        Some(proto) => realm_env_from_value(Some(proto))
        None =>
          match current_env() {
            Some(env) => env
            None =>
              match root_env() {
                Some(env) => env
                None => Env::new(None)
              }
          }
      }
  }
  let new_obj = if species is Undefined {
    if shared {
      let proto_value = match
        value_from_object(shared_array_buffer_proto_for_env(realm_env)) {
        Some(value) => Some(value)
        None => value_from_object(shared_array_buffer_proto())
      }
      new_shared_array_buffer_value_with_options(
        new_len, new_len, false, proto_value,
      )
    } else {
      let proto_value = match
        value_from_object(array_buffer_proto_for_env(realm_env)) {
        Some(value) => Some(value)
        None => value_from_object(array_buffer_proto())
      }
      new_array_buffer_value_with_options(
        new_len,
        new_len,
        false,
        proto=proto_value,
      )
    }
  } else {
    call_constructor(species, [Number(Double::from_int(new_len))])
  }
  if strict_eq(new_obj, this_value) {
    return throw_type_error("cannot use identical ArrayBuffer")
  }
  let (new_buf_obj, new_data) = if shared {
    require_shared_array_buffer(Some(new_obj))
  } else {
    require_non_shared_array_buffer(Some(new_obj))
  }
  if new_data.detached {
    return throw_type_error("ArrayBuffer is detached")
  }
  if new_data.bytes.length() < new_len {
    return throw_type_error("new ArrayBuffer is too small")
  }
  let (_, updated) = if shared {
    require_shared_array_buffer(Some(this_value))
  } else {
    require_non_shared_array_buffer(Some(this_value))
  }
  if updated.detached || updated.bytes.length() < start + new_len {
    return throw_type_error("ArrayBuffer is detached")
  }
  let mut i = 0
  while i < new_len {
    new_data.bytes[i] = updated.bytes[start + i]
    i = i + 1
  }
  new_buf_obj.array_buffer_data = Some(new_data)
  new_obj
}
