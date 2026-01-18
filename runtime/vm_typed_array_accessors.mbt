///|
fn typed_array_get_length_value(this_value : Value?) -> Value raise {
  let (_, data) = require_typed_array(this_value)
  Number(Double::from_int(typed_array_effective_length(data)))
}

///|
fn typed_array_get_buffer_value(this_value : Value?) -> Value raise {
  let (_, data) = require_typed_array(this_value)
  Object(data.buffer)
}

///|
fn typed_array_get_byte_length_value(this_value : Value?) -> Value raise {
  let (_, data) = require_typed_array(this_value)
  if typed_array_is_oob(data) {
    Number(0.0)
  } else {
    Number(Double::from_int(typed_array_effective_byte_length(data)))
  }
}

///|
fn typed_array_get_byte_offset_value(this_value : Value?) -> Value raise {
  let (_, data) = require_typed_array(this_value)
  if typed_array_is_oob(data) {
    Number(0.0)
  } else {
    Number(Double::from_int(data.byte_offset))
  }
}

///|
fn typed_array_get_to_string_tag_value(this_value : Value?) -> Value {
  match this_value {
    Some(value) =>
      match typed_array_kind_from_value(value) {
        Some(kind) => String(typed_array_constructor_name(kind))
        None => Undefined
      }
    None => Undefined
  }
}

///|
fn typed_array_values_value(this_value : Value) -> Value raise {
  let (arr, _) = require_typed_array_checked(Some(this_value))
  new_array_iterator(Array(arr), 1)
}

///|
fn typed_array_keys_value(this_value : Value) -> Value raise {
  let (arr, _) = require_typed_array_checked(Some(this_value))
  new_array_iterator(Array(arr), 0)
}

///|
fn typed_array_entries_value(this_value : Value) -> Value raise {
  let (arr, _) = require_typed_array_checked(Some(this_value))
  new_array_iterator(Array(arr), 2)
}
