///|
fn array_with_value(this_value : Value, args : Array[Value]) -> Value raise {
  let obj = to_object(this_value)
  let len64 = array_like_length_int64(obj)
  let max_int = 2147483647
  if len64 > Int64::from_int(max_int) {
    return throw_range_error("invalid array length")
  }
  let len = Int64::to_int(len64)
  let index_value = if args.is_empty() { Undefined } else { args[0] }
  let idx_num = to_number(index_value)
  let max_i64 = 9223372036854775807L
  let min_i64 = -9223372036854775807L - 1L
  let mut idx64 = if Double::is_nan(idx_num) {
    0L
  } else if Double::is_inf(idx_num) {
    if idx_num < 0.0 {
      min_i64
    } else {
      max_i64
    }
  } else {
    let trunc = Double::trunc(idx_num)
    if trunc > 9223372036854775807.0 {
      max_i64
    } else if trunc < -9223372036854775808.0 {
      min_i64
    } else {
      Double::to_int64(trunc)
    }
  }
  if idx64 < 0L {
    idx64 = len64 + idx64
  }
  if idx64 < 0L || idx64 >= len64 {
    return throw_range_error("invalid array index: \{Int64::to_string(idx64)}")
  }
  let replacement = if args.length() > 1 { args[1] } else { Undefined }
  let array_proto_value = match current_env() {
    Some(env) =>
      match value_from_object(array_proto_for_env(env)) {
        Some(value) => Some(value)
        None => value_from_object(array_proto())
      }
    None => value_from_object(array_proto())
  }
  let result = new_array_value_with_proto(
    Array::make(len, None),
    proto=array_proto_value,
  )
  let mut k = 0
  while k < len {
    let value = if Int64::from_int(k) == idx64 {
      replacement
    } else {
      let key = Int::to_string(k)
      if has_property_value(obj, key) {
        property_get(obj, key)
      } else {
        Undefined
      }
    }
    create_data_property_or_throw(result, Int::to_string(k), value)
    k = k + 1
  }
  result
}

///|
fn array_to_reversed_value(this_value : Value) -> Value raise {
  let obj = to_object(this_value)
  let len64 = array_like_length_int64(obj)
  let max_int = 2147483647
  if len64 > Int64::from_int(max_int) {
    return throw_range_error("invalid array length")
  }
  let len = Int64::to_int(len64)
  let array_proto_value = match current_env() {
    Some(env) =>
      match value_from_object(array_proto_for_env(env)) {
        Some(value) => Some(value)
        None => value_from_object(array_proto())
      }
    None => value_from_object(array_proto())
  }
  let result = new_array_value_with_proto(
    Array::make(len, None),
    proto=array_proto_value,
  )
  let mut out_index = 0
  let mut i = len - 1
  while i >= 0 && out_index < len {
    let key = Int::to_string(i)
    let value = if has_property_value(obj, key) {
      property_get(obj, key)
    } else {
      Undefined
    }
    create_data_property_or_throw(result, Int::to_string(out_index), value)
    out_index = out_index + 1
    if i == 0 {
      break
    }
    i = i - 1
  }
  result
}

///|
fn array_to_sorted_value(
  this_value : Value,
  args : Array[Value],
) -> Value raise {
  let compare_arg = if args.is_empty() { Undefined } else { args[0] }
  if !(compare_arg is Undefined) && !is_callable(compare_arg) {
    return throw_type_error("not a function")
  }
  let compare_fn = if compare_arg is Undefined {
    None
  } else {
    Some(compare_arg)
  }
  let obj = to_object(this_value)
  let len64 = array_like_length_int64(obj)
  let max_int = 2147483647
  if len64 > Int64::from_int(max_int) {
    return throw_range_error("invalid array length")
  }
  let len = Int64::to_int(len64)
  let array_proto_value = match current_env() {
    Some(env) =>
      match value_from_object(array_proto_for_env(env)) {
        Some(value) => Some(value)
        None => value_from_object(array_proto())
      }
    None => value_from_object(array_proto())
  }
  let result = new_array_value_with_proto(
    Array::make(len, None),
    proto=array_proto_value,
  )
  let mut i = 0
  while i < len {
    let key = Int::to_string(i)
    let value = if has_property_value(obj, key) {
      property_get(obj, key)
    } else {
      Undefined
    }
    create_data_property_or_throw(result, Int::to_string(i), value)
    i = i + 1
  }
  array_sort_object(result, compare_fn)
}

///|
fn props_has_array_index(props : Props) -> Bool {
  let mut found = false
  props_iter(props, fn(key, _) {
    if !found && key != "length" {
      match array_index_from_name(key) {
        Some(_) => found = true
        None => ()
      }
    }
  })
  found
}

///|
fn array_to_spliced_value(
  this_value : Value,
  args : Array[Value],
) -> Value raise {
  let obj = to_object(this_value)
  let len64 = array_like_length_int64(obj)
  let mut start = 0L
  if args.length() > 0 {
    start = to_int64_clamp64(args[0], 0L, len64, len64)
  }
  let mut del = 0L
  if args.length() > 0 {
    del = len64 - start
  }
  if args.length() > 1 {
    del = to_int64_clamp64(args[1], 0L, del, 0L)
  }
  let add = if args.length() > 2 {
    Int64::from_int(args.length() - 2)
  } else {
    0L
  }
  let new_len64 = len64 + add - del
  if Int64::to_double(new_len64) > 9007199254740991.0 {
    return throw_type_error("invalid array length")
  }
  if new_len64 > max_array_length {
    return throw_range_error("invalid array length")
  }
  let max_int = 2147483647
  let max_int64 = Int64::from_int(max_int)
  if new_len64 > max_int64 {
    return throw_range_error("invalid array length")
  }
  let new_len = Int64::to_int(new_len64)
  let len_int = if len64 <= max_int64 { Int64::to_int(len64) } else { -1 }
  let array_proto_value = match current_env() {
    Some(env) =>
      match value_from_object(array_proto_for_env(env)) {
        Some(value) => Some(value)
        None => value_from_object(array_proto())
      }
    None => value_from_object(array_proto())
  }
  let result = new_array_value_with_proto(
    Array::make(new_len, None),
    proto=array_proto_value,
  )
  let obj_is_proxy = match obj {
    Object(inner) => inner.proxy_data is Some(_)
    _ => false
  }
  let mut fast_elements : Array[Value?]? = None
  match obj {
    Array(arr) =>
      if arr.typed_array_data is None &&
        len_int >= 0 &&
        arr.elements.length() == len_int &&
        !props_has_array_index(arr.props) {
        fast_elements = Some(arr.elements)
      }
    _ => ()
  }
  let get_value = fn(index : Int64) -> Value raise {
    match fast_elements {
      Some(elements) => {
        let idx = Int64::to_int(index)
        if idx >= 0 && idx < elements.length() {
          match elements[idx] {
            Some(value) => return value
            None => ()
          }
        }
      }
      None => ()
    }
    let key = Int64::to_string(index)
    if obj_is_proxy {
      if has_property_value(obj, key) {
        property_get(obj, key)
      } else {
        Undefined
      }
    } else {
      property_get(obj, key)
    }
  }
  match result {
    Array(arr) => {
      let mut write_index = 0
      let mut i = 0L
      while i < start {
        let value = get_value(i)
        rc_replace_optional_value(arr.elements[write_index], Some(value))
        arr.elements[write_index] = Some(value)
        write_index = write_index + 1
        i = i + 1L
      }
      let mut j = 0
      let add_count = if args.length() > 2 { args.length() - 2 } else { 0 }
      while j < add_count {
        let value = args[2 + j]
        rc_replace_optional_value(arr.elements[write_index], Some(value))
        arr.elements[write_index] = Some(value)
        write_index = write_index + 1
        j = j + 1
      }
      i = start + del
      while i < len64 {
        let value = get_value(i)
        rc_replace_optional_value(arr.elements[write_index], Some(value))
        arr.elements[write_index] = Some(value)
        write_index = write_index + 1
        i = i + 1L
      }
      result
    }
    _ => {
      let mut write_index = 0L
      let mut i = 0L
      while i < start {
        let value = get_value(i)
        create_data_property_or_throw(result, Int64::to_string(write_index), value)
        write_index = write_index + 1L
        i = i + 1L
      }
      let mut j = 0
      while j < Int64::to_int(add) {
        create_data_property_or_throw(
          result,
          Int64::to_string(write_index),
          args[2 + j],
        )
        write_index = write_index + 1L
        j = j + 1
      }
      i = start + del
      while i < len64 {
        let value = get_value(i)
        create_data_property_or_throw(result, Int64::to_string(write_index), value)
        write_index = write_index + 1L
        i = i + 1L
      }
      result
    }
  }
}
