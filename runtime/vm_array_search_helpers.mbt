///|
fn array_index_of_value(this_value : Value, args : Array[Value]) -> Value raise {
  let obj = to_object(this_value)
  let len = array_like_length_int64(obj)
  if len <= 0L {
    return Number(-1.0)
  }
  let search = if args.is_empty() { Undefined } else { args[0] }
  let mut from_index = 0L
  if args.length() > 1 {
    from_index = to_int64_clamp64(args[1], 0L, len, len)
  }
  if from_index >= len {
    return Number(-1.0)
  }
  let mut k = from_index
  while k < len {
    let name = Int64::to_string(k)
    if has_property_value(obj, name) {
      let value = property_get(obj, name)
      if strict_eq(value, search) {
        return Number(Int64::to_double(k))
      }
    }
    k = k + 1
  }
  Number(-1.0)
}

///|
fn array_last_index_of_value(
  this_value : Value,
  args : Array[Value],
) -> Value raise {
  let obj = to_object(this_value)
  let len = array_like_length_int64(obj)
  if len <= 0L {
    return Number(-1.0)
  }
  let search = if args.is_empty() { Undefined } else { args[0] }
  let mut from_index = len - 1L
  if args.length() > 1 {
    from_index = to_int64_clamp64(args[1], -1L, len - 1L, len)
  }
  if from_index < 0 {
    return Number(-1.0)
  }
  let mut k = from_index
  while k >= 0 {
    let name = Int64::to_string(k)
    if has_property_value(obj, name) {
      let value = property_get(obj, name)
      if strict_eq(value, search) {
        return Number(Int64::to_double(k))
      }
    }
    if k == 0 {
      break
    }
    k = k - 1
  }
  Number(-1.0)
}

///|
fn array_includes_value(this_value : Value, args : Array[Value]) -> Value raise {
  let obj = to_object(this_value)
  let len = array_like_length_int64(obj)
  if len <= 0L {
    return Bool(false)
  }
  let search = if args.is_empty() { Undefined } else { args[0] }
  let mut from_index = 0L
  if args.length() > 1 {
    from_index = to_int64_clamp64(args[1], 0L, len, len)
  }
  if from_index >= len {
    return Bool(false)
  }
  let mut k = from_index
  while k < len {
    let value = property_get(obj, Int64::to_string(k))
    if same_value_zero(value, search) {
      return Bool(true)
    }
    k = k + 1
  }
  Bool(false)
}

///|
fn array_find_value(
  this_value : Value,
  args : Array[Value],
  kind : ArrayFindKind,
) -> Value raise {
  let obj = to_object(this_value)
  let len = array_like_length_int64(obj)
  let callback = if args.is_empty() { Undefined } else { args[0] }
  if !is_callable(callback) {
    return throw_type_error("not a function")
  }
  let this_arg = if args.length() > 1 { args[1] } else { Undefined }
  let mut k = 0L
  let mut end = len
  let mut dir = 1L
  let mut is_last = false
  match kind {
    ArrayFindKind::FindLast | ArrayFindKind::FindLastIndex => is_last = true
    _ => ()
  }
  if is_last {
    k = len - 1
    end = -1L
    dir = -1L
  }
  while k != end {
    let value = property_get(obj, Int64::to_string(k))
    let result = call_value_with_this(
      callback,
      [value, Number(Int64::to_double(k)), this_value],
      this_arg,
    )
    if is_truthy(result) {
      return match kind {
        ArrayFindKind::FindIndex | ArrayFindKind::FindLastIndex =>
          Number(Int64::to_double(k))
        _ => value
      }
    }
    k = k + dir
  }
  match kind {
    ArrayFindKind::FindIndex | ArrayFindKind::FindLastIndex => Number(-1.0)
    _ => Undefined
  }
}
