///|
fn array_fill_value(this_value : Value, args : Array[Value]) -> Value raise {
  let obj = to_object(this_value)
  let len = array_like_length_int64(obj)
  let start = if args.length() > 1 && !(args[1] is Undefined) {
    to_int64_clamp64(args[1], 0L, len, len)
  } else {
    0L
  }
  let end = if args.length() > 2 && !(args[2] is Undefined) {
    to_int64_clamp64(args[2], 0L, len, len)
  } else {
    len
  }
  let fill_value = if args.is_empty() { Undefined } else { args[0] }
  let mut k = start
  while k < end {
    let _ = property_set(obj, Int64::to_string(k), fill_value, true)
    k = k + 1L
  }
  obj
}

///|
fn array_copy_within_value(
  this_value : Value,
  args : Array[Value],
) -> Value raise {
  let obj = to_object(this_value)
  let len = array_like_length_int64(obj)
  let to_value = if args.is_empty() { Undefined } else { args[0] }
  let from_value = if args.length() > 1 { args[1] } else { Undefined }
  let to = to_int64_clamp64(to_value, 0L, len, len)
  let from = to_int64_clamp64(from_value, 0L, len, len)
  let mut final_index = len
  if args.length() > 2 && !(args[2] is Undefined) {
    final_index = to_int64_clamp64(args[2], 0L, len, len)
  }
  let mut count = final_index - from
  let max = len - to
  if max < count {
    count = max
  }
  if count <= 0L {
    return obj
  }
  let dir = if from < to && to < from + count { -1L } else { 1L }
  let mut i = 0L
  while i < count {
    let from_index = if dir < 0L { from + count - i - 1L } else { from + i }
    let to_index = if dir < 0L { to + count - i - 1L } else { to + i }
    let from_name = Int64::to_string(from_index)
    let to_name = Int64::to_string(to_index)
    if has_property_value(obj, from_name) {
      let value = property_get(obj, from_name)
      let _ = property_set(obj, to_name, value, true)

    } else {
      delete_property_or_throw(obj, to_name)
    }
    i = i + 1L
  }
  obj
}

///|
fn array_splice_value(this_value : Value, args : Array[Value]) -> Value raise {
  let obj = to_object(this_value)
  let len = array_like_length_int64(obj)
  let mut start = 0L
  if args.length() > 0 {
    start = to_int64_clamp64(args[0], 0L, len, len)
  }
  let mut item_count = 0
  let mut del_count = 0L
  if args.length() == 0 {
    item_count = 0
    del_count = 0L
  } else if args.length() == 1 {
    item_count = 0
    del_count = len - start
  } else {
    item_count = args.length() - 2
    del_count = to_int64_clamp64(args[1], 0L, len - start, 0L)
  }
  let item_count64 = Int64::from_int(item_count)
  let new_len = len + item_count64 - del_count
  if Int64::to_double(new_len) > 9007199254740991.0 {
    return throw_type_error("Array loo long")
  }
  let result = array_species_create64(obj, del_count)
  let mut k = 0L
  while k < del_count {
    let from_name = Int64::to_string(start + k)
    if has_property_value(obj, from_name) {
      let value = property_get(obj, from_name)
      create_data_property_or_throw(result, Int64::to_string(k), value)
    }
    k = k + 1L
  }
  let _ = property_set(
    result,
    "length",
    Number(Int64::to_double(del_count)),
    true,
  )
  if item_count64 < del_count {
    let mut k = start
    while k < len - del_count {
      let from = Int64::to_string(k + del_count)
      let to = Int64::to_string(k + item_count64)
      if has_property_value(obj, from) {
        let value = property_get(obj, from)
        let _ = property_set(obj, to, value, true)

      } else {
        delete_property_or_throw(obj, to)
      }
      k = k + 1L
    }
    let mut k = len
    while k > new_len {
      k = k - 1L
      delete_property_or_throw(obj, Int64::to_string(k))
    }
  } else if item_count64 > del_count {
    let mut k = len - del_count
    while k > start {
      k = k - 1L
      let from = Int64::to_string(k + del_count)
      let to = Int64::to_string(k + item_count64)
      if has_property_value(obj, from) {
        let value = property_get(obj, from)
        let _ = property_set(obj, to, value, true)

      } else {
        delete_property_or_throw(obj, to)
      }
    }
  }
  let mut i = 0
  while i < item_count {
    let _ = property_set(
      obj,
      Int64::to_string(start + Int64::from_int(i)),
      args[2 + i],
      true,
    )
    i = i + 1
  }
  let _ = property_set(obj, "length", Number(Int64::to_double(new_len)), true)
  result
}

///|
fn array_slice_value(this_value : Value, args : Array[Value]) -> Value raise {
  let obj = to_object(this_value)
  let len64 = array_like_length_int64(obj)
  let mut start = 0L
  if args.length() > 0 {
    start = to_int64_clamp64(args[0], 0L, len64, len64)
  }
  let mut end = len64
  if args.length() > 1 && !(args[1] is Undefined) {
    end = to_int64_clamp64(args[1], 0L, len64, len64)
  }
  let count64 = if end > start { end - start } else { 0L }
  let max_int = 2147483647
  if count64 > Int64::from_int(max_int) {
    return throw_range_error("invalid array length")
  }
  let count = Int64::to_int(count64)
  let result = array_species_create(obj, count)
  let mut k = start
  let mut n = 0
  while k < end {
    let name = Int64::to_string(k)
    if has_property(obj, name) {
      let value = property_get(obj, name)
      create_data_property_or_throw(result, Int::to_string(n), value)
    }
    k = k + 1
    n = n + 1
  }
  let _ = property_set(result, "length", Number(Double::from_int(count)), true)
  result
}

///|
fn array_reverse_value(this_value : Value) -> Value raise {
  let obj = to_object(this_value)
  let len = array_like_length_int64(obj)
  if len <= 1L {
    return obj
  }
  let middle = len / 2L
  let mut lower = 0L
  let mut upper = len - 1L
  while lower < middle {
    let lower_name = Int64::to_string(lower)
    let upper_name = Int64::to_string(upper)
    let lower_present = has_property_value(obj, lower_name)
    let mut lower_value = Undefined
    if lower_present {
      lower_value = property_get(obj, lower_name)
    }
    let upper_present = has_property_value(obj, upper_name)
    let mut upper_value = Undefined
    if upper_present {
      upper_value = property_get(obj, upper_name)
    }
    if lower_present {
      if upper_present {
        let _ = property_set(obj, lower_name, upper_value, true)
        let _ = property_set(obj, upper_name, lower_value, true)

      } else {
        delete_property_or_throw(obj, lower_name)
        let _ = property_set(obj, upper_name, lower_value, true)

      }
    } else if upper_present {
      let _ = property_set(obj, lower_name, upper_value, true)
      delete_property_or_throw(obj, upper_name)
    }
    lower = lower + 1L
    upper = upper - 1L
  }
  obj
}

///|
fn array_unshift_value(this_value : Value, args : Array[Value]) -> Value raise {
  let obj = to_object(this_value)
  let len = array_like_length_int64(obj)
  let item_count = args.length()
  if item_count == 0 {
    let _ = property_set(obj, "length", Number(Int64::to_double(len)), true)
    return Number(Int64::to_double(len))
  }
  let item_count64 = Int64::from_int(item_count)
  let new_len = len + item_count64
  let total = Int64::to_double(len) + Double::from_int(item_count)
  if total > 9007199254740991.0 {
    return throw_type_error("Array loo long")
  }
  let mut k = len
  while k > 0L {
    k = k - 1L
    let from = Int64::to_string(k)
    let to = Int64::to_string(k + item_count64)
    if has_property_value(obj, from) {
      let value = property_get(obj, from)
      let _ = property_set(obj, to, value, true)

    } else {
      delete_property_or_throw(obj, to)
    }
  }
  let mut i = 0
  while i < item_count {
    let index = Int64::from_int(i)
    let _ = property_set(obj, Int64::to_string(index), args[i], true)
    i = i + 1
  }
  let _ = property_set(obj, "length", Number(Int64::to_double(new_len)), true)
  Number(Int64::to_double(new_len))
}

///|
fn array_push_value(this_value : Value, args : Array[Value]) -> Value raise {
  let obj = to_object(this_value)
  let len = array_like_length_int64(obj)
  let item_count = args.length()
  if item_count == 0 {
    let _ = property_set(obj, "length", Number(Int64::to_double(len)), true)
    return Number(Int64::to_double(len))
  }
  let total = Int64::to_double(len) + Double::from_int(item_count)
  if total > 9007199254740991.0 {
    return throw_type_error("Array loo long")
  }
  let mut i = 0
  while i < item_count {
    let index = len + Int64::from_int(i)
    let _ = property_set(obj, Int64::to_string(index), args[i], true)
    i = i + 1
  }
  let new_len = len + Int64::from_int(item_count)
  let _ = property_set(obj, "length", Number(Int64::to_double(new_len)), true)
  Number(Int64::to_double(new_len))
}
