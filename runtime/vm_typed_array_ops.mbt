///|
fn to_int64_sat_value(value : Value) -> Int64 raise {
  let num = to_number(value)
  let max_i64 = 9223372036854775807L
  let min_i64 = -9223372036854775807L - 1L
  if Double::is_nan(num) {
    return 0L
  }
  if Double::is_inf(num) {
    return if num < 0.0 { min_i64 } else { max_i64 }
  }
  let trunc = Double::trunc(num)
  if trunc > 9223372036854775807.0 {
    return max_i64
  }
  if trunc < -9223372036854775808.0 {
    return min_i64
  }
  Double::to_int64(trunc)
}

///|
fn typed_array_set_index_or_throw(
  target : Value,
  index : Int,
  value : Value,
) -> Unit raise {
  let _ = property_set(target, Int::to_string(index), value, true)

}

///|
fn typed_array_at_value(this_value : Value, args : Array[Value]) -> Value raise {
  let (_, data) = require_typed_array_checked(Some(this_value))
  let len = typed_array_effective_length(data)
  let index = if args.is_empty() { 0 } else { to_int32_sat(to_number(args[0])) }
  let mut actual = index
  if actual < 0 {
    actual = len + actual
  }
  if actual < 0 || actual >= len {
    Undefined
  } else {
    typed_array_get_index(data, actual)
  }
}

///|
fn typed_array_with_value(
  this_value : Value,
  args : Array[Value],
) -> Value raise {
  let (_, data) = require_typed_array_checked(Some(this_value))
  let len64 = Int64::from_int(typed_array_effective_length(data))
  let max_int = 2147483647
  if len64 > Int64::from_int(max_int) {
    return throw_range_error("invalid typed array length")
  }
  let len = Int64::to_int(len64)
  let index_value = if args.is_empty() { Undefined } else { args[0] }
  let mut idx64 = to_int64_sat_value(index_value)
  if idx64 < 0L {
    idx64 = len64 + idx64
  }
  let replacement_value = if args.length() > 1 { args[1] } else { Undefined }
  let replacement = to_primitive(replacement_value, false)
  let current_len = Int64::from_int(typed_array_effective_length(data))
  if typed_array_is_oob(data) || idx64 < 0L || idx64 >= current_len {
    return throw_range_error("invalid array index")
  }
  let result = new_typed_array_value(data.kind, len)
  let mut k = 0
  while k < len {
    let value = if Int64::from_int(k) == idx64 {
      replacement
    } else {
      typed_array_get_index(data, k)
    }
    typed_array_set_index_or_throw(result, k, value)
    k = k + 1
  }
  result
}

///|
fn typed_array_map_value(
  this_value : Value,
  args : Array[Value],
) -> Value raise {
  let (arr, data) = require_typed_array_checked(Some(this_value))
  let callback = if args.is_empty() { Undefined } else { args[0] }
  let this_arg = if args.length() > 1 { args[1] } else { Undefined }
  if !is_callable(callback) {
    return throw_type_error("not a function")
  }
  let len = typed_array_effective_length(data)
  let result = typed_array_species_create64(
    this_value,
    data.kind,
    Int64::from_int(len),
  )
  let mut k = 0
  while k < len {
    let value = typed_array_get_index(data, k)
    let mapped = call_value_with_this(
      callback,
      [value, Number(Double::from_int(k)), Array(arr)],
      this_arg,
    )
    typed_array_set_index_or_throw(result, k, mapped)
    k = k + 1
  }
  result
}

///|
fn typed_array_filter_value(
  this_value : Value,
  args : Array[Value],
) -> Value raise {
  let (arr, data) = require_typed_array_checked(Some(this_value))
  let callback = if args.is_empty() { Undefined } else { args[0] }
  let this_arg = if args.length() > 1 { args[1] } else { Undefined }
  if !is_callable(callback) {
    return throw_type_error("not a function")
  }
  let len = typed_array_effective_length(data)
  let selected : Array[Value] = []
  let mut k = 0
  while k < len {
    let value = typed_array_get_index(data, k)
    let res = call_value_with_this(
      callback,
      [value, Number(Double::from_int(k)), Array(arr)],
      this_arg,
    )
    if is_truthy(res) {
      selected.push(value)
    }
    k = k + 1
  }
  let result_len = selected.length()
  let result = typed_array_species_create64(
    this_value,
    data.kind,
    Int64::from_int(result_len),
  )
  let mut i = 0
  while i < result_len {
    typed_array_set_index_or_throw(result, i, selected[i])
    i = i + 1
  }
  result
}

///|
fn typed_array_slice_value(
  this_value : Value,
  args : Array[Value],
) -> Value raise {
  let (_, data) = require_typed_array_checked(Some(this_value))
  let len = typed_array_effective_length(data)
  let len64 = Int64::from_int(len)
  let mut start = 0L
  if args.length() > 0 {
    start = to_int64_clamp64(args[0], 0L, len64, len64)
  }
  let mut end = len64
  if args.length() > 1 && !(args[1] is Undefined) {
    end = to_int64_clamp64(args[1], 0L, len64, len64)
  }
  let mut count64 = if end > start { end - start } else { 0L }
  let result = typed_array_species_create64(this_value, data.kind, count64)
  if count64 <= 0L {
    let _ = require_typed_array_checked(Some(result))
    return result
  }
  let (_, data_checked) = require_typed_array_checked(Some(this_value))
  let (_, result_data) = require_typed_array_checked(Some(result))
  let current_len64 = Int64::from_int(
    typed_array_effective_length(data_checked),
  )
  let mut space64 = current_len64 - start
  if space64 < 0L {
    space64 = 0L
  }
  if count64 > space64 {
    count64 = space64
  }
  if data_checked.kind == result_data.kind {
    let src_start = data_checked.byte_offset +
      Int64::to_int(start) * data_checked.bytes_per_element
    let dst_start = result_data.byte_offset
    let byte_count = Int64::to_int(count64) * data_checked.bytes_per_element
    match
      (
        data_checked.buffer.array_buffer_data,
        result_data.buffer.array_buffer_data,
      ) {
      (Some(src_buf), Some(dst_buf)) => {
        let mut idx = 0
        while idx < byte_count {
          dst_buf.bytes[dst_start + idx] = src_buf.bytes[src_start + idx]
          idx = idx + 1
        }
      }
      _ => ()
    }
    return result
  }
  let mut k = 0L
  while k < count64 {
    let value = typed_array_get_index(data_checked, Int64::to_int(start + k))
    typed_array_set_index_or_throw(result, Int64::to_int(k), value)
    k = k + 1L
  }
  result
}

///|
fn typed_array_subarray_value(
  this_value : Value,
  args : Array[Value],
) -> Value raise {
  let (_, data) = require_typed_array(Some(this_value))
  let len64 = Int64::from_int(typed_array_effective_length(data))
  let mut start = 0L
  if args.length() > 0 {
    start = to_int64_clamp64(args[0], 0L, len64, len64)
  }
  let mut end = len64
  let mut tracking = data.length_tracking
  if args.length() > 1 && !(args[1] is Undefined) {
    end = to_int64_clamp64(args[1], 0L, len64, len64)
    tracking = false
  }
  let count64 = if end > start { end - start } else { 0L }
  let offset = data.byte_offset + Int64::to_int(start) * data.bytes_per_element
  let ctor = typed_array_species_constructor(this_value, data.kind)
  let result = if tracking {
    call_constructor(ctor, [
      Object(data.buffer),
      Number(Double::from_int(offset)),
    ])
  } else {
    call_constructor(ctor, [
      Object(data.buffer),
      Number(Double::from_int(offset)),
      Number(Int64::to_double(count64)),
    ])
  }
  let _ = require_typed_array_checked(Some(result))
  result
}

///|
fn typed_array_reverse_value(this_value : Value) -> Value raise {
  let (_, data) = require_typed_array_checked(Some(this_value))
  let len = typed_array_effective_length(data)
  if len > 1 {
    let mut lower = 0
    let mut upper = len - 1
    while lower < upper {
      let lower_value = typed_array_get_index(data, lower)
      let upper_value = typed_array_get_index(data, upper)
      let _ = typed_array_set_index(data, lower, upper_value)
      let _ = typed_array_set_index(data, upper, lower_value)
      lower = lower + 1
      upper = upper - 1
    }
  }
  this_value
}

///|
fn typed_array_to_reversed_value(this_value : Value) -> Value raise {
  let (_, data) = require_typed_array_checked(Some(this_value))
  let len = typed_array_effective_length(data)
  let result = new_typed_array_value(data.kind, len)
  let mut out_index = 0
  let mut i = len - 1
  while i >= 0 && out_index < len {
    let value = typed_array_get_index(data, i)
    typed_array_set_index_or_throw(result, out_index, value)
    out_index = out_index + 1
    if i == 0 {
      break
    }
    i = i - 1
  }
  result
}

///|
fn typed_array_join_impl(
  this_value : Value,
  sep_value : Value?,
  to_locale : Bool,
) -> String raise {
  let (_, data) = require_typed_array_checked(Some(this_value))
  let mut sep = ","
  let old_len = typed_array_effective_length(data)
  let mut new_len = old_len
  if !to_locale {
    match sep_value {
      None => ()
      Some(Undefined) => ()
      Some(value) => {
        sep = to_string_strict(value)
        new_len = typed_array_effective_length(data)
      }
    }
  }
  let len = if new_len < old_len { new_len } else { old_len }
  let sb = StringBuilder::new()
  let mut i = 0
  while i < len {
    if i > 0 {
      sb.write_string(sep)
    }
    let value = typed_array_get_index(data, i)
    match value {
      Undefined | Null => ()
      _ =>
        if to_locale {
          sb.write_string(value_to_locale_string(value))
        } else {
          sb.write_string(to_string_strict(value))
        }
    }
    i = i + 1
  }
  let mut extra_start = new_len
  if extra_start < 1 {
    extra_start = 1
  }
  i = extra_start
  while i < old_len {
    sb.write_string(sep)
    i = i + 1
  }
  sb.to_string()
}

///|
fn typed_array_join_value(
  this_value : Value,
  args : Array[Value],
) -> String raise {
  let sep_value = if args.is_empty() { None } else { Some(args[0]) }
  typed_array_join_impl(this_value, sep_value, false)
}

///|
fn typed_array_to_locale_string_value(this_value : Value) -> String raise {
  typed_array_join_impl(this_value, None, true)
}

///|
fn typed_array_iterate_value(
  this_value : Value,
  callback : Value,
  this_arg : Value,
  kind : ArrayIterKind,
) -> Value raise {
  let (_, data) = require_typed_array_checked(Some(this_value))
  if !is_callable(callback) {
    return throw_type_error("not a function")
  }
  let len = typed_array_effective_length(data)
  let mut k = 0
  while k < len {
    let value = typed_array_get_index(data, k)
    let res = call_value_with_this(
      callback,
      [value, Number(Double::from_int(k)), this_value],
      this_arg,
    )
    match kind {
      ArrayIterKind::Every => if !is_truthy(res) { return Bool(false) }
      ArrayIterKind::SomeMatch => if is_truthy(res) { return Bool(true) }
      ArrayIterKind::ForEach => ()
      _ => ()
    }
    k = k + 1
  }
  match kind {
    ArrayIterKind::Every => Bool(true)
    ArrayIterKind::SomeMatch => Bool(false)
    _ => Undefined
  }
}

///|
fn typed_array_reduce_value(
  this_value : Value,
  args : Array[Value],
  from_right : Bool,
) -> Value raise {
  let (_, data) = require_typed_array_checked(Some(this_value))
  let callback = if args.is_empty() { Undefined } else { args[0] }
  if !is_callable(callback) {
    return throw_type_error("not a function")
  }
  let len = typed_array_effective_length(data)
  let mut k = 0
  let mut acc : Value? = if args.length() > 1 { Some(args[1]) } else { None }
  if acc is None {
    if len == 0 {
      return throw_type_error("Reduce of empty array with no initial value")
    }
    let index = if from_right { len - 1 } else { 0 }
    acc = Some(typed_array_get_index(data, index))
    k = 1
  }
  let mut current = match acc {
    Some(value) => value
    None => Undefined
  }
  while k < len {
    let index = if from_right { len - k - 1 } else { k }
    let value = typed_array_get_index(data, index)
    current = call_value_with_this(
      callback,
      [current, value, Number(Double::from_int(index)), this_value],
      Undefined,
    )
    k = k + 1
  }
  current
}

///|
fn typed_array_find_value(
  this_value : Value,
  args : Array[Value],
  kind : ArrayFindKind,
) -> Value raise {
  let (_, data) = require_typed_array_checked(Some(this_value))
  let len = typed_array_effective_length(data)
  let callback = if args.is_empty() { Undefined } else { args[0] }
  if !is_callable(callback) {
    return throw_type_error("not a function")
  }
  let this_arg = if args.length() > 1 { args[1] } else { Undefined }
  let mut k = 0
  let mut end = len
  let mut dir = 1
  let mut is_last = false
  match kind {
    ArrayFindKind::FindLast | ArrayFindKind::FindLastIndex => is_last = true
    _ => ()
  }
  if is_last {
    k = len - 1
    end = -1
    dir = -1
  }
  while k != end {
    let value = typed_array_get_index(data, k)
    let result = call_value_with_this(
      callback,
      [value, Number(Double::from_int(k)), this_value],
      this_arg,
    )
    if is_truthy(result) {
      return match kind {
        ArrayFindKind::FindIndex | ArrayFindKind::FindLastIndex =>
          Number(Double::from_int(k))
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

///|
fn typed_array_index_of_value(
  this_value : Value,
  args : Array[Value],
) -> Value raise {
  let (_, data) = require_typed_array_checked(Some(this_value))
  let len = typed_array_effective_length(data)
  if len <= 0 {
    return Number(-1.0)
  }
  let search = if args.is_empty() { Undefined } else { args[0] }
  let len64 = Int64::from_int(len)
  let mut from_index = 0L
  if args.length() > 1 {
    from_index = to_int64_clamp64(args[1], 0L, len64, len64)
  }
  if from_index >= len64 {
    return Number(-1.0)
  }
  let (_, data_checked) = require_typed_array(Some(this_value))
  let current_len = typed_array_effective_length(data_checked)
  let search_len = if current_len < len { current_len } else { len }
  if search_len <= 0 {
    return Number(-1.0)
  }
  let search_len64 = Int64::from_int(search_len)
  if from_index >= search_len64 {
    return Number(-1.0)
  }
  let mut k = Int64::to_int(from_index)
  while k < search_len {
    let value = typed_array_get_index(data_checked, k)
    if strict_eq(value, search) {
      return Number(Double::from_int(k))
    }
    k = k + 1
  }
  Number(-1.0)
}

///|
fn typed_array_last_index_of_value(
  this_value : Value,
  args : Array[Value],
) -> Value raise {
  let (_, data) = require_typed_array_checked(Some(this_value))
  let len = typed_array_effective_length(data)
  if len <= 0 {
    return Number(-1.0)
  }
  let search = if args.is_empty() { Undefined } else { args[0] }
  let len64 = Int64::from_int(len)
  let mut from_index = len64 - 1L
  if args.length() > 1 {
    from_index = to_int64_clamp64(args[1], -1L, len64 - 1L, len64)
  }
  if from_index < 0L {
    return Number(-1.0)
  }
  let (_, data_checked) = require_typed_array(Some(this_value))
  let current_len = typed_array_effective_length(data_checked)
  if current_len <= 0 {
    return Number(-1.0)
  }
  let current_len64 = Int64::from_int(current_len)
  if from_index >= current_len64 {
    from_index = current_len64 - 1L
  }
  if from_index < 0L {
    return Number(-1.0)
  }
  let mut k = Int64::to_int(from_index)
  while k >= 0 {
    let value = typed_array_get_index(data_checked, k)
    if strict_eq(value, search) {
      return Number(Double::from_int(k))
    }
    if k == 0 {
      break
    }
    k = k - 1
  }
  Number(-1.0)
}

///|
fn typed_array_includes_value(
  this_value : Value,
  args : Array[Value],
) -> Value raise {
  let (_, data) = require_typed_array_checked(Some(this_value))
  let len = typed_array_effective_length(data)
  if len <= 0 {
    return Bool(false)
  }
  let search = if args.is_empty() { Undefined } else { args[0] }
  let len64 = Int64::from_int(len)
  let mut from_index = 0L
  if args.length() > 1 {
    from_index = to_int64_clamp64(args[1], 0L, len64, len64)
  }
  if from_index >= len64 {
    return Bool(false)
  }
  let (_, data_checked) = require_typed_array(Some(this_value))
  let mut k = Int64::to_int(from_index)
  while k < len {
    let value = typed_array_get_index(data_checked, k)
    if same_value_zero(value, search) {
      return Bool(true)
    }
    k = k + 1
  }
  Bool(false)
}

///|
fn typed_array_from_value(
  this_value : Value,
  args : Array[Value],
) -> Value raise {
  if !is_constructor_value(this_value) {
    return throw_type_error("not a constructor")
  }
  let mapfn = if args.length() > 1 { args[1] } else { Undefined }
  let this_arg = if args.length() > 2 { args[2] } else { Undefined }
  if !(mapfn is Undefined) && !is_callable(mapfn) {
    return throw_type_error("not a function")
  }
  let items = if args.is_empty() { Undefined } else { args[0] }
  if items is Undefined || items is Null {
    return throw_type_error("not an object")
  }
  let iter_method = match symbol_iterator_key() {
    Some(key) => property_get_with_receiver(items, key, items)
    None => Undefined
  }
  let mut array_value = Undefined
  let mut len64 = 0L
  if !(iter_method is Undefined) && !(iter_method is Null) {
    if !is_callable(iter_method) {
      return throw_type_error("value is not iterable")
    }
    let iterator = call_value_with_this(iter_method, [], items)
    let next_method = property_get(iterator, "next")
    if !is_callable(next_method) {
      return throw_type_error("not a function")
    }
    let array_proto_value = match current_env() {
      Some(env) =>
        match value_from_object(array_proto_for_env(env)) {
          Some(value) => Some(value)
          None => value_from_object(array_proto())
        }
      None => value_from_object(array_proto())
    }
    let values : Array[Value?] = []
    for {
      let (done, value) = iterator_step_value(iterator, next_method)
      if done {
        break
      }
      values.push(Some(value))
    }
    array_value = new_array_value_with_proto(values, proto=array_proto_value)
    len64 = Int64::from_int(values.length())
  } else {
    let obj = to_object(items)
    len64 = array_like_length_int64(obj)
    array_value = obj
  }
  let result = typed_array_create_from_constructor(this_value, len64)
  let mut k = 0L
  while k < len64 {
    let k_value = property_get(array_value, Int64::to_string(k))
    let mapped = if mapfn is Undefined {
      k_value
    } else {
      call_value_with_this(
        mapfn,
        [k_value, Number(Int64::to_double(k))],
        this_arg,
      )
    }
    let _ = property_set(result, Int64::to_string(k), mapped, true)
    k = k + 1L
  }
  result
}

///|
fn typed_array_of_value(this_value : Value, args : Array[Value]) -> Value raise {
  if !is_constructor_value(this_value) {
    return throw_type_error("not a constructor")
  }
  let len64 = Int64::from_int(args.length())
  let result = typed_array_create_from_constructor(this_value, len64)
  let mut i = 0
  while i < args.length() {
    let _ = property_set(result, Int::to_string(i), args[i], true)
    i = i + 1
  }
  result
}

///|
fn typed_array_set_value(
  this_value : Value,
  args : Array[Value],
) -> Value raise {
  let (_, data) = require_typed_array(Some(this_value))
  if args.is_empty() {
    return Undefined
  }
  let offset_value = if args.length() > 1 { args[1] } else { Undefined }
  let offset64 = to_int64_sat_value(offset_value)
  if offset64 < 0L {
    return throw_range_error("invalid array length")
  }
  if typed_array_is_oob(data) {
    return throw_type_error("ArrayBuffer is detached")
  }
  let target_len64 = Int64::from_int(typed_array_effective_length(data))
  let source = args[0]
  match source {
    Array(src_arr) =>
      match src_arr.typed_array_data {
        Some(src_data) => {
          if typed_array_is_oob(src_data) {
            return throw_type_error("ArrayBuffer is detached")
          }
          let source_len = typed_array_effective_length(src_data)
          let source_len64 = Int64::from_int(source_len)
          if offset64 > target_len64 - source_len64 {
            return throw_range_error("invalid array length")
          }
          let values : Array[Value] = []
          let mut i = 0
          while i < source_len {
            values.push(typed_array_get_index(src_data, i))
            i = i + 1
          }
          let offset = Int64::to_int(offset64)
          i = 0
          while i < source_len {
            let _ = typed_array_set_index(data, offset + i, values[i])
            i = i + 1
          }
        }
        None => {
          let source_value = Array(src_arr)
          let source_len64 = array_like_length_int64(source_value)
          if offset64 > target_len64 - source_len64 {
            return throw_range_error("invalid array length")
          }
          let offset = Int64::to_int(offset64)
          let mut i = 0L
          while i < source_len64 {
            let value = property_get(source_value, Int64::to_string(i))
            match data.kind {
              TypedArrayKind::BigInt64 | TypedArrayKind::BigUint64 => {
                let _ = to_bigint(value)

              }
              _ => ()
            }
            let _ = typed_array_set_index(
              data,
              offset + Int64::to_int(i),
              value,
            )
            i = i + 1L
          }
        }
      }
    _ => {
      let src_obj = to_object(source)
      let source_len64 = array_like_length_int64(src_obj)
      if offset64 > target_len64 - source_len64 {
        return throw_range_error("invalid array length")
      }
      let offset = Int64::to_int(offset64)
      let mut i = 0L
      while i < source_len64 {
        let value = property_get(src_obj, Int64::to_string(i))
        match data.kind {
          TypedArrayKind::BigInt64 | TypedArrayKind::BigUint64 => {
            let _ = to_bigint(value)

          }
          _ => ()
        }
        let _ = typed_array_set_index(data, offset + Int64::to_int(i), value)
        i = i + 1L
      }
    }
  }
  Undefined
}

///|
fn typed_array_copy_within_value(
  this_value : Value,
  args : Array[Value],
) -> Value raise {
  let (_, data) = require_typed_array_checked(Some(this_value))
  let len = typed_array_effective_length(data)
  let len64 = Int64::from_int(len)
  let to_value = if args.is_empty() { Undefined } else { args[0] }
  let from_value = if args.length() > 1 { args[1] } else { Undefined }
  let to = to_int64_clamp64(to_value, 0L, len64, len64)
  let from = to_int64_clamp64(from_value, 0L, len64, len64)
  let mut final_index = len64
  if args.length() > 2 && !(args[2] is Undefined) {
    final_index = to_int64_clamp64(args[2], 0L, len64, len64)
  }
  let (_, data_checked) = require_typed_array_checked(Some(this_value))
  let current_len64 = Int64::from_int(
    typed_array_effective_length(data_checked),
  )
  let mut count = final_index - from
  let max = len64 - to
  if max < count {
    count = max
  }
  let space = current_len64 - (if from > to { from } else { to })
  if space < count {
    count = space
  }
  if count <= 0L {
    return this_value
  }
  let dir = if from < to && to < from + count { -1L } else { 1L }
  let mut i = 0L
  while i < count {
    let from_index = if dir < 0L { from + count - i - 1L } else { from + i }
    let to_index = if dir < 0L { to + count - i - 1L } else { to + i }
    let value = typed_array_get_index(data_checked, Int64::to_int(from_index))
    let _ = typed_array_set_index(data_checked, Int64::to_int(to_index), value)
    i = i + 1L
  }
  this_value
}

///|
fn typed_array_fill_value(
  this_value : Value,
  args : Array[Value],
) -> Value raise {
  let (_, data) = require_typed_array_checked(Some(this_value))
  let len = typed_array_effective_length(data)
  let len64 = Int64::from_int(len)
  let fill_arg = if args.is_empty() { Undefined } else { args[0] }
  let fill_value = match data.kind {
    TypedArrayKind::BigInt64 | TypedArrayKind::BigUint64 =>
      BigInt(to_bigint(fill_arg))
    _ => Number(to_number(fill_arg))
  }
  let mut start = 0L
  if args.length() > 1 && !(args[1] is Undefined) {
    start = to_int64_clamp64(args[1], 0L, len64, len64)
  }
  let mut end = len64
  if args.length() > 2 && !(args[2] is Undefined) {
    end = to_int64_clamp64(args[2], 0L, len64, len64)
  }
  if typed_array_is_oob(data) {
    return throw_type_error("ArrayBuffer is detached")
  }
  let current_len64 = Int64::from_int(typed_array_effective_length(data))
  if end > current_len64 {
    end = current_len64
  }
  let mut k = start
  while k < end {
    let _ = typed_array_set_index(data, Int64::to_int(k), fill_value)
    k = k + 1L
  }
  this_value
}
