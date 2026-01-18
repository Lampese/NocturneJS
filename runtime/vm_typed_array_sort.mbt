///|
fn typed_array_collect_values(data : TypedArrayData) -> Array[Value] {
  let len = typed_array_effective_length(data)
  let values : Array[Value] = []
  let mut i = 0
  while i < len {
    values.push(typed_array_get_index(data, i))
    i = i + 1
  }
  values
}

///|
fn typed_array_compare_values(
  left : Value,
  right : Value,
  kind : TypedArrayKind,
  compare_fn : Value?,
) -> Int raise {
  match compare_fn {
    Some(func) => {
      let num = to_number(call_value_with_this(func, [left, right], Undefined))
      if Double::is_nan(num) {
        0
      } else if num > 0.0 {
        1
      } else if num < 0.0 {
        -1
      } else {
        0
      }
    }
    None =>
      match kind {
        TypedArrayKind::BigInt64 | TypedArrayKind::BigUint64 => {
          let left_big = to_bigint(left)
          let right_big = to_bigint(right)
          if left_big < right_big {
            -1
          } else if left_big > right_big {
            1
          } else {
            0
          }
        }
        _ => {
          let left_num = to_number(left)
          let right_num = to_number(right)
          if Double::is_nan(left_num) {
            if Double::is_nan(right_num) {
              0
            } else {
              1
            }
          } else if Double::is_nan(right_num) {
            -1
          } else if left_num < right_num {
            -1
          } else if left_num > right_num {
            1
          } else if left_num == 0.0 && right_num == 0.0 {
            let left_neg = is_negative_zero(left_num)
            let right_neg = is_negative_zero(right_num)
            if left_neg == right_neg {
              0
            } else if left_neg {
              -1
            } else {
              1
            }
          } else {
            0
          }
        }
      }
  }
}

///|
fn typed_array_counting_sort_16(
  values : Array[Value],
  signed : Bool,
) -> Array[Value] raise {
  let len = values.length()
  let counts : Array[Int] = Array::make(65536, 0)
  let mut i = 0
  while i < len {
    let value = values[i]
    let num = match value {
      Number(n) => n
      _ => to_number(value)
    }
    let mut idx = to_int32(num)
    if signed {
      idx = idx + 0x8000
    }
    counts[idx] = counts[idx] + 1
    i = i + 1
  }
  let sorted : Array[Value] = Array::make(len, Undefined)
  let mut out = 0
  let mut v = 0
  while v < 65536 {
    let count = counts[v]
    if count > 0 {
      let num = if signed { v - 0x8000 } else { v }
      let val = Number(Double::from_int(num))
      let mut c = 0
      while c < count {
        sorted[out] = val
        out = out + 1
        c = c + 1
      }
    }
    v = v + 1
  }
  sorted
}

///|
fn typed_array_counting_sort_float16(
  values : Array[Value],
) -> Array[Value] raise {
  let len = values.length()
  let counts : Array[Int] = Array::make(65537, 0)
  let mut i = 0
  while i < len {
    let value = values[i]
    let num = match value {
      Number(n) => n
      _ => to_number(value)
    }
    let key = if Double::is_nan(num) {
      65536
    } else {
      let bits = double_to_float16_bits(num)
      let bits_int = UInt::reinterpret_as_int(bits)
      if Int::land(bits_int, 0x8000) != 0 {
        Int::lxor(bits_int, 0xffff)
      } else {
        Int::lxor(bits_int, 0x8000)
      }
    }
    counts[key] = counts[key] + 1
    i = i + 1
  }
  let sorted : Array[Value] = Array::make(len, Undefined)
  let mut out = 0
  let mut key = 0
  while key < 65536 {
    let count = counts[key]
    if count > 0 {
      let bits_int = if key < 0x8000 {
        Int::lxor(key, 0xffff)
      } else {
        Int::lxor(key, 0x8000)
      }
      let bits = Int::reinterpret_as_uint(bits_int)
      let val = Number(float16_bits_to_double(bits))
      let mut c = 0
      while c < count {
        sorted[out] = val
        out = out + 1
        c = c + 1
      }
    }
    key = key + 1
  }
  let nan_count = counts[65536]
  if nan_count > 0 {
    let val = Number(nan())
    let mut c = 0
    while c < nan_count {
      sorted[out] = val
      out = out + 1
      c = c + 1
    }
  }
  sorted
}

///|
fn typed_array_counting_sort_16_data(
  data : TypedArrayData,
  signed : Bool,
) -> Unit {
  let len = typed_array_effective_length(data)
  if len < 2 {
    return
  }
  match data.buffer.array_buffer_data {
    Some(buf) => {
      let counts : Array[Int] = Array::make(65536, 0)
      let mut i = 0
      let mut byte_index = data.byte_offset
      while i < len {
        let raw = buffer_read_u16_le(buf, byte_index)
        let idx = if signed { Int::lxor(raw, 0x8000) } else { raw }
        counts[idx] = counts[idx] + 1
        i = i + 1
        byte_index = byte_index + 2
      }
      byte_index = data.byte_offset
      let mut idx = 0
      while idx < 65536 {
        let count = counts[idx]
        if count > 0 {
          let raw = if signed { Int::lxor(idx, 0x8000) } else { idx }
          let mut c = 0
          while c < count {
            buffer_write_u16_le(buf, byte_index, raw)
            byte_index = byte_index + 2
            c = c + 1
          }
        }
        idx = idx + 1
      }
    }
    None => ()
  }
}

///|
fn typed_array_counting_sort_float16_data(data : TypedArrayData) -> Unit {
  let len = typed_array_effective_length(data)
  if len < 2 {
    return
  }
  match data.buffer.array_buffer_data {
    Some(buf) => {
      let counts : Array[Int] = Array::make(65536, 0)
      let nan_bits : Array[Int] = []
      let mut i = 0
      let mut byte_index = data.byte_offset
      while i < len {
        let raw = buffer_read_u16_le(buf, byte_index)
        let exp = Int::land(raw, 0x7c00)
        let mant = Int::land(raw, 0x03ff)
        if exp == 0x7c00 && mant != 0 {
          nan_bits.push(raw)
        } else {
          let idx = if Int::land(raw, 0x8000) != 0 {
            Int::lxor(raw, 0xffff)
          } else {
            Int::lxor(raw, 0x8000)
          }
          counts[idx] = counts[idx] + 1
        }
        i = i + 1
        byte_index = byte_index + 2
      }
      byte_index = data.byte_offset
      let mut idx = 0
      while idx < 65536 {
        let count = counts[idx]
        if count > 0 {
          let raw = if idx < 0x8000 {
            Int::lxor(idx, 0xffff)
          } else {
            Int::lxor(idx, 0x8000)
          }
          let mut c = 0
          while c < count {
            buffer_write_u16_le(buf, byte_index, raw)
            byte_index = byte_index + 2
            c = c + 1
          }
        }
        idx = idx + 1
      }
      for raw in nan_bits {
        buffer_write_u16_le(buf, byte_index, raw)
        byte_index = byte_index + 2
      }
    }
    None => ()
  }
}

///|
fn typed_array_quick_sort_values(
  values : Array[Value],
  kind : TypedArrayKind,
  compare_fn : Value?,
) -> Unit raise {
  let len = values.length()
  if len < 2 {
    return
  }
  let stack : Array[(Int, Int)] = []
  stack.push((0, len - 1))
  while !stack.is_empty() {
    let last = stack.length() - 1
    let (lo, hi) = stack[last]
    let _ = stack.pop()
    let pivot = values[(lo + hi) / 2]
    let mut i = lo
    let mut j = hi
    while i <= j {
      while typed_array_compare_values(values[i], pivot, kind, compare_fn) < 0 {
        i = i + 1
      }
      while typed_array_compare_values(values[j], pivot, kind, compare_fn) > 0 {
        j = j - 1
      }
      if i <= j {
        if i != j {
          let tmp = values[i]
          values[i] = values[j]
          values[j] = tmp
        }
        i = i + 1
        if j == 0 {
          break
        }
        j = j - 1
      }
    }
    if lo < j {
      stack.push((lo, j))
    }
    if i < hi {
      stack.push((i, hi))
    }
  }
}

///|
fn typed_array_compare_indices(
  values : Array[Value],
  kind : TypedArrayKind,
  compare_fn : Value,
  left : Int,
  right : Int,
) -> Int raise {
  let cmp = typed_array_compare_values(
    values[left],
    values[right],
    kind,
    Some(compare_fn),
  )
  if cmp == 0 {
    if left < right {
      -1
    } else if left > right {
      1
    } else {
      0
    }
  } else {
    cmp
  }
}

///|
fn typed_array_quick_sort_indices(
  indices : Array[Int],
  values : Array[Value],
  kind : TypedArrayKind,
  compare_fn : Value,
) -> Unit raise {
  let len = indices.length()
  if len < 2 {
    return
  }
  let stack : Array[(Int, Int)] = []
  stack.push((0, len - 1))
  while !stack.is_empty() {
    let last = stack.length() - 1
    let (lo, hi) = stack[last]
    let _ = stack.pop()
    let pivot_index = indices[(lo + hi) / 2]
    let mut i = lo
    let mut j = hi
    while i <= j {
      while typed_array_compare_indices(
              values,
              kind,
              compare_fn,
              indices[i],
              pivot_index,
            ) <
            0 {
        i = i + 1
      }
      while typed_array_compare_indices(
              values,
              kind,
              compare_fn,
              indices[j],
              pivot_index,
            ) >
            0 {
        j = j - 1
      }
      if i <= j {
        if i != j {
          let tmp = indices[i]
          indices[i] = indices[j]
          indices[j] = tmp
        }
        i = i + 1
        if j == 0 {
          break
        }
        j = j - 1
      }
    }
    if lo < j {
      stack.push((lo, j))
    }
    if i < hi {
      stack.push((i, hi))
    }
  }
}

///|
fn typed_array_sort_values(
  values : Array[Value],
  kind : TypedArrayKind,
  compare_fn : Value?,
) -> Array[Value] raise {
  match compare_fn {
    Some(func) => {
      let indices : Array[Int] = []
      let mut i = 0
      while i < values.length() {
        indices.push(i)
        i = i + 1
      }
      typed_array_quick_sort_indices(indices, values, kind, func)
      let sorted : Array[Value] = []
      for index in indices {
        sorted.push(values[index])
      }
      sorted
    }
    None => {
      let len = values.length()
      let counting_sort_threshold = 65536 * 4
      if len >= counting_sort_threshold {
        match kind {
          TypedArrayKind::Int16 =>
            return typed_array_counting_sort_16(values, true)
          TypedArrayKind::Uint16 =>
            return typed_array_counting_sort_16(values, false)
          TypedArrayKind::Float16 =>
            return typed_array_counting_sort_float16(values)
          _ => ()
        }
      }
      typed_array_quick_sort_values(values, kind, None)
      values
    }
  }
}

///|
fn typed_array_sort_value(
  this_value : Value,
  args : Array[Value],
) -> Value raise {
  let (arr, data) = require_typed_array_checked(Some(this_value))
  let compare_arg = if args.is_empty() { Undefined } else { args[0] }
  let compare_fn = if compare_arg is Undefined {
    None
  } else if !is_callable(compare_arg) {
    return throw_type_error("not a function")
  } else {
    Some(compare_arg)
  }
  match compare_fn {
    None => {
      let len = typed_array_effective_length(data)
      let counting_sort_threshold = 65536 * 4
      if len >= counting_sort_threshold {
        match data.kind {
          TypedArrayKind::Int16 => {
            typed_array_counting_sort_16_data(data, true)
            return Array(arr)
          }
          TypedArrayKind::Uint16 => {
            typed_array_counting_sort_16_data(data, false)
            return Array(arr)
          }
          TypedArrayKind::Float16 => {
            typed_array_counting_sort_float16_data(data)
            return Array(arr)
          }
          _ => ()
        }
      }
    }
    _ => ()
  }
  let values = typed_array_collect_values(data)
  let sorted = typed_array_sort_values(values, data.kind, compare_fn)
  let mut i = 0
  while i < sorted.length() {
    let _ = typed_array_set_index(data, i, sorted[i])
    i = i + 1
  }
  Array(arr)
}

///|
fn typed_array_to_sorted_value(
  this_value : Value,
  args : Array[Value],
) -> Value raise {
  let (_, data) = require_typed_array_checked(Some(this_value))
  let compare_arg = if args.is_empty() { Undefined } else { args[0] }
  let compare_fn = if compare_arg is Undefined {
    None
  } else if !is_callable(compare_arg) {
    return throw_type_error("not a function")
  } else {
    Some(compare_arg)
  }
  let values = typed_array_collect_values(data)
  let sorted = typed_array_sort_values(values, data.kind, compare_fn)
  let result = new_typed_array_value(data.kind, values.length())
  let mut i = 0
  while i < sorted.length() {
    typed_array_set_index_or_throw(result, i, sorted[i])
    i = i + 1
  }
  result
}
