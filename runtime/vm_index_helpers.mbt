///|
fn clamp_index(num : Double, len : Int) -> Int {
  if Double::is_nan(num) || num <= 0.0 {
    return 0
  }
  let max = Double::from_int(len)
  if num >= max {
    return len
  }
  Int64::to_int(Double::to_int64(Double::trunc(num)))
}

///|
fn slice_index(num : Double, len : Int) -> Int {
  if Double::is_nan(num) {
    return 0
  }
  if Double::is_inf(num) {
    return if num < 0.0 { 0 } else { len }
  }
  let value = Int64::to_int(Double::to_int64(Double::trunc(num)))
  if value < 0 {
    let adjusted = len + value
    if adjusted < 0 {
      0
    } else {
      adjusted
    }
  } else if value > len {
    len
  } else {
    value
  }
}

///|
fn clamp_last_index(num : Double, len : Int) -> Int {
  if Double::is_nan(num) {
    return len
  }
  if num < 0.0 {
    return 0
  }
  let max = Double::from_int(len)
  if num >= max {
    return len
  }
  Int64::to_int(Double::to_int64(Double::trunc(num)))
}

///|
fn to_int32_sat(num : Double) -> Int {
  if Double::is_nan(num) {
    return 0
  }
  if Double::is_inf(num) {
    return if num < 0.0 { -2147483648 } else { 2147483647 }
  }
  Int64::to_int(Double::to_int64(Double::trunc(num)))
}

///|
fn parse_array_index(name : String) -> Int? {
  if name.is_empty() {
    return None
  }
  let mut first = true
  for ch in name {
    if ch < '0' || ch > '9' {
      return None
    }
    if first {
      if ch == '0' && name.length() > 1 {
        return None
      }
      first = false
    }
  }
  try @strconv.parse_int64(name, base=10) catch {
    _ => None
  } noraise {
    value => {
      let max_int = 2147483647
      if value < 0L || value > Int64::from_int(max_int) {
        None
      } else {
        Some(Int64::to_int(value))
      }
    }
  }
}

///|
fn canonical_numeric_index_string(name : String) -> Double? {
  if name == "-0" {
    return Some(negative_zero())
  }
  let num = string_to_number(name)
  let text = number_to_string_radix(num, 10)
  if text == name {
    Some(num)
  } else {
    None
  }
}

///|
fn array_index_from_name(name : String) -> Int64? {
  try @strconv.parse_int64(name, base=10) catch {
    _ => None
  } noraise {
    value =>
      if value >= 0L &&
        value <= max_array_index &&
        Int64::to_string(value) == name {
        Some(value)
      } else {
        None
      }
  }
}
