///|
fn string_from_char_codes(args : Array[Value]) -> String raise {
  let units : Array[Int] = []
  for arg in args {
    let unit = to_uint16_from_double(to_number(arg))
    units.push(unit)
  }
  string_from_code_units(units)
}

///|
fn string_from_code_points(args : Array[Value]) -> String raise {
  if args.is_empty() {
    return ""
  }
  let bytes : Array[Byte] = []
  for arg in args {
    let num = to_number(arg)
    if Double::is_nan(num) || num < 0.0 || num > 0x10ffff.0 {
      let _ = throw_range_error("invalid code point")
      return ""
    }
    let int_val = Double::trunc(num)
    if int_val != num {
      let _ = throw_range_error("invalid code point")
      return ""
    }
    let code_point = Double::to_int(int_val)
    if code_point <= 0xffff {
      let value = code_point & 0xffff
      let low = Int::to_byte(value & 0xff)
      let high = Int::to_byte((value >> 8) & 0xff)
      bytes.push(low)
      bytes.push(high)
    } else {
      let cp = code_point - 0x10000
      let high_unit = 0xD800 + (cp >> 10)
      let low_unit = 0xDC00 + (cp & 0x3ff)
      let high_low = Int::to_byte(high_unit & 0xff)
      let high_high = Int::to_byte((high_unit >> 8) & 0xff)
      let low_low = Int::to_byte(low_unit & 0xff)
      let low_high = Int::to_byte((low_unit >> 8) & 0xff)
      bytes.push(high_low)
      bytes.push(high_high)
      bytes.push(low_low)
      bytes.push(low_high)
    }
  }
  let data = Bytes::from_array(bytes)
  data.to_unchecked_string()
}

///|
fn test262_code_point_range(start : UInt, end : UInt) -> String {
  let max_end = 0x10ffff + 1
  let max_end_u = UInt::trunc_double(Double::from_int(max_end))
  let end_value = if end > max_end_u { max_end_u } else { end }
  let mut start_value = start
  if start_value > end_value {
    start_value = end_value
  }
  let units : Array[Int] = []
  let mut current = start_value
  while current < end_value {
    let code_point = UInt::reinterpret_as_int(current)
    if code_point <= 0xffff {
      units.push(code_point)
    } else {
      let cp = code_point - 0x10000
      let high = 0xD800 + (cp >> 10)
      let low = 0xDC00 + (cp & 0x3ff)
      units.push(high)
      units.push(low)
    }
    current = current + 1
  }
  string_from_code_units(units)
}

///|
fn string_from_code_units(units : Array[Int]) -> String {
  if units.is_empty() {
    return ""
  }
  let bytes : Array[Byte] = []
  for unit in units {
    let value = unit & 0xffff
    let low = Int::to_byte(value & 0xff)
    let high = Int::to_byte((value >> 8) & 0xff)
    bytes.push(low)
    bytes.push(high)
  }
  let data = Bytes::from_array(bytes)
  data.to_unchecked_string()
}
