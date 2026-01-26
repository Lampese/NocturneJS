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
  let start_int = UInt::reinterpret_as_int(start_value)
  let end_int = UInt::reinterpret_as_int(end_value)
  if start_int >= end_int {
    return ""
  }
  let mut unit_count = 0
  if end_int <= 0x10000 {
    unit_count = end_int - start_int
  } else if start_int >= 0x10000 {
    unit_count = (end_int - start_int) * 2
  } else {
    unit_count = (0x10000 - start_int) + (end_int - 0x10000) * 2
  }
  let bytes_len = unit_count * 2
  let bytes : Array[Byte] = Array::make(bytes_len, Int::to_byte(0))
  let mut byte_index = 0
  if end_int <= 0x10000 {
    let mut unit = start_int
    let mut low = unit & 0xff
    let mut high = (unit >> 8) & 0xff
    while unit < end_int {
      bytes[byte_index] = Int::to_byte(low)
      bytes[byte_index + 1] = Int::to_byte(high)
      byte_index = byte_index + 2
      unit = unit + 1
      low = low + 1
      if low == 256 {
        low = 0
        high = high + 1
      }
    }
  } else if start_int >= 0x10000 {
    let mut current = start_int
    let cp = current - 0x10000
    let mut high = 0xD800 + (cp >> 10)
    let mut low = 0xDC00 + (cp & 0x3ff)
    while current < end_int {
      bytes[byte_index] = Int::to_byte(high & 0xff)
      bytes[byte_index + 1] = Int::to_byte((high >> 8) & 0xff)
      bytes[byte_index + 2] = Int::to_byte(low & 0xff)
      bytes[byte_index + 3] = Int::to_byte((low >> 8) & 0xff)
      byte_index = byte_index + 4
      current = current + 1
      low = low + 1
      if low == 0xE000 {
        low = 0xDC00
        high = high + 1
      }
    }
  } else {
    let mut unit = start_int
    let mut low = unit & 0xff
    let mut high = (unit >> 8) & 0xff
    while unit < 0x10000 {
      bytes[byte_index] = Int::to_byte(low)
      bytes[byte_index + 1] = Int::to_byte(high)
      byte_index = byte_index + 2
      unit = unit + 1
      low = low + 1
      if low == 256 {
        low = 0
        high = high + 1
      }
    }
    let mut current = 0x10000
    let mut high2 = 0xD800
    let mut low2 = 0xDC00
    while current < end_int {
      bytes[byte_index] = Int::to_byte(high2 & 0xff)
      bytes[byte_index + 1] = Int::to_byte((high2 >> 8) & 0xff)
      bytes[byte_index + 2] = Int::to_byte(low2 & 0xff)
      bytes[byte_index + 3] = Int::to_byte((low2 >> 8) & 0xff)
      byte_index = byte_index + 4
      current = current + 1
      low2 = low2 + 1
      if low2 == 0xE000 {
        low2 = 0xDC00
        high2 = high2 + 1
      }
    }
  }
  let data = Bytes::from_array(bytes)
  data.to_unchecked_string()
}

///|
fn string_from_code_units(units : Array[Int]) -> String {
  if units.is_empty() {
    return ""
  }
  let bytes_len = units.length() * 2
  let bytes : Array[Byte] = Array::make(bytes_len, Int::to_byte(0))
  let mut index = 0
  for unit in units {
    let value = unit & 0xffff
    bytes[index] = Int::to_byte(value & 0xff)
    bytes[index + 1] = Int::to_byte((value >> 8) & 0xff)
    index = index + 2
  }
  let data = Bytes::from_array(bytes)
  data.to_unchecked_string()
}
