///|
fn hex_digit_upper(value : Int) -> Int {
  if value < 10 {
    48 + value
  } else {
    55 + value
  }
}

///|
fn init_uri_hex_table() -> Array[Int] {
  let table = Array::make(256, -1)
  let mut i = 0
  while i < 10 {
    table[48 + i] = i
    i = i + 1
  }
  i = 0
  while i < 6 {
    table[65 + i] = 10 + i
    table[97 + i] = 10 + i
    i = i + 1
  }
  table
}

let uri_hex_table : Array[Int] = init_uri_hex_table()

///|
fn uri_hex_value(code : Int) -> Int? {
  if code < 0 || code >= 256 {
    None
  } else {
    let value = uri_hex_table[code]
    if value < 0 {
      None
    } else {
      Some(value)
    }
  }
}

///|
fn uri_encode_hex(units : Array[Int], byte : Int) -> Unit {
  let percent = Char::to_int('%')
  units.push(percent)
  units.push(hex_digit_upper((byte >> 4) & 0x0f))
  units.push(hex_digit_upper(byte & 0x0f))
}

///|
fn uri_escape_hex(units : Array[Int], code : Int) -> Unit {
  let percent = Char::to_int('%')
  units.push(percent)
  if code >= 256 {
    units.push(Char::to_int('u'))
    units.push(hex_digit_upper((code >> 12) & 0x0f))
    units.push(hex_digit_upper((code >> 8) & 0x0f))
  }
  units.push(hex_digit_upper((code >> 4) & 0x0f))
  units.push(hex_digit_upper(code & 0x0f))
}

///|
fn is_uri_reserved(code : Int) -> Bool {
  if code < 0 || code >= 0x100 {
    return false
  }
  code == Char::to_int(';') ||
  code == Char::to_int('/') ||
  code == Char::to_int('?') ||
  code == Char::to_int(':') ||
  code == Char::to_int('@') ||
  code == Char::to_int('&') ||
  code == Char::to_int('=') ||
  code == Char::to_int('+') ||
  code == Char::to_int('$') ||
  code == Char::to_int(',') ||
  code == Char::to_int('#')
}

///|
fn is_uri_unescaped(code : Int, is_component : Bool) -> Bool {
  if code < 0 || code >= 0x100 {
    return false
  }
  let is_alpha = (code >= Char::to_int('a') && code <= Char::to_int('z')) ||
    (code >= Char::to_int('A') && code <= Char::to_int('Z'))
  if is_alpha {
    return true
  }
  if code >= Char::to_int('0') && code <= Char::to_int('9') {
    return true
  }
  if code == 45 ||
    code == 95 ||
    code == 46 ||
    code == 33 ||
    code == 126 ||
    code == 42 ||
    code == 39 ||
    code == 40 ||
    code == 41 {
    return true
  }
  !is_component && is_uri_reserved(code)
}

///|
fn is_unescaped(code : Int) -> Bool {
  if code < 0 || code >= 0x100 {
    return false
  }
  let is_alpha = (code >= Char::to_int('a') && code <= Char::to_int('z')) ||
    (code >= Char::to_int('A') && code <= Char::to_int('Z'))
  if is_alpha {
    return true
  }
  if code >= Char::to_int('0') && code <= Char::to_int('9') {
    return true
  }
  code == 64 ||
  code == 42 ||
  code == 95 ||
  code == 43 ||
  code == 45 ||
  code == 46 ||
  code == 47
}

///|
fn is_surrogate_code_point(code : Int) -> Bool {
  code >= 0xD800 && code <= 0xDFFF
}

///|
fn append_code_point_units(units : Array[Int], code_point : Int) -> Unit {
  if code_point <= 0xffff {
    units.push(code_point)
  } else {
    let cp = code_point - 0x10000
    let high = 0xD800 + (cp >> 10)
    let low = 0xDC00 + (cp & 0x3ff)
    units.push(high)
    units.push(low)
  }
}

///|
fn push_code_unit_bytes(bytes : Array[Byte], unit : Int) -> Unit {
  let value = unit & 0xffff
  let low = Int::to_byte(value & 0xff)
  let high = Int::to_byte((value >> 8) & 0xff)
  bytes.push(low)
  bytes.push(high)
}

///|
fn append_code_point_bytes(bytes : Array[Byte], code_point : Int) -> Unit {
  if code_point <= 0xffff {
    push_code_unit_bytes(bytes, code_point)
  } else {
    let cp = code_point - 0x10000
    let high = 0xD800 + (cp >> 10)
    let low = 0xDC00 + (cp & 0x3ff)
    push_code_unit_bytes(bytes, high)
    push_code_unit_bytes(bytes, low)
  }
}

///|
fn uri_hex_decode(text : String, index : Int) -> Int raise {
  let len = text.length()
  let percent = Char::to_int('%')
  if index >= len || UInt16::to_int(text.code_unit_at(index)) != percent {
    let _ = throw_uri_error("expecting %")
    return 0
  }
  if index + 2 >= len {
    let _ = throw_uri_error("expecting hex digit")
    return 0
  }
  let hi = UInt16::to_int(text.code_unit_at(index + 1))
  let lo = UInt16::to_int(text.code_unit_at(index + 2))
  let high = if hi >= 0 && hi < 256 { uri_hex_table[hi] } else { -1 }
  if high < 0 {
    let _ = throw_uri_error("expecting hex digit")
    return 0
  }
  let low = if lo >= 0 && lo < 256 { uri_hex_table[lo] } else { -1 }
  if low < 0 {
    let _ = throw_uri_error("expecting hex digit")
    return 0
  }
  (high << 4) | low
}

///|
fn uri_hex_digit_at(text : String, index : Int) -> Int? {
  let len = text.length()
  if index < 0 || index >= len {
    return None
  }
  let unit = UInt16::to_int(text.code_unit_at(index))
  if unit < 0 || unit >= 256 {
    return None
  }
  let value = uri_hex_table[unit]
  if value < 0 {
    None
  } else {
    Some(value)
  }
}

///|
fn uri_decode_fast_4byte_component(text : String) -> String? {
  if text.length() != 12 {
    return None
  }
  let percent = Char::to_int('%')
  if UInt16::to_int(text.code_unit_at(0)) != percent ||
    UInt16::to_int(text.code_unit_at(3)) != percent ||
    UInt16::to_int(text.code_unit_at(6)) != percent ||
    UInt16::to_int(text.code_unit_at(9)) != percent {
    return None
  }
  let h0 = uri_hex_digit_at(text, 1)
  let h1 = uri_hex_digit_at(text, 2)
  let h2 = uri_hex_digit_at(text, 4)
  let h3 = uri_hex_digit_at(text, 5)
  let h4 = uri_hex_digit_at(text, 7)
  let h5 = uri_hex_digit_at(text, 8)
  let h6 = uri_hex_digit_at(text, 10)
  let h7 = uri_hex_digit_at(text, 11)
  match (h0, h1, h2, h3, h4, h5, h6, h7) {
    (
      Some(a0),
      Some(a1),
      Some(b0),
      Some(b1n),
      Some(c0),
      Some(c1),
      Some(d0),
      Some(d1),
    ) => {
      let byte1 = (a0 << 4) | a1
      let byte2 = (b0 << 4) | b1n
      let byte3 = (c0 << 4) | c1
      let byte4 = (d0 << 4) | d1
      if byte1 < 0xF0 || byte1 > 0xF4 {
        return None
      }
      if byte2 < 0x80 || byte2 > 0xBF || byte3 < 0x80 || byte3 > 0xBF ||
        byte4 < 0x80 || byte4 > 0xBF {
        return None
      }
      if (byte1 == 0xF0 && byte2 < 0xA0) || (byte1 == 0xF4 && byte2 > 0x8F) {
        return None
      }
      let code = ((byte1 & 0x07) << 18) | ((byte2 & 0x3F) << 12) |
        ((byte3 & 0x3F) << 6) | (byte4 & 0x3F)
      let cp = code - 0x10000
      let high = 0xD800 + (cp >> 10)
      let low = 0xDC00 + (cp & 0x3FF)
      let bytes : Array[Byte] = Array::make(4, Int::to_byte(0))
      bytes[0] = Int::to_byte(high & 0xff)
      bytes[1] = Int::to_byte((high >> 8) & 0xff)
      bytes[2] = Int::to_byte(low & 0xff)
      bytes[3] = Int::to_byte((low >> 8) & 0xff)
      let data = Bytes::from_array(bytes)
      Some(data.to_unchecked_string())
    }
    _ => None
  }
}

///|
fn uri_decode(text : String, is_component : Bool) -> String raise {
  if is_component {
    match uri_decode_fast_4byte_component(text) {
      Some(value) => return value
      None => ()
    }
  }
  let len = text.length()
  let mut index = 0
  let bytes : Array[Byte] = []
  let percent = Char::to_int('%')
  while index < len {
    let unit = UInt16::to_int(text.code_unit_at(index))
    if unit != percent {
      push_code_unit_bytes(bytes, unit)
      index = index + 1
      continue
    }
    let byte = uri_hex_decode(text, index)
    index = index + 3
    if byte < 0x80 {
      if !is_component && is_uri_reserved(byte) {
        push_code_unit_bytes(bytes, percent)
        push_code_unit_bytes(bytes, UInt16::to_int(text.code_unit_at(index - 2)))
        push_code_unit_bytes(bytes, UInt16::to_int(text.code_unit_at(index - 1)))
      } else {
        push_code_unit_bytes(bytes, byte)
      }
      continue
    }
    let mut count = 0
    let mut min = 0
    let mut code = byte
    if byte >= 0xc0 && byte <= 0xdf {
      count = 1
      min = 0x80
      code = byte & 0x1f
    } else if byte >= 0xe0 && byte <= 0xef {
      count = 2
      min = 0x800
      code = byte & 0x0f
    } else if byte >= 0xf0 && byte <= 0xf7 {
      count = 3
      min = 0x10000
      code = byte & 0x07
    } else {
      count = 0
      min = 1
      code = 0
    }
    let mut invalid = false
    while count > 0 {
      let next = uri_hex_decode(text, index)
      index = index + 3
      if (next & 0xc0) != 0x80 {
        invalid = true
        break
      }
      code = (code << 6) | (next & 0x3f)
      count = count - 1
    }
    if invalid || code < min || code > 0x10ffff || is_surrogate_code_point(code) {
      let _ = throw_uri_error("malformed UTF-8")
      return ""
    }
    append_code_point_bytes(bytes, code)
  }
  let data = Bytes::from_array(bytes)
  data.to_unchecked_string()
}

///|
fn uri_encode(text : String, is_component : Bool) -> String raise {
  let len = text.length()
  let mut index = 0
  let units : Array[Int] = []
  while index < len {
    let unit = UInt16::to_int(text.code_unit_at(index))
    if is_uri_unescaped(unit, is_component) {
      units.push(unit)
      index = index + 1
      continue
    }
    if is_trail_surrogate(unit) {
      let _ = throw_uri_error("invalid character")
      return ""
    }
    let mut code_point = unit
    if is_lead_surrogate(unit) {
      if index + 1 >= len {
        let _ = throw_uri_error("expecting surrogate pair")
        return ""
      }
      let next = UInt16::to_int(text.code_unit_at(index + 1))
      if !is_trail_surrogate(next) {
        let _ = throw_uri_error("expecting surrogate pair")
        return ""
      }
      code_point = 0x10000 + ((unit - 0xD800) << 10) + (next - 0xDC00)
      index = index + 2
    } else {
      index = index + 1
    }
    if code_point < 0x80 {
      uri_encode_hex(units, code_point)
    } else if code_point < 0x800 {
      uri_encode_hex(units, (code_point >> 6) | 0xc0)
      uri_encode_hex(units, (code_point & 0x3f) | 0x80)
    } else if code_point < 0x10000 {
      uri_encode_hex(units, (code_point >> 12) | 0xe0)
      uri_encode_hex(units, ((code_point >> 6) & 0x3f) | 0x80)
      uri_encode_hex(units, (code_point & 0x3f) | 0x80)
    } else {
      uri_encode_hex(units, (code_point >> 18) | 0xf0)
      uri_encode_hex(units, ((code_point >> 12) & 0x3f) | 0x80)
      uri_encode_hex(units, ((code_point >> 6) & 0x3f) | 0x80)
      uri_encode_hex(units, (code_point & 0x3f) | 0x80)
    }
  }
  string_from_code_units(units)
}

///|
fn uri_escape(text : String) -> String {
  let len = text.length()
  let units : Array[Int] = []
  let mut index = 0
  while index < len {
    let unit = UInt16::to_int(text.code_unit_at(index))
    if is_unescaped(unit) {
      units.push(unit)
    } else {
      uri_escape_hex(units, unit)
    }
    index = index + 1
  }
  string_from_code_units(units)
}

///|
fn parse_hex_digits(text : String, start : Int, count : Int) -> Int? {
  if count <= 0 {
    return Some(0)
  }
  let len = text.length()
  if start < 0 || start + count > len {
    return None
  }
  let mut value = 0
  let mut index = start
  let end = start + count
  while index < end {
    let unit = UInt16::to_int(text.code_unit_at(index))
    match uri_hex_value(unit) {
      Some(digit) => value = (value << 4) | digit
      None => return None
    }
    index = index + 1
  }
  Some(value)
}

///|
fn uri_unescape(text : String) -> String {
  let len = text.length()
  let units : Array[Int] = []
  let mut index = 0
  let percent = Char::to_int('%')
  let letter_u = Char::to_int('u')
  while index < len {
    let mut unit = UInt16::to_int(text.code_unit_at(index))
    if unit == percent {
      if index + 6 <= len &&
        UInt16::to_int(text.code_unit_at(index + 1)) == letter_u {
        match parse_hex_digits(text, index + 2, 4) {
          Some(value) => {
            unit = value
            index = index + 5
          }
          None => ()
        }
      } else if index + 3 <= len {
        match parse_hex_digits(text, index + 1, 2) {
          Some(value) => {
            unit = value
            index = index + 2
          }
          None => ()
        }
      }
    }
    units.push(unit)
    index = index + 1
  }
  string_from_code_units(units)
}
