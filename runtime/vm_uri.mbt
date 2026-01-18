///|
fn hex_digit_upper(value : Int) -> Int {
  if value < 10 {
    48 + value
  } else {
    55 + value
  }
}

///|
fn uri_hex_value(code : Int) -> Int? {
  let zero = Char::to_int('0')
  let nine = Char::to_int('9')
  let upper_a = Char::to_int('A')
  let upper_f = Char::to_int('F')
  let lower_a = Char::to_int('a')
  let lower_f = Char::to_int('f')
  if code >= zero && code <= nine {
    Some(code - zero)
  } else if code >= upper_a && code <= upper_f {
    Some(code - upper_a + 10)
  } else if code >= lower_a && code <= lower_f {
    Some(code - lower_a + 10)
  } else {
    None
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
  let high = match uri_hex_value(hi) {
    Some(value) => value
    None => {
      let _ = throw_uri_error("expecting hex digit")
      return 0
    }
  }
  let low = match uri_hex_value(lo) {
    Some(value) => value
    None => {
      let _ = throw_uri_error("expecting hex digit")
      return 0
    }
  }
  (high << 4) | low
}

///|
fn uri_decode(text : String, is_component : Bool) -> String raise {
  let len = text.length()
  let mut index = 0
  let units : Array[Int] = []
  let percent = Char::to_int('%')
  while index < len {
    let unit = UInt16::to_int(text.code_unit_at(index))
    if unit != percent {
      units.push(unit)
      index = index + 1
      continue
    }
    let byte = uri_hex_decode(text, index)
    index = index + 3
    if byte < 0x80 {
      if !is_component && is_uri_reserved(byte) {
        units.push(percent)
        units.push(UInt16::to_int(text.code_unit_at(index - 2)))
        units.push(UInt16::to_int(text.code_unit_at(index - 1)))
      } else {
        units.push(byte)
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
    append_code_point_units(units, code)
  }
  string_from_code_units(units)
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
