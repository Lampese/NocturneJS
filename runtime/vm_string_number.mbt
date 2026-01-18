///|
fn string_normalize_form(value : String, form : String) -> String raise {
  let valid = form == "NFC" || form == "NFD" || form == "NFKC" || form == "NFKD"
  if !valid {
    let _ = throw_range_error("invalid normalization form")

  }
  match form {
    "NFC" => unicode_normalize_string(value, UNICODE_NFC)
    "NFD" => unicode_normalize_string(value, UNICODE_NFD)
    "NFKC" => unicode_normalize_string(value, UNICODE_NFKC)
    _ => unicode_normalize_string(value, UNICODE_NFKD)
  }
}

///|
fn find_exponent_index(text : String, start : Int) -> Int? {
  let len = text.length()
  let mut index = start
  let lower_e = Char::to_int('e')
  let upper_e = Char::to_int('E')
  while index < len {
    let unit = UInt16::to_int(text.code_unit_at(index))
    if unit == lower_e || unit == upper_e {
      return Some(index)
    }
    index = index + 1
  }
  None
}

///|
fn scan_mantissa_digits(text : String, start : Int, end : Int) -> (Bool, Bool) {
  let mut has_digit = false
  let mut has_non_zero = false
  let mut index = start
  let zero = Char::to_int('0')
  let nine = Char::to_int('9')
  while index < end {
    let unit = UInt16::to_int(text.code_unit_at(index))
    if unit >= zero && unit <= nine {
      has_digit = true
      if unit != zero {
        has_non_zero = true
        break
      }
    }
    index = index + 1
  }
  (has_digit, has_non_zero)
}

///|
fn decimal_overflow_value(text : String) -> Double? {
  let len = text.length()
  if len == 0 {
    return None
  }
  let mut index = 0
  let mut negative = false
  let first = UInt16::to_int(text.code_unit_at(0))
  if first == Char::to_int('+') {
    index = 1
  } else if first == Char::to_int('-') {
    negative = true
    index = 1
  }
  let exp_index = match find_exponent_index(text, index) {
    Some(pos) => pos
    None => return None
  }
  let (has_digit, has_non_zero) = scan_mantissa_digits(text, index, exp_index)
  if !has_digit {
    return None
  }
  if !has_non_zero {
    return Some(if negative { negative_zero() } else { 0.0 })
  }
  let mut exp_start = exp_index + 1
  if exp_start >= len {
    return None
  }
  let mut exp_negative = false
  let exp_sign = UInt16::to_int(text.code_unit_at(exp_start))
  if exp_sign == Char::to_int('+') || exp_sign == Char::to_int('-') {
    exp_negative = exp_sign == Char::to_int('-')
    exp_start = exp_start + 1
  }
  if exp_start >= len {
    return None
  }
  let mut exp_value = 0
  let mut idx = exp_start
  let zero = Char::to_int('0')
  let nine = Char::to_int('9')
  while idx < len {
    let unit = UInt16::to_int(text.code_unit_at(idx))
    if unit < zero || unit > nine {
      return None
    }
    exp_value = exp_value * 10 + (unit - zero)
    idx = idx + 1
  }
  let exp = if exp_negative { -exp_value } else { exp_value }
  if exp > 308 {
    return Some(if negative { Double::neg(inf()) } else { inf() })
  }
  if exp < -324 {
    return Some(if negative { negative_zero() } else { 0.0 })
  }
  None
}

///|
fn string_to_number(value : String) -> Double {
  let trimmed = trim_js_whitespace(value)
  if trimmed.is_empty() {
    return 0.0
  }
  if trimmed.contains("_") {
    return nan()
  }
  if trimmed == "Infinity" || trimmed == "+Infinity" {
    return inf()
  }
  if trimmed == "-Infinity" {
    return 0.0 - inf()
  }
  let trimmed_lower = trimmed.to_lower()
  if trimmed_lower == "infinity" ||
    trimmed_lower == "+infinity" ||
    trimmed_lower == "-infinity" {
    return nan()
  }
  if trimmed.has_prefix("0x") || trimmed.has_prefix("0X") {
    let digits = trimmed
      .unsafe_substring(start=2, end=trimmed.length())
      .to_string_view()
    try Double::convert_uint64(@strconv.parse_uint64(digits, base=16)) catch {
      _ => nan()
    } noraise {
      value => value
    }
  } else if trimmed.has_prefix("0o") || trimmed.has_prefix("0O") {
    let digits = trimmed
      .unsafe_substring(start=2, end=trimmed.length())
      .to_string_view()
    try Double::convert_uint64(@strconv.parse_uint64(digits, base=8)) catch {
      _ => nan()
    } noraise {
      value => value
    }
  } else if trimmed.has_prefix("0b") || trimmed.has_prefix("0B") {
    let digits = trimmed
      .unsafe_substring(start=2, end=trimmed.length())
      .to_string_view()
    try Double::convert_uint64(@strconv.parse_uint64(digits, base=2)) catch {
      _ => nan()
    } noraise {
      value => value
    }
  } else {
    let mut sign = 1.0
    let mut start = 0
    if trimmed.has_prefix("+") {
      start = 1
    } else if trimmed.has_prefix("-") {
      sign = -1.0
      start = 1
    }
    if start == 1 {
      let body = trimmed.unsafe_substring(start=1, end=trimmed.length())
      if body.has_prefix("0x") ||
        body.has_prefix("0X") ||
        body.has_prefix("0o") ||
        body.has_prefix("0O") ||
        body.has_prefix("0b") ||
        body.has_prefix("0B") {
        return nan()
      }
    }
    try @strconv.parse_double(trimmed) catch {
      _ =>
        match decimal_overflow_value(trimmed) {
          Some(value) => value
          None => nan()
        }
    } noraise {
      value =>
        if value == 0.0 {
          let (has_digit, has_non_zero) = match
            find_exponent_index(trimmed, start) {
            Some(exp_index) => scan_mantissa_digits(trimmed, start, exp_index)
            None => scan_mantissa_digits(trimmed, start, trimmed.length())
          }
          if has_digit && !has_non_zero && sign < 0.0 {
            negative_zero()
          } else {
            value
          }
        } else {
          value
        }
    }
  }
}
