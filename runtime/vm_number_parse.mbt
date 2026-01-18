///|
fn parse_int_value(args : Array[Value]) -> Value raise {
  if args.is_empty() {
    return Number(nan())
  }
  let text = to_string_strict(args[0])
  let radix = if args.length() < 2 { 0 } else { to_int32(to_number(args[1])) }
  Number(parse_int_string(text, radix))
}

///|
fn parse_int_string(text : String, radix_arg : Int) -> Double {
  let view = trim_js_whitespace_start(text)
  if view.is_empty() {
    return nan()
  }
  let len = view.length()
  let mut index = 0
  let mut sign = 1.0
  let first = UInt16::to_int(view.code_unit_at(index))
  if first == Char::to_int('+') || first == Char::to_int('-') {
    if first == Char::to_int('-') {
      sign = -1.0
    }
    index = index + 1
  }
  let mut radix = radix_arg
  if radix != 0 && (radix < 2 || radix > 36) {
    return nan()
  }
  if radix == 0 {
    radix = 10
    if index + 1 < len {
      let c0 = UInt16::to_int(view.code_unit_at(index))
      let c1 = UInt16::to_int(view.code_unit_at(index + 1))
      if c0 == Char::to_int('0') &&
        (c1 == Char::to_int('x') || c1 == Char::to_int('X')) {
        radix = 16
        index = index + 2
      }
    }
  } else if radix == 16 {
    if index + 1 < len {
      let c0 = UInt16::to_int(view.code_unit_at(index))
      let c1 = UInt16::to_int(view.code_unit_at(index + 1))
      if c0 == Char::to_int('0') &&
        (c1 == Char::to_int('x') || c1 == Char::to_int('X')) {
        index = index + 2
      }
    }
  }
  let base = Double::from_int(radix)
  let mut value = 0.0
  let mut any = false
  while index < len {
    let code = UInt16::to_int(view.code_unit_at(index))
    let digit = digit_value(code)
    if digit < 0 || digit >= radix {
      break
    }
    value = value * base + Double::from_int(digit)
    any = true
    index = index + 1
  }
  if !any {
    nan()
  } else {
    sign * value
  }
}

///|
fn parse_float_value(args : Array[Value]) -> Value raise {
  if args.is_empty() {
    return Number(nan())
  }
  let text = to_string_strict(args[0])
  Number(parse_float_string(text))
}

///|
fn parse_float_string(text : String) -> Double {
  let view = trim_js_whitespace_start(text)
  if view.is_empty() {
    return nan()
  }
  let len = view.length()
  let mut index = 0
  if len == 0 {
    return nan()
  }
  let first = UInt16::to_int(view.code_unit_at(index))
  if first == Char::to_int('+') || first == Char::to_int('-') {
    index = index + 1
  }
  let tail = view.view(start_offset=index, end_offset=len)
  if tail.has_prefix("Infinity") {
    if first == Char::to_int('-') {
      return 0.0 - inf()
    }
    return inf()
  }
  let mut pos = index
  let mut saw_digit = false
  while pos < len && is_digit_code(UInt16::to_int(view.code_unit_at(pos))) {
    saw_digit = true
    pos = pos + 1
  }
  if pos < len && UInt16::to_int(view.code_unit_at(pos)) == Char::to_int('.') {
    pos = pos + 1
    while pos < len && is_digit_code(UInt16::to_int(view.code_unit_at(pos))) {
      saw_digit = true
      pos = pos + 1
    }
  }
  if !saw_digit {
    return nan()
  }
  let mut end = pos
  if end < len {
    let c = UInt16::to_int(view.code_unit_at(end))
    if c == Char::to_int('e') || c == Char::to_int('E') {
      let mut exp_pos = end + 1
      if exp_pos < len {
        let sign_code = UInt16::to_int(view.code_unit_at(exp_pos))
        if sign_code == Char::to_int('+') || sign_code == Char::to_int('-') {
          exp_pos = exp_pos + 1
        }
      }
      let exp_start = exp_pos
      while exp_pos < len &&
            is_digit_code(UInt16::to_int(view.code_unit_at(exp_pos))) {
        exp_pos = exp_pos + 1
      }
      if exp_pos > exp_start {
        end = exp_pos
      }
    }
  }
  let slice = view.view(start_offset=0, end_offset=end)
  try @strconv.parse_double(slice) catch {
    _ => nan()
  } noraise {
    value => value
  }
}

///|
fn digit_value(code : Int) -> Int {
  if code >= Char::to_int('0') && code <= Char::to_int('9') {
    return code - Char::to_int('0')
  }
  if code >= Char::to_int('a') && code <= Char::to_int('z') {
    return code - Char::to_int('a') + 10
  }
  if code >= Char::to_int('A') && code <= Char::to_int('Z') {
    return code - Char::to_int('A') + 10
  }
  -1
}

///|
fn is_digit_code(code : Int) -> Bool {
  code >= Char::to_int('0') && code <= Char::to_int('9')
}
