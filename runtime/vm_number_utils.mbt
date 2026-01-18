///|
fn nan() -> Double {
  0.0 / 0.0
}

///|
fn inf() -> Double {
  1.0 / 0.0
}

///|
fn negative_zero() -> Double {
  UInt64::reinterpret_as_double(0x8000000000000000UL)
}

///|
fn is_js_whitespace(code : Int) -> Bool {
  code == 0x0009 ||
  code == 0x000A ||
  code == 0x000B ||
  code == 0x000C ||
  code == 0x000D ||
  code == 0x0020 ||
  code == 0x00A0 ||
  code == 0x1680 ||
  (code >= 0x2000 && code <= 0x200A) ||
  code == 0x2028 ||
  code == 0x2029 ||
  code == 0x202F ||
  code == 0x205F ||
  code == 0x3000 ||
  code == 0xFEFF
}

///|
fn trim_js_whitespace(value : String) -> String {
  let len = value.length()
  let mut start = 0
  while start < len {
    let unit = UInt16::to_int(value.code_unit_at(start))
    if !is_js_whitespace(unit) {
      break
    }
    start = start + 1
  }
  let mut end = len
  while end > start {
    let unit = UInt16::to_int(value.code_unit_at(end - 1))
    if !is_js_whitespace(unit) {
      break
    }
    end = end - 1
  }
  if start == 0 && end == len {
    value
  } else {
    value.unsafe_substring(start~, end~)
  }
}

///|
fn trim_js_whitespace_start(value : String) -> String {
  let len = value.length()
  let mut start = 0
  while start < len {
    let unit = UInt16::to_int(value.code_unit_at(start))
    if !is_js_whitespace(unit) {
      break
    }
    start = start + 1
  }
  if start == 0 {
    value
  } else {
    value.unsafe_substring(start~, end=len)
  }
}

///|
fn trim_js_whitespace_end(value : String) -> String {
  let mut end = value.length()
  while end > 0 {
    let unit = UInt16::to_int(value.code_unit_at(end - 1))
    if !is_js_whitespace(unit) {
      break
    }
    end = end - 1
  }
  if end == value.length() {
    value
  } else {
    value.unsafe_substring(start=0, end~)
  }
}
