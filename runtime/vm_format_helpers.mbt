///|
fn repeat_char(ch : Char, count : Int) -> String {
  let sb = StringBuilder::new()
  let mut i = 0
  while i < count {
    sb.write_char(ch)
    i = i + 1
  }
  sb.to_string()
}

///|
fn pad_left(text : String, width : Int, pad : Char) -> String {
  if text.length() >= width {
    return text
  }
  let needed = width - text.length()
  if pad == '0' && text.has_prefix("-") {
    "-" +
    repeat_char(pad, needed) +
    text.unsafe_substring(start=1, end=text.length())
  } else {
    repeat_char(pad, needed) + text
  }
}

///|
fn hex_digit(value : Int) -> Char {
  let code = if value < 10 { 48 + value } else { 87 + value }
  match Int::to_char(code) {
    Some(ch) => ch
    None => '0'
  }
}

///|
fn format_hex_uint64(value : UInt64) -> String {
  if value == 0UL {
    return "0"
  }
  let mut v = value
  let digits : Array[Char] = []
  while v > 0UL {
    let digit = UInt64::to_int(v & 15UL)
    digits.push(hex_digit(digit))
    v = v >> 4
  }
  let sb = StringBuilder::new()
  let mut i = digits.length()
  while i > 0 {
    i = i - 1
    sb.write_char(digits[i])
  }
  sb.to_string()
}

///|
fn format_hex_uint(value : UInt) -> String {
  format_hex_uint64(UInt::to_uint64(value))
}

///|
fn pow10_int(exp : Int) -> Int64 {
  let mut value = 1L
  let mut i = 0
  while i < exp {
    value = value * 10L
    i = i + 1
  }
  value
}

///|
fn format_float_fixed(value : Double, precision : Int) -> String {
  let negative = value < 0.0
  let abs_value = Double::abs(value)
  let scale = pow10_int(precision)
  let scaled = abs_value * scale.to_double()
  let rounded = Double::floor(scaled + 0.5)
  let scaled_int = Double::to_int64(rounded)
  let int_part = scaled_int / scale
  let frac_part = scaled_int % scale
  let int_str = int_part.to_string()
  if precision == 0 {
    if negative {
      "-" + int_str
    } else {
      int_str
    }
  } else {
    let frac_str = pad_left(frac_part.to_string(), precision, '0')
    let result = "\{int_str}.\{frac_str}"
    if negative {
      "-" + result
    } else {
      result
    }
  }
}

///|
fn std_sprintf(args : Array[Value]) -> Value raise {
  if args.is_empty() {
    return String("")
  }
  let format = to_string_value(args[0])
  let chars : Array[Char] = []
  for ch in format {
    chars.push(ch)
  }
  let sb = StringBuilder::new()
  let mut i = 0
  let mut arg_index = 1
  while i < chars.length() {
    let ch = chars[i]
    if ch != '%' {
      sb.write_char(ch)
      i = i + 1
      continue
    }
    if i + 1 < chars.length() && chars[i + 1] == '%' {
      sb.write_char('%')
      i = i + 2
      continue
    }
    i = i + 1
    let mut zero_pad = false
    let mut alt = false
    while i < chars.length() {
      match chars[i] {
        '0' => {
          zero_pad = true
          i = i + 1
        }
        '#' => {
          alt = true
          i = i + 1
        }
        _ => break
      }
    }
    let mut width = 0
    if i < chars.length() && chars[i] == '*' {
      if arg_index < args.length() {
        width = to_int32(to_number(args[arg_index]))
      }
      arg_index = arg_index + 1
      i = i + 1
    } else {
      while i < chars.length() && chars[i] >= '0' && chars[i] <= '9' {
        width = width * 10 + (chars[i].to_int() - '0'.to_int())
        i = i + 1
      }
    }
    let mut precision : Int? = None
    if i < chars.length() && chars[i] == '.' {
      i = i + 1
      if i < chars.length() && chars[i] == '*' {
        if arg_index < args.length() {
          precision = Some(to_int32(to_number(args[arg_index])))
        } else {
          precision = Some(0)
        }
        arg_index = arg_index + 1
        i = i + 1
      } else {
        let mut prec = 0
        while i < chars.length() && chars[i] >= '0' && chars[i] <= '9' {
          prec = prec * 10 + (chars[i].to_int() - '0'.to_int())
          i = i + 1
        }
        precision = Some(prec)
      }
    }
    let mut long = false
    if i < chars.length() && chars[i] == 'l' {
      long = true
      i = i + 1
    }
    if i >= chars.length() {
      break
    }
    let spec = chars[i]
    i = i + 1
    let value = if arg_index < args.length() {
      args[arg_index]
    } else {
      Undefined
    }
    arg_index = arg_index + 1
    let mut rendered = ""
    match spec {
      's' => rendered = to_string_value(value)
      'd' => {
        let num = to_int32(to_number(value))
        rendered = num.to_string()
      }
      'x' => {
        let num = to_uint32(to_number(value))
        rendered = format_hex_uint(num)
      }
      'f' => {
        let prec = match precision {
          Some(p) => p
          None => 6
        }
        rendered = format_float_fixed(to_number(value), prec)
      }
      _ => if spec == 'x' && long { rendered = "0" }
    }
    if long && spec == 'x' {
      match value {
        BigInt(big) => rendered = bigint_to_string_radix(big, 16)
        _ => {
          let num = Double::to_int64(Double::trunc(to_number(value)))
          let bits = Int64::reinterpret_as_uint64(num)
          rendered = format_hex_uint64(bits)
        }
      }
    }
    if alt && spec == 'x' {
      rendered = "0x" + rendered
    }
    if width > 0 {
      let pad = if zero_pad { '0' } else { ' ' }
      rendered = pad_left(rendered, width, pad)
    }
    sb.write_string(rendered)
  }
  String(sb.to_string())
}
