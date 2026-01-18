///|
fn bigint_to_number(value : @bigint.BigInt) -> Double {
  let text = value.to_string()
  try @strconv.parse_double(text) catch {
    _ => if text.has_prefix("-") { 0.0 - inf() } else { inf() }
  } noraise {
    value => value
  }
}

///|
fn bigint_from_double(value : Double) -> @bigint.BigInt? {
  if Double::is_nan(value) || Double::is_inf(value) {
    return None
  }
  let negative = value < 0.0
  let abs_value = Double::abs(value)
  let (mantissa, exp2) = double_to_parts(abs_value)
  let (num, den) = parts_to_fraction(mantissa, exp2)
  if den.is_zero() {
    return None
  }
  if !num.mod(den).is_zero() {
    return None
  }
  let mut result = num.div(den)
  if negative && !result.is_zero() {
    result = result.neg()
  }
  Some(result)
}

///|
fn bigint_from_value(value : Value) -> @bigint.BigInt raise {
  match value {
    BigInt(b) => b
    Bool(v) => @bigint.BigInt::from_int(if v { 1 } else { 0 })
    Number(n) =>
      match bigint_from_double(n) {
        Some(b) => b
        None => {
          let _ = throw_range_error("cannot convert to BigInt")
          @bigint.BigInt::from_int(0)
        }
      }
    String(s) => parse_bigint_string(s)
    Object(_) => {
      let prim = to_primitive(value, false)
      bigint_from_value(prim)
    }
    Null => {
      let _ = throw_type_error("cannot convert to BigInt")
      @bigint.BigInt::from_int(0)
    }
    Undefined => {
      let _ = throw_type_error("cannot convert to BigInt")
      @bigint.BigInt::from_int(0)
    }
    Array(_) | Builtin(_) | Function(_) | BoundFunction(_) | Arguments(_) => {
      let prim = to_primitive(value, false)
      bigint_from_value(prim)
    }
    _ => {
      let _ = throw_type_error("cannot convert to BigInt")
      @bigint.BigInt::from_int(0)
    }
  }
}

///|
fn to_bigint(value : Value) -> @bigint.BigInt raise {
  match value {
    BigInt(b) => b
    Bool(v) => @bigint.BigInt::from_int(if v { 1 } else { 0 })
    String(s) => parse_bigint_string(s)
    Object(_)
    | Array(_)
    | Builtin(_)
    | Function(_)
    | BoundFunction(_)
    | Arguments(_) => {
      let prim = to_primitive(value, false)
      to_bigint(prim)
    }
    _ => {
      let _ = throw_type_error("cannot convert to bigint")
      @bigint.BigInt::from_int(0)
    }
  }
}

///|
fn bigint_as_n(
  bits_value : Value,
  bigint_value : Value,
  as_int : Bool,
) -> Value raise {
  let bits64 = to_index_int64(bits_value)
  let value = to_bigint(bigint_value)
  let max_int = 2147483647
  if bits64 > Int64::from_int(max_int) {
    let _ = throw_range_error("invalid bit length")
    return BigInt(@bigint.BigInt::from_int(0))
  }
  if bits64 == 0L {
    return BigInt(@bigint.BigInt::from_int(0))
  }
  let bits = Int64::to_int(bits64)
  let modulus = @bigint.BigInt::from_int(1) << bits
  let mut result = value.mod(modulus)
  if result.compare_int(0) < 0 {
    result = result.add(modulus)
  }
  if as_int {
    let half = modulus >> 1
    if result.compare(half) >= 0 {
      result = result.sub(modulus)
    }
  }
  BigInt(result)
}

///|
fn bigint_to_string_radix(value : @bigint.BigInt, radix : Int) -> String {
  if value.is_zero() {
    return "0"
  }
  let negative = value.compare_int(0) < 0
  let abs_value = if negative { value.neg() } else { value }
  let digits = bigint_to_radix(abs_value, radix)
  if negative {
    "-" + digits
  } else {
    digits
  }
}

///|
fn strip_bigint_separators(
  lexeme : String,
  start : Int,
  end : Int,
  radix : Int,
) -> String raise {
  let mut index = start
  let mut any = false
  let mut prev_digit = false
  let underscore = Char::to_int('_')
  let sb = StringBuilder::new()
  while index < end {
    let unit = lexeme.code_unit_at(index)
    let code = UInt16::to_int(unit)
    if code == underscore {
      if !prev_digit || index + 1 >= end {
        fail("invalid bigint literal")
      }
      let next_unit = lexeme.code_unit_at(index + 1)
      let next_digit = digit_value(UInt16::to_int(next_unit))
      if next_digit < 0 || next_digit >= radix {
        fail("invalid bigint literal")
      }
      prev_digit = false
      index = index + 1
      continue
    }
    let digit = digit_value(code)
    if digit < 0 || digit >= radix {
      fail("invalid bigint literal")
    }
    sb.write_char(UInt16::unsafe_to_char(unit))
    any = true
    prev_digit = true
    index = index + 1
  }
  if !any || !prev_digit {
    fail("invalid bigint literal")
  }
  sb.to_string()
}

///|
fn parse_bigint_literal(lexeme : String) -> @bigint.BigInt raise {
  let mut index = 0
  let len = lexeme.length()
  let mut radix = 10
  if lexeme.has_prefix("0x") || lexeme.has_prefix("0X") {
    radix = 16
    index = 2
  } else if lexeme.has_prefix("0o") || lexeme.has_prefix("0O") {
    radix = 8
    index = 2
  } else if lexeme.has_prefix("0b") || lexeme.has_prefix("0B") {
    radix = 2
    index = 2
  }
  let digits = strip_bigint_separators(lexeme, index, len, radix)
  let view = digits.view(start_offset=0, end_offset=digits.length())
  let (value, ok) = parse_bigint_digits(view, radix)
  if !ok {
    fail("invalid bigint literal")
  }
  value
}

///|
fn parse_bigint_string(text : String) -> @bigint.BigInt raise {
  match parse_bigint_string_inner(text) {
    Some(value) => value
    None => {
      let _ = throw_syntax_error("invalid BigInt literal")
      @bigint.BigInt::from_int(0)
    }
  }
}

///|
fn parse_bigint_string_inner(text : String) -> @bigint.BigInt? {
  let trimmed = text.trim()
  if trimmed.is_empty() {
    return Some(@bigint.BigInt::from_int(0))
  }
  let len = trimmed.length()
  let mut index = 0
  let mut negative = false
  let mut has_sign = false
  let first = UInt16::to_int(trimmed.code_unit_at(0))
  if first == Char::to_int('+') || first == Char::to_int('-') {
    has_sign = true
    if first == Char::to_int('-') {
      negative = true
    }
    index = 1
  }
  if index >= len {
    return None
  }
  let mut radix = 10
  if index + 1 < len {
    let c0 = UInt16::to_int(trimmed.code_unit_at(index))
    let c1 = UInt16::to_int(trimmed.code_unit_at(index + 1))
    if c0 == Char::to_int('0') &&
      (c1 == Char::to_int('x') || c1 == Char::to_int('X')) {
      if has_sign {
        return None
      }
      radix = 16
      index = index + 2
    } else if c0 == Char::to_int('0') &&
      (c1 == Char::to_int('o') || c1 == Char::to_int('O')) {
      if has_sign {
        return None
      }
      radix = 8
      index = index + 2
    } else if c0 == Char::to_int('0') &&
      (c1 == Char::to_int('b') || c1 == Char::to_int('B')) {
      if has_sign {
        return None
      }
      radix = 2
      index = index + 2
    }
  }
  if index >= len {
    return None
  }
  let view = trimmed.view(start_offset=index, end_offset=len)
  let (value, ok) = parse_bigint_digits(view, radix)
  if !ok {
    return None
  }
  if negative && !value.is_zero() {
    Some(value.neg())
  } else {
    Some(value)
  }
}

///|
fn parse_bigint_digits(
  view : StringView,
  radix : Int,
) -> (@bigint.BigInt, Bool) {
  let mut value = @bigint.BigInt::from_int(0)
  let base = @bigint.BigInt::from_int(radix)
  let mut any = false
  for ch in view {
    let digit = digit_value(Char::to_int(ch))
    if digit < 0 || digit >= radix {
      return (value, false)
    }
    value = value.mul(base).add(@bigint.BigInt::from_int(digit))
    any = true
  }
  (value, any)
}

///|
fn bigint_bitwise_digit(op_kind : Int, x_digit : Int, y_digit : Int) -> Int {
  match op_kind {
    0 => if x_digit != 0 && y_digit != 0 { 1 } else { 0 }
    1 => if x_digit != 0 || y_digit != 0 { 1 } else { 0 }
    _ => if x_digit != y_digit { 1 } else { 0 }
  }
}

///|
fn bigint_mod2(value : @bigint.BigInt, two : @bigint.BigInt) -> Int {
  let mut rem = value.mod(two)
  if rem.compare_int(0) < 0 {
    rem = rem.add(two)
  }
  rem.to_int()
}

///|
fn bigint_is_terminal(value : @bigint.BigInt) -> Bool {
  value.compare_int(0) == 0 || value.compare_int(-1) == 0
}

///|
fn bigint_bitwise_op(
  left : @bigint.BigInt,
  right : @bigint.BigInt,
  op_kind : Int,
) -> @bigint.BigInt {
  let one = @bigint.BigInt::from_int(1)
  let two = @bigint.BigInt::from_int(2)
  let mut x = left
  let mut y = right
  let mut result = @bigint.BigInt::from_int(0)
  let mut pow2 = @bigint.BigInt::from_int(1)
  while !(bigint_is_terminal(x) && bigint_is_terminal(y)) {
    let x_digit = bigint_mod2(x, two)
    let y_digit = bigint_mod2(y, two)
    let digit = bigint_bitwise_digit(op_kind, x_digit, y_digit)
    if digit != 0 {
      result = result.add(pow2)
    }
    if x_digit != 0 {
      x = x.sub(one)
    }
    if y_digit != 0 {
      y = y.sub(one)
    }
    x = x.div(two)
    y = y.div(two)
    pow2 = pow2.mul(two)
  }
  let x_digit = bigint_mod2(x, two)
  let y_digit = bigint_mod2(y, two)
  let digit = bigint_bitwise_digit(op_kind, x_digit, y_digit)
  if digit != 0 {
    result = result.sub(pow2)
  }
  result
}

///|
fn bigint_to_uint64_bits(value : Value, as_int : Bool) -> UInt64 raise {
  let big = to_bigint(value)
  let modulus = @bigint.BigInt::from_int(1) << 64
  let mut result = big.mod(modulus)
  if result.compare_int(0) < 0 {
    result = result.add(modulus)
  }
  if as_int {
    let half = modulus >> 1
    if result.compare(half) >= 0 {
      result = result.sub(modulus)
    }
    let signed = result.to_int64()
    Int64::reinterpret_as_uint64(signed)
  } else {
    result.to_uint64()
  }
}

///|
fn bigint_property_get(value : @bigint.BigInt, name : String) -> Value raise {
  match bigint_proto() {
    Some(proto) =>
      property_get_from_chain(proto.props, proto.proto, name, BigInt(value))
    None => Undefined
  }
}
