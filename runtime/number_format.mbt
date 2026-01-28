///|
fn number_epsilon() -> Double {
  @math.pow(2.0, -52.0)
}

///|
fn number_min_value() -> Double {
  string_to_number("5e-324")
}

///|
fn trim_trailing_zeros(digits : String) -> String {
  let mut end = digits.length()
  while end > 0 {
    let unit = UInt16::to_int(digits.code_unit_at(end - 1))
    if unit != Char::to_int('0') {
      break
    }
    end = end - 1
  }
  if end == digits.length() {
    digits
  } else if end == 0 {
    ""
  } else {
    digits.unsafe_substring(start=0, end~)
  }
}

///|
fn number_to_string_radix(value : Double, radix : Int) -> String {
  if Double::is_nan(value) {
    return "NaN"
  }
  if Double::is_inf(value) {
    return if value < 0.0 { "-Infinity" } else { "Infinity" }
  }
  if value == 0.0 {
    return "0"
  }
  if radix == 10 {
    return number_to_string_base10(value)
  }
  let sign = value < 0.0 && !is_negative_zero(value)
  let abs_value = Double::abs(value)
  let (mantissa, exp2) = double_to_parts(abs_value)
  let (num, den) = parts_to_fraction(mantissa, exp2)
  let mut int_part = num.div(den)
  let mut rem = num.mod(den)
  let mut int_str = bigint_to_radix(int_part, radix)
  if rem.is_zero() {
    return if sign { "-" + int_str } else { int_str }
  }
  let base = @bigint.BigInt::from_int(radix)
  let max_digits = max_fraction_digits(radix)
  let frac_digits : Array[Int] = []
  while !rem.is_zero() && frac_digits.length() < max_digits {
    rem = rem.mul(base)
    let digit_big = rem.div(den)
    rem = rem.mod(den)
    frac_digits.push(digit_big.to_int())
  }
  if !rem.is_zero() {
    let twice = rem.mul(@bigint.BigInt::from_int(2))
    let cmp = twice.compare(den)
    if cmp >= 0 {
      if increment_digits(frac_digits, radix) {
        int_part = int_part.add(@bigint.BigInt::from_int(1))
        int_str = bigint_to_radix(int_part, radix)
      }
    }
  }
  let frac_str = digits_to_string(frac_digits)
  let sb = StringBuilder::new()
  if sign {
    sb.write_char('-')
  }
  sb.write_string(int_str)
  sb.write_char('.')
  sb.write_string(frac_str)
  sb.to_string()
}

///|
fn number_to_fixed(value : Double, digits : Int) -> String {
  if Double::is_nan(value) {
    return "NaN"
  }
  if Double::is_inf(value) {
    return if value < 0.0 { "-Infinity" } else { "Infinity" }
  }
  let sign = value < 0.0
  let abs_value = Double::abs(value)
  if abs_value >= 1.0e21 {
    return number_to_string_base10(value)
  }
  let (mantissa, exp2) = double_to_parts(abs_value)
  let (num, den) = parts_to_fraction(mantissa, exp2)
  let scale = pow10_bigint(digits)
  let scaled = num.mul(scale)
  let rounded = div_round_half_up(scaled, den)
  let mut digits_str = rounded.to_string()
  if digits == 0 {
    return if sign && rounded.is_zero() {
      "-0"
    } else if sign {
      "-" + digits_str
    } else {
      digits_str
    }
  }
  let needed = digits + 1
  if digits_str.length() < needed {
    let zeros = "0".repeat(needed - digits_str.length())
    digits_str = zeros + digits_str
  }
  let split_at = digits_str.length() - digits
  let int_part = digits_str.unsafe_substring(start=0, end=split_at)
  let frac_part = digits_str.unsafe_substring(
    start=split_at,
    end=digits_str.length(),
  )
  let sb = StringBuilder::new()
  if sign && rounded.is_zero() {
    sb.write_char('-')
  } else if sign {
    sb.write_char('-')
  }
  sb.write_string(int_part)
  sb.write_char('.')
  sb.write_string(frac_part)
  sb.to_string()
}

///|
fn number_to_exponential(value : Double, digits : Int) -> String {
  if Double::is_nan(value) {
    return "NaN"
  }
  if Double::is_inf(value) {
    return if value < 0.0 { "-Infinity" } else { "Infinity" }
  }
  let sign = value < 0.0
  let abs_value = Double::abs(value)
  if abs_value == 0.0 {
    let zeros = if digits == 0 { "" } else { "0".repeat(digits) }
    let base = if digits == 0 { "0" } else { "0.\{zeros}" }
    let result = "\{base}e+0"
    return if sign { "-" + result } else { result }
  }
  let (mantissa, exp2) = double_to_parts(abs_value)
  let (num, den) = parts_to_fraction(mantissa, exp2)
  let mut exp10 = decimal_exponent(num, den)
  let sig_digits = digits + 1
  let scale = sig_digits - 1 - exp10
  let (scaled_num, scaled_den) = if scale >= 0 {
    (num.mul(pow10_bigint(scale)), den)
  } else {
    (num, den.mul(pow10_bigint(-scale)))
  }
  let mut sig = div_round_half_up(scaled_num, scaled_den)
  let limit = pow10_bigint(sig_digits)
  if sig.compare(limit) >= 0 {
    sig = sig.div(@bigint.BigInt::from_int(10))
    exp10 = exp10 + 1
  }
  let mut sig_str = sig.to_string()
  if sig_str.length() < sig_digits {
    let zeros = "0".repeat(sig_digits - sig_str.length())
    sig_str = zeros + sig_str
  }
  let lead = sig_str.unsafe_substring(start=0, end=1)
  let tail = sig_str.unsafe_substring(start=1, end=sig_str.length())
  let mantissa_str = if digits == 0 { lead } else { "\{lead}.\{tail}" }
  let exp_sign = if exp10 < 0 { "-" } else { "+" }
  let exp_abs = Int::abs(exp10).to_string()
  let sb = StringBuilder::new()
  if sign {
    sb.write_char('-')
  }
  sb.write_string(mantissa_str)
  sb.write_char('e')
  sb.write_string(exp_sign)
  sb.write_string(exp_abs)
  sb.to_string()
}

///|
fn number_to_exponential_free(value : Double) -> String {
  if Double::is_nan(value) {
    return "NaN"
  }
  if Double::is_inf(value) {
    return if value < 0.0 { "-Infinity" } else { "Infinity" }
  }
  let sign = value < 0.0 && !is_negative_zero(value)
  let abs_value = Double::abs(value)
  if abs_value == 0.0 {
    let result = "0e+0"
    return if sign { "-" + result } else { result }
  }
  let base = number_to_string_base10(abs_value)
  if base.contains("e") {
    return if sign { "-" + base } else { base }
  }
  let (int_part, frac_part) = match base.find(".") {
    Some(idx) =>
      (
        base.unsafe_substring(start=0, end=idx),
        base.unsafe_substring(start=idx + 1, end=base.length()),
      )
    None => (base, "")
  }
  let (raw_digits, exp10) = if int_part != "0" {
    (int_part + frac_part, int_part.length() - 1)
  } else {
    let mut idx = 0
    while idx < frac_part.length() {
      let unit = UInt16::to_int(frac_part.code_unit_at(idx))
      if unit != Char::to_int('0') {
        break
      }
      idx = idx + 1
    }
    let exp = 0 - (idx + 1)
    (frac_part.unsafe_substring(start=idx, end=frac_part.length()), exp)
  }
  let digits = trim_trailing_zeros(raw_digits)
  let lead = digits.unsafe_substring(start=0, end=1)
  let tail = digits.unsafe_substring(start=1, end=digits.length())
  let mantissa = if tail.is_empty() { lead } else { "\{lead}.\{tail}" }
  let exp_sign = if exp10 < 0 { "-" } else { "+" }
  let exp_abs = Int::abs(exp10).to_string()
  let sb = StringBuilder::new()
  if sign {
    sb.write_char('-')
  }
  sb.write_string(mantissa)
  sb.write_char('e')
  sb.write_string(exp_sign)
  sb.write_string(exp_abs)
  sb.to_string()
}

///|
fn number_to_precision(value : Double, precision : Int) -> String {
  if Double::is_nan(value) {
    return "NaN"
  }
  if Double::is_inf(value) {
    return if value < 0.0 { "-Infinity" } else { "Infinity" }
  }
  if precision <= 0 {
    return number_to_string_base10(value)
  }
  let abs_value = Double::abs(value)
  if abs_value == 0.0 {
    if precision == 1 {
      return if value < 0.0 { "-0" } else { "0" }
    }
    let zeros = "0".repeat(precision - 1)
    let result = "0.\{zeros}"
    return if value < 0.0 { "-" + result } else { result }
  }
  let exp_form = number_to_exponential(value, precision - 1)
  let (sign, core) = if exp_form.has_prefix("-") {
    ("-", exp_form.unsafe_substring(start=1, end=exp_form.length()))
  } else {
    ("", exp_form)
  }
  let exp_index = match core.find("e") {
    Some(idx) => idx
    None => return exp_form
  }
  let mantissa = core.unsafe_substring(start=0, end=exp_index)
  let exp_text = core.unsafe_substring(start=exp_index + 1, end=core.length())
  let exp10 = Double::to_int(Double::trunc(string_to_number(exp_text)))
  if exp10 < -6 || exp10 >= precision {
    return exp_form
  }
  let (int_part, frac_part) = match mantissa.find(".") {
    Some(idx) =>
      (
        mantissa.unsafe_substring(start=0, end=idx),
        mantissa.unsafe_substring(start=idx + 1, end=mantissa.length()),
      )
    None => (mantissa, "")
  }
  let digits = int_part + frac_part
  let int_len = exp10 + 1
  let result = if int_len <= 0 {
    let zeros = "0".repeat(-int_len)
    "0.\{zeros}\{digits}"
  } else if int_len >= digits.length() {
    let zeros = "0".repeat(int_len - digits.length())
    "\{digits}\{zeros}"
  } else {
    let head = digits.unsafe_substring(start=0, end=int_len)
    let tail = digits.unsafe_substring(start=int_len, end=digits.length())
    "\{head}.\{tail}"
  }
  let sb = StringBuilder::new()
  sb.write_string(sign)
  sb.write_string(result)
  sb.to_string()
}

///|
fn number_to_string_base10(value : Double) -> String {
  if Double::is_nan(value) {
    return "NaN"
  }
  if Double::is_inf(value) {
    return if value < 0.0 { "-Infinity" } else { "Infinity" }
  }
  if value == 0.0 {
    return "0"
  }
  let sign = value < 0.0
  let abs_value = Double::abs(value)
  let (mantissa, exp2) = double_to_parts(abs_value)
  let (num, den) = parts_to_fraction(mantissa, exp2)
  if den.is_zero() {
    return "0"
  }
  if num.mod(den).is_zero() {
    let int_part = num.div(den)
    let digits = shortest_integer_string(abs_value, int_part)
    let exp10 = digits.length() - 1
    if exp10 >= 21 {
      let lead = digits.unsafe_substring(start=0, end=1)
      let tail = digits.unsafe_substring(start=1, end=digits.length())
      let trimmed_tail = trim_trailing_zeros(tail)
      let mantissa_str = if trimmed_tail.is_empty() {
        lead
      } else {
        "\{lead}.\{trimmed_tail}"
      }
      let sb = StringBuilder::new()
      if sign {
        sb.write_char('-')
      }
      sb.write_string(mantissa_str)
      sb.write_string("e+")
      sb.write_string(Int::to_string(exp10))
      return sb.to_string()
    }
    return if sign { "-" + digits } else { digits }
  }
  let rendered = Double::to_string(value)
  rendered
}

///|
fn shortest_integer_string(
  abs_value : Double,
  int_part : @bigint.BigInt,
) -> String {
  let digits = int_part.to_string()
  let len = digits.length()
  let mut best = digits
  let mut best_drop = 0
  let mut drop = 1
  while drop < len {
    let base = pow10_bigint(drop)
    let q = int_part.div(base)
    let cand0 = q.mul(base)
    let cand0_str = cand0.to_string()
    if Double::equal(string_to_number(cand0_str), abs_value) {
      if cand0_str.length() < best.length() ||
        (cand0_str.length() == best.length() && drop > best_drop) {
        best = cand0_str
        best_drop = drop
      }
    }
    let cand1 = q.add(@bigint.BigInt::from_int(1)).mul(base)
    let cand1_str = cand1.to_string()
    if Double::equal(string_to_number(cand1_str), abs_value) {
      if cand1_str.length() < best.length() ||
        (cand1_str.length() == best.length() && drop > best_drop) {
        best = cand1_str
        best_drop = drop
      }
    }
    drop = drop + 1
  }
  best
}

///|
fn double_to_parts(value : Double) -> (@bigint.BigInt, Int) {
  let bits = Double::reinterpret_as_uint64(value)
  let exp_bits = UInt::reinterpret_as_int(UInt64::to_uint(bits >> 52)) & 0x7ff
  let frac_mask = (1UL << 52) - 1UL
  let frac = bits & frac_mask
  if exp_bits == 0 {
    if frac == 0UL {
      return (@bigint.BigInt::from_int(0), 0)
    }
    let exp2 = 1 - 1023 - 52
    return (@bigint.BigInt::from_uint64(frac), exp2)
  }
  let exp2 = exp_bits - 1023 - 52
  let mantissa = frac | (1UL << 52)
  (@bigint.BigInt::from_uint64(mantissa), exp2)
}

///|
fn parts_to_fraction(
  mantissa : @bigint.BigInt,
  exp2 : Int,
) -> (@bigint.BigInt, @bigint.BigInt) {
  if exp2 >= 0 {
    let num = mantissa << exp2
    (num, @bigint.BigInt::from_int(1))
  } else {
    let den = @bigint.BigInt::from_int(1) << -exp2
    (mantissa, den)
  }
}

///|
fn pow10_bigint(exp : Int) -> @bigint.BigInt {
  if exp <= 0 {
    return @bigint.BigInt::from_int(1)
  }
  @bigint.BigInt::pow(
    @bigint.BigInt::from_int(10),
    @bigint.BigInt::from_int(exp),
  )
}

///|
fn max_fraction_digits(radix : Int) -> Int {
  let base = @bigint.BigInt::from_int(radix)
  let limit = @bigint.BigInt::from_uint64(1UL << 53)
  let mut acc = @bigint.BigInt::from_int(1)
  let mut digits = 0
  while acc.compare(limit) < 0 {
    acc = acc.mul(base)
    digits = digits + 1
  }
  digits
}

///|
fn bigint_to_radix(value : @bigint.BigInt, radix : Int) -> String {
  if value.is_zero() {
    return "0"
  }
  let base = @bigint.BigInt::from_int(radix)
  let mut current = value
  let digits : Array[Int] = []
  while !current.is_zero() {
    let q = current.div(base)
    let r = current.mod(base)
    digits.push(r.to_int())
    current = q
  }
  digits_to_string(digits.rev())
}

///|
fn digits_to_string(digits : Array[Int]) -> String {
  let table = "0123456789abcdefghijklmnopqrstuvwxyz"
  let sb = StringBuilder::new()
  for digit in digits {
    let slice = table.unsafe_substring(start=digit, end=digit + 1)
    sb.write_string(slice)
  }
  sb.to_string()
}

///|
fn increment_digits(digits : Array[Int], radix : Int) -> Bool {
  if digits.is_empty() {
    return true
  }
  let mut i = digits.length() - 1
  while i >= 0 {
    let next = digits[i] + 1
    if next < radix {
      digits[i] = next
      return false
    }
    digits[i] = 0
    if i == 0 {
      return true
    }
    i = i - 1
  }
  true
}

///|
fn div_round_half_up(
  num : @bigint.BigInt,
  den : @bigint.BigInt,
) -> @bigint.BigInt {
  let q = num.div(den)
  let r = num.mod(den)
  if r.is_zero() {
    return q
  }
  let twice = r.mul(@bigint.BigInt::from_int(2))
  let cmp = twice.compare(den)
  if cmp >= 0 {
    q.add(@bigint.BigInt::from_int(1))
  } else {
    q
  }
}

///|
fn decimal_exponent(num : @bigint.BigInt, den : @bigint.BigInt) -> Int {
  let int_part = num.div(den)
  if !int_part.is_zero() {
    return int_part.to_string().length() - 1
  }
  let mut exp = 0
  let mut scaled = num
  while scaled.compare(den) < 0 {
    scaled = scaled.mul(@bigint.BigInt::from_int(10))
    exp = exp - 1
  }
  exp
}
