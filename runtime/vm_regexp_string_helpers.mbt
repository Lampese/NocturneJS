///|
fn string_get_substitution(
  matched : String,
  input : String,
  position : Int,
  captures : Array[String?],
  named_captures : Value,
  replacement : String,
) -> String raise {
  let _ = string_get_substitution_len(
    matched, input, position, captures, named_captures, replacement,
  )
  let result = StringBuilder::new()
  let mut result_len = 0
  let len = replacement.length()
  let captures_len = captures.length()
  let mut i = 0
  while true {
    let j = string_index_of(replacement, "$", i)
    if j < 0 || j + 1 >= len {
      break
    }
    let prefix = replacement.unsafe_substring(start=i, end=j)
    result_len = add_string_length(result_len, prefix.length())
    result.write_string(prefix)
    let j0 = j
    let mut k = j + 1
    let code = UInt16::to_int(replacement.code_unit_at(k))
    k = k + 1
    if code == Char::to_int('$') {
      result_len = add_string_length(result_len, 1)
      result.write_string("$")
    } else if code == Char::to_int('&') {
      result_len = add_string_length(result_len, matched.length())
      result.write_string(matched)
    } else if code == Char::to_int('`') {
      let end_pos = if position > input.length() {
        input.length()
      } else {
        position
      }
      result_len = add_string_length(result_len, end_pos)
      result.write_string(input.unsafe_substring(start=0, end=end_pos))
    } else if code == Char::to_int('\'') {
      let mut tail_pos = position + matched.length()
      if tail_pos > input.length() {
        tail_pos = input.length()
      }
      result_len = add_string_length(result_len, input.length() - tail_pos)
      result.write_string(
        input.unsafe_substring(start=tail_pos, end=input.length()),
      )
    } else if code >= Char::to_int('0') && code <= Char::to_int('9') {
      let mut index = code - Char::to_int('0')
      if k < len {
        let next = UInt16::to_int(replacement.code_unit_at(k))
        if next >= Char::to_int('0') && next <= Char::to_int('9') {
          let next_index = index * 10 + (next - Char::to_int('0'))
          if next_index >= 1 && next_index < captures_len {
            index = next_index
            k = k + 1
          }
        }
      }
      if index >= 1 && index < captures_len {
        match captures[index] {
          Some(text) => {
            result_len = add_string_length(result_len, text.length())
            result.write_string(text)
          }
          None => ()
        }
      } else {
        let literal = replacement.unsafe_substring(start=j0, end=k)
        result_len = add_string_length(result_len, literal.length())
        result.write_string(literal)
      }
    } else if code == Char::to_int('<') && !(named_captures is Undefined) {
      let close = string_index_of(replacement, ">", k)
      if close < 0 {
        let literal = replacement.unsafe_substring(start=j0, end=k)
        result_len = add_string_length(result_len, literal.length())
        result.write_string(literal)
      } else {
        let name = replacement.unsafe_substring(start=k, end=close)
        let target = to_object(named_captures)
        let capture = property_get(target, name)
        if !(capture is Undefined) {
          let text = to_string_strict(capture)
          result_len = add_string_length(result_len, text.length())
          result.write_string(text)
        }
        k = close + 1
      }
    } else {
      let literal = replacement.unsafe_substring(start=j0, end=k)
      result_len = add_string_length(result_len, literal.length())
      result.write_string(literal)
    }
    i = k
  }
  let tail = replacement.unsafe_substring(start=i, end=len)
  result_len = add_string_length(result_len, tail.length())
  result.write_string(tail)
  result.to_string()
}

///|
fn string_get_substitution_len(
  matched : String,
  input : String,
  position : Int,
  captures : Array[String?],
  named_captures : Value,
  replacement : String,
) -> Int raise {
  let len = replacement.length()
  let input_len = input.length()
  let captures_len = captures.length()
  let mut total = 0
  let mut i = 0
  while true {
    let j = string_index_of(replacement, "$", i)
    if j < 0 || j + 1 >= len {
      break
    }
    total = add_string_length(total, j - i)
    let j0 = j
    let mut k = j + 1
    let code = UInt16::to_int(replacement.code_unit_at(k))
    k = k + 1
    if code == Char::to_int('$') {
      total = add_string_length(total, 1)
    } else if code == Char::to_int('&') {
      total = add_string_length(total, matched.length())
    } else if code == Char::to_int('`') {
      let end_pos = if position > input_len { input_len } else { position }
      total = add_string_length(total, end_pos)
    } else if code == Char::to_int('\'') {
      let mut tail_pos = position + matched.length()
      if tail_pos > input_len {
        tail_pos = input_len
      }
      total = add_string_length(total, input_len - tail_pos)
    } else if code >= Char::to_int('0') && code <= Char::to_int('9') {
      let mut index = code - Char::to_int('0')
      if k < len {
        let next = UInt16::to_int(replacement.code_unit_at(k))
        if next >= Char::to_int('0') && next <= Char::to_int('9') {
          let next_index = index * 10 + (next - Char::to_int('0'))
          if next_index >= 1 && next_index < captures_len {
            index = next_index
            k = k + 1
          }
        }
      }
      if index >= 1 && index < captures_len {
        match captures[index] {
          Some(text) => total = add_string_length(total, text.length())
          None => ()
        }
      } else {
        total = add_string_length(total, k - j0)
      }
    } else if code == Char::to_int('<') && !(named_captures is Undefined) {
      let close = string_index_of(replacement, ">", k)
      if close < 0 {
        total = add_string_length(total, k - j0)
      } else {
        let name = replacement.unsafe_substring(start=k, end=close)
        let target = to_object(named_captures)
        let capture = property_get(target, name)
        if !(capture is Undefined) {
          let text = to_string_strict(capture)
          total = add_string_length(total, text.length())
        }
        k = close + 1
      }
    } else {
      total = add_string_length(total, k - j0)
    }
    i = k
  }
  total = add_string_length(total, len - i)
  total
}

///|
fn string_replace_internal(
  this_value : Value,
  args : Array[Value],
  replace_all : Bool,
) -> Value raise {
  match this_value {
    Null | Undefined => {
      let _ = throw_type_error("cannot convert to object")
      return String("")
    }
    _ => ()
  }
  if args.is_empty() {
    return String(to_string_strict(this_value))
  }
  let search = args[0]
  let replace_value = if args.length() < 2 { Undefined } else { args[1] }
  if is_object_like(search) {
    if replace_all {
      check_regexp_g_flag(search)
    }
    match symbol_replace_key() {
      Some(key) =>
        match property_get(search, key) {
          Undefined | Null => ()
          replacer =>
            return call_value_with_this(
              replacer,
              [this_value, replace_value],
              search,
            )
        }
      None => ()
    }
  }
  let value = to_string_strict(this_value)
  let search_str = to_string_strict(search)
  let functional_replace = is_callable(replace_value)
  let replace_str = if functional_replace {
    ""
  } else {
    to_string_strict(replace_value)
  }
  let len = value.length()
  let result = StringBuilder::new()
  let mut end_of_last_match = 0
  let mut is_first = true
  while true {
    let pos = if search_str.is_empty() {
      if is_first {
        0
      } else if end_of_last_match >= len {
        -1
      } else {
        end_of_last_match + 1
      }
    } else {
      string_index_of(value, search_str, end_of_last_match)
    }
    if pos < 0 {
      if is_first {
        return String(value)
      }
      break
    }
    result.write_string(
      value.unsafe_substring(start=end_of_last_match, end=pos),
    )
    let replacement = if functional_replace {
      let res = call_value_with_this(
        replace_value,
        [String(search_str), Number(Double::from_int(pos)), String(value)],
        Undefined,
      )
      to_string_strict(res)
    } else {
      string_get_substitution(
        search_str,
        value,
        pos,
        [Some(search_str)],
        Undefined,
        replace_str,
      )
    }
    result.write_string(replacement)
    end_of_last_match = pos + search_str.length()
    is_first = false
    if !replace_all {
      break
    }
  }
  result.write_string(value.unsafe_substring(start=end_of_last_match, end=len))
  String(result.to_string())
}

///|
fn string_replace_value(this_value : Value, args : Array[Value]) -> Value raise {
  string_replace_internal(this_value, args, false)
}

///|
fn string_replace_all_value(
  this_value : Value,
  args : Array[Value],
) -> Value raise {
  string_replace_internal(this_value, args, true)
}

///|
fn string_match_symbol(
  this_value : Value,
  value : String,
  search : Value,
  symbol_key : String?,
  match_all : Bool,
) -> Value raise {
  if is_object_like(search) {
    if match_all {
      check_regexp_g_flag(search)
    }
    match symbol_key {
      Some(key) =>
        match property_get(search, key) {
          Undefined | Null => ()
          matcher => return call_value_with_this(matcher, [this_value], search)
        }
      None => ()
    }
  }
  let args : Array[Value] = [search]
  if match_all {
    args.push(String("g"))
  }
  let rx = regexp_from_args(args, true)
  match symbol_key {
    Some(key) => {
      let method_value = property_get(rx, key)
      call_value_with_this(method_value, [String(value)], rx)
    }
    None => {
      let input = String(value)
      regexp_exec_method(rx, input)
    }
  }
}

///|
fn string_match_value(
  this_value : Value,
  value : String,
  search : Value,
) -> Value raise {
  string_match_symbol(this_value, value, search, symbol_match_key(), false)
}

///|
fn string_match_all_value(
  this_value : Value,
  value : String,
  search : Value,
) -> Value raise {
  string_match_symbol(this_value, value, search, symbol_match_all_key(), true)
}

///|
fn string_search_value(
  this_value : Value,
  value : String,
  search : Value,
) -> Value raise {
  string_match_symbol(this_value, value, search, symbol_search_key(), false)
}

///|
fn to_length_index(num : Double, len : Int) -> Int {
  if Double::is_nan(num) || num <= 0.0 {
    return 0
  }
  if Double::is_inf(num) {
    return len
  }
  let trunc = Double::trunc(num)
  let max = Double::from_int(len)
  if trunc >= max {
    return len
  }
  Int64::to_int(Double::to_int64(trunc))
}

///|
fn to_length_int64(num : Double) -> Int64 {
  if Double::is_nan(num) || num <= 0.0 {
    return 0L
  }
  let max_safe = 9007199254740991.0
  if Double::is_inf(num) || num >= max_safe {
    return 9007199254740991L
  }
  let trunc = Double::trunc(num)
  if trunc >= max_safe {
    return 9007199254740991L
  }
  Double::to_int64(trunc)
}

///|
fn to_index_int64(value : Value) -> Int64 raise {
  let num = to_number(value)
  if Double::is_nan(num) || num == 0.0 {
    return 0L
  }
  if Double::is_inf(num) {
    let _ = throw_range_error("invalid array buffer length")
    return 0L
  }
  let trunc = Double::trunc(num)
  if trunc < 0.0 || trunc > 9007199254740991.0 {
    let _ = throw_range_error("invalid array buffer length")
    return 0L
  }
  Double::to_int64(trunc)
}

///|
fn to_int64_clamp(
  value : Value,
  min : Int,
  max : Int,
  neg_offset : Int,
) -> Int raise {
  let num = to_number(value)
  if Double::is_inf(num) {
    return if num < 0.0 { min } else { max }
  }
  let mut int_val = if Double::is_nan(num) {
    0L
  } else {
    Double::to_int64(Double::trunc(num))
  }
  if int_val < 0L {
    int_val = int_val + Int64::from_int(neg_offset)
  }
  let min_val = Int64::from_int(min)
  let max_val = Int64::from_int(max)
  if int_val < min_val {
    return min
  }
  if int_val > max_val {
    return max
  }
  Int64::to_int(int_val)
}

///|
fn to_int64_clamp64(
  value : Value,
  min : Int64,
  max : Int64,
  neg_offset : Int64,
) -> Int64 raise {
  let num = to_number(value)
  if Double::is_inf(num) {
    return if num < 0.0 { min } else { max }
  }
  let mut int_val = if Double::is_nan(num) {
    0L
  } else {
    Double::to_int64(Double::trunc(num))
  }
  if int_val < 0L {
    int_val = int_val + neg_offset
  }
  if int_val < min {
    return min
  }
  if int_val > max {
    return max
  }
  int_val
}

///|
fn is_valid_unicode_index(value : String, index : Int) -> Bool {
  if index <= 0 {
    return true
  }
  let len = value.length()
  if index >= len {
    return true
  }
  let prev = UInt16::to_int(value.code_unit_at(index - 1))
  let curr = UInt16::to_int(value.code_unit_at(index))
  !(is_lead_surrogate(prev) && is_trail_surrogate(curr))
}

///|
fn advance_string_index(value : String, index : Int, unicode : Bool) -> Int {
  if !unicode {
    return index + 1
  }
  let len = value.length()
  if index + 1 >= len {
    return index + 1
  }
  let first = UInt16::to_int(value.code_unit_at(index))
  let second = UInt16::to_int(value.code_unit_at(index + 1))
  if is_lead_surrogate(first) && is_trail_surrogate(second) {
    index + 2
  } else {
    index + 1
  }
}

///|
fn advance_string_index_int64(
  value : String,
  index : Int64,
  unicode : Bool,
) -> Int64 {
  if !unicode {
    return index + 1L
  }
  let len = Int64::from_int(value.length())
  if index + 1L >= len {
    return index + 1L
  }
  let idx = Int64::to_int(index)
  let first = UInt16::to_int(value.code_unit_at(idx))
  let second = UInt16::to_int(value.code_unit_at(idx + 1))
  if is_lead_surrogate(first) && is_trail_surrogate(second) {
    index + 2L
  } else {
    index + 1L
  }
}

///|
fn is_lead_surrogate(code : Int) -> Bool {
  code >= 0xD800 && code <= 0xDBFF
}

///|
fn is_trail_surrogate(code : Int) -> Bool {
  code >= 0xDC00 && code <= 0xDFFF
}

///|
fn is_surrogate(code : Int) -> Bool {
  code >= 0xD800 && code <= 0xDFFF
}

///|
fn string_find_invalid_codepoint(value : String) -> Int {
  let len = value.length()
  let mut i = 0
  while i < len {
    let unit = UInt16::to_int(value.code_unit_at(i))
    if is_lead_surrogate(unit) {
      if i + 1 < len {
        let next = UInt16::to_int(value.code_unit_at(i + 1))
        if is_trail_surrogate(next) {
          i = i + 2
          continue
        }
      }
      return i
    } else if is_trail_surrogate(unit) {
      return i
    }
    i = i + 1
  }
  -1
}

///|
fn string_to_well_formed(value : String) -> String {
  let len = value.length()
  let mut i = 0
  let result = StringBuilder::new()
  while i < len {
    let unit = UInt16::to_int(value.code_unit_at(i))
    if is_lead_surrogate(unit) {
      if i + 1 < len {
        let next = UInt16::to_int(value.code_unit_at(i + 1))
        if is_trail_surrogate(next) {
          result.write_string(value.unsafe_substring(start=i, end=i + 2))
          i = i + 2
          continue
        }
      }
      result.write_string("\uFFFD")
      i = i + 1
      continue
    }
    if is_trail_surrogate(unit) {
      result.write_string("\uFFFD")
      i = i + 1
      continue
    }
    result.write_string(value.unsafe_substring(start=i, end=i + 1))
    i = i + 1
  }
  result.to_string()
}

///|
fn regexp_source_escape(pattern : String) -> String {
  if pattern.is_empty() {
    return "(?:)"
  }
  let result = StringBuilder::new()
  let len = pattern.length()
  let mut i = 0
  let mut in_class = false
  while i < len {
    let start = i
    let code = UInt16::to_int(pattern.code_unit_at(i))
    i = i + 1
    if code == Char::to_int('\\') {
      if i < len {
        i = i + 1
      }
      result.write_string(pattern.unsafe_substring(start~, end=i))
      continue
    }
    if code == Char::to_int(']') {
      in_class = false
      result.write_string(pattern.unsafe_substring(start~, end=i))
      continue
    }
    if code == Char::to_int('[') && !in_class {
      if i < len && UInt16::to_int(pattern.code_unit_at(i)) == Char::to_int(']') {
        i = i + 1
      }
      in_class = true
      result.write_string(pattern.unsafe_substring(start~, end=i))
      continue
    }
    if code == Char::to_int('\n') {
      result.write_string("\\n")
      continue
    }
    if code == Char::to_int('\r') {
      result.write_string("\\r")
      continue
    }
    if code == Char::to_int('/') && !in_class {
      result.write_string("\\/")
      continue
    }
    result.write_string(pattern.unsafe_substring(start~, end=i))
  }
  result.to_string()
}

///|
fn append_hex2(sb : StringBuilder, code : Int) -> Unit {
  sb.write_string("\\x")
  sb.write_char(hex_digit(Int::land(code >> 4, 0xf)))
  sb.write_char(hex_digit(Int::land(code, 0xf)))
}

///|
fn append_hex4(sb : StringBuilder, code : Int) -> Unit {
  sb.write_string("\\u")
  sb.write_char(hex_digit(Int::land(code >> 12, 0xf)))
  sb.write_char(hex_digit(Int::land(code >> 8, 0xf)))
  sb.write_char(hex_digit(Int::land(code >> 4, 0xf)))
  sb.write_char(hex_digit(Int::land(code, 0xf)))
}

///|
fn regexp_escape_string(value : String) -> String {
  let result = StringBuilder::new()
  let len = value.length()
  let mut i = 0
  while i < len {
    let code = UInt16::to_int(value.code_unit_at(i))
    if code < 33 {
      if code >= 9 && code <= 13 {
        result.write_char('\\')
        let map = "tnvfr"
        let index = code - 9
        result.write_string(map.unsafe_substring(start=index, end=index + 1))
      } else {
        append_hex2(result, code)
      }
    } else if code < 128 {
      if (code >= Char::to_int('0') && code <= Char::to_int('9')) ||
        (code >= Char::to_int('A') && code <= Char::to_int('Z')) ||
        (code >= Char::to_int('a') && code <= Char::to_int('z')) {
        if i == 0 {
          append_hex2(result, code)
        } else {
          result.write_string(value.unsafe_substring(start=i, end=i + 1))
        }
      } else if ",-=<>#&!%:;@~'`\"".contains(
          value.unsafe_substring(start=i, end=i + 1),
        ) {
        append_hex2(result, code)
      } else {
        if code != Char::to_int('_') {
          result.write_char('\\')
        }
        result.write_string(value.unsafe_substring(start=i, end=i + 1))
      }
    } else if code < 256 {
      append_hex2(result, code)
    } else if is_surrogate(code) || is_js_whitespace(code) {
      append_hex4(result, code)
    } else {
      result.write_string(value.unsafe_substring(start=i, end=i + 1))
    }
    i = i + 1
  }
  result.to_string()
}
