///|
fn std_parse_ext_json(text : String) -> Value raise {
  let normalized = normalize_ext_json(text)
  let json = @json5.parse(normalized) catch {
    err => {
      let _ = throw_syntax_error(err.to_string())
      Json::null()
    }
  }
  json_to_value(json)
}

///|
fn is_octal_digit(ch : Char) -> Bool {
  ch >= '0' && ch <= '7'
}

///|
fn normalize_ext_json(text : String) -> String {
  let chars : Array[Char] = []
  for ch in text {
    chars.push(ch)
  }
  let sb = StringBuilder::new()
  let len = chars.length()
  let mut i = 0
  let mut in_single = false
  let mut in_double = false
  let mut in_line_comment = false
  let mut in_block_comment = false
  let mut escape = false
  while i < len {
    let ch = chars[i]
    if ch == '\\' && i + 1 < len {
      let next = chars[i + 1]
      if next == '\n' || next == '\r' {
        i = i + 2
        if next == '\r' && i < len && chars[i] == '\n' {
          i = i + 1
        }
        while i < len && (chars[i] == ' ' || chars[i] == '\t') {
          i = i + 1
        }
        continue
      }
    }
    if in_line_comment {
      sb.write_char(ch)
      if ch == '\n' {
        in_line_comment = false
      }
      i = i + 1
      continue
    }
    if in_block_comment {
      sb.write_char(ch)
      if ch == '*' && i + 1 < len && chars[i + 1] == '/' {
        sb.write_char('/')
        i = i + 2
        in_block_comment = false
        continue
      }
      i = i + 1
      continue
    }
    if in_single || in_double {
      if ch == '\n' || ch == '\r' {
        i = i + 1
        if ch == '\r' && i < len && chars[i] == '\n' {
          i = i + 1
        }
        while i < len && (chars[i] == ' ' || chars[i] == '\t') {
          i = i + 1
        }
        continue
      }
      if ch == '\\' && i + 1 < len {
        let next = chars[i + 1]
        if next == '\n' || next == '\r' {
          i = i + 2
          if next == '\r' && i < len && chars[i] == '\n' {
            i = i + 1
          }
          while i < len && (chars[i] == ' ' || chars[i] == '\t') {
            i = i + 1
          }
          continue
        }
      }
      sb.write_char(ch)
      if escape {
        escape = false
        i = i + 1
        continue
      }
      if ch == '\\' {
        escape = true
        i = i + 1
        continue
      }
      if in_single && ch == '\'' {
        in_single = false
      } else if in_double && ch == '"' {
        in_double = false
      }
      i = i + 1
      continue
    }
    if ch == '/' && i + 1 < len {
      let next = chars[i + 1]
      if next == '/' {
        in_line_comment = true
        sb.write_char(ch)
        sb.write_char(next)
        i = i + 2
        continue
      }
      if next == '*' {
        in_block_comment = true
        sb.write_char(ch)
        sb.write_char(next)
        i = i + 2
        continue
      }
    }
    if ch == '\'' {
      in_single = true
      sb.write_char(ch)
      i = i + 1
      continue
    }
    if ch == '"' {
      in_double = true
      sb.write_char(ch)
      i = i + 1
      continue
    }
    if ch == '0' && i + 1 < len && (chars[i + 1] == 'o' || chars[i + 1] == 'O') {
      let mut j = i + 2
      if j < len && is_octal_digit(chars[j]) {
        let mut value = 0L
        while j < len && is_octal_digit(chars[j]) {
          let digit = Int64::from_int(chars[j].to_int() - '0'.to_int())
          value = value * 8L + digit
          j = j + 1
        }
        sb.write_string(Int64::to_string(value))
        i = j
        continue
      }
    }
    sb.write_char(ch)
    i = i + 1
  }
  sb.to_string()
}

///|
fn json_to_value(value : Json) -> Value {
  match value {
    Json::Null => Null
    Json::True => Bool(true)
    Json::False => Bool(false)
    Json::Number(num, ..) => Number(num)
    Json::String(text) => String(text)
    Json::Array(items) => {
      let elements : Array[Value?] = []
      for item in items {
        elements.push(Some(json_to_value(item)))
      }
      new_array_value(elements)
    }
    Json::Object(entries) => {
      let obj = new_object_value()
      match obj {
        Object(object) =>
          for key, entry in entries {
            object.props.set(key, property_data(json_to_value(entry)))
          }
        _ => ()
      }
      obj
    }
  }
}

///|
fn json_parse_error_pos(err : @json.ParseError) -> (Int, Int) {
  match err {
    @json.ParseError::InvalidChar(pos, _) => (pos.line, pos.column)
    @json.ParseError::InvalidNumber(pos, _) => (pos.line, pos.column)
    @json.ParseError::InvalidIdentEscape(pos) => (pos.line, pos.column)
    @json.ParseError::InvalidEof => (1, 0)
    @json.ParseError::DepthLimitExceeded => (1, 0)
  }
}

///|
fn is_json_whitespace(code : Int) -> Bool {
  code == 0x0009 || code == 0x000A || code == 0x000D || code == 0x0020
}

///|
fn json_throw_syntax_error_at(
  text : String,
  index : Int,
  message : String,
) -> Unit raise {
  let (line, column) = offset_to_line_column(text, index)
  raise ThrowSignal(
    new_error_value_with_pos(syntax_error_proto(), message, line, column + 1),
  )
}

///|
fn json_validate_whitespace(text : String) -> Unit raise {
  let len = text.length()
  let mut i = 0
  let mut in_string = false
  let mut escaped = false
  let quote = Char::to_int('"')
  let backslash = Char::to_int('\\')
  while i < len {
    let unit = UInt16::to_int(text.code_unit_at(i))
    if in_string {
      if escaped {
        escaped = false
      } else if unit == backslash {
        escaped = true
      } else if unit == quote {
        in_string = false
      }
    } else if unit == quote {
      in_string = true
    } else if is_js_whitespace(unit) && !is_json_whitespace(unit) {
      json_throw_syntax_error_at(text, i, "invalid JSON whitespace")
    }
    i = i + 1
  }
}

///|
fn json_number_negative_zero_flags(text : String) -> Array[Bool] {
  let flags : Array[Bool] = []
  let len = text.length()
  let mut i = 0
  let mut in_string = false
  let mut escaped = false
  let quote = Char::to_int('"')
  let backslash = Char::to_int('\\')
  let minus = Char::to_int('-')
  let dot = Char::to_int('.')
  let zero = Char::to_int('0')
  let nine = Char::to_int('9')
  let lower_e = Char::to_int('e')
  let upper_e = Char::to_int('E')
  let plus = Char::to_int('+')
  while i < len {
    let unit = UInt16::to_int(text.code_unit_at(i))
    if in_string {
      if escaped {
        escaped = false
      } else if unit == backslash {
        escaped = true
      } else if unit == quote {
        in_string = false
      }
      i = i + 1
      continue
    }
    if unit == quote {
      in_string = true
      i = i + 1
      continue
    }
    if unit == minus || (unit >= zero && unit <= nine) {
      let mut negative = false
      let mut has_non_zero = false
      if unit == minus {
        negative = true
        i = i + 1
      }
      if i >= len {
        flags.push(false)
        break
      }
      let int_unit = UInt16::to_int(text.code_unit_at(i))
      if int_unit == zero {
        i = i + 1
      } else {
        let mut digit_unit = int_unit
        while digit_unit >= zero && digit_unit <= nine {
          if digit_unit != zero {
            has_non_zero = true
          }
          i = i + 1
          if i >= len {
            break
          }
          digit_unit = UInt16::to_int(text.code_unit_at(i))
        }
      }
      if i < len && UInt16::to_int(text.code_unit_at(i)) == dot {
        i = i + 1
        while i < len {
          let frac_unit = UInt16::to_int(text.code_unit_at(i))
          if frac_unit < zero || frac_unit > nine {
            break
          }
          if frac_unit != zero {
            has_non_zero = true
          }
          i = i + 1
        }
      }
      if i < len {
        let exp_unit = UInt16::to_int(text.code_unit_at(i))
        if exp_unit == lower_e || exp_unit == upper_e {
          i = i + 1
          if i < len {
            let sign_unit = UInt16::to_int(text.code_unit_at(i))
            if sign_unit == plus || sign_unit == minus {
              i = i + 1
            }
          }
          while i < len {
            let exp_digit = UInt16::to_int(text.code_unit_at(i))
            if exp_digit < zero || exp_digit > nine {
              break
            }
            i = i + 1
          }
        }
      }
      flags.push(negative && !has_non_zero)
      continue
    }
    i = i + 1
  }
  flags
}

///|
fn json_to_value_with_number_flags(
  value : Json,
  flags : Array[Bool],
  index : Int,
) -> (Value, Int) {
  match value {
    Json::Null => (Null, index)
    Json::True => (Bool(true), index)
    Json::False => (Bool(false), index)
    Json::Number(num, ..) => {
      let use_negative_zero = index < flags.length() && flags[index]
      let number_value = if use_negative_zero { negative_zero() } else { num }
      (Number(number_value), index + 1)
    }
    Json::String(text) => (String(text), index)
    Json::Array(items) => {
      let elements : Array[Value?] = []
      let mut current = index
      for item in items {
        let (value_item, next) = json_to_value_with_number_flags(
          item, flags, current,
        )
        current = next
        elements.push(Some(value_item))
      }
      (new_array_value(elements), current)
    }
    Json::Object(entries) => {
      let obj = new_object_value()
      let mut current = index
      match obj {
        Object(object) =>
          for key, entry in entries {
            let (value_entry, next) = json_to_value_with_number_flags(
              entry, flags, current,
            )
            current = next
            object.props.set(key, property_data(value_entry))
          }
        _ => ()
      }
      (obj, current)
    }
  }
}

///|
fn json_parse_text(text : String) -> Value raise {
  json_validate_whitespace(text)
  let json = @json.parse(text[:]) catch {
    err => {
      let (line, column) = json_parse_error_pos(err)
      raise ThrowSignal(
        new_error_value_with_pos(
          syntax_error_proto(),
          err.to_string(),
          line,
          column + 1,
        ),
      )
      Json::null()
    }
  }
  let flags = json_number_negative_zero_flags(text)
  let (value, _) = json_to_value_with_number_flags(json, flags, 0)
  value
}

///|
fn json_internalize_property(
  holder : Value,
  name : String,
  reviver : Value,
) -> Value raise {
  let val = property_get(holder, name)
  if is_object_like(val) {
    let is_array = is_array_value(val)
    if is_array {
      let len_value = property_get(val, "length")
      let len = to_length_int64(to_number(len_value))
      let mut i : Int64 = 0
      while i < len {
        let key = Int64::to_string(i)
        let new_element = json_internalize_property(val, key, reviver)
        if new_element is Undefined {
          let _ = delete_property_value(val, key)

        } else {
          let _ = create_data_property(val, key, new_element)

        }
        i = i + 1
      }
    } else {
      let keys = own_enumerable_string_keys(val)
      for key in keys {
        let new_element = json_internalize_property(val, key, reviver)
        if new_element is Undefined {
          let _ = delete_property_value(val, key)

        } else {
          let _ = create_data_property(val, key, new_element)

        }
      }
    }
  }
  call_value_with_this(reviver, [String(name), val], holder)
}

///|
fn json_parse_with_reviver(text_value : Value, reviver : Value) -> Value raise {
  let text = to_string_strict(text_value)
  let parsed = json_parse_text(text)
  if is_callable(reviver) {
    let root = new_object_value()
    create_data_property_or_throw(root, "", parsed)
    return json_internalize_property(root, "", reviver)
  }
  parsed
}

///|
fn json_parse_value(text : String) -> Value raise {
  json_parse_text(text)
}
