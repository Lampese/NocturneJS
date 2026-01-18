///|
fn is_line_terminator(code : Int) -> Bool {
  code == 10 || code == 13 || code == 0x2028 || code == 0x2029
}

///|
fn is_word_char(code : Int) -> Bool {
  (code >= 48 && code <= 57) ||
  (code >= 65 && code <= 90) ||
  (code >= 97 && code <= 122) ||
  code == 95
}

///|
fn is_word_char_for_flags(code : Int, flags : RegexpFlags) -> Bool {
  if code < 256 {
    return is_word_char(code)
  }
  if flags.ignore_case && (flags.unicode || flags.unicode_sets) {
    return code == 0x017F || code == 0x212A
  }
  false
}

///|
fn hex_digit_value(code : Int) -> Int? {
  if code >= 48 && code <= 57 {
    Some(code - 48)
  } else if code >= 65 && code <= 70 {
    Some(code - 55)
  } else if code >= 97 && code <= 102 {
    Some(code - 87)
  } else {
    None
  }
}

///|
fn parse_hex(p : RegexpParser, count : Int) -> Int? {
  let mut value = 0
  let mut i = 0
  while i < count {
    match parser_next(p) {
      Some(code) =>
        match hex_digit_value(code) {
          Some(digit) => value = value * 16 + digit
          None => return None
        }
      None => return None
    }
    i = i + 1
  }
  Some(value)
}

///|
fn parse_decimal(p : RegexpParser) -> Int? {
  let max_int = 2147483647
  let max_int64 = Int64::from_int(max_int)
  let mut value = 0L
  let mut found = false
  while true {
    match parser_peek(p) {
      Some(code) =>
        if code >= 48 && code <= 57 {
          let _ = parser_next(p)
          if value < max_int64 {
            value = value * 10L + Int64::from_int(code - 48)
            if value > max_int64 {
              value = max_int64
            }
          }
          found = true
          continue
        }
      None => ()
    }
    break
  }
  if found {
    Some(Int64::to_int(value))
  } else {
    None
  }
}

///|
fn class_set_digits() -> ClassSet {
  class_set_range(48, 57)
}

///|
fn class_set_word() -> ClassSet {
  let ranges = [
    Range::{ start: 48, end: 57 },
    Range::{ start: 65, end: 90 },
    Range::{ start: 95, end: 95 },
    Range::{ start: 97, end: 122 },
  ]
  ClassSet::{ ranges, strings: [] }
}

///|
fn ranges_equal(a : Array[Range], b : Array[Range]) -> Bool {
  if a.length() != b.length() {
    return false
  }
  let aa = ranges_sorted(a)
  let bb = ranges_sorted(b)
  let mut i = 0
  while i < aa.length() {
    if aa[i].start != bb[i].start || aa[i].end != bb[i].end {
      return false
    }
    i = i + 1
  }
  true
}

///|
fn class_set_is_word_complement(set : ClassSet) -> Bool {
  if !set.strings.is_empty() {
    return false
  }
  let complement = class_set_invert(class_set_word())
  ranges_equal(set.ranges, complement.ranges)
}

///|
fn class_set_space() -> ClassSet {
  let ranges = [
    Range::{ start: 9, end: 13 },
    Range::{ start: 32, end: 32 },
    Range::{ start: 0x00A0, end: 0x00A0 },
    Range::{ start: 0x1680, end: 0x1680 },
    Range::{ start: 0x2000, end: 0x200A },
    Range::{ start: 0x2028, end: 0x2029 },
    Range::{ start: 0x202F, end: 0x202F },
    Range::{ start: 0x205F, end: 0x205F },
    Range::{ start: 0x3000, end: 0x3000 },
    Range::{ start: 0xFEFF, end: 0xFEFF },
  ]
  ClassSet::{ ranges, strings: [] }
}

///|
fn parse_unicode_property(p : RegexpParser, inverted : Bool) -> ClassSet raise {
  parser_expect(p, 123)
  let mut name = ""
  let mut value = ""
  let mut seen_equal = false
  let sb = StringBuilder::new()
  while true {
    match parser_peek(p) {
      Some(code) =>
        if code == 125 {
          let _ = parser_next(p)
          if seen_equal {
            value = sb.to_string()
          } else {
            name = sb.to_string()
          }
          break
        } else if code == 61 {
          let _ = parser_next(p)
          name = sb.to_string()
          sb.reset()
          seen_equal = true
        } else if (code >= 48 && code <= 57) ||
          (code >= 65 && code <= 90) ||
          (code >= 97 && code <= 122) ||
          code == 95 {
          let _ = parser_next(p)
          sb.write_char(Int::unsafe_to_char(code))
        } else {
          fail("regexp parse error")
        }
      None => fail("regexp parse error")
    }
  }
  let mut result : ClassSet? = None
  if name == "Script" || name == "sc" {
    result = unicode_script_set(value, false)
  } else if name == "Script_Extensions" || name == "scx" {
    result = unicode_script_set(value, true)
  } else if name == "General_Category" || name == "gc" {
    result = unicode_general_category_set(value)
  } else if value == "" {
    result = unicode_general_category_set(name)
    if result is None {
      result = unicode_prop_set(name)
    }
    if result is None && !inverted && p.flags.unicode_sets {
      result = unicode_sequence_prop_set(name)
    }
  } else {
    fail("regexp parse error")
  }
  match result {
    Some(value) => {
      let mut set = value
      if p.flags.ignore_case && p.flags.unicode_sets {
        set = class_set_canonicalize(
          set,
          p.flags.unicode || p.flags.unicode_sets,
        )
      }
      if inverted {
        set = class_set_invert(set)
      }
      if p.flags.ignore_case && !p.flags.unicode_sets {
        set = class_set_canonicalize(
          set,
          p.flags.unicode || p.flags.unicode_sets,
        )
      }
      class_set_sort_strings(set)
    }
    None => fail("regexp parse error")
  }
}

///|
fn is_ident_start_char(ch : Char) -> Bool {
  if ch.is_ascii() {
    ch.is_ascii_alphabetic() || ch == '_' || ch == '$'
  } else {
    @engine.unicode_is_ident_start(ch.to_int())
  }
}

///|
fn is_ident_continue_char(ch : Char) -> Bool {
  if ch == '\u200C' || ch == '\u200D' {
    return true
  }
  if ch.is_ascii() {
    is_ident_start_char(ch) || ch.is_ascii_digit()
  } else {
    @engine.unicode_is_ident_continue(ch.to_int())
  }
}

///|
fn parse_group_name(p : RegexpParser) -> String raise {
  let sb = StringBuilder::new()
  let mut first = true
  while true {
    match parser_peek(p) {
      Some(code) =>
        if code == 62 { // '>'
          let _ = parser_next(p)
          break
        } else {
          let mut from_escape = false
          let mut value = if code == 92 { // '\'
            from_escape = true
            let _ = parser_next(p)
            match parser_peek(p) {
              Some(next) =>
                if next != 117 { // 'u'
                  fail("regexp parse error")
                } else {
                  match parse_escape_char(p, true) {
                    Some(escaped) => escaped
                    None => fail("regexp parse error")
                  }
                }
              None => fail("regexp parse error")
            }
          } else {
            let _ = parser_next(p)
            code
          }
          if is_lead_surrogate(value) {
            if from_escape {
              let saved_pos = p.pos
              match parser_peek(p) {
                Some(next) =>
                  if next == 92 { // '\'
                    let _ = parser_next(p)
                    match parser_peek(p) {
                      Some(marker) =>
                        if marker == 117 { // 'u'
                          match parse_escape_char(p, true) {
                            Some(low) =>
                              if is_trail_surrogate(low) {
                                let high = value - 0xD800
                                let low_part = low - 0xDC00
                                value = 0x10000 + (high << 10) + low_part
                              } else {
                                p.pos = saved_pos
                              }
                            None => p.pos = saved_pos
                          }
                        } else {
                          p.pos = saved_pos
                        }
                      None => p.pos = saved_pos
                    }
                  }
                None => ()
              }
            } else {
              match parser_peek(p) {
                Some(next) =>
                  if is_trail_surrogate(next) {
                    let _ = parser_next(p)
                    let high = value - 0xD800
                    let low_part = next - 0xDC00
                    value = 0x10000 + (high << 10) + low_part
                  }
                None => ()
              }
            }
          }
          if value > 0x10ffff {
            fail("regexp parse error")
          }
          let ch = Int::unsafe_to_char(value)
          if first {
            if !is_ident_start_char(ch) {
              fail("regexp parse error")
            }
          } else if !is_ident_continue_char(ch) {
            fail("regexp parse error")
          }
          sb.write_char(ch)
          first = false
        }
      None => fail("regexp parse error")
    }
  }
  if first {
    fail("regexp parse error")
  }
  sb.to_string()
}

///|
fn is_duplicate_group_name(p : RegexpParser, name : String) -> Bool {
  match p.name_scopes.get(name) {
    Some(scope) => scope == p.group_name_scope
    None => false
  }
}

///|
fn register_group_name(
  p : RegexpParser,
  name : String,
  capture_index : Int,
) -> Unit {
  p.name_scopes.set(name, p.group_name_scope)
  let indices = match p.name_indices.get(name) {
    Some(existing) => existing
    None => []
  }
  indices.push(capture_index)
  p.name_indices.set(name, indices)
}
