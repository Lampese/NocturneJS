///|
priv struct RegexpParser {
  pattern : String
  mut pos : Int
  flags : RegexpFlags
  mut group_name_scope : Int
  mut capture_count : Int
  capture_names : Array[String?]
  name_scopes : Map[String, Int]
  name_indices : Map[String, Array[Int]]
  total_capture_count : Int
  has_named_capture_hint : Bool
}

///|
fn parser_new(pattern : String, flags : RegexpFlags) -> RegexpParser {
  let (total_capture_count, has_named_capture_hint) = scan_capture_info(pattern)
  RegexpParser::{
    pattern,
    pos: 0,
    flags,
    group_name_scope: 0,
    capture_count: 0,
    capture_names: [],
    name_scopes: Map::new(),
    name_indices: Map::new(),
    total_capture_count,
    has_named_capture_hint,
  }
}

///|
fn parser_len(p : RegexpParser) -> Int {
  p.pattern.length()
}

///|
fn parser_peek(p : RegexpParser) -> Int? {
  if p.pos >= parser_len(p) {
    None
  } else {
    Some(UInt16::to_int(p.pattern.code_unit_at(p.pos)))
  }
}

///|
fn parser_next(p : RegexpParser) -> Int? {
  match parser_peek(p) {
    Some(code) => {
      p.pos = p.pos + 1
      Some(code)
    }
    None => None
  }
}

///|
fn parser_match(p : RegexpParser, code : Int) -> Bool {
  match parser_peek(p) {
    Some(value) =>
      if value == code {
        p.pos = p.pos + 1
        true
      } else {
        false
      }
    None => false
  }
}

///|
fn parser_expect(p : RegexpParser, code : Int) -> Unit raise {
  if !parser_match(p, code) {
    fail("regexp parse error")
  }
}

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
fn scan_capture_info(pattern : String) -> (Int, Bool) {
  let mut count = 0
  let mut has_named = false
  let len = pattern.length()
  let mut i = 0
  while i < len {
    let code = UInt16::to_int(pattern.code_unit_at(i))
    if code == 92 { // '\'
      i = i + 2
      continue
    }
    if code == 91 { // '['
      i = i + 1
      while i < len {
        let inner = UInt16::to_int(pattern.code_unit_at(i))
        if inner == 92 { // '\'
          i = i + 2
          continue
        }
        if inner == 93 { // ']'
          i = i + 1
          break
        }
        i = i + 1
      }
      continue
    }
    if code == 40 { // '('
      if i + 1 < len && UInt16::to_int(pattern.code_unit_at(i + 1)) == 63 { // '?'
        if i + 2 < len && UInt16::to_int(pattern.code_unit_at(i + 2)) == 60 { // '<'
          if i + 3 < len {
            let next = UInt16::to_int(pattern.code_unit_at(i + 3))
            if next != 61 && next != 33 { // '=' or '!'
              count = count + 1
              has_named = true
            }
          }
        }
      } else {
        count = count + 1
      }
    }
    i = i + 1
  }
  (count, has_named)
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

///|
fn parse_legacy_octal_escape(p : RegexpParser) -> Int raise {
  let first = match parser_peek(p) {
    Some(code) if code >= 48 && code <= 55 => code
    Some(code) => {
      let _ = parser_next(p)
      return code
    }
    None => fail("regexp parse error")
  }
  let _ = parser_next(p)
  let first_digit = first - 48
  match parser_peek(p) {
    Some(second) if second >= 48 && second <= 55 => {
      let _ = parser_next(p)
      let second_digit = second - 48
      if first_digit <= 3 {
        match parser_peek(p) {
          Some(third) if third >= 48 && third <= 55 => {
            let _ = parser_next(p)
            first_digit * 64 + second_digit * 8 + (third - 48)
          }
          _ => first_digit * 8 + second_digit
        }
      } else {
        first_digit * 8 + second_digit
      }
    }
    _ => first_digit
  }
}

///|
fn is_syntax_identity_escape(code : Int) -> Bool {
  match code {
    94 | 36 | 92 | 46 | 42 | 43 | 63 | 40 | 41 | 91 | 93 | 123 | 125 | 124 =>
      true
    _ => false
  }
}

///|
fn is_identity_escape_outside_unicode(code : Int) -> Bool {
  is_syntax_identity_escape(code) || code == 47
}

///|
fn is_identity_escape_in_class_unicode(code : Int) -> Bool {
  is_identity_escape_outside_unicode(code) || code == 45
}

///|
priv enum ClassAtom {
  Char(Int)
  Set(ClassSet)
}

///|
fn parse_escape_char(p : RegexpParser, allow_utf16 : Bool) -> Int? {
  match parser_next(p) {
    Some(code) =>
      match code {
        110 => Some(10) // 'n'
        114 => Some(13) // 'r'
        116 => Some(9) // 't'
        118 => Some(11) // 'v'
        102 => Some(12) // 'f'
        98 => Some(8) // 'b'
        48 | 49 | 50 | 51 | 52 | 53 | 54 | 55 =>
          if allow_utf16 {
            if code != 48 {
              None
            } else {
              match parser_peek(p) {
                Some(next) =>
                  if next >= 48 && next <= 57 {
                    None
                  } else {
                    Some(0)
                  }
                None => Some(0)
              }
            }
          } else {
            Some(code - 48)
          }
        120 =>
          match parse_hex(p, 2) {
            Some(value) => Some(value)
            None => None
          }
        117 =>
          match parser_peek(p) {
            Some(next) =>
              if next == 123 {
                if !allow_utf16 {
                  return None
                }
                let _ = parser_next(p)
                let mut value = 0L
                let mut found = false
                while true {
                  match parser_peek(p) {
                    Some(ch) =>
                      if ch == 125 {
                        let _ = parser_next(p)
                        break
                      } else {
                        match hex_digit_value(ch) {
                          Some(digit) => {
                            let _ = parser_next(p)
                            value = value * 16L + Int64::from_int(digit)
                            found = true
                            if value > 0x10ffffL {
                              return None
                            }
                          }
                          None => return None
                        }
                      }
                    None => return None
                  }
                }
                if !found {
                  None
                } else {
                  Some(Int64::to_int(value))
                }
              } else {
                match parse_hex(p, 4) {
                  Some(value) =>
                    if allow_utf16 && is_lead_surrogate(value) {
                      let pos = p.pos
                      let len = parser_len(p)
                      if pos + 5 < len {
                        let c0 = UInt16::to_int(p.pattern.code_unit_at(pos))
                        let c1 = UInt16::to_int(p.pattern.code_unit_at(pos + 1))
                        if c0 == 92 && c1 == 117 { // '\u'
                          let mut low = 0
                          let mut ok = true
                          let mut i = 0
                          while i < 4 {
                            let code = UInt16::to_int(
                              p.pattern.code_unit_at(pos + 2 + i),
                            )
                            match hex_digit_value(code) {
                              Some(digit) => low = low * 16 + digit
                              None => {
                                ok = false
                                break
                              }
                            }
                            i = i + 1
                          }
                          if ok && is_trail_surrogate(low) {
                            let high = value - 0xD800
                            let low_part = low - 0xDC00
                            p.pos = pos + 6
                            Some(0x10000 + (high << 10) + low_part)
                          } else {
                            Some(value)
                          }
                        } else {
                          Some(value)
                        }
                      } else {
                        Some(value)
                      }
                    } else {
                      Some(value)
                    }
                  None => None
                }
              }
            None => None
          }
        _ => None
      }
    None => None
  }
}

///|
fn parse_escape_outside(p : RegexpParser) -> RegexNode raise {
  match parser_next(p) {
    Some(code) =>
      match code {
        98 => WordBoundary(true) // 'b'
        66 => WordBoundary(false) // 'B'
        100 => CharClass(class_set_digits()) // 'd'
        68 => CharClass(class_set_invert(class_set_digits())) // 'D'
        115 => CharClass(class_set_space()) // 's'
        83 => CharClass(class_set_invert(class_set_space())) // 'S'
        119 => CharClass(class_set_word()) // 'w'
        87 => CharClass(class_set_invert(class_set_word())) // 'W'
        110 => Literal(10) // 'n'
        114 => Literal(13) // 'r'
        116 => Literal(9) // 't'
        118 => Literal(11) // 'v'
        102 => Literal(12) // 'f'
        112 | 80 =>
          if p.flags.unicode || p.flags.unicode_sets {
            let inverted = code == 80
            let set = parse_unicode_property(p, inverted)
            CharClass(set)
          } else {
            Literal(code)
          }
        107 => // 'k'
          match parser_peek(p) {
            Some(next) =>
              if next == 60 { // '<'
                if p.flags.unicode || p.has_named_capture_hint {
                  let _ = parser_next(p)
                  let name = parse_group_name(p)
                  NamedBackReference(name)
                } else {
                  Literal(107)
                }
              } else if p.flags.unicode || p.has_named_capture_hint {
                fail("regexp parse error")
              } else {
                Literal(107)
              }
            None =>
              if p.flags.unicode || p.has_named_capture_hint {
                fail("regexp parse error")
              } else {
                Literal(107)
              }
          }
        113 => // 'q'
          if p.flags.unicode || p.flags.unicode_sets {
            fail("regexp parse error")
          } else {
            Literal(113)
          }
        99 =>
          match parser_peek(p) {
            Some(next) =>
              if (next >= 65 && next <= 90) || (next >= 97 && next <= 122) {
                let _ = parser_next(p)
                let upper = if next >= 97 && next <= 122 {
                  next - 32
                } else {
                  next
                }
                Literal(upper & 0x1f)
              } else if p.flags.unicode {
                fail("regexp parse error")
              } else {
                p.pos = p.pos - 1
                Literal(92)
              }
            None =>
              if p.flags.unicode {
                fail("regexp parse error")
              } else {
                Literal(92)
              }
          }
        48 =>
          if p.flags.unicode {
            match parser_peek(p) {
              Some(next) =>
                if next >= 48 && next <= 57 {
                  fail("regexp parse error")
                } else {
                  Literal(0)
                }
              None => Literal(0)
            }
          } else {
            let mut value = 0
            let mut digits = 1
            while digits < 3 {
              match parser_peek(p) {
                Some(next) =>
                  if next >= 48 && next <= 55 {
                    let _ = parser_next(p)
                    value = value * 8 + (next - 48)
                    digits = digits + 1
                    continue
                  }
                None => ()
              }
              break
            }
            Literal(value)
          }
        49 | 50 | 51 | 52 | 53 | 54 | 55 | 56 | 57 => {
          let start_pos = p.pos - 1
          let mut value = code - 48
          while true {
            match parser_peek(p) {
              Some(next) =>
                if next >= 48 && next <= 57 {
                  let _ = parser_next(p)
                  value = value * 10 + (next - 48)
                  continue
                }
              None => ()
            }
            break
          }
          if value <= p.total_capture_count {
            NumericBackReference(value)
          } else if p.flags.unicode {
            fail("regexp parse error")
          } else {
            p.pos = start_pos
            let legacy = parse_legacy_octal_escape(p)
            Literal(legacy)
          }
        }
        120 | 117 => {
          p.pos = p.pos - 1
          match parse_escape_char(p, p.flags.unicode) {
            Some(value) => Literal(value)
            None =>
              if p.flags.unicode {
                fail("regexp parse error")
              } else {
                Literal(code)
              }
          }
        }
        _ =>
          if p.flags.unicode || p.flags.unicode_sets {
            if is_identity_escape_outside_unicode(code) {
              Literal(code)
            } else {
              fail("regexp parse error")
            }
          } else {
            Literal(code)
          }
      }
    None => fail("regexp parse error")
  }
}

///|
fn parse_class_atom_char_only(p : RegexpParser) -> Int raise {
  match parser_next(p) {
    Some(code) =>
      if code == 92 {
        match parser_next(p) {
          Some(next) =>
            match next {
              48 | 49 | 50 | 51 | 52 | 53 | 54 | 55 | 56 | 57 =>
                if p.flags.unicode || p.flags.unicode_sets {
                  p.pos = p.pos - 1
                  match parse_escape_char(p, p.flags.unicode) {
                    Some(value) => value
                    None => fail("regexp parse error")
                  }
                } else {
                  p.pos = p.pos - 1
                  parse_legacy_octal_escape(p)
                }
              99 => // 'c'
                match parser_peek(p) {
                  Some(ch) =>
                    if (ch >= 65 && ch <= 90) || (ch >= 97 && ch <= 122) {
                      let _ = parser_next(p)
                      let upper = if ch >= 97 && ch <= 122 {
                        ch - 32
                      } else {
                        ch
                      }
                      upper & 0x1f
                    } else if p.flags.unicode {
                      fail("regexp parse error")
                    } else {
                      p.pos = p.pos - 1
                      92
                    }
                  None =>
                    if p.flags.unicode {
                      fail("regexp parse error")
                    } else {
                      92
                    }
                }
              120 | 117 => {
                p.pos = p.pos - 1
                match parse_escape_char(p, p.flags.unicode) {
                  Some(value) => value
                  None =>
                    if p.flags.unicode {
                      fail("regexp parse error")
                    } else {
                      next
                    }
                }
              }
              _ =>
                if (p.flags.unicode || p.flags.unicode_sets) &&
                  is_identity_escape_in_class_unicode(next) {
                  next
                } else {
                  p.pos = p.pos - 1
                  match parse_escape_char(p, p.flags.unicode) {
                    Some(value) => value
                    None =>
                      if p.flags.unicode {
                        fail("regexp parse error")
                      } else {
                        next
                      }
                  }
                }
            }
          None => fail("regexp parse error")
        }
      } else {
        let literal = parse_class_literal_codepoint(p, code)
        if p.flags.unicode_sets {
          if is_invalid_unicode_sets_class_char(literal) {
            fail("regexp parse error")
          }
          if is_unicode_sets_double_op_char(literal) &&
            parser_peek(p) == Some(literal) {
            fail("regexp parse error")
          }
        }
        literal
      }
    None => fail("regexp parse error")
  }
}

///|
fn is_invalid_unicode_sets_class_char(code : Int) -> Bool {
  match code {
    40 | 41 | 91 | 93 | 123 | 125 | 47 | 45 | 124 => true
    _ => false
  }
}

///|
fn is_unicode_sets_double_op_char(code : Int) -> Bool {
  match code {
    38
    | 33
    | 35
    | 36
    | 37
    | 42
    | 43
    | 44
    | 46
    | 58
    | 59
    | 60
    | 61
    | 62
    | 63
    | 64
    | 94
    | 96
    | 126 => true
    _ => false
  }
}

///|
fn parse_class_string_disjunction(p : RegexpParser) -> ClassSet raise {
  parser_expect(p, 123)
  let strings : Array[String] = []
  let sb = StringBuilder::new()
  while true {
    sb.reset()
    while true {
      match parser_peek(p) {
        Some(code) =>
          if code == 125 || code == 124 {
            break
          } else {
            let ch = parse_class_atom_char_only(p)
            sb.write_char(Int::unsafe_to_char(ch))
          }
        None => fail("regexp parse error")
      }
    }
    let text = sb.to_string()
    strings.push(text)
    match parser_peek(p) {
      Some(code) =>
        if code == 125 {
          let _ = parser_next(p)
          break
        } else if code == 124 {
          let _ = parser_next(p)

        } else {
          fail("regexp parse error")
        }
      None => fail("regexp parse error")
    }
  }
  let mut set = ClassSet::{ ranges: [], strings }
  if p.flags.ignore_case {
    set = class_set_canonicalize(set, p.flags.unicode || p.flags.unicode_sets)
  }
  class_set_sort_strings(set)
}

///|
fn parse_class_atom(p : RegexpParser, in_class : Bool) -> ClassAtom raise {
  match parser_next(p) {
    Some(code) =>
      if code == 92 {
        match parser_next(p) {
          Some(next) =>
            match next {
              48 | 49 | 50 | 51 | 52 | 53 | 54 | 55 | 56 | 57 =>
                if p.flags.unicode || p.flags.unicode_sets {
                  p.pos = p.pos - 1
                  match parse_escape_char(p, p.flags.unicode) {
                    Some(value) => Char(value)
                    None => fail("regexp parse error")
                  }
                } else {
                  p.pos = p.pos - 1
                  Char(parse_legacy_octal_escape(p))
                }
              100 => Set(class_set_digits())
              68 => Set(class_set_invert(class_set_digits()))
              115 => Set(class_set_space())
              83 => Set(class_set_invert(class_set_space()))
              119 => Set(class_set_word())
              87 => Set(class_set_invert(class_set_word()))
              112 | 80 =>
                if p.flags.unicode || p.flags.unicode_sets {
                  let inverted = next == 80
                  let set = parse_unicode_property(p, inverted)
                  Set(set)
                } else {
                  Char(next)
                }
              113 =>
                if p.flags.unicode_sets && in_class {
                  let set = parse_class_string_disjunction(p)
                  Set(set)
                } else if p.flags.unicode {
                  fail("regexp parse error")
                } else {
                  Char(next)
                }
              99 =>
                match parser_peek(p) {
                  Some(ch) =>
                    if (ch >= 65 && ch <= 90) ||
                      (ch >= 97 && ch <= 122) ||
                      (
                        (!p.flags.unicode && in_class) &&
                        ((ch >= 48 && ch <= 57) || ch == 95)
                      ) {
                      let _ = parser_next(p)
                      let upper = if ch >= 97 && ch <= 122 {
                        ch - 32
                      } else {
                        ch
                      }
                      Char(upper & 0x1f)
                    } else if p.flags.unicode {
                      fail("regexp parse error")
                    } else {
                      p.pos = p.pos - 1
                      Char(92)
                    }
                  None =>
                    if p.flags.unicode {
                      fail("regexp parse error")
                    } else {
                      Char(92)
                    }
                }
              45 =>
                if !in_class && p.flags.unicode {
                  fail("regexp parse error")
                } else {
                  Char(45)
                }
              98 => Char(8)
              120 | 117 => {
                p.pos = p.pos - 1
                match parse_escape_char(p, p.flags.unicode) {
                  Some(value) => Char(value)
                  None =>
                    if p.flags.unicode {
                      fail("regexp parse error")
                    } else {
                      Char(next)
                    }
                }
              }
              _ =>
                if (p.flags.unicode || p.flags.unicode_sets) &&
                  is_identity_escape_in_class_unicode(next) {
                  Char(next)
                } else {
                  p.pos = p.pos - 1
                  match parse_escape_char(p, p.flags.unicode) {
                    Some(value) => Char(value)
                    None =>
                      if p.flags.unicode {
                        fail("regexp parse error")
                      } else {
                        Char(next)
                      }
                  }
                }
            }
          None => fail("regexp parse error")
        }
      } else {
        let literal = parse_class_literal_codepoint(p, code)
        if p.flags.unicode_sets && in_class {
          if is_invalid_unicode_sets_class_char(literal) {
            fail("regexp parse error")
          }
          if is_unicode_sets_double_op_char(literal) &&
            parser_peek(p) == Some(literal) {
            fail("regexp parse error")
          }
        }
        if literal == 0 {
          Char(0)
        } else {
          Char(literal)
        }
      }
    None => fail("regexp parse error")
  }
}

///|
fn parse_class_literal_codepoint(p : RegexpParser, code : Int) -> Int {
  if (p.flags.unicode || p.flags.unicode_sets) && is_lead_surrogate(code) {
    match parser_peek(p) {
      Some(next) if is_trail_surrogate(next) => {
        let _ = parser_next(p)
        let high = code - 0xD800
        let low = next - 0xDC00
        0x10000 + (high << 10) + low
      }
      _ => code
    }
  } else {
    code
  }
}

///|
fn parse_nested_class(p : RegexpParser) -> ClassSet raise {
  let mut invert = false
  if parser_match(p, 94) {
    invert = true
  }
  let mut set = class_set_empty()
  let mut first = true
  while true {
    match parser_peek(p) {
      Some(code) =>
        if code == 93 { // ']'
          let _ = parser_next(p)
          break
        } else {
          let mut atom_set = class_set_empty()
          let mut is_char = false
          let mut char_value = 0
          let unicode_range = p.flags.unicode || p.flags.unicode_sets
          if p.flags.unicode_sets && code == 91 {
            let _ = parser_next(p)
            atom_set = parse_nested_class(p)
          } else {
            match parse_class_atom(p, true) {
              Char(value) => {
                is_char = true
                char_value = value
              }
              Set(value) => atom_set = value
            }
          }
          if is_char {
            if p.flags.ignore_case && p.flags.unicode_sets {
              char_value = ascii_fold(char_value)
            }
            if parser_peek(p) == Some(45) { // '-'
              let saved_pos = p.pos
              let _ = parser_next(p)
              match parser_peek(p) {
                Some(next) =>
                  if next != 93 {
                    if p.flags.unicode_sets && first && next == 45 {
                      p.pos = saved_pos
                      atom_set = class_set_single(char_value)
                      is_char = false
                    } else {
                      match parse_class_atom(p, true) {
                        Char(end_value) => {
                          let end_folded = if p.flags.ignore_case &&
                            p.flags.unicode_sets {
                            ascii_fold(end_value)
                          } else {
                            end_value
                          }
                          if end_folded < char_value {
                            fail("regexp parse error")
                          }
                          atom_set = class_set_range(char_value, end_folded)
                          is_char = false
                        }
                        Set(_) => {
                          if unicode_range {
                            fail("regexp parse error")
                          }
                          p.pos = saved_pos
                          atom_set = class_set_single(char_value)
                          is_char = false
                        }
                      }
                    }
                  } else {
                    p.pos = saved_pos
                    atom_set = class_set_single(char_value)
                    is_char = false
                  }
                None => {
                  p.pos = saved_pos
                  atom_set = class_set_single(char_value)
                  is_char = false
                }
              }
            } else {
              atom_set = class_set_single(char_value)
              is_char = false
            }
          } else if parser_peek(p) == Some(45) { // '-'
            let saved_pos = p.pos
            let _ = parser_next(p)
            match parser_peek(p) {
              Some(next) =>
                if next != 93 &&
                  !(p.flags.unicode_sets && first && next == 45) &&
                  unicode_range {
                  fail("regexp parse error")
                }
              None => ()
            }
            p.pos = saved_pos
          }
          if p.flags.ignore_case && p.flags.unicode_sets {
            atom_set = class_set_canonicalize(
              atom_set,
              p.flags.unicode || p.flags.unicode_sets,
            )
          }
          set = class_set_union(set, atom_set)
          if p.flags.unicode_sets && first {
            if parser_peek(p) == Some(38) {
              let saved = p.pos
              let _ = parser_next(p)
              if parser_match(p, 38) {
                while true {
                  let operand = parse_class_set_operand(p)
                  set = class_set_inter(set, operand)
                  if parser_peek(p) == Some(93) {
                    let _ = parser_next(p)
                    break
                  }
                  if parser_peek(p) == Some(38) {
                    let _ = parser_next(p)
                    if parser_match(p, 38) {
                      continue
                    }
                  }
                  fail("regexp parse error")
                }
                break
              } else {
                p.pos = saved
              }
            } else if parser_peek(p) == Some(45) {
              let saved = p.pos
              let _ = parser_next(p)
              if parser_match(p, 45) {
                while true {
                  let operand = parse_class_set_operand(p)
                  set = class_set_sub(set, operand)
                  if parser_peek(p) == Some(93) {
                    let _ = parser_next(p)
                    break
                  }
                  if parser_peek(p) == Some(45) {
                    let _ = parser_next(p)
                    if parser_match(p, 45) {
                      continue
                    }
                  }
                  fail("regexp parse error")
                }
                break
              } else {
                p.pos = saved
              }
            }
          }
          first = false
        }
      None => fail("regexp parse error")
    }
  }
  if invert {
    if p.flags.ignore_case && !p.flags.unicode_sets {
      if class_set_is_word_complement(set) {
        set = class_set_invert(set)
        set = class_set_canonicalize(
          set,
          p.flags.unicode || p.flags.unicode_sets,
        )
        return class_set_sort_strings(set)
      }
      set = class_set_canonicalize(set, p.flags.unicode || p.flags.unicode_sets)
    }
    if !set.strings.is_empty() {
      fail("regexp parse error")
    }
    set = class_set_invert(set)
  } else if p.flags.ignore_case && !p.flags.unicode_sets {
    set = class_set_canonicalize(set, p.flags.unicode || p.flags.unicode_sets)
  }
  class_set_sort_strings(set)
}

///|
fn parse_class_set_operand(p : RegexpParser) -> ClassSet raise {
  match parser_peek(p) {
    Some(code) =>
      if code == 91 {
        let _ = parser_next(p)
        parse_nested_class(p)
      } else {
        match parse_class_atom(p, true) {
          Char(value) => {
            let folded = if p.flags.ignore_case && p.flags.unicode_sets {
              unicode_canonicalize(
                value,
                p.flags.unicode || p.flags.unicode_sets,
              )
            } else {
              value
            }
            class_set_single(folded)
          }
          Set(value) =>
            if p.flags.ignore_case && p.flags.unicode_sets {
              class_set_canonicalize(
                value,
                p.flags.unicode || p.flags.unicode_sets,
              )
            } else {
              value
            }
        }
      }
    None => fail("regexp parse error")
  }
}

///|
fn parse_char_class(p : RegexpParser) -> RegexNode raise {
  let _ = parser_next(p)
  let set = parse_nested_class(p)
  CharClass(set)
}

///|
fn parse_group(p : RegexpParser) -> RegexNode raise {
  parser_expect(p, 40) // '('
  if parser_match(p, 63) { // '?'
    match parser_peek(p) {
      Some(code) =>
        match code {
          105 | 109 | 115 | 45 => { // 'i' | 'm' | 's' | '-'
            let saved_ignore = p.flags.ignore_case
            let saved_multiline = p.flags.multiline
            let saved_dot_all = p.flags.dot_all
            let add_mask = parse_modifiers(p)
            let mut remove_mask = 0
            if parser_match(p, 45) { // '-'
              remove_mask = parse_modifiers(p)
            }
            if (add_mask == 0 && remove_mask == 0) ||
              (add_mask & remove_mask) != 0 {
              fail("regexp parse error")
            }
            parser_expect(p, 58) // ':'
            let next_ignore = update_modifier(
              saved_ignore, add_mask, remove_mask, 1,
            )
            let next_multiline = update_modifier(
              saved_multiline, add_mask, remove_mask, 2,
            )
            let next_dot_all = update_modifier(
              saved_dot_all, add_mask, remove_mask, 4,
            )
            let scoped_flags = flags_with_overrides(
              p.flags,
              next_ignore,
              next_multiline,
              next_dot_all,
            )
            p.flags.ignore_case = next_ignore
            p.flags.multiline = next_multiline
            p.flags.dot_all = next_dot_all
            let inner = parse_disjunction(p)
            parser_expect(p, 41)
            p.flags.ignore_case = saved_ignore
            p.flags.multiline = saved_multiline
            p.flags.dot_all = saved_dot_all
            ScopedFlags(scoped_flags, inner)
          }
          58 => { // '?:'
            let _ = parser_next(p)
            let inner = parse_disjunction(p)
            parser_expect(p, 41)
            inner
          }
          61 => { // '?='
            let _ = parser_next(p)
            let inner = parse_disjunction(p)
            parser_expect(p, 41)
            LookAhead(inner, true)
          }
          33 => { // '?!'
            let _ = parser_next(p)
            let inner = parse_disjunction(p)
            parser_expect(p, 41)
            LookAhead(inner, false)
          }
          60 => { // '?<'
            let _ = parser_next(p)
            match parser_peek(p) {
              Some(next) =>
                if next == 61 || next == 33 { // '=' or '!'
                  let positive = next == 61
                  let _ = parser_next(p)
                  let inner = parse_disjunction(p)
                  parser_expect(p, 41)
                  LookBehind(inner, positive)
                } else {
                  let name = parse_group_name(p)
                  if is_duplicate_group_name(p, name) {
                    fail("regexp parse error")
                  }
                  let capture_index = p.capture_count
                  p.capture_count = p.capture_count + 1
                  p.capture_names.push(Some(name))
                  register_group_name(p, name, capture_index)
                  let inner = parse_disjunction(p)
                  parser_expect(p, 41)
                  Capture(capture_index, inner)
                }
              None => fail("regexp parse error")
            }
          }
          _ => fail("regexp parse error")
        }
      None => fail("regexp parse error")
    }
  } else {
    let capture_index = p.capture_count
    p.capture_count = p.capture_count + 1
    p.capture_names.push(None)
    let inner = parse_disjunction(p)
    parser_expect(p, 41)
    Capture(capture_index, inner)
  }
}

///|
fn parse_modifiers(p : RegexpParser) -> Int raise {
  let mut mask = 0
  while true {
    match parser_peek(p) {
      Some(code) =>
        if code == 105 || code == 109 || code == 115 { // 'i' | 'm' | 's'
          let bit = if code == 105 { 1 } else if code == 109 { 2 } else { 4 }
          if (mask & bit) != 0 {
            fail("regexp parse error")
          }
          let _ = parser_next(p)
          mask = mask | bit
          continue
        }
      None => ()
    }
    break
  }
  mask
}

///|
fn update_modifier(
  value : Bool,
  add_mask : Int,
  remove_mask : Int,
  mask : Int,
) -> Bool {
  let mut result = value
  if (add_mask & mask) != 0 {
    result = true
  }
  if (remove_mask & mask) != 0 {
    result = false
  }
  result
}

///|
fn flags_with_overrides(
  base : RegexpFlags,
  ignore_case : Bool,
  multiline : Bool,
  dot_all : Bool,
) -> RegexpFlags {
  RegexpFlags::{
    compile: base.compile,
    global: base.global,
    sticky: base.sticky,
    unicode: base.unicode,
    dot_all,
    ignore_case,
    multiline,
    has_indices: base.has_indices,
    unicode_sets: base.unicode_sets,
  }
}

///|
fn parse_literal_codepoint(p : RegexpParser) -> Int raise {
  match parser_next(p) {
    Some(code) =>
      if (p.flags.unicode || p.flags.unicode_sets) &&
        code >= 0xD800 &&
        code <= 0xDBFF {
        match parser_peek(p) {
          Some(next) if next >= 0xDC00 && next <= 0xDFFF => {
            let _ = parser_next(p)
            let high = code - 0xD800
            let low = next - 0xDC00
            0x10000 + (high << 10) + low
          }
          _ => code
        }
      } else {
        code
      }
    None => fail("regexp parse error")
  }
}

///|
fn parse_atom(p : RegexpParser) -> RegexNode raise {
  match parser_peek(p) {
    Some(code) =>
      match code {
        40 => parse_group(p) // '('
        91 => parse_char_class(p) // '['
        46 => {
          let _ = parser_next(p)
          Dot
        }
        94 => {
          let _ = parser_next(p)
          AnchorStart
        }
        36 => {
          let _ = parser_next(p)
          AnchorEnd
        }
        92 => {
          let _ = parser_next(p)
          parse_escape_outside(p)
        }
        42 | 43 | 63 => fail("regexp parse error")
        123 => {
          if p.flags.unicode {
            fail("regexp parse error")
          }
          let saved = p.pos
          let _ = parser_next(p)
          match parser_peek(p) {
            Some(next) =>
              if next >= 48 && next <= 57 {
                let _ = parse_decimal(p)
                if parser_peek(p) == Some(44) {
                  let _ = parser_next(p)
                  let _ = parse_decimal(p)

                }
                if parser_peek(p) == Some(125) {
                  fail("regexp parse error")
                }
                p.pos = saved + 1
                Literal(123)
              } else {
                p.pos = saved + 1
                Literal(123)
              }
            None => {
              p.pos = saved + 1
              Literal(123)
            }
          }
        }
        93 | 125 =>
          if p.flags.unicode {
            fail("regexp parse error")
          } else {
            Literal(parse_literal_codepoint(p))
          }
        _ => Literal(parse_literal_codepoint(p))
      }
    None => Empty
  }
}

///|
fn parse_quantifier(p : RegexpParser, atom : RegexNode) -> RegexNode raise {
  match parser_peek(p) {
    Some(code) =>
      match code {
        42 | 43 | 63 | 123 => {
          let unicode = p.flags.unicode || p.flags.unicode_sets
          let allow_quantifier = match atom {
            LookAhead(_, _) => !unicode
            LookBehind(_, _) => false
            AnchorStart | AnchorEnd | WordBoundary(_) => false
            _ => true
          }
          if !allow_quantifier {
            fail("regexp parse error")
          }
          let mut min = 0
          let mut max = 0
          let mut is_quant = true
          if code == 42 {
            let _ = parser_next(p)
            min = 0
            max = -1
          } else if code == 43 {
            let _ = parser_next(p)
            min = 1
            max = -1
          } else if code == 63 {
            let _ = parser_next(p)
            min = 0
            max = 1
          } else {
            let saved = p.pos
            let _ = parser_next(p)
            match parser_peek(p) {
              Some(next) =>
                if next < 48 || next > 57 {
                  if p.flags.unicode {
                    fail("regexp parse error")
                  }
                  p.pos = saved
                  is_quant = false
                } else {
                  match parse_decimal(p) {
                    Some(value) => {
                      min = value
                      max = value
                    }
                    None => {
                      p.pos = saved
                      is_quant = false
                    }
                  }
                  if is_quant {
                    if parser_peek(p) == Some(44) {
                      let _ = parser_next(p)
                      match parse_decimal(p) {
                        Some(value) => {
                          if value < min {
                            fail("regexp parse error")
                          }
                          max = value
                        }
                        None => max = -1
                      }
                    }
                    if parser_peek(p) == Some(125) {
                      let _ = parser_next(p)

                    } else if p.flags.unicode {
                      fail("regexp parse error")
                    } else {
                      p.pos = saved
                      is_quant = false
                    }
                  }
                }
              None => {
                p.pos = saved
                is_quant = false
              }
            }
          }
          if !is_quant {
            atom
          } else {
            let mut greedy = true
            if parser_peek(p) == Some(63) {
              let _ = parser_next(p)
              greedy = false
            }
            Repeat(atom, min, max, greedy)
          }
        }
        _ => atom
      }
    None => atom
  }
}

///|
fn parse_sequence(p : RegexpParser) -> RegexNode raise {
  let nodes : Array[RegexNode] = []
  while true {
    match parser_peek(p) {
      Some(code) =>
        if code == 124 || code == 41 {
          break
        } else {
          let atom = parse_atom(p)
          let node = parse_quantifier(p, atom)
          nodes.push(node)
        }
      None => break
    }
  }
  if nodes.is_empty() {
    Empty
  } else if nodes.length() == 1 {
    nodes[0]
  } else {
    Sequence(nodes)
  }
}

///|
fn parse_disjunction(p : RegexpParser) -> RegexNode raise {
  let branches : Array[RegexNode] = []
  branches.push(parse_sequence(p))
  while parser_match(p, 124) {
    p.group_name_scope = p.group_name_scope + 1
    branches.push(parse_sequence(p))
  }
  if branches.length() == 1 {
    branches[0]
  } else {
    Alternation(branches)
  }
}

///|
fn resolve_backreferences(
  node : RegexNode,
  name_indices : Map[String, Array[Int]],
  capture_count : Int,
) -> RegexNode raise {
  match node {
    NamedBackReference(name) =>
      match name_indices.get(name) {
        Some(indices) => BackReference(indices)
        None => fail("regexp parse error")
      }
    NumericBackReference(index) =>
      if index <= 0 || index > capture_count {
        fail("regexp parse error")
      } else {
        BackReference([index - 1])
      }
    Sequence(nodes) => {
      let resolved : Array[RegexNode] = []
      for entry in nodes {
        resolved.push(
          resolve_backreferences(entry, name_indices, capture_count),
        )
      }
      Sequence(resolved)
    }
    Alternation(nodes) => {
      let resolved : Array[RegexNode] = []
      for entry in nodes {
        resolved.push(
          resolve_backreferences(entry, name_indices, capture_count),
        )
      }
      Alternation(resolved)
    }
    Capture(index, inner) =>
      Capture(index, resolve_backreferences(inner, name_indices, capture_count))
    Repeat(inner, min, max, greedy) =>
      Repeat(
        resolve_backreferences(inner, name_indices, capture_count),
        min,
        max,
        greedy,
      )
    ScopedFlags(flags, inner) =>
      ScopedFlags(
        flags,
        resolve_backreferences(inner, name_indices, capture_count),
      )
    LookAhead(inner, positive) =>
      LookAhead(
        resolve_backreferences(inner, name_indices, capture_count),
        positive,
      )
    LookBehind(inner, positive) =>
      LookBehind(
        resolve_backreferences(inner, name_indices, capture_count),
        positive,
      )
    _ => node
  }
}

///|
fn regexp_compile(pattern : String, flags : RegexpFlags) -> RegexpProgram raise {
  let parser = parser_new(pattern, flags)
  let root = parse_disjunction(parser)
  let resolved = resolve_backreferences(
    root,
    parser.name_indices,
    parser.capture_count,
  )
  if parser.pos < parser_len(parser) {
    fail("regexp parse error")
  }
  RegexpProgram::{
    root: resolved,
    capture_count: parser.capture_count,
    capture_names: parser.capture_names,
    flags,
  }
}

///|
fn read_codepoint(input : String, index : Int, unicode : Bool) -> (Int, Int)? {
  let len = input.length()
  if index < 0 || index >= len {
    return None
  }
  let first = UInt16::to_int(input.code_unit_at(index))
  if unicode && first >= 0xD800 && first <= 0xDBFF {
    if index + 1 < len {
      let second = UInt16::to_int(input.code_unit_at(index + 1))
      if second >= 0xDC00 && second <= 0xDFFF {
        let high = first - 0xD800
        let low = second - 0xDC00
        let codepoint = 0x10000 + (high << 10) + low
        return Some((codepoint, index + 2))
      }
    }
  }
  Some((first, index + 1))
}

///|
fn clone_captures(values : Array[CaptureSpan?]) -> Array[CaptureSpan?] {
  let result : Array[CaptureSpan?] = []
  for entry in values {
    result.push(entry)
  }
  result
}

///|
fn int_array_contains(values : Array[Int], target : Int) -> Bool {
  for value in values {
    if value == target {
      return true
    }
  }
  false
}

///|
fn collect_capture_indices_into(node : RegexNode, out : Array[Int]) -> Unit {
  match node {
    Capture(index, inner) => {
      if !int_array_contains(out, index) {
        out.push(index)
      }
      collect_capture_indices_into(inner, out)
    }
    Sequence(nodes) =>
      for entry in nodes {
        collect_capture_indices_into(entry, out)
      }
    Alternation(nodes) =>
      for entry in nodes {
        collect_capture_indices_into(entry, out)
      }
    Repeat(inner, _, _, _) => collect_capture_indices_into(inner, out)
    ScopedFlags(_, inner) => collect_capture_indices_into(inner, out)
    LookAhead(inner, _) => collect_capture_indices_into(inner, out)
    LookBehind(inner, _) => collect_capture_indices_into(inner, out)
    _ => ()
  }
}

///|
fn collect_capture_indices(node : RegexNode) -> Array[Int] {
  let result : Array[Int] = []
  collect_capture_indices_into(node, result)
  result
}

///|
fn capture_indices_diff(all : Array[Int], subset : Array[Int]) -> Array[Int] {
  let result : Array[Int] = []
  for index in all {
    if !int_array_contains(subset, index) {
      result.push(index)
    }
  }
  result
}

///|
fn clear_captures(state : MatchState, indices : Array[Int]) -> MatchState {
  if indices.is_empty() {
    return state
  }
  let captures = clone_captures(state.captures)
  for index in indices {
    captures[index] = None
  }
  MatchState::{ index: state.index, captures }
}

///|
fn canonicalize_codepoint(code : Int, flags : RegexpFlags) -> Int {
  if !flags.ignore_case {
    return code
  }
  unicode_canonicalize(code, flags.unicode || flags.unicode_sets)
}

///|
fn match_string_at(
  input : String,
  index : Int,
  pattern : String,
  flags : RegexpFlags,
) -> Int? {
  let len = input.length()
  let plen = pattern.length()
  if index + plen > len {
    return None
  }
  let unicode = flags.unicode || flags.unicode_sets
  let mut i = 0
  while i < plen {
    let a = UInt16::to_int(input.code_unit_at(index + i))
    let b = UInt16::to_int(pattern.code_unit_at(i))
    if unicode {
      if is_lead_surrogate(b) {
        let pattern_has_trail = i + 1 < plen &&
          is_trail_surrogate(UInt16::to_int(pattern.code_unit_at(i + 1)))
        let input_has_trail = index + i + 1 < len &&
          is_trail_surrogate(UInt16::to_int(input.code_unit_at(index + i + 1)))
        if pattern_has_trail != input_has_trail {
          return None
        }
      } else if is_trail_surrogate(b) {
        let pattern_has_lead = i > 0 &&
          is_lead_surrogate(UInt16::to_int(pattern.code_unit_at(i - 1)))
        let input_has_lead = index + i > 0 &&
          is_lead_surrogate(UInt16::to_int(input.code_unit_at(index + i - 1)))
        if pattern_has_lead != input_has_lead {
          return None
        }
      }
    }
    let ca = canonicalize_codepoint(a, flags)
    let cb = canonicalize_codepoint(b, flags)
    if ca != cb {
      return None
    }
    i = i + 1
  }
  Some(index + plen)
}

///|
fn match_class_set(
  set : ClassSet,
  input : String,
  index : Int,
  flags : RegexpFlags,
) -> Array[Int] {
  let results : Array[Int] = []
  if !set.strings.is_empty() {
    for text in set.strings {
      match match_string_at(input, index, text, flags) {
        Some(next) => results.push(next)
        None => ()
      }
    }
  }
  match read_codepoint(input, index, flags.unicode || flags.unicode_sets) {
    Some((codepoint, next_index)) => {
      let check = canonicalize_codepoint(codepoint, flags)
      if class_set_contains_range(set, check) {
        results.push(next_index)
      }
    }
    None => ()
  }
  results
}

///|
fn reverse_string_units(value : String) -> String {
  let len = value.length()
  if len == 0 {
    return ""
  }
  let sb = StringBuilder::new()
  let mut i = len
  while i > 0 {
    i = i - 1
    let code = UInt16::to_int(value.code_unit_at(i))
    sb.write_char(Int::unsafe_to_char(code))
  }
  sb.to_string()
}

///|
fn reverse_input_for_flags(input : String, flags : RegexpFlags) -> String {
  if flags.unicode || flags.unicode_sets {
    input.rev()
  } else {
    reverse_string_units(input)
  }
}

///|
fn reverse_capture_span(len : Int, span : CaptureSpan) -> CaptureSpan {
  CaptureSpan::{ start: len - span.end, end: len - span.start }
}

///|
fn reverse_captures(
  len : Int,
  captures : Array[CaptureSpan?],
) -> Array[CaptureSpan?] {
  let result : Array[CaptureSpan?] = []
  for item in captures {
    match item {
      Some(span) => result.push(Some(reverse_capture_span(len, span)))
      None => result.push(None)
    }
  }
  result
}

///|
fn reverse_class_set_strings(set : ClassSet, unicode : Bool) -> ClassSet {
  if set.strings.is_empty() {
    set
  } else {
    let strings : Array[String] = []
    for text in set.strings {
      let reversed = if unicode {
        text.rev()
      } else {
        reverse_string_units(text)
      }
      strings.push(reversed)
    }
    ClassSet::{ ranges: set.ranges, strings }
  }
}

///|
fn reverse_regex_node(node : RegexNode, unicode : Bool) -> RegexNode {
  match node {
    Empty => Empty
    Sequence(nodes) =>
      if nodes.is_empty() {
        Sequence([])
      } else {
        let reversed : Array[RegexNode] = []
        let mut i = nodes.length()
        while i > 0 {
          i = i - 1
          reversed.push(reverse_regex_node(nodes[i], unicode))
        }
        Sequence(reversed)
      }
    Alternation(branches) => {
      let reversed : Array[RegexNode] = []
      for branch in branches {
        reversed.push(reverse_regex_node(branch, unicode))
      }
      Alternation(reversed)
    }
    Literal(code) => Literal(code)
    Dot => Dot
    CharClass(set) => CharClass(reverse_class_set_strings(set, unicode))
    Capture(index, inner) => Capture(index, reverse_regex_node(inner, unicode))
    BackReference(indices) => BackReference(indices)
    NamedBackReference(name) => NamedBackReference(name)
    NumericBackReference(index) => NumericBackReference(index)
    ScopedFlags(scoped_flags, inner) =>
      ScopedFlags(scoped_flags, reverse_regex_node(inner, unicode))
    LookAhead(inner, positive) =>
      LookBehind(reverse_regex_node(inner, unicode), positive)
    LookBehind(inner, positive) =>
      LookAhead(reverse_regex_node(inner, unicode), positive)
    AnchorStart => AnchorEnd
    AnchorEnd => AnchorStart
    WordBoundary(pos) => WordBoundary(pos)
    Repeat(inner, min, max, greedy) =>
      Repeat(reverse_regex_node(inner, unicode), min, max, greedy)
  }
}

///|
fn match_backreference(
  indices : Array[Int],
  input : String,
  state : MatchState,
  flags : RegexpFlags,
) -> Array[MatchState] {
  let mut chosen : CaptureSpan? = None
  for index in indices {
    if index < 0 || index >= state.captures.length() {
      continue
    }
    match state.captures[index] {
      Some(span) => {
        chosen = Some(span)
        break
      }
      None => ()
    }
  }
  match chosen {
    None => [state]
    Some(span) => {
      let text = input.unsafe_substring(start=span.start, end=span.end)
      match match_string_at(input, state.index, text, flags) {
        Some(next_index) =>
          [MatchState::{ index: next_index, captures: state.captures }]
        None => []
      }
    }
  }
}

///|
fn match_lookbehind(
  inner : RegexNode,
  input : String,
  state : MatchState,
  flags : RegexpFlags,
  positive : Bool,
) -> Array[MatchState] {
  let len = input.length()
  let unicode = flags.unicode || flags.unicode_sets
  let reversed_input = reverse_input_for_flags(input, flags)
  let reversed_inner = reverse_regex_node(inner, unicode)
  let reversed_captures = reverse_captures(len, state.captures)
  let reversed_state = MatchState::{
    index: len - state.index,
    captures: reversed_captures,
  }
  let matches = match_node_with_policy(
    reversed_inner, reversed_input, reversed_state, flags, false,
  )
  if matches.is_empty() {
    if positive {
      []
    } else {
      [state]
    }
  } else if positive {
    let next = matches[0]
    let restored = reverse_captures(len, next.captures)
    [MatchState::{ index: state.index, captures: restored }]
  } else {
    []
  }
}

///|
fn match_node_with_policy(
  node : RegexNode,
  input : String,
  state : MatchState,
  flags : RegexpFlags,
  capture_first : Bool,
) -> Array[MatchState] {
  match node {
    Empty => [state]
    Sequence(nodes) => match_sequence(nodes, input, state, flags, capture_first)
    Alternation(branches) => {
      let results : Array[MatchState] = []
      let all_indices : Array[Int] = []
      for branch in branches {
        collect_capture_indices_into(branch, all_indices)
      }
      for branch in branches {
        let branch_indices = collect_capture_indices(branch)
        let clear_indices = capture_indices_diff(all_indices, branch_indices)
        for
          next in match_node_with_policy(
            branch, input, state, flags, capture_first,
          ) {
          results.push(clear_captures(next, clear_indices))
        }
      }
      results
    }
    Literal(code) =>
      match
        read_codepoint(input, state.index, flags.unicode || flags.unicode_sets) {
        Some((value, next_index)) => {
          let a = canonicalize_codepoint(value, flags)
          let b = canonicalize_codepoint(code, flags)
          if a == b {
            [MatchState::{ index: next_index, captures: state.captures }]
          } else {
            []
          }
        }
        None => []
      }
    Dot =>
      match
        read_codepoint(input, state.index, flags.unicode || flags.unicode_sets) {
        Some((value, next_index)) =>
          if flags.dot_all || !is_line_terminator(value) {
            [MatchState::{ index: next_index, captures: state.captures }]
          } else {
            []
          }
        None => []
      }
    CharClass(set) => {
      let results : Array[MatchState] = []
      for next_index in match_class_set(set, input, state.index, flags) {
        results.push(MatchState::{ index: next_index, captures: state.captures })
      }
      results
    }
    Capture(index, inner) => {
      let results : Array[MatchState] = []
      let start = state.index
      for
        next in match_node_with_policy(
          inner, input, state, flags, capture_first,
        ) {
        let captures = clone_captures(next.captures)
        if !(capture_first && captures[index] is Some(_)) {
          captures[index] = Some(CaptureSpan::{ start, end: next.index })
        }
        results.push(MatchState::{ index: next.index, captures })
      }
      results
    }
    BackReference(indices) => match_backreference(indices, input, state, flags)
    NamedBackReference(_) => []
    NumericBackReference(_) => []
    ScopedFlags(scoped_flags, inner) =>
      match_node_with_policy(inner, input, state, scoped_flags, capture_first)
    LookAhead(inner, positive) => {
      let matches = match_node_with_policy(
        inner, input, state, flags, capture_first,
      )
      if positive {
        if matches.is_empty() {
          []
        } else {
          let first = matches[0]
          [MatchState::{ index: state.index, captures: first.captures }]
        }
      } else if matches.is_empty() {
        [state]
      } else {
        []
      }
    }
    LookBehind(inner, positive) =>
      match_lookbehind(inner, input, state, flags, positive)
    AnchorStart =>
      if state.index == 0 {
        [state]
      } else if flags.multiline {
        match read_codepoint(input, state.index - 1, false) {
          Some((prev, _)) => if is_line_terminator(prev) { [state] } else { [] }
          None => []
        }
      } else {
        []
      }
    AnchorEnd => {
      let len = input.length()
      if state.index == len {
        [state]
      } else if flags.multiline {
        match read_codepoint(input, state.index, false) {
          Some((curr, _)) => if is_line_terminator(curr) { [state] } else { [] }
          None => []
        }
      } else {
        []
      }
    }
    WordBoundary(pos) => {
      let before = if state.index == 0 {
        false
      } else {
        match read_codepoint(input, state.index - 1, false) {
          Some((prev, _)) => is_word_char_for_flags(prev, flags)
          None => false
        }
      }
      let after = match read_codepoint(input, state.index, false) {
        Some((curr, _)) => is_word_char_for_flags(curr, flags)
        None => false
      }
      let boundary = before != after
      if boundary == pos {
        [state]
      } else {
        []
      }
    }
    Repeat(inner, min, max, greedy) =>
      match_repeat(
        inner, input, state, flags, min, max, greedy, capture_first, true,
      )
  }
}

///|
fn match_sequence_iter(
  nodes : Array[RegexNode],
  start : Int,
  input : String,
  state : MatchState,
  flags : RegexpFlags,
  capture_first : Bool,
  emit : (MatchState) -> Bool,
) -> Bool {
  if start >= nodes.length() {
    return emit(state)
  }
  let node = nodes[start]
  match_node_with_policy_iter(node, input, state, flags, capture_first, fn(
    next,
  ) {
    match_sequence_iter(
      nodes,
      start + 1,
      input,
      next,
      flags,
      capture_first,
      emit,
    )
  })
}

///|
fn match_node_with_policy_iter(
  node : RegexNode,
  input : String,
  state : MatchState,
  flags : RegexpFlags,
  capture_first : Bool,
  emit : (MatchState) -> Bool,
) -> Bool {
  match node {
    Empty => emit(state)
    Sequence(nodes) =>
      match_sequence_iter(nodes, 0, input, state, flags, capture_first, emit)
    Alternation(branches) => {
      let all_indices : Array[Int] = []
      for branch in branches {
        collect_capture_indices_into(branch, all_indices)
      }
      for branch in branches {
        let branch_indices = collect_capture_indices(branch)
        let clear_indices = capture_indices_diff(all_indices, branch_indices)
        let stopped = match_node_with_policy_iter(
          branch,
          input,
          state,
          flags,
          capture_first,
          fn(next) { emit(clear_captures(next, clear_indices)) },
        )
        if stopped {
          return true
        }
      }
      false
    }
    Literal(code) =>
      match
        read_codepoint(input, state.index, flags.unicode || flags.unicode_sets) {
        Some((value, next_index)) => {
          let a = canonicalize_codepoint(value, flags)
          let b = canonicalize_codepoint(code, flags)
          if a == b {
            emit(MatchState::{ index: next_index, captures: state.captures })
          } else {
            false
          }
        }
        None => false
      }
    Dot =>
      match
        read_codepoint(input, state.index, flags.unicode || flags.unicode_sets) {
        Some((value, next_index)) =>
          if flags.dot_all || !is_line_terminator(value) {
            emit(MatchState::{ index: next_index, captures: state.captures })
          } else {
            false
          }
        None => false
      }
    CharClass(set) => {
      for next_index in match_class_set(set, input, state.index, flags) {
        if emit(MatchState::{ index: next_index, captures: state.captures }) {
          return true
        }
      }
      false
    }
    Capture(index, inner) => {
      let start = state.index
      match_node_with_policy_iter(inner, input, state, flags, capture_first, fn(
        next,
      ) {
        let captures = clone_captures(next.captures)
        if !(capture_first && captures[index] is Some(_)) {
          captures[index] = Some(CaptureSpan::{ start, end: next.index })
        }
        emit(MatchState::{ index: next.index, captures })
      })
    }
    BackReference(indices) => {
      for next in match_backreference(indices, input, state, flags) {
        if emit(next) {
          return true
        }
      }
      false
    }
    NamedBackReference(_) => false
    NumericBackReference(_) => false
    ScopedFlags(scoped_flags, inner) =>
      match_node_with_policy_iter(
        inner, input, state, scoped_flags, capture_first, emit,
      )
    LookAhead(inner, positive) => {
      let mut first : MatchState? = None
      let _ = match_node_with_policy_iter(
        inner,
        input,
        state,
        flags,
        capture_first,
        fn(next) {
          first = Some(next)
          true
        },
      )
      match (positive, first) {
        (true, Some(next)) =>
          emit(MatchState::{ index: state.index, captures: next.captures })
        (true, None) => false
        (false, Some(_)) => false
        (false, None) => emit(state)
      }
    }
    LookBehind(inner, positive) => {
      for next in match_lookbehind(inner, input, state, flags, positive) {
        if emit(next) {
          return true
        }
      }
      false
    }
    AnchorStart =>
      if state.index == 0 {
        emit(state)
      } else if flags.multiline {
        match read_codepoint(input, state.index - 1, false) {
          Some((prev, _)) =>
            if is_line_terminator(prev) {
              emit(state)
            } else {
              false
            }
          None => false
        }
      } else {
        false
      }
    AnchorEnd => {
      let len = input.length()
      if state.index == len {
        emit(state)
      } else if flags.multiline {
        match read_codepoint(input, state.index, false) {
          Some((curr, _)) =>
            if is_line_terminator(curr) {
              emit(state)
            } else {
              false
            }
          None => false
        }
      } else {
        false
      }
    }
    WordBoundary(pos) => {
      let before = if state.index == 0 {
        false
      } else {
        match read_codepoint(input, state.index - 1, false) {
          Some((prev, _)) => is_word_char_for_flags(prev, flags)
          None => false
        }
      }
      let after = match read_codepoint(input, state.index, false) {
        Some((curr, _)) => is_word_char_for_flags(curr, flags)
        None => false
      }
      let boundary = before != after
      if boundary == pos {
        emit(state)
      } else {
        false
      }
    }
    Repeat(inner, min, max, greedy) =>
      match_repeat_iter(
        inner, input, state, flags, min, max, greedy, capture_first, true, emit,
      )
  }
}

///|
fn match_repeat_iter(
  inner : RegexNode,
  input : String,
  state : MatchState,
  flags : RegexpFlags,
  min : Int,
  max : Int,
  greedy : Bool,
  capture_first : Bool,
  clear_on_zero : Bool,
  emit : (MatchState) -> Bool,
) -> Bool {
  let inner_indices = collect_capture_indices(inner)
  let iter_state = repeat_iteration_state(state, inner_indices)
  match inner {
    LookAhead(_, _) => {
      let zero_state = if min == 0 && clear_on_zero {
        clear_captures(state, inner_indices)
      } else {
        state
      }
      if max == 0 {
        return if min == 0 { emit(zero_state) } else { false }
      }
      if min == 0 {
        if !greedy {
          if emit(zero_state) {
            return true
          }
        }
        let stopped = match_node_with_policy_iter(
          inner,
          input,
          iter_state,
          flags,
          capture_first,
          fn(next) {
            if next.index != state.index {
              emit(next)
            } else {
              false
            }
          },
        )
        if stopped {
          return true
        }
        if greedy {
          return emit(zero_state)
        }
        return false
      }
      return match_node_with_policy_iter(
        inner, input, state, flags, capture_first, emit,
      )
    }
    _ => ()
  }
  match inner {
    CharClass(set) =>
      if !set.strings.is_empty() {
        match
          match_repeat_string_set(
            set, input, state, flags, min, max, greedy, clear_on_zero,
          ) {
          Some(results) => {
            for next in results {
              if emit(next) {
                return true
              }
            }
            return false
          }
          None => ()
        }
      }
    _ => ()
  }
  match
    match_repeat_simple(
      inner, input, state, flags, min, max, greedy, capture_first, clear_on_zero,
    ) {
    Some(results) => {
      for next in results {
        if emit(next) {
          return true
        }
      }
      return false
    }
    None => ()
  }
  let zero_state = if min == 0 && clear_on_zero {
    clear_captures(state, inner_indices)
  } else {
    state
  }
  if max == 0 {
    return if min == 0 { emit(zero_state) } else { false }
  } else if min == 0 {
    if !greedy {
      if emit(zero_state) {
        return true
      }
    }
    let stopped = match_node_with_policy_iter(
      inner,
      input,
      iter_state,
      flags,
      capture_first,
      fn(next) {
        if next.index == state.index {
          false
        } else {
          let next_max = if max == -1 { -1 } else { max - 1 }
          match_repeat_iter(
            inner, input, next, flags, 0, next_max, greedy, capture_first, false,
            emit,
          )
        }
      },
    )
    if stopped {
      return true
    }
    if greedy {
      return emit(zero_state)
    }
    false
  } else {
    match_node_with_policy_iter(
      inner,
      input,
      iter_state,
      flags,
      capture_first,
      fn(next) {
        if next.index == state.index {
          if min <= 1 {
            emit(next)
          } else {
            false
          }
        } else {
          let next_max = if max == -1 { -1 } else { max - 1 }
          match_repeat_iter(
            inner,
            input,
            next,
            flags,
            min - 1,
            next_max,
            greedy,
            capture_first,
            false,
            emit,
          )
        }
      },
    )
  }
}

///|
fn match_sequence_first_at(
  nodes : Array[RegexNode],
  start : Int,
  input : String,
  state : MatchState,
  flags : RegexpFlags,
  capture_first : Bool,
) -> MatchState? {
  if start >= nodes.length() {
    return Some(state)
  }
  match nodes[start] {
    Repeat(inner, min, max, greedy) =>
      match_repeat_first(
        inner,
        min,
        max,
        greedy,
        nodes,
        start + 1,
        input,
        state,
        flags,
        capture_first,
      )
    node => {
      let mut found : MatchState? = None
      let stopped = match_node_with_policy_iter(
        node,
        input,
        state,
        flags,
        capture_first,
        fn(next) {
          match
            match_sequence_first_at(
              nodes,
              start + 1,
              input,
              next,
              flags,
              capture_first,
            ) {
            Some(result) => {
              found = Some(result)
              true
            }
            None => false
          }
        },
      )
      if stopped {
        return found
      }
      None
    }
  }
}

///|
fn match_repeat_first(
  inner : RegexNode,
  min : Int,
  max : Int,
  greedy : Bool,
  nodes : Array[RegexNode],
  next_index : Int,
  input : String,
  state : MatchState,
  flags : RegexpFlags,
  capture_first : Bool,
) -> MatchState? {
  let inner_indices = collect_capture_indices(inner)
  let iter_state = repeat_iteration_state(state, inner_indices)
  match inner {
    LookAhead(_, _) => {
      let zero_state = if min == 0 {
        clear_captures(state, inner_indices)
      } else {
        state
      }
      if max == 0 {
        if min == 0 {
          return match_sequence_first_at(
            nodes, next_index, input, zero_state, flags, capture_first,
          )
        }
        return None
      }
      if min == 0 {
        return match_sequence_first_at(
          nodes, next_index, input, zero_state, flags, capture_first,
        )
      }
      let mut found : MatchState? = None
      let stopped = match_node_with_policy_iter(
        inner,
        input,
        iter_state,
        flags,
        capture_first,
        fn(next) {
          match
            match_sequence_first_at(
              nodes, next_index, input, next, flags, capture_first,
            ) {
            Some(result) => {
              found = Some(result)
              true
            }
            None => false
          }
        },
      )
      if stopped {
        return found
      }
      return None
    }
    _ => ()
  }
  if is_simple_repeat_node(inner) {
    return match_repeat_first_simple(
      inner, min, max, greedy, nodes, next_index, input, state, flags, capture_first,
    )
  }
  fn repeat_step(
    inner : RegexNode,
    min : Int,
    max : Int,
    greedy : Bool,
    nodes : Array[RegexNode],
    next_index : Int,
    input : String,
    state : MatchState,
    flags : RegexpFlags,
    capture_first : Bool,
    count : Int,
  ) -> MatchState? {
    let iter_state = repeat_iteration_state(state, inner_indices)
    if greedy {
      if max == -1 || count < max {
        let mut found : MatchState? = None
        let stopped = match_node_with_policy_iter(
          inner,
          input,
          iter_state,
          flags,
          capture_first,
          fn(next) {
            if next.index == state.index {
              if min > 0 && count + 1 >= min {
                match
                  match_sequence_first_at(
                    nodes, next_index, input, next, flags, capture_first,
                  ) {
                  Some(result) => {
                    found = Some(result)
                    true
                  }
                  None => false
                }
              } else {
                false
              }
            } else {
              match
                repeat_step(
                  inner,
                  min,
                  max,
                  greedy,
                  nodes,
                  next_index,
                  input,
                  next,
                  flags,
                  capture_first,
                  count + 1,
                ) {
                Some(result) => {
                  found = Some(result)
                  true
                }
                None => false
              }
            }
          },
        )
        if stopped {
          return found
        }
      }
      if count >= min {
        return match_sequence_first_at(
          nodes, next_index, input, state, flags, capture_first,
        )
      }
      None
    } else {
      if count >= min {
        match
          match_sequence_first_at(
            nodes, next_index, input, state, flags, capture_first,
          ) {
          Some(result) => return Some(result)
          None => ()
        }
      }
      if max != -1 && count >= max {
        return None
      }
      let mut found : MatchState? = None
      let stopped = match_node_with_policy_iter(
        inner,
        input,
        iter_state,
        flags,
        capture_first,
        fn(next) {
          if next.index == state.index {
            if min > 0 && count + 1 >= min {
              match
                match_sequence_first_at(
                  nodes, next_index, input, next, flags, capture_first,
                ) {
                Some(result) => {
                  found = Some(result)
                  true
                }
                None => false
              }
            } else {
              false
            }
          } else {
            match
              repeat_step(
                inner,
                min,
                max,
                greedy,
                nodes,
                next_index,
                input,
                next,
                flags,
                capture_first,
                count + 1,
              ) {
              Some(result) => {
                found = Some(result)
                true
              }
              None => false
            }
          }
        },
      )
      if stopped {
        return found
      }
      None
    }
  }

  repeat_step(
    inner, min, max, greedy, nodes, next_index, input, state, flags, capture_first,
    0,
  )
}

///|
fn is_simple_repeat_node(node : RegexNode) -> Bool {
  match node {
    Literal(_) => true
    Dot => true
    CharClass(set) => set.strings.is_empty()
    _ => false
  }
}

///|
fn match_repeat_first_simple(
  inner : RegexNode,
  min : Int,
  max : Int,
  greedy : Bool,
  nodes : Array[RegexNode],
  next_index : Int,
  input : String,
  state : MatchState,
  flags : RegexpFlags,
  capture_first : Bool,
) -> MatchState? {
  let positions : Array[Int] = [state.index]
  let mut count = 0
  let mut current = state.index
  while max == -1 || count < max {
    match match_simple_repeat_next(inner, input, current, flags) {
      Some(next_index) => {
        if next_index == current {
          break
        }
        positions.push(next_index)
        current = next_index
        count = count + 1
      }
      None => break
    }
  }
  let total = positions.length() - 1
  if total < min {
    return None
  }
  if greedy {
    let mut i = total
    while true {
      if i < min {
        break
      }
      let idx = positions[i]
      let next_state = MatchState::{ index: idx, captures: state.captures }
      match
        match_sequence_first_at(
          nodes, next_index, input, next_state, flags, capture_first,
        ) {
        Some(result) => return Some(result)
        None => ()
      }
      if i == min {
        break
      }
      i = i - 1
    }
  } else {
    let mut i = min
    while i <= total {
      let idx = positions[i]
      let next_state = MatchState::{ index: idx, captures: state.captures }
      match
        match_sequence_first_at(
          nodes, next_index, input, next_state, flags, capture_first,
        ) {
        Some(result) => return Some(result)
        None => ()
      }
      i = i + 1
    }
  }
  None
}

///|
fn match_node_first(
  node : RegexNode,
  input : String,
  state : MatchState,
  flags : RegexpFlags,
) -> MatchState? {
  match node {
    Sequence(nodes) =>
      match_sequence_first_at(nodes, 0, input, state, flags, false)
    _ => match_sequence_first_at([node], 0, input, state, flags, false)
  }
}

///|
fn match_sequence(
  nodes : Array[RegexNode],
  input : String,
  state : MatchState,
  flags : RegexpFlags,
  capture_first : Bool,
) -> Array[MatchState] {
  let mut states : Array[MatchState] = [state]
  for node in nodes {
    let next_states : Array[MatchState] = []
    for current in states {
      for
        next in match_node_with_policy(
          node, input, current, flags, capture_first,
        ) {
        next_states.push(next)
      }
    }
    states = next_states
    if states.is_empty() {
      break
    }
  }
  states
}

///|
fn match_simple_repeat_next(
  inner : RegexNode,
  input : String,
  index : Int,
  flags : RegexpFlags,
) -> Int? {
  match inner {
    Literal(code) =>
      match read_codepoint(input, index, flags.unicode || flags.unicode_sets) {
        Some((value, next_index)) => {
          let a = canonicalize_codepoint(value, flags)
          let b = canonicalize_codepoint(code, flags)
          if a == b {
            Some(next_index)
          } else {
            None
          }
        }
        None => None
      }
    Dot =>
      match read_codepoint(input, index, flags.unicode || flags.unicode_sets) {
        Some((value, next_index)) =>
          if flags.dot_all || !is_line_terminator(value) {
            Some(next_index)
          } else {
            None
          }
        None => None
      }
    CharClass(set) =>
      if !set.strings.is_empty() {
        None
      } else {
        match
          read_codepoint(input, index, flags.unicode || flags.unicode_sets) {
          Some((codepoint, next_index)) => {
            let check = canonicalize_codepoint(codepoint, flags)
            if class_set_contains_range(set, check) {
              Some(next_index)
            } else {
              None
            }
          }
          None => None
        }
      }
    _ => None
  }
}

///|
fn match_repeat_simple(
  inner : RegexNode,
  input : String,
  state : MatchState,
  flags : RegexpFlags,
  min : Int,
  max : Int,
  greedy : Bool,
  capture_first : Bool,
  clear_on_zero : Bool,
) -> Array[MatchState]? {
  let _ = capture_first
  match match_simple_repeat_next(inner, input, state.index, flags) {
    None => None
    Some(_) => {
      if max == 0 {
        if min == 0 {
          let zero_state = if clear_on_zero {
            clear_captures(state, collect_capture_indices(inner))
          } else {
            state
          }
          return Some([zero_state])
        }
        return Some([])
      }
      let zero_state = if min == 0 && clear_on_zero {
        clear_captures(state, collect_capture_indices(inner))
      } else {
        state
      }
      let positions : Array[Int] = [state.index]
      let mut count = 0
      let mut current = state.index
      while max == -1 || count < max {
        match match_simple_repeat_next(inner, input, current, flags) {
          Some(next_index) => {
            if next_index == current {
              break
            }
            positions.push(next_index)
            current = next_index
            count = count + 1
          }
          None => break
        }
      }
      let total = positions.length() - 1
      if total < min {
        return Some([])
      }
      let results : Array[MatchState] = []
      if greedy {
        let mut i = total
        while true {
          if i < min {
            break
          }
          let idx = positions[i]
          if i == 0 {
            results.push(MatchState::{
              index: idx,
              captures: zero_state.captures,
            })
          } else {
            results.push(MatchState::{ index: idx, captures: state.captures })
          }
          if i == min {
            break
          }
          i = i - 1
        }
      } else {
        let mut i = min
        while i <= total {
          let idx = positions[i]
          if i == 0 {
            results.push(MatchState::{
              index: idx,
              captures: zero_state.captures,
            })
          } else {
            results.push(MatchState::{ index: idx, captures: state.captures })
          }
          i = i + 1
        }
      }
      Some(results)
    }
  }
}

///|
fn match_repeat_string_set(
  set : ClassSet,
  input : String,
  state : MatchState,
  flags : RegexpFlags,
  min : Int,
  max : Int,
  greedy : Bool,
  clear_on_zero : Bool,
) -> Array[MatchState]? {
  if class_set_has_empty_string(set) {
    return None
  }
  if max == 0 {
    if min == 0 {
      let zero_state = if clear_on_zero {
        clear_captures(state, collect_capture_indices(CharClass(set)))
      } else {
        state
      }
      return Some([zero_state])
    }
    return Some([])
  }
  let zero_state = if min == 0 && clear_on_zero {
    clear_captures(state, collect_capture_indices(CharClass(set)))
  } else {
    state
  }
  let input_len = input.length()
  let max_count = if max == -1 || max > input_len { input_len } else { max }
  let positions_by_count : Array[Array[Int]] = [[state.index]]
  let next_cache : Map[Int, Array[Int]] = Map::new()
  let mut count = 0
  while count < max_count {
    let current = positions_by_count[count]
    if current.is_empty() {
      break
    }
    let next_positions : Array[Int] = []
    let seen : Map[Int, Bool] = Map::new()
    for pos in current {
      let nexts = match next_cache.get(pos) {
        Some(value) => value
        None => {
          let computed = match_class_set(set, input, pos, flags)
          next_cache.set(pos, computed)
          computed
        }
      }
      for next in nexts {
        if next == pos {
          continue
        }
        if !seen.contains(next) {
          seen.set(next, true)
          next_positions.push(next)
        }
      }
    }
    positions_by_count.push(next_positions)
    count = count + 1
  }
  let max_reached = positions_by_count.length() - 1
  if max_reached < min {
    return Some([])
  }
  let results : Array[MatchState] = []
  if greedy {
    let mut i = max_reached
    while true {
      if i < min {
        break
      }
      let positions = positions_by_count[i]
      for idx in positions {
        if i == 0 {
          results.push(MatchState::{ index: idx, captures: zero_state.captures })
        } else {
          results.push(MatchState::{ index: idx, captures: state.captures })
        }
      }
      if i == min {
        break
      }
      i = i - 1
    }
  } else {
    let mut i = min
    while i <= max_reached {
      let positions = positions_by_count[i]
      for idx in positions {
        if i == 0 {
          results.push(MatchState::{ index: idx, captures: zero_state.captures })
        } else {
          results.push(MatchState::{ index: idx, captures: state.captures })
        }
      }
      i = i + 1
    }
  }
  Some(results)
}

///|
fn repeat_iteration_state(
  state : MatchState,
  inner_indices : Array[Int],
) -> MatchState {
  if inner_indices.is_empty() {
    state
  } else {
    clear_captures(state, inner_indices)
  }
}

///|
fn match_repeat(
  inner : RegexNode,
  input : String,
  state : MatchState,
  flags : RegexpFlags,
  min : Int,
  max : Int,
  greedy : Bool,
  capture_first : Bool,
  clear_on_zero : Bool,
) -> Array[MatchState] {
  let inner_indices = collect_capture_indices(inner)
  let iter_state = repeat_iteration_state(state, inner_indices)
  match inner {
    LookAhead(_, _) => {
      let zero_state = if min == 0 && clear_on_zero {
        clear_captures(state, inner_indices)
      } else {
        state
      }
      if max == 0 {
        return if min == 0 { [zero_state] } else { [] }
      }
      if min == 0 {
        let results : Array[MatchState] = []
        if !greedy {
          results.push(zero_state)
        }
        let next_states = match_node_with_policy(
          inner, input, iter_state, flags, capture_first,
        )
        for next in next_states {
          if next.index != state.index {
            results.push(next)
          }
        }
        if greedy {
          results.push(zero_state)
        }
        return results
      } else {
        return match_node_with_policy(inner, input, state, flags, capture_first)
      }
    }
    _ => ()
  }
  match inner {
    CharClass(set) =>
      if !set.strings.is_empty() {
        match
          match_repeat_string_set(
            set, input, state, flags, min, max, greedy, clear_on_zero,
          ) {
          Some(results) => return results
          None => ()
        }
      }
    _ => ()
  }
  match
    match_repeat_simple(
      inner, input, state, flags, min, max, greedy, capture_first, clear_on_zero,
    ) {
    Some(results) => return results
    None => ()
  }
  let zero_state = if min == 0 && clear_on_zero {
    clear_captures(state, inner_indices)
  } else {
    state
  }
  if max == 0 {
    if min == 0 {
      [zero_state]
    } else {
      []
    }
  } else if min == 0 {
    let results : Array[MatchState] = []
    if !greedy {
      results.push(zero_state)
    }
    let next_states = match_node_with_policy(
      inner, input, iter_state, flags, capture_first,
    )
    for next in next_states {
      if next.index != state.index {
        let next_max = if max == -1 { -1 } else { max - 1 }
        for
          tail in match_repeat(
            inner, input, next, flags, 0, next_max, greedy, capture_first, false,
          ) {
          results.push(tail)
        }
      }
    }
    if greedy {
      results.push(zero_state)
    }
    results
  } else {
    let results : Array[MatchState] = []
    let next_states = match_node_with_policy(
      inner, input, iter_state, flags, capture_first,
    )
    for next in next_states {
      if next.index == state.index {
        if min <= 1 {
          results.push(next)
        }
      } else {
        let next_max = if max == -1 { -1 } else { max - 1 }
        for
          tail in match_repeat(
            inner,
            input,
            next,
            flags,
            min - 1,
            next_max,
            greedy,
            capture_first,
            false,
          ) {
          results.push(tail)
        }
      }
    }
    results
  }
}

///|
fn regexp_match_from_program(
  program : RegexpProgram,
  input : String,
  start : Int,
  sticky : Bool,
) -> RegexpMatch? {
  let len = input.length()
  if start > len {
    return None
  }
  let mut index = start
  while true {
    let initial = MatchState::{
      index,
      captures: Array::make(program.capture_count, None),
    }
    match match_node_first(program.root, input, initial, program.flags) {
      Some(chosen) => {
        let start_index = index
        let end_index = chosen.index
        let groups : Array[String?] = []
        groups.push(
          Some(input.unsafe_substring(start=start_index, end=end_index)),
        )
        for entry in chosen.captures {
          match entry {
            Some(span) =>
              groups.push(
                Some(input.unsafe_substring(start=span.start, end=span.end)),
              )
            None => groups.push(None)
          }
        }
        let indices = chosen.captures
        return Some(RegexpMatch::{
          start: start_index,
          end: end_index,
          groups,
          indices,
        })
      }
      None => ()
    }
    if sticky {
      return None
    }
    if index >= len {
      return None
    }
    index = advance_string_index(
      input,
      index,
      program.flags.unicode || program.flags.unicode_sets,
    )
  }
  None
}
