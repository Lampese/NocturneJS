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
