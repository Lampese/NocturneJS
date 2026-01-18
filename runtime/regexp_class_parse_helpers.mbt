///|
priv enum ClassAtom {
  Char(Int)
  Set(ClassSet)
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
