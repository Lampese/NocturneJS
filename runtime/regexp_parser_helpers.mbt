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
