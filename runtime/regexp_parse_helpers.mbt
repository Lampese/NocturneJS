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
    AlternationCached(nodes, _) => {
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
  let cached = cache_alternations(resolved)
  if parser.pos < parser_len(parser) {
    fail("regexp parse error")
  }
  RegexpProgram::{
    root: cached,
    capture_count: parser.capture_count,
    capture_names: parser.capture_names,
    flags,
  }
}

///|
fn cache_alternations(node : RegexNode) -> RegexNode {
  match node {
    Sequence(nodes) => {
      let cached : Array[RegexNode] = []
      for entry in nodes {
        cached.push(cache_alternations(entry))
      }
      Sequence(cached)
    }
    Alternation(nodes) => {
      let cached_nodes : Array[RegexNode] = []
      for entry in nodes {
        cached_nodes.push(cache_alternations(entry))
      }
      let all_indices : Array[Int] = []
      for entry in cached_nodes {
        collect_capture_indices_into(entry, all_indices)
      }
      let clear_by_branch : Array[Array[Int]] = []
      for entry in cached_nodes {
        let branch_indices = collect_capture_indices(entry)
        clear_by_branch.push(capture_indices_diff(all_indices, branch_indices))
      }
      AlternationCached(cached_nodes, clear_by_branch)
    }
    AlternationCached(nodes, _) => cache_alternations(Alternation(nodes))
    Capture(index, inner) => Capture(index, cache_alternations(inner))
    Repeat(inner, min, max, greedy) =>
      Repeat(cache_alternations(inner), min, max, greedy)
    ScopedFlags(flags, inner) =>
      ScopedFlags(flags, cache_alternations(inner))
    LookAhead(inner, positive) =>
      LookAhead(cache_alternations(inner), positive)
    LookBehind(inner, positive) =>
      LookBehind(cache_alternations(inner), positive)
    _ => node
  }
}
