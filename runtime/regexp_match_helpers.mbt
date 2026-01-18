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
