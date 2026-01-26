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
    AlternationCached(branches, clear_indices) => {
      let reversed : Array[RegexNode] = []
      let reversed_clear : Array[Array[Int]] = []
      let mut i = branches.length()
      while i > 0 {
        i = i - 1
        reversed.push(reverse_regex_node(branches[i], unicode))
        reversed_clear.push(clear_indices[i])
      }
      AlternationCached(reversed, reversed_clear)
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
