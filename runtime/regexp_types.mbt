///|
priv struct Range {
  start : Int
  end : Int
}

///|
priv struct ClassSet {
  ranges : Array[Range]
  strings : Array[String]
}

///|
priv struct RegexpFlags {
  compile : String
  global : Bool
  sticky : Bool
  unicode : Bool
  mut dot_all : Bool
  mut ignore_case : Bool
  mut multiline : Bool
  has_indices : Bool
  unicode_sets : Bool
}

///|
priv enum RegexNode {
  Empty
  Sequence(Array[RegexNode])
  Alternation(Array[RegexNode])
  AlternationCached(Array[RegexNode], Array[Array[Int]])
  Literal(Int)
  Dot
  CharClass(ClassSet)
  Capture(Int, RegexNode)
  BackReference(Array[Int])
  NamedBackReference(String)
  NumericBackReference(Int)
  ScopedFlags(RegexpFlags, RegexNode)
  LookAhead(RegexNode, Bool)
  LookBehind(RegexNode, Bool)
  AnchorStart
  AnchorEnd
  WordBoundary(Bool)
  Repeat(RegexNode, Int, Int, Bool)
}

///|
struct RegexpProgram {
  root : RegexNode
  capture_count : Int
  capture_names : Array[String?]
  flags : RegexpFlags
}

///|
priv struct CaptureSpan {
  start : Int
  end : Int
}

///|
priv struct MatchState {
  index : Int
  captures : Array[CaptureSpan?]
}

///|
priv struct RegexpMatch {
  start : Int
  end : Int
  indices : Array[CaptureSpan?]
}

///|
fn ascii_fold(code : Int) -> Int {
  if code >= 65 && code <= 90 {
    code + 32
  } else {
    code
  }
}

///|
fn class_set_empty() -> ClassSet {
  ClassSet::{ ranges: [], strings: [] }
}

///|
fn class_set_single(code : Int) -> ClassSet {
  ClassSet::{ ranges: [Range::{ start: code, end: code }], strings: [] }
}

///|
fn class_set_range(start : Int, end : Int) -> ClassSet {
  if start > end {
    class_set_empty()
  } else {
    ClassSet::{ ranges: [Range::{ start, end }], strings: [] }
  }
}

///|
fn ranges_sorted(ranges : Array[Range]) -> Array[Range] {
  let sorted = ranges.copy()
  sorted.sort_by(fn(a, b) { a.start - b.start })
  sorted
}

///|
fn ranges_normalize(ranges : Array[Range]) -> Array[Range] {
  if ranges.is_empty() {
    return []
  }
  let sorted = ranges_sorted(ranges)
  let result : Array[Range] = []
  let mut current = sorted[0]
  let mut i = 1
  while i < sorted.length() {
    let next = sorted[i]
    if next.start <= current.end + 1 {
      if next.end > current.end {
        current = Range::{ start: current.start, end: next.end }
      }
    } else {
      result.push(current)
      current = next
    }
    i = i + 1
  }
  result.push(current)
  result
}

///|
fn ranges_union(a : Array[Range], b : Array[Range]) -> Array[Range] {
  let combined : Array[Range] = []
  for r in a {
    combined.push(r)
  }
  for r in b {
    combined.push(r)
  }
  ranges_normalize(combined)
}

///|
fn ranges_inter(a : Array[Range], b : Array[Range]) -> Array[Range] {
  let aa = ranges_sorted(a)
  let bb = ranges_sorted(b)
  let result : Array[Range] = []
  let mut i = 0
  let mut j = 0
  while i < aa.length() && j < bb.length() {
    let ra = aa[i]
    let rb = bb[j]
    let start = if ra.start > rb.start { ra.start } else { rb.start }
    let end = if ra.end < rb.end { ra.end } else { rb.end }
    if start <= end {
      result.push(Range::{ start, end })
    }
    if ra.end < rb.end {
      i = i + 1
    } else {
      j = j + 1
    }
  }
  result
}

///|
fn ranges_sub(a : Array[Range], b : Array[Range]) -> Array[Range] {
  let aa = ranges_sorted(a)
  let bb = ranges_sorted(b)
  let result : Array[Range] = []
  let mut j = 0
  for ra in aa {
    let mut start = ra.start
    let end = ra.end
    while j < bb.length() && bb[j].end < start {
      j = j + 1
    }
    let mut k = j
    while k < bb.length() && bb[k].start <= end {
      let rb = bb[k]
      if rb.start > start {
        result.push(Range::{ start, end: rb.start - 1 })
      }
      if rb.end + 1 > start {
        start = rb.end + 1
      }
      if start > end {
        break
      }
      k = k + 1
    }
    if start <= end {
      result.push(Range::{ start, end })
    }
  }
  result
}

///|
fn strings_contains(values : Array[String], target : String) -> Bool {
  for value in values {
    if value == target {
      return true
    }
  }
  false
}

///|
fn strings_union(a : Array[String], b : Array[String]) -> Array[String] {
  let result : Array[String] = []
  for value in a {
    if !strings_contains(result, value) {
      result.push(value)
    }
  }
  for value in b {
    if !strings_contains(result, value) {
      result.push(value)
    }
  }
  result
}

///|
fn strings_inter(a : Array[String], b : Array[String]) -> Array[String] {
  let result : Array[String] = []
  for value in a {
    if strings_contains(b, value) && !strings_contains(result, value) {
      result.push(value)
    }
  }
  result
}

///|
fn strings_sub(a : Array[String], b : Array[String]) -> Array[String] {
  let result : Array[String] = []
  for value in a {
    if !strings_contains(b, value) {
      result.push(value)
    }
  }
  result
}

///|
fn ranges_contains(ranges : Array[Range], code : Int) -> Bool {
  for r in ranges {
    if code >= r.start && code <= r.end {
      return true
    }
  }
  false
}

///|
fn strings_inter_with_ranges(
  strings : Array[String],
  ranges : Array[Range],
) -> Array[String] {
  let result : Array[String] = []
  for value in strings {
    if value.length() == 1 {
      let code = UInt16::to_int(value.code_unit_at(0))
      if ranges_contains(ranges, code) {
        result.push(value)
      }
    }
  }
  result
}

///|
fn strings_sub_with_ranges(
  strings : Array[String],
  ranges : Array[Range],
) -> Array[String] {
  let result : Array[String] = []
  for value in strings {
    if value.length() == 1 {
      let code = UInt16::to_int(value.code_unit_at(0))
      if !ranges_contains(ranges, code) {
        result.push(value)
      }
    } else {
      result.push(value)
    }
  }
  result
}

///|
fn class_set_union(a : ClassSet, b : ClassSet) -> ClassSet {
  ClassSet::{
    ranges: ranges_union(a.ranges, b.ranges),
    strings: strings_union(a.strings, b.strings),
  }
}

///|
fn class_set_inter(a : ClassSet, b : ClassSet) -> ClassSet {
  let from_a = strings_inter_with_ranges(a.strings, b.ranges)
  let from_b = strings_inter_with_ranges(b.strings, a.ranges)
  let inter_strings = strings_inter(a.strings, b.strings)
  let merged = strings_union(inter_strings, strings_union(from_a, from_b))
  ClassSet::{ ranges: ranges_inter(a.ranges, b.ranges), strings: merged }
}

///|
fn class_set_sub(a : ClassSet, b : ClassSet) -> ClassSet {
  let without_ranges = strings_sub_with_ranges(a.strings, b.ranges)
  ClassSet::{
    ranges: ranges_sub(a.ranges, b.ranges),
    strings: strings_sub(without_ranges, b.strings),
  }
}

///|
fn class_set_invert(set : ClassSet) -> ClassSet {
  let max_codepoint = 0x10ffff
  let base = [Range::{ start: 0, end: max_codepoint }]
  ClassSet::{ ranges: ranges_sub(base, set.ranges), strings: [] }
}

///|
fn class_set_canonicalize(set : ClassSet, unicode : Bool) -> ClassSet {
  let case_mask = if unicode { CASE_F } else { CASE_U }
  let mask_ranges = unicode_case_ranges(case_mask)
  let inter_ranges = ranges_inter(set.ranges, mask_ranges)
  let sub_ranges = ranges_sub(set.ranges, mask_ranges)
  let result_ranges : Array[Range] = []
  let mut d_start = -1
  let mut d_end = -1
  for r in inter_ranges {
    let mut c = r.start
    while c <= r.end {
      let d = unicode_canonicalize(c, unicode)
      if d_start < 0 {
        d_start = d
        d_end = d + 1
      } else if d_end == d {
        d_end = d_end + 1
      } else {
        result_ranges.push(Range::{ start: d_start, end: d_end - 1 })
        d_start = d
        d_end = d + 1
      }
      c = c + 1
    }
  }
  if d_start >= 0 {
    result_ranges.push(Range::{ start: d_start, end: d_end - 1 })
  }
  let canonical_ranges = ranges_union(result_ranges, sub_ranges)
  let result_strings : Array[String] = []
  for value in set.strings {
    let sb = StringBuilder::new()
    for ch in value {
      let code = ch.to_int()
      sb.write_char(Int::unsafe_to_char(unicode_canonicalize(code, unicode)))
    }
    let folded = sb.to_string()
    if !strings_contains(result_strings, folded) {
      result_strings.push(folded)
    }
  }
  ClassSet::{ ranges: canonical_ranges, strings: result_strings }
}

///|
fn class_set_sort_strings(set : ClassSet) -> ClassSet {
  if set.strings.length() <= 1 {
    set
  } else {
    let strings = set.strings.copy()
    strings.sort_by(fn(a, b) { b.length() - a.length() })
    ClassSet::{ ranges: set.ranges, strings }
  }
}

///|
fn class_set_has_empty_string(set : ClassSet) -> Bool {
  for value in set.strings {
    if value.length() == 0 {
      return true
    }
  }
  false
}

///|
fn class_set_contains_range(set : ClassSet, code : Int) -> Bool {
  let ranges = set.ranges
  if ranges.is_empty() {
    return false
  }
  let mut low = 0
  let mut high = ranges.length() - 1
  while low <= high {
    let mid = (low + high) / 2
    let r = ranges[mid]
    if code < r.start {
      high = mid - 1
    } else if code > r.end {
      low = mid + 1
    } else {
      return true
    }
  }
  false
}
