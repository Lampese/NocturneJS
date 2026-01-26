///|
fn byte_at(data : Bytes, index : Int) -> Int {
  Byte::to_int(data[index])
}

///|
fn add_range_exclusive(
  out : Array[Range],
  start : Int,
  end_exclusive : Int,
) -> Unit {
  if start < end_exclusive {
    out.push(Range::{ start, end: end_exclusive - 1 })
  }
}

///|
const RUN_TYPE_U : Int = 0

///|
const RUN_TYPE_L : Int = 1

///|
const RUN_TYPE_UF : Int = 2

///|
const RUN_TYPE_LF : Int = 3

///|
const RUN_TYPE_UL : Int = 4

///|
const RUN_TYPE_LSU : Int = 5

///|
const RUN_TYPE_U2L_399_EXT2 : Int = 6

///|
const RUN_TYPE_UF_D20 : Int = 7

///|
const RUN_TYPE_UF_D1_EXT : Int = 8

///|
const RUN_TYPE_U_EXT : Int = 9

///|
const RUN_TYPE_LF_EXT : Int = 10

///|
const RUN_TYPE_UF_EXT2 : Int = 11

///|
const RUN_TYPE_LF_EXT2 : Int = 12

///|
const RUN_TYPE_UF_EXT3 : Int = 13

///|
const CASE_U : Int = 1

///|
const CASE_L : Int = 2

///|
const CASE_F : Int = 4

///|
fn ranges_invert(ranges : Array[Range]) -> Array[Range] {
  let normalized = ranges_normalize(ranges)
  let result : Array[Range] = []
  let mut start = 0
  for r in normalized {
    if start < r.start {
      result.push(Range::{ start, end: r.start - 1 })
    }
    start = r.end + 1
  }
  let max_codepoint = 0x10ffff
  if start <= max_codepoint {
    result.push(Range::{ start, end: max_codepoint })
  }
  result
}

///|
fn entry_matches(table : String, start : Int, end : Int, name : String) -> Bool {
  let mut seg_start = start
  let mut i = start
  while i <= end {
    if i == end || UInt16::to_int(table.code_unit_at(i)) == 44 { // ','
      if i - seg_start == name.length() {
        let seg = table.unsafe_substring(start=seg_start, end=i)
        if seg == name {
          return true
        }
      }
      seg_start = i + 1
    }
    i = i + 1
  }
  false
}

///|
fn unicode_find_name(table : String, name : String) -> Int? {
  let len = table.length()
  let mut pos = 0
  let mut entry = 0
  while pos < len {
    let start = pos
    while pos < len && UInt16::to_int(table.code_unit_at(pos)) != 0 {
      pos = pos + 1
    }
    let end = pos
    if entry_matches(table, start, end, name) {
      return Some(entry)
    }
    pos = pos + 1
    entry = entry + 1
  }
  None
}

///|
let unicode_prop_name_map_ref : Ref[Map[String, Int]?] = Ref::new(None)

///|
let unicode_gc_name_map_ref : Ref[Map[String, Int]?] = Ref::new(None)

///|
let unicode_script_name_map_ref : Ref[Map[String, Int]?] = Ref::new(None)

///|
let unicode_sequence_name_map_ref : Ref[Map[String, Int]?] = Ref::new(None)

///|
let unicode_prop_set_cache_ref : Ref[Map[String, ClassSet]?] = Ref::new(None)

///|
let unicode_gc_set_cache_ref : Ref[Map[String, ClassSet]?] = Ref::new(None)

///|
let unicode_script_set_cache_ref : Ref[Map[String, ClassSet]?] = Ref::new(None)

///|
let unicode_sequence_set_cache_ref : Ref[Map[String, ClassSet]?] = Ref::new(None)

///|
let unicode_script_ext_set_cache_ref : Ref[Map[String, ClassSet]?] = Ref::new(None)

///|
fn unicode_name_map(table : String) -> Map[String, Int] {
  let map = Map::new()
  let len = table.length()
  let mut pos = 0
  let mut entry = 0
  while pos < len {
    let start = pos
    while pos < len && UInt16::to_int(table.code_unit_at(pos)) != 0 {
      pos = pos + 1
    }
    let end = pos
    let mut seg_start = start
    let mut i = start
    while i <= end {
      if i == end || UInt16::to_int(table.code_unit_at(i)) == 44 { // ','
        if seg_start < i {
          let name = table.unsafe_substring(start=seg_start, end=i)
          map.set(name, entry)
        }
        seg_start = i + 1
      }
      i = i + 1
    }
    pos = pos + 1
    entry = entry + 1
  }
  map
}

///|
fn unicode_name_map_from_ref(
  cell : Ref[Map[String, Int]?],
  table : String,
) -> Map[String, Int] {
  let mut result : Map[String, Int] = Map::new()
  cell.update(fn(current) {
    match current {
      Some(map) => {
        result = map
        current
      }
      None => {
        let map = unicode_name_map(table)
        result = map
        Some(map)
      }
    }
  })
  result
}

///|
fn unicode_class_set_cache_get(
  cell : Ref[Map[String, ClassSet]?],
  name : String,
) -> ClassSet? {
  let mut result : ClassSet? = None
  cell.update(fn(current) {
    match current {
      Some(map) =>
        match map.get(name) {
          Some(value) => {
            result = Some(value)
            current
          }
          None => current
        }
      None => None
    }
  })
  result
}

///|
fn unicode_class_set_cache_set(
  cell : Ref[Map[String, ClassSet]?],
  name : String,
  value : ClassSet,
) -> Unit {
  cell.update(fn(current) {
    let map : Map[String, ClassSet] = match current {
      Some(map) => map
      None => Map::new()
    }
    map.set(name, value)
    Some(map)
  })
}

///|
fn unicode_prop_name_index(name : String) -> Int? {
  let map = unicode_name_map_from_ref(
    unicode_prop_name_map_ref,
    unicode_prop_name_table,
  )
  map.get(name)
}

///|
fn unicode_gc_name_index(name : String) -> Int? {
  let map = unicode_name_map_from_ref(
    unicode_gc_name_map_ref,
    unicode_gc_name_table,
  )
  map.get(name)
}

///|
fn unicode_script_name_index(name : String) -> Int? {
  let map = unicode_name_map_from_ref(
    unicode_script_name_map_ref,
    unicode_script_name_table,
  )
  map.get(name)
}

///|
fn unicode_sequence_name_index(name : String) -> Int? {
  let map = unicode_name_map_from_ref(
    unicode_sequence_name_map_ref,
    unicode_sequence_prop_name_table,
  )
  map.get(name)
}

///|
fn unicode_gc_mask_table() -> Array[Int] {
  [
    (1 << UNICODE_GC_Lu) | (1 << UNICODE_GC_Ll) | (1 << UNICODE_GC_Lt),
    (1 << UNICODE_GC_Lu) |
    (1 << UNICODE_GC_Ll) |
    (1 << UNICODE_GC_Lt) |
    (1 << UNICODE_GC_Lm) |
    (1 << UNICODE_GC_Lo),
    (1 << UNICODE_GC_Mn) | (1 << UNICODE_GC_Mc) | (1 << UNICODE_GC_Me),
    (1 << UNICODE_GC_Nd) | (1 << UNICODE_GC_Nl) | (1 << UNICODE_GC_No),
    (1 << UNICODE_GC_Sm) |
    (1 << UNICODE_GC_Sc) |
    (1 << UNICODE_GC_Sk) |
    (1 << UNICODE_GC_So),
    (1 << UNICODE_GC_Pc) |
    (1 << UNICODE_GC_Pd) |
    (1 << UNICODE_GC_Ps) |
    (1 << UNICODE_GC_Pe) |
    (1 << UNICODE_GC_Pi) |
    (1 << UNICODE_GC_Pf) |
    (1 << UNICODE_GC_Po),
    (1 << UNICODE_GC_Zs) | (1 << UNICODE_GC_Zl) | (1 << UNICODE_GC_Zp),
    (1 << UNICODE_GC_Cc) |
    (1 << UNICODE_GC_Cf) |
    (1 << UNICODE_GC_Cs) |
    (1 << UNICODE_GC_Co) |
    (1 << UNICODE_GC_Cn),
  ]
}

///|
fn unicode_general_category_ranges(gc_mask : Int) -> Array[Range] {
  let ranges : Array[Range] = []
  let data = unicode_gc_table
  let mut i = 0
  let mut c = 0
  while i < data.length() {
    let b = byte_at(data, i)
    i = i + 1
    let mut n = b >> 5
    let v = b & 0x1f
    if n == 7 {
      let n1 = byte_at(data, i)
      i = i + 1
      if n1 < 128 {
        n = n1 + 7
      } else if n1 < 128 + 64 {
        n = ((n1 - 128) << 8) | byte_at(data, i)
        i = i + 1
        n = n + 7 + 128
      } else {
        n = ((n1 - 128 - 64) << 16) |
          (byte_at(data, i) << 8) |
          byte_at(data, i + 1)
        i = i + 2
        n = n + 7 + 128 + (1 << 14)
      }
    }
    let start = c
    c = c + n + 1
    if v == 31 {
      let mask = gc_mask & ((1 << UNICODE_GC_Lu) | (1 << UNICODE_GC_Ll))
      if mask != 0 {
        if mask == ((1 << UNICODE_GC_Lu) | (1 << UNICODE_GC_Ll)) {
          add_range_exclusive(ranges, start, c)
        } else {
          let mut cp = start +
            (if (gc_mask & (1 << UNICODE_GC_Ll)) != 0 { 1 } else { 0 })
          while cp < c {
            add_range_exclusive(ranges, cp, cp + 1)
            cp = cp + 2
          }
        }
      }
    } else if ((gc_mask >> v) & 1) != 0 {
      add_range_exclusive(ranges, start, c)
    }
  }
  ranges_normalize(ranges)
}

///|
fn unicode_general_category_set(name : String) -> ClassSet? {
  match unicode_class_set_cache_get(unicode_gc_set_cache_ref, name) {
    Some(value) => return Some(value)
    None => ()
  }
  match unicode_gc_name_index(name) {
    None => None
    Some(index) => {
      let mask = if index <= UNICODE_GC_Co {
        1 << index
      } else {
        let masks = unicode_gc_mask_table()
        masks[index - UNICODE_GC_LC]
      }
      let ranges = unicode_general_category_ranges(mask)
      let result = ClassSet::{ ranges, strings: [] }
      unicode_class_set_cache_set(unicode_gc_set_cache_ref, name, result)
      Some(result)
    }
  }
}

///|
fn unicode_prop_ranges(prop_idx : Int) -> Array[Range] {
  if prop_idx < 0 || prop_idx >= unicode_prop_tables.length() {
    return []
  }
  let data = unicode_prop_tables[prop_idx]
  let ranges : Array[Range] = []
  let mut c = 0
  let mut bit = 0
  let mut i = 0
  while i < data.length() {
    let start = c
    let b = byte_at(data, i)
    i = i + 1
    if b < 64 {
      c = c + (b >> 3) + 1
      if bit != 0 {
        add_range_exclusive(ranges, start, c)
      }
      bit = bit ^ 1
      let start2 = c
      c = c + (b & 7) + 1
      if bit != 0 {
        add_range_exclusive(ranges, start2, c)
      }
      bit = bit ^ 1
      continue
    }
    if b >= 0x80 {
      c = c + (b - 0x80) + 1
    } else if b < 0x60 {
      c = c + (((b - 0x40) << 8) | byte_at(data, i)) + 1
      i = i + 1
    } else {
      c = c +
        (((b - 0x60) << 16) | (byte_at(data, i) << 8) | byte_at(data, i + 1)) +
        1
      i = i + 2
    }
    if bit != 0 {
      add_range_exclusive(ranges, start, c)
    }
    bit = bit ^ 1
  }
  ranges_normalize(ranges)
}

///|
fn unicode_script_ranges(script_idx : Int, is_ext : Bool) -> Array[Range] {
  let mut ranges : Array[Range] = []
  let data = unicode_script_table
  let mut i = 0
  let mut c = 0
  while i < data.length() {
    let b = byte_at(data, i)
    i = i + 1
    let typ = b >> 7
    let mut n = b & 0x7f
    if n < 96 {
      ()
    } else if n < 112 {
      n = ((n - 96) << 8) | byte_at(data, i)
      i = i + 1
      n = n + 96
    } else {
      n = ((n - 112) << 16) | (byte_at(data, i) << 8) | byte_at(data, i + 1)
      i = i + 2
      n = n + 96 + (1 << 12)
    }
    let end = c + n + 1
    if typ != 0 {
      let v = byte_at(data, i)
      i = i + 1
      if v == script_idx || script_idx == UNICODE_SCRIPT_Unknown {
        add_range_exclusive(ranges, c, end)
      }
    }
    c = end
  }
  if script_idx == UNICODE_SCRIPT_Unknown {
    ranges = ranges_invert(ranges)
  }
  if !is_ext {
    return ranges_normalize(ranges)
  }
  let is_common = script_idx == UNICODE_SCRIPT_Common ||
    script_idx == UNICODE_SCRIPT_Inherited
  let ext_ranges : Array[Range] = []
  let ext = unicode_script_ext_table
  i = 0
  c = 0
  while i < ext.length() {
    let b = byte_at(ext, i)
    i = i + 1
    let mut n = 0
    if b < 128 {
      n = b
    } else if b < 128 + 64 {
      n = ((b - 128) << 8) | byte_at(ext, i)
      i = i + 1
      n = n + 128
    } else {
      n = ((b - 128 - 64) << 16) | (byte_at(ext, i) << 8) | byte_at(ext, i + 1)
      i = i + 2
      n = n + 128 + (1 << 14)
    }
    let end = c + n + 1
    let v_len = byte_at(ext, i)
    i = i + 1
    if is_common {
      if v_len != 0 {
        add_range_exclusive(ext_ranges, c, end)
      }
    } else {
      let mut j = 0
      while j < v_len {
        if byte_at(ext, i + j) == script_idx {
          add_range_exclusive(ext_ranges, c, end)
          break
        }
        j = j + 1
      }
    }
    i = i + v_len
    c = end
  }
  if is_common {
    let inverted = ranges_invert(ext_ranges)
    return ranges_inter(ranges_normalize(ranges), inverted)
  }
  ranges_union(ranges_normalize(ranges), ranges_normalize(ext_ranges))
}

///|
fn unicode_script_set(name : String, is_ext : Bool) -> ClassSet? {
  let cache_ref =
    if is_ext {
      unicode_script_ext_set_cache_ref
    } else {
      unicode_script_set_cache_ref
    }
  match unicode_class_set_cache_get(cache_ref, name) {
    Some(value) => return Some(value)
    None => ()
  }
  match unicode_script_name_index(name) {
    None => None
    Some(index) => {
      let ranges = unicode_script_ranges(index, is_ext)
      let result = ClassSet::{ ranges, strings: [] }
      unicode_class_set_cache_set(cache_ref, name, result)
      Some(result)
    }
  }
}

///|
priv enum PropOp {
  Gc(Int)
  Prop(Int)
  Case(Int)
  Union
  Inter
  Xor
  Invert
}

///|
fn ranges_xor(a : Array[Range], b : Array[Range]) -> Array[Range] {
  let left = ranges_sub(a, b)
  let right = ranges_sub(b, a)
  ranges_union(left, right)
}

///|
fn unicode_case_ranges(case_mask : Int) -> Array[Range] {
  let ranges : Array[Range] = []
  if case_mask == 0 {
    return ranges
  }
  let run_mask : ReadOnlyArray[Int] = [
    (1 << RUN_TYPE_U) |
    (1 << RUN_TYPE_UF) |
    (1 << RUN_TYPE_UL) |
    (1 << RUN_TYPE_LSU) |
    (1 << RUN_TYPE_U2L_399_EXT2) |
    (1 << RUN_TYPE_UF_D20) |
    (1 << RUN_TYPE_UF_D1_EXT) |
    (1 << RUN_TYPE_U_EXT) |
    (1 << RUN_TYPE_UF_EXT2) |
    (1 << RUN_TYPE_UF_EXT3),
    (1 << RUN_TYPE_L) |
    (1 << RUN_TYPE_LF) |
    (1 << RUN_TYPE_UL) |
    (1 << RUN_TYPE_LSU) |
    (1 << RUN_TYPE_U2L_399_EXT2) |
    (1 << RUN_TYPE_LF_EXT) |
    (1 << RUN_TYPE_LF_EXT2),
    (1 << RUN_TYPE_UF) |
    (1 << RUN_TYPE_LF) |
    (1 << RUN_TYPE_UL) |
    (1 << RUN_TYPE_LSU) |
    (1 << RUN_TYPE_U2L_399_EXT2) |
    (1 << RUN_TYPE_LF_EXT) |
    (1 << RUN_TYPE_LF_EXT2) |
    (1 << RUN_TYPE_UF_D20) |
    (1 << RUN_TYPE_UF_D1_EXT) |
    (1 << RUN_TYPE_UF_EXT2) |
    (1 << RUN_TYPE_UF_EXT3),
  ]
  let mut mask = 0
  let mut i = 0
  while i < 3 {
    if ((case_mask >> i) & 1) != 0 {
      mask = mask | run_mask[i]
    }
    i = i + 1
  }
  for v in case_conv_table1 {
    let typ = UInt::reinterpret_as_int((v >> (32 - 17 - 7 - 4)) & 0xfU)
    let code = UInt::reinterpret_as_int(v >> (32 - 17))
    let len = UInt::reinterpret_as_int((v >> (32 - 17 - 7)) & 0x7fU)
    if ((mask >> typ) & 1) == 0 {
      continue
    }
    if typ == RUN_TYPE_UL {
      if (case_mask & CASE_U) != 0 && (case_mask & (CASE_L | CASE_F)) != 0 {
        add_range_exclusive(ranges, code, code + len)
      } else {
        let start = code + (if (case_mask & CASE_U) != 0 { 1 } else { 0 })
        let mut cp = 0
        while cp < len {
          add_range_exclusive(ranges, start + cp, start + cp + 1)
          cp = cp + 2
        }
      }
    } else if typ == RUN_TYPE_LSU {
      if (case_mask & CASE_U) != 0 && (case_mask & (CASE_L | CASE_F)) != 0 {
        add_range_exclusive(ranges, code, code + len)
      } else {
        if (case_mask & CASE_U) == 0 {
          add_range_exclusive(ranges, code, code + 1)
        }
        add_range_exclusive(ranges, code + 1, code + 2)
        if (case_mask & CASE_U) != 0 {
          add_range_exclusive(ranges, code + 2, code + 3)
        }
      }
    } else {
      add_range_exclusive(ranges, code, code + len)
    }
  }
  ranges_normalize(ranges)
}

///|
fn unicode_prop_ops(ops : Array[PropOp]) -> Array[Range] {
  let stack : Array[Array[Range]] = []
  for op in ops {
    match op {
      Gc(mask) => stack.push(unicode_general_category_ranges(mask))
      Prop(idx) => stack.push(unicode_prop_ranges(idx))
      Case(mask) => stack.push(unicode_case_ranges(mask))
      Union =>
        if stack.length() >= 2 {
          let b = match stack.pop() {
            Some(value) => value
            None => []
          }
          let a = match stack.pop() {
            Some(value) => value
            None => []
          }
          stack.push(ranges_union(a, b))
        }
      Inter =>
        if stack.length() >= 2 {
          let b = match stack.pop() {
            Some(value) => value
            None => []
          }
          let a = match stack.pop() {
            Some(value) => value
            None => []
          }
          stack.push(ranges_inter(a, b))
        }
      Xor =>
        if stack.length() >= 2 {
          let b = match stack.pop() {
            Some(value) => value
            None => []
          }
          let a = match stack.pop() {
            Some(value) => value
            None => []
          }
          stack.push(ranges_xor(a, b))
        }
      Invert =>
        if stack.length() >= 1 {
          let a = match stack.pop() {
            Some(value) => value
            None => []
          }
          stack.push(ranges_invert(a))
        }
    }
  }
  if stack.is_empty() {
    []
  } else {
    stack[stack.length() - 1]
  }
}

///|
fn unicode_prop_set(name : String) -> ClassSet? {
  match unicode_class_set_cache_get(unicode_prop_set_cache_ref, name) {
    Some(value) => return Some(value)
    None => ()
  }
  let prop_index = unicode_prop_name_index(name)
  match prop_index {
    None => None
    Some(index) => {
      let prop_idx = index + UNICODE_PROP_ASCII_Hex_Digit
      let mut ranges : Array[Range] = []
      if prop_idx == UNICODE_PROP_ASCII {
        add_range_exclusive(ranges, 0x00, 0x7f + 1)
      } else if prop_idx == UNICODE_PROP_Any {
        add_range_exclusive(ranges, 0x00000, 0x10ffff + 1)
      } else if prop_idx == UNICODE_PROP_Assigned {
        ranges = unicode_prop_ops([Gc(1 << UNICODE_GC_Cn), Invert])
      } else if prop_idx == UNICODE_PROP_Math {
        ranges = unicode_prop_ops([
          Gc(1 << UNICODE_GC_Sm),
          Prop(UNICODE_PROP_Other_Math),
          Union,
        ])
      } else if prop_idx == UNICODE_PROP_Lowercase {
        ranges = unicode_prop_ops([
          Gc(1 << UNICODE_GC_Ll),
          Prop(UNICODE_PROP_Other_Lowercase),
          Union,
        ])
      } else if prop_idx == UNICODE_PROP_Uppercase {
        ranges = unicode_prop_ops([
          Gc(1 << UNICODE_GC_Lu),
          Prop(UNICODE_PROP_Other_Uppercase),
          Union,
        ])
      } else if prop_idx == UNICODE_PROP_Cased {
        ranges = unicode_prop_ops([
          Gc((1 << UNICODE_GC_Lu) | (1 << UNICODE_GC_Ll) | (1 << UNICODE_GC_Lt)),
          Prop(UNICODE_PROP_Other_Uppercase),
          Union,
          Prop(UNICODE_PROP_Other_Lowercase),
          Union,
        ])
      } else if prop_idx == UNICODE_PROP_Alphabetic {
        ranges = unicode_prop_ops([
          Gc(
            (1 << UNICODE_GC_Lu) |
            (1 << UNICODE_GC_Ll) |
            (1 << UNICODE_GC_Lt) |
            (1 << UNICODE_GC_Lm) |
            (1 << UNICODE_GC_Lo) |
            (1 << UNICODE_GC_Nl),
          ),
          Prop(UNICODE_PROP_Other_Uppercase),
          Union,
          Prop(UNICODE_PROP_Other_Lowercase),
          Union,
          Prop(UNICODE_PROP_Other_Alphabetic),
          Union,
        ])
      } else if prop_idx == UNICODE_PROP_Grapheme_Base {
        ranges = unicode_prop_ops([
          Gc(
            (1 << UNICODE_GC_Cc) |
            (1 << UNICODE_GC_Cf) |
            (1 << UNICODE_GC_Cs) |
            (1 << UNICODE_GC_Co) |
            (1 << UNICODE_GC_Cn) |
            (1 << UNICODE_GC_Zl) |
            (1 << UNICODE_GC_Zp) |
            (1 << UNICODE_GC_Me) |
            (1 << UNICODE_GC_Mn),
          ),
          Prop(UNICODE_PROP_Other_Grapheme_Extend),
          Union,
          Invert,
        ])
      } else if prop_idx == UNICODE_PROP_Grapheme_Extend {
        ranges = unicode_prop_ops([
          Gc((1 << UNICODE_GC_Me) | (1 << UNICODE_GC_Mn)),
          Prop(UNICODE_PROP_Other_Grapheme_Extend),
          Union,
        ])
      } else if prop_idx == UNICODE_PROP_XID_Start {
        ranges = unicode_prop_ops([
          Gc(
            (1 << UNICODE_GC_Lu) |
            (1 << UNICODE_GC_Ll) |
            (1 << UNICODE_GC_Lt) |
            (1 << UNICODE_GC_Lm) |
            (1 << UNICODE_GC_Lo) |
            (1 << UNICODE_GC_Nl),
          ),
          Prop(UNICODE_PROP_Other_ID_Start),
          Union,
          Prop(UNICODE_PROP_Pattern_Syntax),
          Prop(UNICODE_PROP_Pattern_White_Space),
          Union,
          Prop(UNICODE_PROP_XID_Start1),
          Union,
          Invert,
          Inter,
        ])
      } else if prop_idx == UNICODE_PROP_XID_Continue {
        ranges = unicode_prop_ops([
          Gc(
            (1 << UNICODE_GC_Lu) |
            (1 << UNICODE_GC_Ll) |
            (1 << UNICODE_GC_Lt) |
            (1 << UNICODE_GC_Lm) |
            (1 << UNICODE_GC_Lo) |
            (1 << UNICODE_GC_Nl) |
            (1 << UNICODE_GC_Mn) |
            (1 << UNICODE_GC_Mc) |
            (1 << UNICODE_GC_Nd) |
            (1 << UNICODE_GC_Pc),
          ),
          Prop(UNICODE_PROP_Other_ID_Start),
          Union,
          Prop(UNICODE_PROP_Other_ID_Continue),
          Union,
          Prop(UNICODE_PROP_Pattern_Syntax),
          Prop(UNICODE_PROP_Pattern_White_Space),
          Union,
          Prop(UNICODE_PROP_XID_Continue1),
          Union,
          Invert,
          Inter,
        ])
      } else if prop_idx == UNICODE_PROP_Changes_When_Uppercased {
        ranges = unicode_case_ranges(CASE_U)
      } else if prop_idx == UNICODE_PROP_Changes_When_Lowercased {
        ranges = unicode_case_ranges(CASE_L)
      } else if prop_idx == UNICODE_PROP_Changes_When_Casemapped {
        ranges = unicode_case_ranges(CASE_U | CASE_L | CASE_F)
      } else if prop_idx == UNICODE_PROP_Changes_When_Titlecased {
        ranges = unicode_prop_ops([
          Case(CASE_U),
          Prop(UNICODE_PROP_Changes_When_Titlecased1),
          Xor,
        ])
      } else if prop_idx == UNICODE_PROP_Changes_When_Casefolded {
        ranges = unicode_prop_ops([
          Case(CASE_F),
          Prop(UNICODE_PROP_Changes_When_Casefolded1),
          Xor,
        ])
      } else if prop_idx == UNICODE_PROP_Changes_When_NFKC_Casefolded {
        ranges = unicode_prop_ops([
          Case(CASE_F),
          Prop(UNICODE_PROP_Changes_When_NFKC_Casefolded1),
          Xor,
        ])
      } else if prop_idx == UNICODE_PROP_ID_Continue {
        ranges = unicode_prop_ops([
          Prop(UNICODE_PROP_ID_Start),
          Prop(UNICODE_PROP_ID_Continue1),
          Xor,
        ])
      } else if prop_idx < unicode_prop_tables.length() {
        ranges = unicode_prop_ranges(prop_idx)
      } else {
        return None
      }
      let result = ClassSet::{ ranges, strings: [] }
      unicode_class_set_cache_set(unicode_prop_set_cache_ref, name, result)
      Some(result)
    }
  }
}

///|
fn add_sequence(
  strings : Array[String],
  seen : Map[String, Bool],
  seq : Array[Int],
  len : Int,
) -> Unit {
  let sb = StringBuilder::new()
  let mut i = 0
  while i < len {
    sb.write_char(Int::unsafe_to_char(seq[i]))
    i = i + 1
  }
  let value = sb.to_string()
  match seen.get(value) {
    Some(_) => ()
    None => {
      seen[value] = true
      strings.push(value)
    }
  }
}

///|
fn unicode_sequence_prop_set(name : String) -> ClassSet? {
  match unicode_class_set_cache_get(unicode_sequence_set_cache_ref, name) {
    Some(value) => return Some(value)
    None => ()
  }
  let seq_index = unicode_sequence_name_index(name)
  match seq_index {
    None => None
    Some(index) => {
      let strings : Array[String] = []
      let seen = Map::new()
      let seq : Array[Int] = Array::make(32, 0)
      match index {
        UNICODE_SEQUENCE_PROP_Basic_Emoji => {
          let ranges1 = unicode_prop_ranges(UNICODE_PROP_Basic_Emoji1)
          for r in ranges1 {
            let mut c = r.start
            while c <= r.end {
              seq[0] = c
              add_sequence(strings, seen, seq, 1)
              c = c + 1
            }
          }
          let ranges2 = unicode_prop_ranges(UNICODE_PROP_Basic_Emoji2)
          for r in ranges2 {
            let mut c = r.start
            while c <= r.end {
              seq[0] = c
              seq[1] = 0xfe0f
              add_sequence(strings, seen, seq, 2)
              c = c + 1
            }
          }
        }
        UNICODE_SEQUENCE_PROP_RGI_Emoji_Modifier_Sequence => {
          let ranges = unicode_prop_ranges(UNICODE_PROP_Emoji_Modifier_Base)
          for r in ranges {
            let mut c = r.start
            while c <= r.end {
              let mut j = 0
              while j < 5 {
                seq[0] = c
                seq[1] = 0x1f3fb + j
                add_sequence(strings, seen, seq, 2)
                j = j + 1
              }
              c = c + 1
            }
          }
        }
        UNICODE_SEQUENCE_PROP_RGI_Emoji_Flag_Sequence => {
          let ranges = unicode_prop_ranges(UNICODE_PROP_RGI_Emoji_Flag_Sequence)
          for r in ranges {
            let mut c = r.start
            while c <= r.end {
              let c0 = c / 26
              let c1 = c % 26
              seq[0] = 0x1f1e6 + c0
              seq[1] = 0x1f1e6 + c1
              add_sequence(strings, seen, seq, 2)
              c = c + 1
            }
          }
        }
        UNICODE_SEQUENCE_PROP_RGI_Emoji_ZWJ_Sequence => {
          let tab = unicode_rgi_emoji_zwj_sequence
          let mut i = 0
          while i < tab.length() {
            let len = byte_at(tab, i)
            i = i + 1
            let mut k = 0
            let mut mod = 0
            let mut mod_count = 0
            let mod_pos : FixedArray[Int] = [0, 0]
            let mut hc_pos = -1
            let mut j = 0
            while j < len {
              let mut code = byte_at(tab, i)
              let hi = byte_at(tab, i + 1)
              i = i + 2
              code = code | (hi << 8)
              let pres = code >> 15
              let mod1 = (code >> 13) & 3
              code = code & 0x1fff
              let c = if code < 0x1000 {
                code + 0x2000
              } else {
                0x1f000 + (code - 0x1000)
              }
              if c == 0x1f9b0 {
                hc_pos = k
              }
              seq[k] = c
              k = k + 1
              if mod1 != 0 {
                mod = mod1
                mod_pos[mod_count] = k
                mod_count = mod_count + 1
                seq[k] = 0
                k = k + 1
              }
              if pres != 0 {
                seq[k] = 0xfe0f
                k = k + 1
              }
              if j < len - 1 {
                seq[k] = 0x200d
                k = k + 1
              }
              j = j + 1
            }
            let n_mod = if mod == 1 {
              5
            } else if mod == 2 {
              25
            } else if mod == 3 {
              20
            } else {
              1
            }
            let n_hc = if hc_pos >= 0 { 4 } else { 1 }
            let mut hc_idx = 0
            while hc_idx < n_hc {
              let mut mod_idx = 0
              while mod_idx < n_mod {
                if hc_pos >= 0 {
                  seq[hc_pos] = 0x1f9b0 + hc_idx
                }
                if mod == 1 {
                  seq[mod_pos[0]] = 0x1f3fb + mod_idx
                } else if mod == 2 || mod == 3 {
                  let mut i0 = mod_idx / 5
                  let i1 = mod_idx % 5
                  if mod == 3 && i0 >= i1 {
                    i0 = i0 + 1
                  }
                  seq[mod_pos[0]] = 0x1f3fb + i0
                  seq[mod_pos[1]] = 0x1f3fb + i1
                }
                add_sequence(strings, seen, seq, k)
                mod_idx = mod_idx + 1
              }
              hc_idx = hc_idx + 1
            }
          }
        }
        UNICODE_SEQUENCE_PROP_RGI_Emoji_Tag_Sequence => {
          let tab = unicode_rgi_emoji_tag_sequence
          let mut i = 0
          while i < tab.length() {
            let mut k = 0
            seq[k] = 0x1f3f4
            k = k + 1
            while true {
              let c = byte_at(tab, i)
              i = i + 1
              if c == 0x00 {
                break
              }
              seq[k] = 0xe0000 + c
              k = k + 1
            }
            seq[k] = 0xe007f
            k = k + 1
            add_sequence(strings, seen, seq, k)
          }
        }
        UNICODE_SEQUENCE_PROP_Emoji_Keycap_Sequence => {
          let ranges = unicode_prop_ranges(UNICODE_PROP_Emoji_Keycap_Sequence)
          for r in ranges {
            let mut c = r.start
            while c <= r.end {
              seq[0] = c
              seq[1] = 0xfe0f
              seq[2] = 0x20e3
              add_sequence(strings, seen, seq, 3)
              c = c + 1
            }
          }
        }
        UNICODE_SEQUENCE_PROP_RGI_Emoji => {
          let props = [
            UNICODE_SEQUENCE_PROP_Basic_Emoji,
            UNICODE_SEQUENCE_PROP_Emoji_Keycap_Sequence,
            UNICODE_SEQUENCE_PROP_RGI_Emoji_Modifier_Sequence,
            UNICODE_SEQUENCE_PROP_RGI_Emoji_Flag_Sequence,
            UNICODE_SEQUENCE_PROP_RGI_Emoji_Tag_Sequence,
            UNICODE_SEQUENCE_PROP_RGI_Emoji_ZWJ_Sequence,
          ]
          for prop in props {
            match unicode_sequence_prop_set(name_for_sequence(prop)) {
              Some(value) =>
                for s in value.strings {
                  match seen.get(s) {
                    Some(_) => ()
                    None => {
                      seen[s] = true
                      strings.push(s)
                    }
                  }
                }
              None => ()
            }
          }
        }
        _ => ()
      }
      let result = ClassSet::{ ranges: [], strings }
      unicode_class_set_cache_set(unicode_sequence_set_cache_ref, name, result)
      Some(result)
    }
  }
}

///|
fn name_for_sequence(idx : Int) -> String {
  let names : ReadOnlyArray[String] = [
    "Basic_Emoji", "Emoji_Keycap_Sequence", "RGI_Emoji_Modifier_Sequence", "RGI_Emoji_Flag_Sequence",
    "RGI_Emoji_Tag_Sequence", "RGI_Emoji_ZWJ_Sequence", "RGI_Emoji",
  ]
  names[idx]
}
