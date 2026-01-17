///|
let case_ignorable_ranges : Array[Range] = unicode_prop_ranges(
  UNICODE_PROP_Case_Ignorable,
)

///|
let cased_ranges : Array[Range] = match unicode_prop_set("Cased") {
  Some(set) => set.ranges
  None => []
}

///|
fn unicode_is_case_ignorable(code : Int) -> Bool {
  ranges_contains(case_ignorable_ranges, code)
}

///|
fn unicode_is_cased(code : Int) -> Bool {
  ranges_contains(cased_ranges, code)
}

///|
fn string_prev_codepoint(value : String, index : Int) -> (Int, Int) {
  if index <= 0 {
    return (0, 0)
  }
  let mut idx = index - 1
  let mut c = UInt16::to_int(value.code_unit_at(idx))
  if is_trail_surrogate(c) && idx > 0 {
    let c1 = UInt16::to_int(value.code_unit_at(idx - 1))
    if is_lead_surrogate(c1) {
      let high = c1 - 0xD800
      let low = c - 0xDC00
      c = 0x10000 + (high << 10) + low
      idx = idx - 1
    }
  }
  (c, idx)
}

///|
fn test_final_sigma(value : String, sigma_pos : Int) -> Bool {
  let mut k = sigma_pos
  let mut c1 = 0
  while true {
    let (code, next_k) = string_prev_codepoint(value, k)
    k = next_k
    if code == 0 {
      return false
    }
    if !unicode_is_case_ignorable(code) {
      c1 = code
      break
    }
  }
  if !unicode_is_cased(c1) {
    return false
  }
  let mut idx = sigma_pos + 1
  while idx < value.length() {
    match read_codepoint(value, idx, true) {
      Some((code, next_idx)) => {
        idx = next_idx
        if !unicode_is_case_ignorable(code) {
          return !unicode_is_cased(code)
        }
      }
      None => break
    }
  }
  true
}

///|
fn string_case_convert(value : String, to_lower : Bool) -> String {
  if value.length() == 0 {
    return value
  }
  let units : Array[Int] = []
  let mut index = 0
  while index < value.length() {
    match read_codepoint(value, index, true) {
      Some((code, next_index)) => {
        let (len, r0, r1, r2) = if to_lower &&
          code == 0x3a3 &&
          test_final_sigma(value, index) {
          (1, 0x3c2, 0, 0)
        } else {
          case_conv(code, if to_lower { 1 } else { 0 })
        }
        append_code_point_units(units, r0)
        if len > 1 {
          append_code_point_units(units, r1)
        }
        if len > 2 {
          append_code_point_units(units, r2)
        }
        index = next_index
      }
      None => break
    }
  }
  string_from_code_units(units)
}
