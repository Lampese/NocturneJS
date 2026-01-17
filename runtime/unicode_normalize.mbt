///|
const UNICODE_NFC : Int = 0

///|
const UNICODE_NFD : Int = 1

///|
const UNICODE_NFKC : Int = 2

///|
const UNICODE_NFKD : Int = 3

///|
const UNICODE_DECOMP_LEN_MAX : Int = 18

///|
const DECOMP_TYPE_C1 : Int = 0

///|
const DECOMP_TYPE_L1 : Int = 1

///|
const DECOMP_TYPE_L2 : Int = 2

///|
const DECOMP_TYPE_L3 : Int = 3

///|
const DECOMP_TYPE_L4 : Int = 4

///|
const DECOMP_TYPE_L5 : Int = 5

///|
const DECOMP_TYPE_L6 : Int = 6

///|
const DECOMP_TYPE_L7 : Int = 7

///|
const DECOMP_TYPE_LL1 : Int = 8

///|
const DECOMP_TYPE_LL2 : Int = 9

///|
const DECOMP_TYPE_S1 : Int = 10

///|
const DECOMP_TYPE_S2 : Int = 11

///|
const DECOMP_TYPE_S3 : Int = 12

///|
const DECOMP_TYPE_S4 : Int = 13

///|
const DECOMP_TYPE_S5 : Int = 14

///|
const DECOMP_TYPE_I1 : Int = 15

///|
const DECOMP_TYPE_I2_0 : Int = 16

///|
const DECOMP_TYPE_I2_1 : Int = 17

///|
const DECOMP_TYPE_I3_1 : Int = 18

///|
const DECOMP_TYPE_I3_2 : Int = 19

///|
const DECOMP_TYPE_I4_1 : Int = 20

///|
const DECOMP_TYPE_I4_2 : Int = 21

///|
const DECOMP_TYPE_B1 : Int = 22

///|
const DECOMP_TYPE_B2 : Int = 23

///|
const DECOMP_TYPE_B3 : Int = 24

///|
const DECOMP_TYPE_B4 : Int = 25

///|
const DECOMP_TYPE_B5 : Int = 26

///|
const DECOMP_TYPE_B6 : Int = 27

///|
const DECOMP_TYPE_B7 : Int = 28

///|
const DECOMP_TYPE_B8 : Int = 29

///|
const DECOMP_TYPE_B18 : Int = 30

///|
const DECOMP_TYPE_LS2 : Int = 31

///|
const DECOMP_TYPE_PAT3 : Int = 32

///|
const DECOMP_TYPE_S2_UL : Int = 33

///|
const DECOMP_TYPE_LS2_UL : Int = 34

///|
const UNICODE_INDEX_BLOCK_LEN : Int = 32

///|
fn unicode_get_short_code(code : Int) -> Int {
  if code < 0x80 {
    code
  } else if code < 0x80 + 0x50 {
    code - 0x80 + 0x300
  } else {
    let unicode_short_table : Array[Int] = [0x2044, 0x2215]
    unicode_short_table[code - 0x80 - 0x50]
  }
}

///|
fn unicode_get_lower_simple(code : Int) -> Int {
  if code < 0x100 || (code >= 0x410 && code <= 0x42f) {
    code + 0x20
  } else {
    code + 1
  }
}

///|
fn unicode_get16(data : Bytes, index : Int) -> Int {
  byte_at(data, index) | (byte_at(data, index + 1) << 8)
}

///|
fn unicode_decomp_entry(
  res : Array[Int],
  code_point : Int,
  idx : Int,
  base : Int,
  len : Int,
  typ : Int,
) -> Int {
  if typ == DECOMP_TYPE_C1 {
    res[0] = unicode_decomp_table2[idx]
    return 1
  }
  let mut d = unicode_decomp_table2[idx]
  match typ {
    DECOMP_TYPE_L1
    | DECOMP_TYPE_L2
    | DECOMP_TYPE_L3
    | DECOMP_TYPE_L4
    | DECOMP_TYPE_L5
    | DECOMP_TYPE_L6
    | DECOMP_TYPE_L7 => {
      let l = typ - DECOMP_TYPE_L1 + 1
      d = d + (code_point - base) * l * 2
      let mut i = 0
      while i < l {
        let v = unicode_get16(unicode_decomp_data, d + 2 * i)
        if v == 0 {
          return 0
        }
        res[i] = v
        i = i + 1
      }
      l
    }
    DECOMP_TYPE_LL1 | DECOMP_TYPE_LL2 => {
      let l = typ - DECOMP_TYPE_LL1 + 1
      let mut k = (code_point - base) * l
      let p = len * l * 2
      let mut i = 0
      while i < l {
        let hi = (
            (byte_at(unicode_decomp_data, d + p + k / 4) >> (k % 4 * 2)) & 3
          ) <<
          16
        let v = unicode_get16(unicode_decomp_data, d + 2 * k) | hi
        if v == 0 {
          return 0
        }
        res[i] = v
        k = k + 1
        i = i + 1
      }
      l
    }
    DECOMP_TYPE_S1
    | DECOMP_TYPE_S2
    | DECOMP_TYPE_S3
    | DECOMP_TYPE_S4
    | DECOMP_TYPE_S5 => {
      let l = typ - DECOMP_TYPE_S1 + 1
      d = d + (code_point - base) * l
      let mut i = 0
      while i < l {
        let v = unicode_get_short_code(byte_at(unicode_decomp_data, d + i))
        if v == 0 {
          return 0
        }
        res[i] = v
        i = i + 1
      }
      l
    }
    DECOMP_TYPE_I1
    | DECOMP_TYPE_I2_0
    | DECOMP_TYPE_I2_1
    | DECOMP_TYPE_I3_1
    | DECOMP_TYPE_I3_2
    | DECOMP_TYPE_I4_1
    | DECOMP_TYPE_I4_2 => {
      let l = if typ == DECOMP_TYPE_I1 {
        1
      } else {
        2 + ((typ - DECOMP_TYPE_I2_0) >> 1)
      }
      let p = if typ == DECOMP_TYPE_I1 {
        0
      } else {
        ((typ - DECOMP_TYPE_I2_0) & 1) + (if l > 2 { 1 } else { 0 })
      }
      let mut i = 0
      while i < l {
        let mut v = unicode_get16(unicode_decomp_data, d + 2 * i)
        if i == p {
          v = v + (code_point - base)
        }
        res[i] = v
        i = i + 1
      }
      l
    }
    DECOMP_TYPE_B18
    | DECOMP_TYPE_B1
    | DECOMP_TYPE_B2
    | DECOMP_TYPE_B3
    | DECOMP_TYPE_B4
    | DECOMP_TYPE_B5
    | DECOMP_TYPE_B6
    | DECOMP_TYPE_B7
    | DECOMP_TYPE_B8 => {
      let l = if typ == DECOMP_TYPE_B18 { 18 } else { typ - DECOMP_TYPE_B1 + 1 }
      let c_min = unicode_get16(unicode_decomp_data, d)
      d = d + 2 + (code_point - base) * l
      let mut i = 0
      while i < l {
        let mut v = byte_at(unicode_decomp_data, d + i)
        if v == 0xff {
          v = 0x20
        } else {
          v = v + c_min
        }
        res[i] = v
        i = i + 1
      }
      l
    }
    DECOMP_TYPE_LS2 => {
      d = d + (code_point - base) * 3
      let first = unicode_get16(unicode_decomp_data, d)
      if first == 0 {
        return 0
      }
      res[0] = first
      res[1] = unicode_get_short_code(byte_at(unicode_decomp_data, d + 2))
      2
    }
    DECOMP_TYPE_PAT3 => {
      res[0] = unicode_get16(unicode_decomp_data, d)
      res[2] = unicode_get16(unicode_decomp_data, d + 2)
      d = d + 4 + (code_point - base) * 2
      res[1] = unicode_get16(unicode_decomp_data, d)
      3
    }
    DECOMP_TYPE_S2_UL | DECOMP_TYPE_LS2_UL => {
      let c1 = code_point - base
      let mut c = 0
      if typ == DECOMP_TYPE_S2_UL {
        d = d + Int::land(c1, -2)
        c = unicode_get_short_code(byte_at(unicode_decomp_data, d))
        d = d + 1
      } else {
        d = d + (c1 >> 1) * 3
        c = unicode_get16(unicode_decomp_data, d)
        d = d + 2
      }
      if (c1 & 1) != 0 {
        c = unicode_get_lower_simple(c)
      }
      res[0] = c
      res[1] = unicode_get_short_code(byte_at(unicode_decomp_data, d))
      2
    }
    _ => 0
  }
}

///|
fn unicode_decomp_char(
  res : Array[Int],
  code_point : Int,
  is_compat : Bool,
) -> Int {
  let mut idx_min = 0
  let mut idx_max = unicode_decomp_table1.length() - 1
  while idx_min <= idx_max {
    let idx = (idx_max + idx_min) / 2
    let v = unicode_decomp_table1[idx]
    let base = UInt::reinterpret_as_int(v >> (32 - 18))
    let len = UInt::reinterpret_as_int((v >> (32 - 18 - 7)) & 0x7fU)
    if code_point < base {
      idx_max = idx - 1
    } else if code_point >= base + len {
      idx_min = idx + 1
    } else {
      let entry_is_compat = (v & 1U) != 0U
      if !is_compat && entry_is_compat {
        break
      }
      let typ = UInt::reinterpret_as_int((v >> (32 - 18 - 7 - 6)) & 0x3fU)
      return unicode_decomp_entry(res, code_point, idx, base, len, typ)
    }
  }
  0
}

///|
fn unicode_compose_pair(c0 : Int, c1 : Int) -> Int {
  let mut idx_min = 0
  let mut idx_max = unicode_comp_table.length() - 1
  let pair : Array[Int] = [0, 0]
  while idx_min <= idx_max {
    let idx = (idx_max + idx_min) / 2
    let idx1 = unicode_comp_table[idx]
    let d_idx = idx1 >> 6
    let d_offset = idx1 & 0x3f
    let v = unicode_decomp_table1[d_idx]
    let base = UInt::reinterpret_as_int(v >> (32 - 18))
    let len = UInt::reinterpret_as_int((v >> (32 - 18 - 7)) & 0x7fU)
    let typ = UInt::reinterpret_as_int((v >> (32 - 18 - 7 - 6)) & 0x3fU)
    let ch = base + d_offset
    let _ = unicode_decomp_entry(pair, ch, d_idx, base, len, typ)
    let mut d = c0 - pair[0]
    if d == 0 {
      d = c1 - pair[1]
    }
    if d < 0 {
      idx_max = idx - 1
    } else if d > 0 {
      idx_min = idx + 1
    } else {
      return ch
    }
  }
  0
}

///|
fn get_le24(data : Bytes, index : Int) -> Int {
  byte_at(data, index) |
  (byte_at(data, index + 1) << 8) |
  (byte_at(data, index + 2) << 16)
}

///|
fn get_index_pos(code_point : Int, index_table : Bytes) -> (Int, Int)? {
  let mut idx_min = 0
  let mut v = get_le24(index_table, 0)
  let mut code = v & ((1 << 21) - 1)
  if code_point < code {
    return Some((0, 0))
  }
  let mut idx_max = index_table.length() / 3 - 1
  code = get_le24(index_table, idx_max * 3) & ((1 << 21) - 1)
  if code_point >= code {
    return None
  }
  while idx_max - idx_min > 1 {
    let idx = (idx_max + idx_min) / 2
    v = get_le24(index_table, idx * 3)
    code = v & ((1 << 21) - 1)
    if code_point < code {
      idx_max = idx
    } else {
      idx_min = idx
    }
  }
  v = get_le24(index_table, idx_min * 3)
  code = v & ((1 << 21) - 1)
  let pos = (idx_min + 1) * UNICODE_INDEX_BLOCK_LEN + (v >> 21)
  Some((code, pos))
}

///|
fn unicode_get_cc(code_point : Int) -> Int {
  match get_index_pos(code_point, unicode_cc_index) {
    None => 0
    Some((base_code, pos)) => {
      let mut code = base_code
      let mut p = pos
      while true {
        let b = byte_at(unicode_cc_table, p)
        p = p + 1
        let typ = b >> 6
        let mut n = b & 0x3f
        if n < 48 {

        } else if n < 56 {
          n = (n - 48) << 8
          n = n | byte_at(unicode_cc_table, p)
          p = p + 1
          n = n + 48
        } else {
          n = (n - 56) << 8
          n = n | (byte_at(unicode_cc_table, p) << 8)
          p = p + 1
          n = n | byte_at(unicode_cc_table, p)
          p = p + 1
          n = n + 48 + (1 << 11)
        }
        if typ <= 1 {
          p = p + 1
        }
        let c1 = code + n + 1
        if code_point < c1 {
          match typ {
            0 => return byte_at(unicode_cc_table, p - 1)
            1 => return byte_at(unicode_cc_table, p - 1) + code_point - code
            2 => return 0
            _ => return 230
          }
        }
        code = c1
      }
      0
    }
  }
}

///|
fn sort_cc(buf : Array[Int]) -> Unit {
  let len = buf.length()
  let mut i = 0
  while i < len {
    let cc = unicode_get_cc(buf[i])
    if cc != 0 {
      let start = i
      let mut j = i + 1
      while j < len {
        let ch1 = buf[j]
        let cc1 = unicode_get_cc(ch1)
        if cc1 == 0 {
          break
        }
        let mut k = j - 1
        while k >= start {
          if unicode_get_cc(buf[k]) <= cc1 {
            break
          }
          buf[k + 1] = buf[k]
          k = k - 1
        }
        buf[k + 1] = ch1
        j = j + 1
      }
      i = j + 1
    } else {
      i = i + 1
    }
  }
}

///|
fn to_nfd_rec(
  out : Array[Int],
  src : Array[Int],
  start : Int,
  length : Int,
  is_compat : Bool,
) -> Unit {
  let mut i = 0
  while i < length {
    let c = src[start + i]
    if c >= 0xac00 && c < 0xd7a4 {
      let v = c - 0xac00
      out.push(0x1100 + v / 588)
      out.push(0x1161 + v % 588 / 28)
      let t = v % 28
      if t != 0 {
        out.push(0x11a7 + t)
      }
    } else {
      let res = Array::make(UNICODE_DECOMP_LEN_MAX, 0)
      let l = unicode_decomp_char(res, c, is_compat)
      if l > 0 {
        to_nfd_rec(out, res, 0, l, is_compat)
      } else {
        out.push(c)
      }
    }
    i = i + 1
  }
}

///|
fn compose_pair(c0 : Int, c1 : Int) -> Int {
  if c0 >= 0x1100 && c0 < 0x1100 + 19 && c1 >= 0x1161 && c1 < 0x1161 + 21 {
    0xac00 + (c0 - 0x1100) * 588 + (c1 - 0x1161) * 28
  } else if c0 >= 0xac00 &&
    c0 < 0xac00 + 11172 &&
    (c0 - 0xac00) % 28 == 0 &&
    c1 >= 0x11a7 &&
    c1 < 0x11a7 + 28 {
    c0 + c1 - 0x11a7
  } else {
    unicode_compose_pair(c0, c1)
  }
}

///|
fn unicode_normalize(src : Array[Int], n_type : Int) -> Array[Int] {
  let is_compat = n_type >> 1 != 0
  if n_type == UNICODE_NFC {
    let mut all_latin1 = true
    for c in src {
      if c >= 0x100 {
        all_latin1 = false
        break
      }
    }
    if all_latin1 {
      return src.copy()
    }
  }
  let buf : Array[Int] = []
  to_nfd_rec(buf, src, 0, src.length(), is_compat)
  sort_cc(buf)
  if buf.length() <= 1 || (n_type & 1) != 0 {
    return buf
  }
  let mut i = 1
  let mut out_len = 1
  while i < buf.length() {
    let mut last_cc = unicode_get_cc(buf[i])
    let mut starter_pos = out_len - 1
    while starter_pos >= 0 {
      let cc = unicode_get_cc(buf[starter_pos])
      if cc == 0 {
        break
      }
      if cc >= last_cc {
        starter_pos = -1
        break
      }
      last_cc = 256
      starter_pos = starter_pos - 1
    }
    if starter_pos >= 0 {
      let composed = compose_pair(buf[starter_pos], buf[i])
      if composed != 0 {
        buf[starter_pos] = composed
        i = i + 1
        continue
      }
    }
    buf[out_len] = buf[i]
    out_len = out_len + 1
    i = i + 1
  }
  let result : Array[Int] = []
  let mut k = 0
  while k < out_len {
    result.push(buf[k])
    k = k + 1
  }
  result
}

///|
fn string_to_codepoints(value : String) -> Array[Int] {
  let result : Array[Int] = []
  let mut index = 0
  while index < value.length() {
    match read_codepoint(value, index, true) {
      Some((code, next_index)) => {
        result.push(code)
        index = next_index
      }
      None => break
    }
  }
  result
}

///|
fn string_from_codepoints_array(values : Array[Int]) -> String {
  let units : Array[Int] = []
  for code in values {
    append_code_point_units(units, code)
  }
  string_from_code_units(units)
}

///|
fn unicode_normalize_string(value : String, n_type : Int) -> String {
  let src = string_to_codepoints(value)
  let out = unicode_normalize(src, n_type)
  string_from_codepoints_array(out)
}
