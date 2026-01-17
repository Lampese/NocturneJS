///|
fn case_conv_entry(
  code : Int,
  conv_type : Int,
  idx : Int,
  v : UInt,
) -> (Int, Int, Int, Int) {
  let is_lower = conv_type != 0
  let typ = UInt::reinterpret_as_int((v >> (32 - 17 - 7 - 4)) & 0xfU)
  let data = (UInt::reinterpret_as_int(v & 0xfU) << 8) |
    Byte::to_int(case_conv_table2[idx])
  let base = UInt::reinterpret_as_int(v >> (32 - 17))
  let mut r0 = code
  let mut r1 = 0
  let mut r2 = 0
  let mut len = 1
  match typ {
    RUN_TYPE_U | RUN_TYPE_L | RUN_TYPE_UF | RUN_TYPE_LF => {
      let target = typ & 1
      if conv_type == target || (typ >= RUN_TYPE_UF && conv_type == 2) {
        let mapped = UInt::reinterpret_as_int(
          case_conv_table1[data] >> (32 - 17),
        )
        r0 = code - base + mapped
      }
    }
    RUN_TYPE_UL => {
      let a = code - base
      let expected = if is_lower { 0 } else { 1 }
      if (a & 1) == expected {
        r0 = (a ^ 1) + base
      }
    }
    RUN_TYPE_LSU => {
      let a = code - base
      let delta = if is_lower { 1 } else { -1 }
      if a == 1 {
        r0 = code + delta
      } else if a == (if is_lower { 0 } else { 2 }) {
        r0 = code + delta * 2
      }
    }
    RUN_TYPE_U2L_399_EXT2 =>
      if !is_lower {
        r0 = code - base + case_conv_ext[data >> 6]
        r1 = 0x399
        len = 2
      } else {
        r0 = code - base + case_conv_ext[data & 0x3f]
      }
    RUN_TYPE_UF_D20 =>
      if conv_type != 1 {
        r0 = data + (if conv_type == 2 { 0x20 } else { 0 })
      }
    RUN_TYPE_UF_D1_EXT =>
      if conv_type != 1 {
        r0 = case_conv_ext[data] + (if conv_type == 2 { 1 } else { 0 })
      }
    RUN_TYPE_U_EXT | RUN_TYPE_LF_EXT => {
      let required = typ - RUN_TYPE_U_EXT
      if (if is_lower { 1 } else { 0 }) == required {
        r0 = case_conv_ext[data]
      }
    }
    RUN_TYPE_LF_EXT2 =>
      if is_lower {
        r0 = code - base + case_conv_ext[data >> 6]
        r1 = case_conv_ext[data & 0x3f]
        len = 2
      }
    RUN_TYPE_UF_EXT2 =>
      if conv_type != 1 {
        r0 = code - base + case_conv_ext[data >> 6]
        r1 = case_conv_ext[data & 0x3f]
        if conv_type == 2 {
          r0 = case_conv1(r0, 1)
          r1 = case_conv1(r1, 1)
        }
        len = 2
      }
    RUN_TYPE_UF_EXT3 =>
      if conv_type != 1 {
        r0 = case_conv_ext[data >> 8]
        r1 = case_conv_ext[(data >> 4) & 0xf]
        r2 = case_conv_ext[data & 0xf]
        if conv_type == 2 {
          r0 = case_conv1(r0, 1)
          r1 = case_conv1(r1, 1)
          r2 = case_conv1(r2, 1)
        }
        len = 3
      }
    _ => ()
  }
  (len, r0, r1, r2)
}

///|
fn case_conv(code : Int, conv_type : Int) -> (Int, Int, Int, Int) {
  if code < 128 {
    let mut c = code
    if conv_type != 0 {
      if c >= 65 && c <= 90 {
        c = c + 32
      }
    } else if c >= 97 && c <= 122 {
      c = c - 32
    }
    return (1, c, 0, 0)
  }
  let mut idx_min = 0
  let mut idx_max = case_conv_table1.length() - 1
  while idx_min <= idx_max {
    let idx = (idx_max + idx_min) / 2
    let v = case_conv_table1[idx]
    let base = UInt::reinterpret_as_int(v >> (32 - 17))
    let len = UInt::reinterpret_as_int((v >> (32 - 17 - 7)) & 0x7fU)
    if code < base {
      idx_max = idx - 1
    } else if code >= base + len {
      idx_min = idx + 1
    } else {
      return case_conv_entry(code, conv_type, idx, v)
    }
  }
  (1, code, 0, 0)
}

///|
fn case_conv1(code : Int, conv_type : Int) -> Int {
  let (_, r0, _, _) = case_conv(code, conv_type)
  r0
}

///|
fn case_fold(code : Int, is_unicode : Bool) -> Int {
  if is_unicode {
    let (len, r0, _, _) = case_conv(code, 2)
    if len == 1 {
      return r0
    }
    // QuickJS special-case handling for multi-character folds.
    if code == 0xfb06 {
      return 0xfb05
    } else if code == 0x01fd3 {
      return 0x390
    } else if code == 0x01fe3 {
      return 0x3b0
    }
    code
  } else {
    if code < 128 {
      if code >= 97 && code <= 122 {
        return code - 32
      }
      return code
    }
    let (len, r0, _, _) = case_conv(code, 0)
    if len == 1 && r0 >= 128 {
      r0
    } else {
      code
    }
  }
}

///|
fn unicode_canonicalize(code : Int, is_unicode : Bool) -> Int {
  if code < 128 {
    if is_unicode {
      if code >= 65 && code <= 90 {
        return code + 32
      }
    } else if code >= 97 && code <= 122 {
      return code - 32
    }
    return code
  }
  case_fold(code, is_unicode)
}
