///|
fn parse_digits(text : String, start : Int, count : Int) -> Int? {
  if start < 0 || start + count > text.length() {
    return None
  }
  let mut value = 0
  let mut i = 0
  while i < count {
    let code = UInt16::to_int(text.code_unit_at(start + i))
    if !is_digit_code(code) {
      return None
    }
    value = value * 10 + (code - 48)
    i = i + 1
  }
  Some(value)
}

///|
fn parse_var_digits(
  text : String,
  start : Int,
  min_digits : Int,
  max_digits : Int,
) -> (Int, Int)? {
  if start < 0 || start >= text.length() {
    return None
  }
  let mut value = 0
  let mut i = start
  let mut count = 0
  let len = text.length()
  while i < len && count < max_digits {
    let code = UInt16::to_int(text.code_unit_at(i))
    if !is_digit_code(code) {
      break
    }
    value = value * 10 + (code - 48)
    i = i + 1
    count = count + 1
  }
  if count < min_digits {
    return None
  }
  Some((value, i))
}

///|
fn ascii_upper(code : Int) -> Int {
  if code >= 'a'.to_int() && code <= 'z'.to_int() {
    code - 32
  } else {
    code
  }
}

///|
fn string_get_digits(
  text : String,
  start : Int,
  min_digits : Int,
  max_digits : Int,
) -> (Int, Int, Int)? {
  let mut value = 0
  let mut p = start
  let mut count = 0
  let len = text.length()
  while p < len {
    let code = UInt16::to_int(text.code_unit_at(p))
    if !is_digit_code(code) {
      break
    }
    if value >= 100000000 {
      return None
    }
    value = value * 10 + (code - 48)
    p = p + 1
    count = count + 1
    if max_digits > 0 && count == max_digits {
      break
    }
  }
  if count < min_digits {
    return None
  }
  Some((value, p, count))
}

///|
fn string_contains_char(list : String, code : Int) -> Bool {
  let mut i = 0
  let len = list.length()
  while i < len {
    if UInt16::to_int(list.code_unit_at(i)) == code {
      return true
    }
    i = i + 1
  }
  false
}

///|
fn string_skip_until(text : String, start : Int, stoplist : String) -> Int {
  let mut p = start
  let len = text.length()
  while p < len {
    let code = UInt16::to_int(text.code_unit_at(p))
    if string_contains_char(stoplist, code) {
      break
    }
    p = p + 1
  }
  p
}

///|
fn string_skip_spaces(text : String, start : Int) -> Int {
  let mut p = start
  let len = text.length()
  while p < len && UInt16::to_int(text.code_unit_at(p)) == ' '.to_int() {
    p = p + 1
  }
  p
}

///|
fn string_skip_separators(text : String, start : Int) -> Int {
  let mut p = start
  let len = text.length()
  while p < len {
    let code = UInt16::to_int(text.code_unit_at(p))
    if code != '-'.to_int() &&
      code != '/'.to_int() &&
      code != '.'.to_int() &&
      code != ','.to_int() {
      break
    }
    p = p + 1
  }
  p
}

///|
fn string_match(text : String, start : Int, pattern : String) -> Int? {
  let mut p = start
  let mut i = 0
  let len = text.length()
  let pat_len = pattern.length()
  if start + pat_len > len {
    return None
  }
  while i < pat_len {
    let code = UInt16::to_int(text.code_unit_at(p))
    let pat_code = UInt16::to_int(pattern.code_unit_at(i))
    if ascii_upper(code) != ascii_upper(pat_code) {
      return None
    }
    p = p + 1
    i = i + 1
  }
  Some(p)
}

///|
fn string_get_month(text : String, start : Int) -> (Int, Int)? {
  if start + 3 > text.length() {
    return None
  }
  let token = text.unsafe_substring(start~, end=start + 3)
  match month_from_name(token) {
    Some(value) => Some((value, start + 3))
    None => None
  }
}

///|
fn string_get_tzabbr(text : String, start : Int) -> (Int, Int)? {
  let items : Array[(String, Int)] = [
    ("GMT", 0),
    ("UTC", 0),
    ("UT", 0),
    ("Z", 0),
    ("EDT", -4 * 60),
    ("EST", -5 * 60),
    ("CDT", -5 * 60),
    ("CST", -6 * 60),
    ("MDT", -6 * 60),
    ("MST", -7 * 60),
    ("PDT", -7 * 60),
    ("PST", -8 * 60),
    ("WET", 0),
    ("WEST", 1 * 60),
    ("CET", 1 * 60),
    ("CEST", 2 * 60),
    ("EET", 2 * 60),
    ("EEST", 3 * 60),
  ]
  let mut i = 0
  while i < items.length() {
    let (name, offset) = items[i]
    match string_match(text, start, name) {
      Some(next_pos) => return Some((offset, next_pos))
      None => ()
    }
    i = i + 1
  }
  None
}

///|
fn adjust_year_for_parse(value : Int) -> Int {
  if value < 100 {
    if value < 50 {
      value + 2000
    } else {
      value + 1900
    }
  } else {
    value
  }
}

///|
fn parse_fraction_ms(frac : String) -> Int {
  let mut value = 0
  let mut i = 0
  while i < 3 {
    let digit = if i < frac.length() {
      let code = UInt16::to_int(frac.code_unit_at(i))
      if is_digit_code(code) {
        code - 48
      } else {
        0
      }
    } else {
      0
    }
    value = value * 10 + digit
    i = i + 1
  }
  value
}

///|
fn string_get_milliseconds(text : String, start : Int) -> (Int, Int) {
  let len = text.length()
  if start >= len {
    return (0, start)
  }
  let code = UInt16::to_int(text.code_unit_at(start))
  if code != '.'.to_int() && code != ','.to_int() {
    return (0, start)
  }
  let mut p = start + 1
  let mut ms = 0
  let mut mul = 100
  let mut count = 0
  while p < len && count < 9 {
    let digit_code = UInt16::to_int(text.code_unit_at(p))
    if !is_digit_code(digit_code) {
      break
    }
    if mul > 0 {
      ms = ms + (digit_code - 48) * mul
      mul = mul / 10
    }
    p = p + 1
    count = count + 1
  }
  if p == start + 1 {
    return (0, start)
  }
  (ms, p)
}

///|
fn parse_time_token(token : String) -> (Int, Int, Int, Int)? {
  let len = token.length()
  if len < 5 {
    return None
  }
  let hour = match parse_digits(token, 0, 2) {
    Some(value) => value
    None => return None
  }
  let colon = UInt16::to_int(token.code_unit_at(2))
  if colon != ':'.to_int() {
    return None
  }
  let minute = match parse_digits(token, 3, 2) {
    Some(value) => value
    None => return None
  }
  let mut pos = 5
  let mut second = 0
  let mut ms = 0
  if pos < len && UInt16::to_int(token.code_unit_at(pos)) == ':'.to_int() {
    let sec = match parse_digits(token, pos + 1, 2) {
      Some(value) => value
      None => return None
    }
    second = sec
    pos = pos + 3
  }
  if pos < len && UInt16::to_int(token.code_unit_at(pos)) == '.'.to_int() {
    let start = pos + 1
    let mut end = start
    while end < len {
      let code = UInt16::to_int(token.code_unit_at(end))
      if !is_digit_code(code) {
        break
      }
      end = end + 1
    }
    if end == start {
      return None
    }
    let frac = token.unsafe_substring(start~, end~)
    ms = parse_fraction_ms(frac)
    pos = end
  }
  if pos != len {
    return None
  }
  Some((hour, minute, second, ms))
}

///|
fn parse_tz_offset(text : String, pos : Int) -> (Int, Int)? {
  if pos + 1 >= text.length() {
    return None
  }
  let hour = match parse_digits(text, pos, 2) {
    Some(value) => value
    None => return None
  }
  let mut p = pos + 2
  let mut minute = 0
  if p < text.length() && UInt16::to_int(text.code_unit_at(p)) == ':'.to_int() {
    minute = match parse_digits(text, p + 1, 2) {
      Some(value) => value
      None => return None
    }
    p = p + 3
  } else {
    if p + 1 >= text.length() {
      return None
    }
    minute = match parse_digits(text, p, 2) {
      Some(value) => value
      None => return None
    }
    p = p + 2
  }
  Some((hour * 60 + minute, p))
}

///|
fn parse_tz_offset_non_strict(text : String, pos : Int) -> (Int, Int)? {
  if pos >= text.length() {
    return None
  }
  let sign_code = UInt16::to_int(text.code_unit_at(pos))
  if sign_code != '+'.to_int() &&
    sign_code != '-'.to_int() &&
    sign_code != 'Z'.to_int() {
    return None
  }
  if sign_code == 'Z'.to_int() {
    return Some((0, pos + 1))
  }
  let sign = if sign_code == '+'.to_int() { 1 } else { -1 }
  let (raw_val, next_pos) = match parse_var_digits(text, pos + 1, 1, 4) {
    Some(value) => value
    None => return None
  }
  let mut p = next_pos
  let mut hour = raw_val
  let mut minute = 0
  if p < text.length() && UInt16::to_int(text.code_unit_at(p)) == ':'.to_int() {
    let (min_val, next_p) = match parse_var_digits(text, p + 1, 2, 2) {
      Some(value) => value
      None => return None
    }
    minute = min_val
    p = next_p
  } else if hour >= 100 {
    minute = hour % 100
    hour = hour / 100
  }
  if hour > 23 || minute > 59 {
    return None
  }
  Some((sign * (hour * 60 + minute), p))
}

///|
fn parse_iso_time_and_zone(
  text : String,
  start : Int,
) -> (Int, Int, Int, Int, Int, Int)? {
  let len = text.length()
  let mut tz_pos = len
  let mut i = start
  while i < len {
    let code = UInt16::to_int(text.code_unit_at(i))
    if code == 'Z'.to_int() || code == '+'.to_int() || code == '-'.to_int() {
      tz_pos = i
      break
    }
    i = i + 1
  }
  let time_part = text.unsafe_substring(start~, end=tz_pos)
  let (hour, minute, second, ms) = match parse_time_token(time_part) {
    Some(value) => value
    None => return None
  }
  let mut offset = 0
  let mut end_pos = tz_pos
  if tz_pos < len {
    let code = UInt16::to_int(text.code_unit_at(tz_pos))
    if code == 'Z'.to_int() {
      offset = 0
      end_pos = tz_pos + 1
    } else {
      let sign = if code == '+'.to_int() { 1 } else { -1 }
      let (mins, next_pos) = match parse_tz_offset(text, tz_pos + 1) {
        Some(value) => value
        None => return None
      }
      offset = sign * mins
      end_pos = next_pos
    }
  }
  Some((hour, minute, second, ms, offset, end_pos))
}

///|
fn parse_iso_date(text : String) -> Double? {
  let len = text.length()
  if len < 4 {
    return None
  }
  let mut pos = 0
  let mut year = 0
  let first = UInt16::to_int(text.code_unit_at(0))
  if first == '+'.to_int() || first == '-'.to_int() {
    if len < 7 {
      return None
    }
    let sign = if first == '+'.to_int() { 1 } else { -1 }
    let year_value = match parse_digits(text, 1, 6) {
      Some(value) => value
      None => return None
    }
    if sign == -1 && year_value == 0 {
      return None
    }
    year = sign * year_value
    pos = 7
  } else {
    let year_value = match parse_digits(text, 0, 4) {
      Some(value) => value
      None => return None
    }
    year = year_value
    pos = 4
  }
  let mut month = 1
  let mut day = 1
  if pos == len {
    return Some(
      date_make_utc_from_fields(
        Double::from_int(year),
        Double::from_int(month - 1),
        Double::from_int(day),
        0.0,
        0.0,
        0.0,
        0.0,
      ),
    )
  }
  let code = UInt16::to_int(text.code_unit_at(pos))
  if code == 'T'.to_int() {
    let (hour, minute, second, ms, offset, end_pos) = match
      parse_iso_time_and_zone(text, pos + 1) {
      Some(value) => value
      None => return None
    }
    if end_pos != len {
      return None
    }
    let base = date_make_utc_from_fields(
      Double::from_int(year),
      Double::from_int(month - 1),
      Double::from_int(day),
      Double::from_int(hour),
      Double::from_int(minute),
      Double::from_int(second),
      Double::from_int(ms),
    )
    return Some(base - Double::from_int(offset * 60000))
  }
  if code != '-'.to_int() {
    return None
  }
  month = match parse_digits(text, pos + 1, 2) {
    Some(value) => value
    None => return None
  }
  if month < 1 || month > 12 {
    return None
  }
  pos = pos + 3
  if pos == len {
    return Some(
      date_make_utc_from_fields(
        Double::from_int(year),
        Double::from_int(month - 1),
        Double::from_int(day),
        0.0,
        0.0,
        0.0,
        0.0,
      ),
    )
  }
  let code2 = UInt16::to_int(text.code_unit_at(pos))
  if code2 == 'T'.to_int() {
    let (hour, minute, second, ms, offset, end_pos) = match
      parse_iso_time_and_zone(text, pos + 1) {
      Some(value) => value
      None => return None
    }
    if end_pos != len {
      return None
    }
    let base = date_make_utc_from_fields(
      Double::from_int(year),
      Double::from_int(month - 1),
      Double::from_int(day),
      Double::from_int(hour),
      Double::from_int(minute),
      Double::from_int(second),
      Double::from_int(ms),
    )
    return Some(base - Double::from_int(offset * 60000))
  }
  if code2 != '-'.to_int() {
    return None
  }
  day = match parse_digits(text, pos + 1, 2) {
    Some(value) => value
    None => return None
  }
  pos = pos + 3
  if pos == len {
    return Some(
      date_make_utc_from_fields(
        Double::from_int(year),
        Double::from_int(month - 1),
        Double::from_int(day),
        0.0,
        0.0,
        0.0,
        0.0,
      ),
    )
  }
  let code3 = UInt16::to_int(text.code_unit_at(pos))
  if code3 != 'T'.to_int() {
    return None
  }
  let (hour, minute, second, ms, offset, end_pos) = match
    parse_iso_time_and_zone(text, pos + 1) {
    Some(value) => value
    None => return None
  }
  if end_pos != len {
    return None
  }
  let base = date_make_utc_from_fields(
    Double::from_int(year),
    Double::from_int(month - 1),
    Double::from_int(day),
    Double::from_int(hour),
    Double::from_int(minute),
    Double::from_int(second),
    Double::from_int(ms),
  )
  Some(base - Double::from_int(offset * 60000))
}

///|
fn parse_non_iso_date(text : String) -> Double? {
  let len = text.length()
  if len == 0 {
    return None
  }
  let mut year = 2001
  let mut month = 1
  let mut day = 1
  let mut hour = 0
  let mut minute = 0
  let mut second = 0
  let mut ms = 0
  let mut offset = 0
  let mut has_year = false
  let mut has_mon = false
  let mut has_time = false
  let mut num_index = 0
  let nums : Array[Int] = [0, 0, 0]
  let mut p = 0
  while true {
    p = string_skip_spaces(text, p)
    if p >= len {
      break
    }
    let code = UInt16::to_int(text.code_unit_at(p))
    if code == '+'.to_int() || code == '-'.to_int() {
      let mut parsed_tz = false
      if has_time {
        match parse_tz_offset_non_strict(text, p) {
          Some(value) => {
            let (mins, next_pos) = value
            offset = mins
            p = next_pos
            parsed_tz = true
          }
          None => ()
        }
      }
      if !parsed_tz {
        let sign = if code == '+'.to_int() { 1 } else { -1 }
        p = p + 1
        match string_get_digits(text, p, 1, 0) {
          Some(value) => {
            let (val, next_pos, _) = value
            if sign == -1 && val == 0 {
              return None
            }
            year = sign * val
            has_year = true
            p = next_pos
          }
          None => ()
        }
      }
    } else {
      match string_get_digits(text, p, 1, 0) {
        Some(value) => {
          let (val, next_pos, digits) = value
          if next_pos < len &&
            UInt16::to_int(text.code_unit_at(next_pos)) == ':'.to_int() {
            hour = val
            p = next_pos + 1
            let (min_val, min_pos, _) = match string_get_digits(text, p, 1, 2) {
              Some(v) => v
              None => return None
            }
            minute = min_val
            p = min_pos
            if p < len && UInt16::to_int(text.code_unit_at(p)) == ':'.to_int() {
              let (sec_val, sec_pos, _) = match
                string_get_digits(text, p + 1, 1, 2) {
                Some(v) => v
                None => return None
              }
              second = sec_val
              let (ms_val, ms_pos) = string_get_milliseconds(text, sec_pos)
              ms = ms_val
              p = ms_pos
            }
            has_time = true
            if p < len {
              let tz_code = UInt16::to_int(text.code_unit_at(p))
              if tz_code == '+'.to_int() || tz_code == '-'.to_int() {
                match parse_tz_offset_non_strict(text, p) {
                  Some(value) => {
                    let (mins, next_pos) = value
                    offset = mins
                    p = next_pos
                  }
                  None => ()
                }
              }
            }
          } else {
            if digits > 2 && !has_year {
              year = val
              has_year = true
            } else if (val < 1 || val > 31) && !has_year {
              year = adjust_year_for_parse(val)
              has_year = true
            } else {
              if num_index == 3 {
                return None
              }
              nums[num_index] = val
              num_index = num_index + 1
            }
            p = next_pos
          }
        }
        None =>
          match string_get_month(text, p) {
            Some(value) => {
              let (mon, next_pos) = value
              month = mon
              has_mon = true
              p = string_skip_until(text, next_pos, "0123456789 -/(")
            }
            None => {
              if has_time {
                match string_match(text, p, "PM") {
                  Some(next_pos) => {
                    if hour < 12 {
                      hour = hour + 12
                    }
                    p = next_pos
                    continue
                  }
                  None => ()
                }
                match string_match(text, p, "AM") {
                  Some(next_pos) => {
                    if hour == 12 {
                      hour = 0
                    }
                    p = next_pos
                    continue
                  }
                  None => ()
                }
              }
              match string_get_tzabbr(text, p) {
                Some(value) => {
                  let (mins, next_pos) = value
                  offset = mins
                  p = next_pos
                  continue
                }
                None => ()
              }
              if code == '('.to_int() {
                let mut level = 0
                while p < len {
                  let c = UInt16::to_int(text.code_unit_at(p))
                  p = p + 1
                  if c == '('.to_int() {
                    level = level + 1
                  } else if c == ')'.to_int() {
                    level = level - 1
                    if level == 0 {
                      break
                    }
                  }
                }
                if level > 0 {
                  return None
                }
              } else if code == ')'.to_int() {
                return None
              } else {
                if has_year || has_mon || has_time || num_index > 0 {
                  return None
                }
                p = string_skip_until(text, p, " -/(")
              }
            }
          }
      }
    }
    p = string_skip_separators(text, p)
  }
  if num_index + (if has_year { 1 } else { 0 }) + (if has_mon { 1 } else { 0 }) >
    3 {
    return None
  }
  match num_index {
    0 => if !has_year { return None }
    1 => if has_mon { day = nums[0] } else { month = nums[0] }
    2 =>
      if has_year {
        month = nums[0]
        day = nums[1]
      } else if has_mon {
        year = adjust_year_for_parse(nums[1])
        day = nums[0]
      } else {
        month = nums[0]
        day = nums[1]
      }
    3 => {
      year = adjust_year_for_parse(nums[2])
      month = nums[0]
      day = nums[1]
    }
    _ => return None
  }
  if month < 1 || day < 1 {
    return None
  }
  month = month - 1
  if month > 11 || day > 31 || hour > 24 || minute > 59 || second > 59 {
    return None
  }
  if hour == 24 && (minute != 0 || second != 0 || ms != 0) {
    return None
  }
  let base = date_make_utc_from_fields(
    Double::from_int(year),
    Double::from_int(month),
    Double::from_int(day),
    Double::from_int(hour),
    Double::from_int(minute),
    Double::from_int(second),
    Double::from_int(ms),
  )
  Some(base - Double::from_int(offset * 60000))
}

///|
fn month_from_name(token : String) -> Int? {
  let lower = token.to_lower()
  let short = if lower.length() >= 3 {
    lower.unsafe_substring(start=0, end=3)
  } else {
    lower
  }
  match short {
    "jan" => Some(1)
    "feb" => Some(2)
    "mar" => Some(3)
    "apr" => Some(4)
    "may" => Some(5)
    "jun" => Some(6)
    "jul" => Some(7)
    "aug" => Some(8)
    "sep" => Some(9)
    "oct" => Some(10)
    "nov" => Some(11)
    "dec" => Some(12)
    _ => None
  }
}

///|
fn date_parse_string(text : String) -> Double {
  let trimmed = text.trim().to_string()
  if trimmed.is_empty() {
    return nan()
  }
  match parse_iso_date(trimmed) {
    Some(value) => value
    None =>
      match parse_non_iso_date(trimmed) {
        Some(value) => value
        None => nan()
      }
  }
}
