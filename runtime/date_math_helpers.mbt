///|
fn is_finite(num : Double) -> Bool {
  !Double::is_nan(num) && !Double::is_inf(num)
}

///|
fn math_mod(a : Int64, b : Int64) -> Int64 {
  let m = a % b
  if m < 0 {
    m + b
  } else {
    m
  }
}

///|
fn floor_div(a : Int64, b : Int64) -> Int64 {
  let m = a % b
  let adjust = if m < 0 { m + b } else { m }
  (a - adjust) / b
}

///|
fn days_from_year(y : Int64) -> Int64 {
  365 * (y - 1970) +
  floor_div(y - 1969, 4) -
  floor_div(y - 1901, 100) +
  floor_div(y - 1601, 400)
}

///|
fn days_in_year(y : Int64) -> Int64 {
  let leap4 = if y % 4 == 0 { 1L } else { 0L }
  let leap100 = if y % 100 == 0 { 1L } else { 0L }
  let leap400 = if y % 400 == 0 { 1L } else { 0L }
  365L + leap4 - leap100 + leap400
}

///|
fn year_from_days(days_input : Int64) -> (Int64, Int64) {
  let d = days_input
  let mut y = floor_div(d * 10000, 3652425) + 1970
  while true {
    let mut d1 = d - days_from_year(y)
    if d1 < 0 {
      y = y - 1
      d1 = d1 + days_in_year(y)
      continue
    }
    let nd = days_in_year(y)
    if d1 < nd {
      return (y, d1)
    }
    d1 = d1 - nd
    y = y + 1
  }
  (y, 0)
}

///|
fn date_time_clip(t : Double) -> Double {
  if t >= -8.64e15 && t <= 8.64e15 {
    let value = Double::trunc(t)
    if value == 0.0 {
      0.0
    } else {
      value
    }
  } else {
    nan()
  }
}

///|
fn date_make_utc_from_fields(
  year : Double,
  month : Double,
  day : Double,
  hour : Double,
  minute : Double,
  second : Double,
  millisecond : Double,
  adjust_year? : Bool = false,
) -> Double {
  if !is_finite(year) ||
    !is_finite(month) ||
    !is_finite(day) ||
    !is_finite(hour) ||
    !is_finite(minute) ||
    !is_finite(second) ||
    !is_finite(millisecond) {
    return nan()
  }
  let mut y = Double::trunc(year)
  let m = Double::trunc(month)
  let dt = Double::trunc(day)
  let h = Double::trunc(hour)
  let min = Double::trunc(minute)
  let s = Double::trunc(second)
  let ms = Double::trunc(millisecond)
  if adjust_year && y >= 0.0 && y < 100.0 {
    y = y + 1900.0
  }
  let ym = y + Double::floor(m / 12.0)
  let mut mn = Double::mod(m, 12.0)
  if mn < 0.0 {
    mn = mn + 12.0
  }
  if ym < -271821.0 || ym > 275760.0 {
    return nan()
  }
  let yi = Double::to_int64(Double::trunc(ym))
  let mi = Double::to_int(Double::trunc(mn))
  let mut days = days_from_year(yi)
  let mut i = 0
  while i < mi {
    let mut md = date_month_days[i]
    if i == 1 {
      md = md + days_in_year(yi) - 365
    }
    days = days + md
    i = i + 1
  }
  let day_value = Int64::to_double(days) + dt - 1.0
  let mut time = h * 3600000.0
  let temp = min * 60000.0
  time = time + temp
  let temp2 = s * 1000.0
  time = time + temp2
  time = time + ms
  let tv = day_value * 86400000.0 + time
  if !is_finite(tv) {
    nan()
  } else {
    date_time_clip(tv)
  }
}

///|
fn date_fields_from_ms(ms : Double) -> (Int, Int, Int, Int, Int, Int, Int) {
  let d = Double::to_int64(Double::trunc(ms))
  let mut h = math_mod(d, 86400000)
  let days = (d - h) / 86400000
  let milli = Int64::to_int(math_mod(h, 1000))
  h = (h - Int64::from_int(milli)) / 1000
  let sec = Int64::to_int(math_mod(h, 60))
  h = (h - Int64::from_int(sec)) / 60
  let min = Int64::to_int(math_mod(h, 60))
  h = (h - Int64::from_int(min)) / 60
  let hour = Int64::to_int(h)
  let (year_i64, day_of_year) = year_from_days(days)
  let mut month = 0
  let mut dday = day_of_year
  let mut i = 0
  while i < 12 {
    let mut md = date_month_days[i]
    if i == 1 {
      md = md + days_in_year(year_i64) - 365
    }
    if dday < md {
      break
    }
    dday = dday - md
    month = month + 1
    i = i + 1
  }
  let day = Int64::to_int(dday + 1)
  (Int64::to_int(year_i64), month, day, hour, min, sec, milli)
}
