///|
fn pad2(value : Int) -> String {
  if value < 10 {
    "0" + Int::to_string(value)
  } else {
    Int::to_string(value)
  }
}

///|
fn pad3(value : Int) -> String {
  if value < 10 {
    "00" + Int::to_string(value)
  } else if value < 100 {
    "0" + Int::to_string(value)
  } else {
    Int::to_string(value)
  }
}

///|
fn pad4(value : Int) -> String {
  if value < 10 {
    "000" + Int::to_string(value)
  } else if value < 100 {
    "00" + Int::to_string(value)
  } else if value < 1000 {
    "0" + Int::to_string(value)
  } else {
    Int::to_string(value)
  }
}

///|
fn pad6(value : Int) -> String {
  if value < 10 {
    "00000" + Int::to_string(value)
  } else if value < 100 {
    "0000" + Int::to_string(value)
  } else if value < 1000 {
    "000" + Int::to_string(value)
  } else if value < 10000 {
    "00" + Int::to_string(value)
  } else if value < 100000 {
    "0" + Int::to_string(value)
  } else {
    Int::to_string(value)
  }
}

///|
fn format_year_min4(year : Int) -> String {
  if year < 0 {
    "-" + pad4(-year)
  } else {
    pad4(year)
  }
}

///|
fn format_year_iso(year : Int) -> String {
  if year >= 0 && year <= 9999 {
    pad4(year)
  } else {
    let sign = if year < 0 { "-" } else { "+" }
    let abs_year = if year < 0 { -year } else { year }
    sign + pad6(abs_year)
  }
}

///|
fn date_weekday_from_ms(ms : Double) -> Int {
  let d = Double::to_int64(Double::trunc(ms))
  let days = floor_div(d, 86400000)
  Int64::to_int(math_mod(days + 4, 7))
}

///|
fn date_to_iso_string(ms : Double) -> String {
  let (year, month, day, hour, min, sec, milli) = date_fields_from_ms(ms)
  let year_str = format_year_iso(year)
  year_str +
  "-" +
  pad2(month + 1) +
  "-" +
  pad2(day) +
  "T" +
  pad2(hour) +
  ":" +
  pad2(min) +
  ":" +
  pad2(sec) +
  "." +
  pad3(milli) +
  "Z"
}

///|
fn date_to_string(ms : Double) -> String {
  date_to_date_string(ms) + " " + date_to_time_string(ms)
}

///|
fn date_to_utc_string(ms : Double) -> String {
  let (year, month, day, hour, min, sec, _) = date_fields_from_ms(ms)
  let weekday = date_weekday_from_ms(ms)
  date_day_names[weekday] +
  ", " +
  pad2(day) +
  " " +
  date_month_names[month] +
  " " +
  format_year_min4(year) +
  " " +
  pad2(hour) +
  ":" +
  pad2(min) +
  ":" +
  pad2(sec) +
  " GMT"
}

///|
fn date_to_date_string(ms : Double) -> String {
  let (year, month, day, _, _, _, _) = date_fields_from_ms(ms)
  let weekday = date_weekday_from_ms(ms)
  date_day_names[weekday] +
  " " +
  date_month_names[month] +
  " " +
  pad2(day) +
  " " +
  format_year_min4(year)
}

///|
fn date_to_time_string(ms : Double) -> String {
  let (_, _, _, hour, min, sec, _) = date_fields_from_ms(ms)
  pad2(hour) + ":" + pad2(min) + ":" + pad2(sec) + " GMT+0000"
}
