///|
fn require_date_object(value : Value) -> ObjectValue raise {
  match value {
    Object(obj) =>
      match obj.date_data {
        Some(_) => obj
        None => {
          let _ = throw_type_error("not a Date object")
          new_object_struct(None)
        }
      }
    _ => {
      let _ = throw_type_error("not a Date object")
      new_object_struct(None)
    }
  }
}

///|
fn date_value_of(value : Value) -> Double raise {
  let obj = require_date_object(value)
  match obj.date_data {
    Some(v) => v
    None => nan()
  }
}

///|
fn date_is_valid(ms : Double) -> Bool {
  !(Double::is_nan(ms) || Double::is_inf(ms))
}

///|
fn date_day_of_week(ms : Double) -> Int {
  let d = Double::to_int64(Double::trunc(ms))
  let days = floor_div(d, 86400000)
  Int64::to_int(math_mod(days + 4, 7))
}

///|
fn date_fields_or_none(ms : Double) -> (Int, Int, Int, Int, Int, Int, Int)? {
  if !date_is_valid(ms) {
    None
  } else {
    Some(date_fields_from_ms(ms))
  }
}

///|
fn date_set_field_value(
  obj : ObjectValue,
  args : Array[Value],
  first_field : Int,
  end_field : Int,
  force : Bool,
) -> Double raise {
  let current = match obj.date_data {
    Some(value) => value
    None => nan()
  }
  let mut base = current
  let mut res1 = date_is_valid(current)
  if !res1 && force {
    res1 = true
    base = 0.0
  }
  let (year, month, day, hour, min, sec, milli) = if res1 {
    date_fields_from_ms(base)
  } else {
    (1970, 0, 1, 0, 0, 0, 0)
  }
  let fields : Array[Double] = [
    Double::from_int(year),
    Double::from_int(month),
    Double::from_int(day),
    Double::from_int(hour),
    Double::from_int(min),
    Double::from_int(sec),
    Double::from_int(milli),
  ]
  let mut res = true
  let mut i = 0
  let limit = end_field - first_field
  let count = if args.length() < limit { args.length() } else { limit }
  while i < count {
    let num = to_number(args[i])
    if !date_is_valid(num) {
      res = false
    }
    fields[first_field + i] = Double::trunc(num)
    i = i + 1
  }
  if !res1 {
    return nan()
  }
  let mut result = nan()
  if res && !args.is_empty() {
    result = date_make_utc_from_fields(
      fields[0],
      fields[1],
      fields[2],
      fields[3],
      fields[4],
      fields[5],
      fields[6],
    )
  }
  obj.date_data = Some(result)
  result
}
