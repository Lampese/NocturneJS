///|
fn date_now_ms() -> Double {
  UInt64::to_double(@env.now())
}

///|
fn date_from_args(args : Array[Value]) -> Double raise {
  if args.is_empty() {
    return date_now_ms()
  }
  if args.length() == 1 {
    match args[0] {
      Object(obj) =>
        match obj.date_data {
          Some(value) => return date_time_clip(value)
          None => ()
        }
      _ => ()
    }
    let prim = to_primitive_default(args[0])
    match prim {
      String(text) => date_parse_string(text)
      _ => date_time_clip(to_number(prim))
    }
  } else {
    let year = to_number(args[0])
    let month = if args.length() > 1 { to_number(args[1]) } else { 0.0 }
    let day = if args.length() > 2 { to_number(args[2]) } else { 1.0 }
    let hour = if args.length() > 3 { to_number(args[3]) } else { 0.0 }
    let minute = if args.length() > 4 { to_number(args[4]) } else { 0.0 }
    let second = if args.length() > 5 { to_number(args[5]) } else { 0.0 }
    let ms = if args.length() > 6 { to_number(args[6]) } else { 0.0 }
    date_make_utc_from_fields(
      year,
      month,
      day,
      hour,
      minute,
      second,
      ms,
      adjust_year=true,
    )
  }
}

///|
fn date_utc_from_args(args : Array[Value]) -> Double raise {
  if args.is_empty() {
    return nan()
  }
  let year = to_number(args[0])
  let month = if args.length() > 1 { to_number(args[1]) } else { 0.0 }
  let day = if args.length() > 2 { to_number(args[2]) } else { 1.0 }
  let hour = if args.length() > 3 { to_number(args[3]) } else { 0.0 }
  let minute = if args.length() > 4 { to_number(args[4]) } else { 0.0 }
  let second = if args.length() > 5 { to_number(args[5]) } else { 0.0 }
  let ms = if args.length() > 6 { to_number(args[6]) } else { 0.0 }
  date_make_utc_from_fields(
    year,
    month,
    day,
    hour,
    minute,
    second,
    ms,
    adjust_year=true,
  )
}
