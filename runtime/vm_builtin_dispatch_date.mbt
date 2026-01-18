///|
fn call_builtin_date(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match builtin.kind {
    BuiltinFunction::DateConstructor => {
      let ms = date_now_ms()
      String(date_to_string(ms))
    }
    BuiltinFunction::DateParse => {
      let text = if args.is_empty() {
        "undefined"
      } else {
        to_string_value(args[0])
      }
      Number(date_parse_string(text))
    }
    BuiltinFunction::DateUTC => Number(date_utc_from_args(args))
    BuiltinFunction::DateNow => Number(date_now_ms())
    BuiltinFunction::DateToISOString => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      if Double::is_nan(ms) || Double::is_inf(ms) {
        return throw_range_error("Date value is NaN")
      }
      String(date_to_iso_string(ms))
    }
    BuiltinFunction::DateToString => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      if Double::is_nan(ms) || Double::is_inf(ms) {
        String("Invalid Date")
      } else {
        String(date_to_string(ms))
      }
    }
    BuiltinFunction::DateSymbolToPrimitive => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      if !is_object_like(this_value) {
        return throw_type_error("not an object")
      }
      if args.is_empty() {
        return throw_type_error("invalid hint")
      }
      let hint = match args[0] {
        String(value) =>
          if value == "number" {
            "number"
          } else if value == "string" || value == "default" {
            "string"
          } else {
            return throw_type_error("invalid hint")
          }
        _ => return throw_type_error("invalid hint")
      }
      let first = if hint == "string" { "toString" } else { "valueOf" }
      let second = if hint == "string" { "valueOf" } else { "toString" }
      match call_primitive_method(this_value, first) {
        Some(result) => result
        None =>
          match call_primitive_method(this_value, second) {
            Some(result) => result
            None => throw_type_error("cannot convert object to primitive")
          }
      }
    }
    BuiltinFunction::DateValueOf => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      Number(date_value_of(this_value))
    }
    BuiltinFunction::DateToUTCString => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      if !date_is_valid(ms) {
        String("Invalid Date")
      } else {
        String(date_to_utc_string(ms))
      }
    }
    BuiltinFunction::DateToDateString => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      if !date_is_valid(ms) {
        String("Invalid Date")
      } else {
        String(date_to_date_string(ms))
      }
    }
    BuiltinFunction::DateToTimeString => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      if !date_is_valid(ms) {
        String("Invalid Date")
      } else {
        String(date_to_time_string(ms))
      }
    }
    BuiltinFunction::DateToLocaleString => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      if !date_is_valid(ms) {
        String("Invalid Date")
      } else {
        String(date_to_string(ms))
      }
    }
    BuiltinFunction::DateToLocaleDateString => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      if !date_is_valid(ms) {
        String("Invalid Date")
      } else {
        String(date_to_string(ms))
      }
    }
    BuiltinFunction::DateToLocaleTimeString => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      if !date_is_valid(ms) {
        String("Invalid Date")
      } else {
        String(date_to_string(ms))
      }
    }
    BuiltinFunction::DateToJSON => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = to_object(this_value)
      let prim = to_primitive(obj, false)
      match prim {
        Number(num) =>
          if Double::is_nan(num) || Double::is_inf(num) {
            return Null
          }
        _ => ()
      }
      let iso_method = property_get(obj, "toISOString")
      if !is_callable(iso_method) {
        return throw_type_error("object needs toISOString method")
      }
      call_value_with_this(iso_method, [], obj)
    }
    BuiltinFunction::DateGetTimezoneOffset => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      if !date_is_valid(ms) {
        Number(nan())
      } else {
        Number(0.0)
      }
    }
    BuiltinFunction::DateGetTime => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      Number(date_value_of(this_value))
    }
    BuiltinFunction::DateGetYear => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      match date_fields_or_none(ms) {
        Some((year, _, _, _, _, _, _)) => Number(Double::from_int(year - 1900))
        None => Number(nan())
      }
    }
    BuiltinFunction::DateGetFullYear => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      match date_fields_or_none(ms) {
        Some((year, _, _, _, _, _, _)) => Number(Double::from_int(year))
        None => Number(nan())
      }
    }
    BuiltinFunction::DateGetUTCFullYear => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      match date_fields_or_none(ms) {
        Some((year, _, _, _, _, _, _)) => Number(Double::from_int(year))
        None => Number(nan())
      }
    }
    BuiltinFunction::DateGetMonth => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      match date_fields_or_none(ms) {
        Some((_, month, _, _, _, _, _)) => Number(Double::from_int(month))
        None => Number(nan())
      }
    }
    BuiltinFunction::DateGetUTCMonth => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      match date_fields_or_none(ms) {
        Some((_, month, _, _, _, _, _)) => Number(Double::from_int(month))
        None => Number(nan())
      }
    }
    BuiltinFunction::DateGetDate => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      match date_fields_or_none(ms) {
        Some((_, _, day, _, _, _, _)) => Number(Double::from_int(day))
        None => Number(nan())
      }
    }
    BuiltinFunction::DateGetUTCDate => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      match date_fields_or_none(ms) {
        Some((_, _, day, _, _, _, _)) => Number(Double::from_int(day))
        None => Number(nan())
      }
    }
    BuiltinFunction::DateGetDay => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      if !date_is_valid(ms) {
        Number(nan())
      } else {
        Number(Double::from_int(date_day_of_week(ms)))
      }
    }
    BuiltinFunction::DateGetUTCDay => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      if !date_is_valid(ms) {
        Number(nan())
      } else {
        Number(Double::from_int(date_day_of_week(ms)))
      }
    }
    BuiltinFunction::DateGetHours => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      match date_fields_or_none(ms) {
        Some((_, _, _, hour, _, _, _)) => Number(Double::from_int(hour))
        None => Number(nan())
      }
    }
    BuiltinFunction::DateGetUTCHours => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      match date_fields_or_none(ms) {
        Some((_, _, _, hour, _, _, _)) => Number(Double::from_int(hour))
        None => Number(nan())
      }
    }
    BuiltinFunction::DateGetMinutes => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      match date_fields_or_none(ms) {
        Some((_, _, _, _, minute, _, _)) => Number(Double::from_int(minute))
        None => Number(nan())
      }
    }
    BuiltinFunction::DateGetUTCMinutes => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      match date_fields_or_none(ms) {
        Some((_, _, _, _, minute, _, _)) => Number(Double::from_int(minute))
        None => Number(nan())
      }
    }
    BuiltinFunction::DateGetSeconds => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      match date_fields_or_none(ms) {
        Some((_, _, _, _, _, sec, _)) => Number(Double::from_int(sec))
        None => Number(nan())
      }
    }
    BuiltinFunction::DateGetUTCSeconds => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      match date_fields_or_none(ms) {
        Some((_, _, _, _, _, sec, _)) => Number(Double::from_int(sec))
        None => Number(nan())
      }
    }
    BuiltinFunction::DateGetMilliseconds => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      match date_fields_or_none(ms) {
        Some((_, _, _, _, _, _, milli)) => Number(Double::from_int(milli))
        None => Number(nan())
      }
    }
    BuiltinFunction::DateGetUTCMilliseconds => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let ms = date_value_of(this_value)
      match date_fields_or_none(ms) {
        Some((_, _, _, _, _, _, milli)) => Number(Double::from_int(milli))
        None => Number(nan())
      }
    }
    BuiltinFunction::DateSetTime => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = require_date_object(this_value)
      let value = if args.is_empty() {
        to_number(Undefined)
      } else {
        to_number(args[0])
      }
      let new_ms = date_time_clip(value)
      obj.date_data = Some(new_ms)
      Number(new_ms)
    }
    BuiltinFunction::DateSetMilliseconds => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = require_date_object(this_value)
      let new_ms = date_set_field_value(obj, args, 6, 7, false)
      Number(new_ms)
    }
    BuiltinFunction::DateSetUTCMilliseconds => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = require_date_object(this_value)
      let new_ms = date_set_field_value(obj, args, 6, 7, false)
      Number(new_ms)
    }
    BuiltinFunction::DateSetSeconds => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = require_date_object(this_value)
      let new_ms = date_set_field_value(obj, args, 5, 7, false)
      Number(new_ms)
    }
    BuiltinFunction::DateSetUTCSeconds => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = require_date_object(this_value)
      let new_ms = date_set_field_value(obj, args, 5, 7, false)
      Number(new_ms)
    }
    BuiltinFunction::DateSetMinutes => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = require_date_object(this_value)
      let new_ms = date_set_field_value(obj, args, 4, 7, false)
      Number(new_ms)
    }
    BuiltinFunction::DateSetUTCMinutes => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = require_date_object(this_value)
      let new_ms = date_set_field_value(obj, args, 4, 7, false)
      Number(new_ms)
    }
    BuiltinFunction::DateSetHours => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = require_date_object(this_value)
      let new_ms = date_set_field_value(obj, args, 3, 7, false)
      Number(new_ms)
    }
    BuiltinFunction::DateSetUTCHours => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = require_date_object(this_value)
      let new_ms = date_set_field_value(obj, args, 3, 7, false)
      Number(new_ms)
    }
    BuiltinFunction::DateSetDate => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = require_date_object(this_value)
      let new_ms = date_set_field_value(obj, args, 2, 3, false)
      Number(new_ms)
    }
    BuiltinFunction::DateSetUTCDate => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = require_date_object(this_value)
      let new_ms = date_set_field_value(obj, args, 2, 3, false)
      Number(new_ms)
    }
    BuiltinFunction::DateSetMonth => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = require_date_object(this_value)
      let new_ms = date_set_field_value(obj, args, 1, 3, false)
      Number(new_ms)
    }
    BuiltinFunction::DateSetUTCMonth => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = require_date_object(this_value)
      let new_ms = date_set_field_value(obj, args, 1, 3, false)
      Number(new_ms)
    }
    BuiltinFunction::DateSetYear => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = require_date_object(this_value)
      let mut year_val = if args.is_empty() {
        to_number(Undefined)
      } else {
        to_number(args[0])
      }
      if date_is_valid(year_val) {
        year_val = Double::trunc(year_val)
        if year_val >= 0.0 && year_val < 100.0 {
          year_val = year_val + 1900.0
        }
      }
      let year_args : Array[Value] = [Number(year_val)]
      let new_ms = date_set_field_value(obj, year_args, 0, 1, true)
      Number(new_ms)
    }
    BuiltinFunction::DateSetFullYear => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = require_date_object(this_value)
      let new_ms = date_set_field_value(obj, args, 0, 3, true)
      Number(new_ms)
    }
    BuiltinFunction::DateSetUTCFullYear => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = require_date_object(this_value)
      let new_ms = date_set_field_value(obj, args, 0, 3, true)
      Number(new_ms)
    }
    _ => throw_type_error("invalid builtin")
  }
}
