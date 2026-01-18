///|
fn call_builtin_number(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match builtin.kind {
    BuiltinFunction::NumberConstructor => {
      if args.is_empty() {
        return Number(0.0)
      }
      let prim = if is_object_like(args[0]) {
        to_primitive(args[0], false)
      } else {
        args[0]
      }
      match prim {
        Symbol(_) => throw_type_error("cannot convert symbol to number")
        BigInt(b) => Number(bigint_to_number(b))
        _ => Number(to_number(prim))
      }
    }
    BuiltinFunction::NumberIsNaN => {
      if args.is_empty() {
        return Bool(false)
      }
      match args[0] {
        Number(value) => Bool(Double::is_nan(value))
        _ => Bool(false)
      }
    }
    BuiltinFunction::NumberIsFinite => {
      if args.is_empty() {
        return Bool(false)
      }
      match args[0] {
        Number(value) => Bool(!Double::is_nan(value) && !Double::is_inf(value))
        _ => Bool(false)
      }
    }
    BuiltinFunction::NumberIsInteger => {
      if args.is_empty() {
        return Bool(false)
      }
      match args[0] {
        Number(value) =>
          if Double::is_nan(value) || Double::is_inf(value) {
            Bool(false)
          } else {
            Bool(Double::trunc(value) == value)
          }
        _ => Bool(false)
      }
    }
    BuiltinFunction::NumberIsSafeInteger => {
      if args.is_empty() {
        return Bool(false)
      }
      match args[0] {
        Number(value) =>
          if Double::is_nan(value) || Double::is_inf(value) {
            Bool(false)
          } else if Double::trunc(value) != value {
            Bool(false)
          } else {
            Bool(Double::abs(value) <= 9007199254740991.0)
          }
        _ => Bool(false)
      }
    }
    BuiltinFunction::NumberToString => {
      let value = this_to_number(builtin.this_value)
      if args.is_empty() || args[0] is Undefined {
        String(number_to_string_radix(value, 10))
      } else {
        let radix = Double::to_int(Double::trunc(to_number(args[0])))
        if radix < 2 || radix > 36 {
          throw_range_error("radix must be between 2 and 36")
        } else {
          String(number_to_string_radix(value, radix))
        }
      }
    }
    BuiltinFunction::NumberToLocaleString => {
      let value = this_to_number(builtin.this_value)
      String(number_to_string_radix(value, 10))
    }
    BuiltinFunction::NumberToFixed => {
      let value = this_to_number(builtin.this_value)
      let arg = if args.is_empty() { Undefined } else { args[0] }
      let digits = to_int32_sat(to_number(arg))
      if digits < 0 || digits > 100 {
        throw_range_error("invalid number of digits")
      } else {
        String(number_to_fixed(value, digits))
      }
    }
    BuiltinFunction::NumberToExponential => {
      let value = this_to_number(builtin.this_value)
      let arg = if args.is_empty() { Undefined } else { args[0] }
      let digits = to_int32_sat(to_number(arg))
      if Double::is_nan(value) || Double::is_inf(value) {
        String(number_to_string_radix(value, 10))
      } else if arg is Undefined {
        String(number_to_exponential_free(value))
      } else if digits < 0 || digits > 100 {
        throw_range_error("invalid number of digits")
      } else {
        String(number_to_exponential(value, digits))
      }
    }
    BuiltinFunction::NumberToPrecision => {
      let value = this_to_number(builtin.this_value)
      let arg = if args.is_empty() { Undefined } else { args[0] }
      let precision = to_int32_sat(to_number(arg))
      if arg is Undefined {
        String(number_to_string_radix(value, 10))
      } else if Double::is_nan(value) || Double::is_inf(value) {
        String(number_to_string_radix(value, 10))
      } else if precision < 1 || precision > 100 {
        throw_range_error("invalid number of digits")
      } else {
        String(number_to_precision(value, precision))
      }
    }
    BuiltinFunction::NumberValueOf => Number(this_to_number(builtin.this_value))
    _ => throw_type_error("invalid builtin")
  }
}
