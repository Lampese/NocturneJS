///|
fn call_builtin_bigint(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match builtin.kind {
    BuiltinFunction::BigIntConstructor => {
      if args.is_empty() {
        return throw_type_error("cannot convert to BigInt")
      }
      BigInt(bigint_from_value(args[0]))
    }
    BuiltinFunction::BigIntToString => {
      let value = this_to_bigint(builtin.this_value)
      if args.is_empty() || args[0] is Undefined {
        String(bigint_to_string_radix(value, 10))
      } else {
        let radix = Double::to_int(Double::trunc(to_number(args[0])))
        if radix < 2 || radix > 36 {
          throw_range_error("invalid radix")
        } else {
          String(bigint_to_string_radix(value, radix))
        }
      }
    }
    BuiltinFunction::BigIntValueOf => BigInt(this_to_bigint(builtin.this_value))
    BuiltinFunction::BigIntAsIntN => {
      let bits_value = if args.is_empty() { Undefined } else { args[0] }
      let bigint_value = if args.length() > 1 { args[1] } else { Undefined }
      bigint_as_n(bits_value, bigint_value, true)
    }
    BuiltinFunction::BigIntAsUintN => {
      let bits_value = if args.is_empty() { Undefined } else { args[0] }
      let bigint_value = if args.length() > 1 { args[1] } else { Undefined }
      bigint_as_n(bits_value, bigint_value, false)
    }
    _ => throw_type_error("invalid builtin")
  }
}
