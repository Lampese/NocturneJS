///|
fn call_builtin_math(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match builtin.kind {
    BuiltinFunction::MathAbs =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(Double::abs(to_number(args[0])))
      }
    BuiltinFunction::MathAcos =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(@math.acos(to_number(args[0])))
      }
    BuiltinFunction::MathAcosh =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(@math.acosh(to_number(args[0])))
      }
    BuiltinFunction::MathAsin =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(@math.asin(to_number(args[0])))
      }
    BuiltinFunction::MathAsinh =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(@math.asinh(to_number(args[0])))
      }
    BuiltinFunction::MathAtan =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(@math.atan(to_number(args[0])))
      }
    BuiltinFunction::MathAtan2 => {
      let y = if args.is_empty() { nan() } else { to_number(args[0]) }
      let x = if args.length() < 2 { nan() } else { to_number(args[1]) }
      Number(@math.atan2(y, x))
    }
    BuiltinFunction::MathAtanh =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(@math.atanh(to_number(args[0])))
      }
    BuiltinFunction::MathCbrt =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(@math.cbrt(to_number(args[0])))
      }
    BuiltinFunction::MathCeil =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(Double::ceil(to_number(args[0])))
      }
    BuiltinFunction::MathClz32 => {
      let value = if args.is_empty() { nan() } else { to_number(args[0]) }
      Number(Double::from_int(math_clz32_value(value)))
    }
    BuiltinFunction::MathCos =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(@math.cos(to_number(args[0])))
      }
    BuiltinFunction::MathCosh =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(math_cosh_value(to_number(args[0])))
      }
    BuiltinFunction::MathExp =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(@math.exp(to_number(args[0])))
      }
    BuiltinFunction::MathExpm1 =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(@math.expm1(to_number(args[0])))
      }
    BuiltinFunction::MathFloor =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(Double::floor(to_number(args[0])))
      }
    BuiltinFunction::MathFround =>
      if args.is_empty() {
        Number(nan())
      } else {
        let value = Float::from_double(to_number(args[0])).to_double()
        Number(value)
      }
    BuiltinFunction::MathF16Round =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(math_f16round_value(to_number(args[0])))
      }
    BuiltinFunction::MathHypot => Number(math_hypot(args))
    BuiltinFunction::MathImul => {
      let a = if args.is_empty() { 0.0 } else { to_number(args[0]) }
      let b = if args.length() < 2 { 0.0 } else { to_number(args[1]) }
      Number(Double::from_int(imul(a, b)))
    }
    BuiltinFunction::MathLog =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(@math.ln(to_number(args[0])))
      }
    BuiltinFunction::MathLog1p =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(@math.ln_1p(to_number(args[0])))
      }
    BuiltinFunction::MathLog2 =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(@math.log2(to_number(args[0])))
      }
    BuiltinFunction::MathLog10 =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(@math.log10(to_number(args[0])))
      }
    BuiltinFunction::MathMax => Number(math_max(args))
    BuiltinFunction::MathMin => Number(math_min(args))
    BuiltinFunction::MathPow => {
      let base = if args.is_empty() { nan() } else { to_number(args[0]) }
      let exp = if args.length() < 2 { nan() } else { to_number(args[1]) }
      Number(@math.pow(base, exp))
    }
    BuiltinFunction::MathRandom => Number(math_random_value())
    BuiltinFunction::MathRound =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(math_round_value(to_number(args[0])))
      }
    BuiltinFunction::MathSign =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(math_sign_value(to_number(args[0])))
      }
    BuiltinFunction::MathSin =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(@math.sin(to_number(args[0])))
      }
    BuiltinFunction::MathSinh =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(@math.sinh(to_number(args[0])))
      }
    BuiltinFunction::MathSqrt =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(Double::sqrt(to_number(args[0])))
      }
    BuiltinFunction::MathTan =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(@math.tan(to_number(args[0])))
      }
    BuiltinFunction::MathTanh =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(@math.tanh(to_number(args[0])))
      }
    BuiltinFunction::MathTrunc =>
      if args.is_empty() {
        Number(nan())
      } else {
        Number(math_trunc_value(to_number(args[0])))
      }
    BuiltinFunction::MathSumPrecise => math_sum_precise(args)
    _ => throw_type_error("invalid builtin")
  }
}
