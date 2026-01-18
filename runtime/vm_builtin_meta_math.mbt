///|
fn builtin_name_math(func : BuiltinFunction) -> String {
  match func {
    BuiltinFunction::MathAbs => "abs"
    BuiltinFunction::MathAcos => "acos"
    BuiltinFunction::MathAcosh => "acosh"
    BuiltinFunction::MathAsin => "asin"
    BuiltinFunction::MathAsinh => "asinh"
    BuiltinFunction::MathAtan => "atan"
    BuiltinFunction::MathAtan2 => "atan2"
    BuiltinFunction::MathAtanh => "atanh"
    BuiltinFunction::MathCbrt => "cbrt"
    BuiltinFunction::MathCeil => "ceil"
    BuiltinFunction::MathClz32 => "clz32"
    BuiltinFunction::MathCos => "cos"
    BuiltinFunction::MathCosh => "cosh"
    BuiltinFunction::MathExp => "exp"
    BuiltinFunction::MathExpm1 => "expm1"
    BuiltinFunction::MathFloor => "floor"
    BuiltinFunction::MathFround => "fround"
    BuiltinFunction::MathF16Round => "f16round"
    BuiltinFunction::MathHypot => "hypot"
    BuiltinFunction::MathImul => "imul"
    BuiltinFunction::MathLog => "log"
    BuiltinFunction::MathLog1p => "log1p"
    BuiltinFunction::MathLog2 => "log2"
    BuiltinFunction::MathLog10 => "log10"
    BuiltinFunction::MathMax => "max"
    BuiltinFunction::MathMin => "min"
    BuiltinFunction::MathPow => "pow"
    BuiltinFunction::MathRandom => "random"
    BuiltinFunction::MathRound => "round"
    BuiltinFunction::MathSign => "sign"
    BuiltinFunction::MathSin => "sin"
    BuiltinFunction::MathSinh => "sinh"
    BuiltinFunction::MathSqrt => "sqrt"
    BuiltinFunction::MathTan => "tan"
    BuiltinFunction::MathTanh => "tanh"
    BuiltinFunction::MathTrunc => "trunc"
    BuiltinFunction::MathSumPrecise => "sumPrecise"
    _ => ""
  }
}

///|
fn builtin_length_math(func : BuiltinFunction) -> Int {
  match func {
    BuiltinFunction::MathAbs => 1
    BuiltinFunction::MathAcos => 1
    BuiltinFunction::MathAcosh => 1
    BuiltinFunction::MathAsin => 1
    BuiltinFunction::MathAsinh => 1
    BuiltinFunction::MathAtan => 1
    BuiltinFunction::MathAtan2 => 2
    BuiltinFunction::MathAtanh => 1
    BuiltinFunction::MathCbrt => 1
    BuiltinFunction::MathCeil => 1
    BuiltinFunction::MathClz32 => 1
    BuiltinFunction::MathCos => 1
    BuiltinFunction::MathCosh => 1
    BuiltinFunction::MathExp => 1
    BuiltinFunction::MathExpm1 => 1
    BuiltinFunction::MathFloor => 1
    BuiltinFunction::MathFround => 1
    BuiltinFunction::MathF16Round => 1
    BuiltinFunction::MathHypot => 2
    BuiltinFunction::MathImul => 2
    BuiltinFunction::MathLog => 1
    BuiltinFunction::MathLog1p => 1
    BuiltinFunction::MathLog2 => 1
    BuiltinFunction::MathLog10 => 1
    BuiltinFunction::MathMax => 2
    BuiltinFunction::MathMin => 2
    BuiltinFunction::MathPow => 2
    BuiltinFunction::MathRandom => 0
    BuiltinFunction::MathRound => 1
    BuiltinFunction::MathSign => 1
    BuiltinFunction::MathSin => 1
    BuiltinFunction::MathSinh => 1
    BuiltinFunction::MathSqrt => 1
    BuiltinFunction::MathTan => 1
    BuiltinFunction::MathTanh => 1
    BuiltinFunction::MathTrunc => 1
    BuiltinFunction::MathSumPrecise => 1
    _ => 0
  }
}
