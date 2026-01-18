///|
fn builtin_name_bigint(func : BuiltinFunction) -> String {
  match func {
    BuiltinFunction::BigIntConstructor => "BigInt"
    BuiltinFunction::BigIntToString => "toString"
    BuiltinFunction::BigIntValueOf => "valueOf"
    BuiltinFunction::BigIntAsIntN => "asIntN"
    BuiltinFunction::BigIntAsUintN => "asUintN"
    _ => ""
  }
}

///|
fn builtin_length_bigint(func : BuiltinFunction) -> Int {
  match func {
    BuiltinFunction::BigIntConstructor => 1
    BuiltinFunction::BigIntToString => 0
    BuiltinFunction::BigIntValueOf => 0
    BuiltinFunction::BigIntAsIntN => 2
    BuiltinFunction::BigIntAsUintN => 2
    _ => 0
  }
}
