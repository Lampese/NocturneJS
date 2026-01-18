///|
fn builtin_name_boolean(func : BuiltinFunction) -> String {
  match func {
    BuiltinFunction::BooleanConstructor => "Boolean"
    BuiltinFunction::BooleanToString => "toString"
    BuiltinFunction::BooleanValueOf => "valueOf"
    _ => ""
  }
}

///|
fn builtin_length_boolean(func : BuiltinFunction) -> Int {
  match func {
    BuiltinFunction::BooleanConstructor => 1
    BuiltinFunction::BooleanToString => 0
    BuiltinFunction::BooleanValueOf => 0
    _ => 0
  }
}
