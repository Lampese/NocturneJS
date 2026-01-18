///|
fn builtin_name_error(func : BuiltinFunction) -> String {
  match func {
    BuiltinFunction::ErrorToString => "toString"
    BuiltinFunction::ErrorIsError => "isError"
    BuiltinFunction::ErrorConstructor => "Error"
    BuiltinFunction::AggregateErrorConstructor => "AggregateError"
    BuiltinFunction::EvalErrorConstructor => "EvalError"
    BuiltinFunction::SyntaxErrorConstructor => "SyntaxError"
    BuiltinFunction::RangeErrorConstructor => "RangeError"
    BuiltinFunction::TypeErrorConstructor => "TypeError"
    BuiltinFunction::ReferenceErrorConstructor => "ReferenceError"
    BuiltinFunction::UriErrorConstructor => "URIError"
    BuiltinFunction::ThrowTypeError => ""
    _ => ""
  }
}

///|
fn builtin_length_error(func : BuiltinFunction) -> Int {
  match func {
    BuiltinFunction::ErrorToString => 0
    BuiltinFunction::ErrorIsError => 1
    BuiltinFunction::ErrorConstructor => 1
    BuiltinFunction::AggregateErrorConstructor => 2
    BuiltinFunction::EvalErrorConstructor => 1
    BuiltinFunction::SyntaxErrorConstructor => 1
    BuiltinFunction::RangeErrorConstructor => 1
    BuiltinFunction::TypeErrorConstructor => 1
    BuiltinFunction::ReferenceErrorConstructor => 1
    BuiltinFunction::UriErrorConstructor => 1
    BuiltinFunction::ThrowTypeError => 0
    _ => 0
  }
}
