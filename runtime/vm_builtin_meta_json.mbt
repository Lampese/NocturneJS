///|
fn builtin_name_json(func : BuiltinFunction) -> String {
  match func {
    BuiltinFunction::JsonStringify => "stringify"
    BuiltinFunction::JsonParse => "parse"
    BuiltinFunction::BjsonRead => "read"
    BuiltinFunction::BjsonWrite => "write"
    _ => ""
  }
}

///|
fn builtin_length_json(func : BuiltinFunction) -> Int {
  match func {
    BuiltinFunction::JsonStringify => 3
    BuiltinFunction::JsonParse => 2
    BuiltinFunction::BjsonRead => 4
    BuiltinFunction::BjsonWrite => 2
    _ => 0
  }
}
