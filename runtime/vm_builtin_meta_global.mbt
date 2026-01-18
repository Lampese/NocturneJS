///|
fn builtin_name_global(func : BuiltinFunction) -> String {
  match func {
    BuiltinFunction::ParseInt => "parseInt"
    BuiltinFunction::ParseFloat => "parseFloat"
    BuiltinFunction::DecodeURI => "decodeURI"
    BuiltinFunction::DecodeURIComponent => "decodeURIComponent"
    BuiltinFunction::EncodeURI => "encodeURI"
    BuiltinFunction::EncodeURIComponent => "encodeURIComponent"
    BuiltinFunction::Eval => "eval"
    BuiltinFunction::IsNaN => "isNaN"
    BuiltinFunction::IsFinite => "isFinite"
    _ => ""
  }
}

///|
fn builtin_length_global(func : BuiltinFunction) -> Int {
  match func {
    BuiltinFunction::ParseInt => 2
    BuiltinFunction::ParseFloat => 1
    BuiltinFunction::DecodeURI => 1
    BuiltinFunction::DecodeURIComponent => 1
    BuiltinFunction::EncodeURI => 1
    BuiltinFunction::EncodeURIComponent => 1
    BuiltinFunction::Eval => 1
    BuiltinFunction::IsNaN => 1
    BuiltinFunction::IsFinite => 1
    _ => 0
  }
}
