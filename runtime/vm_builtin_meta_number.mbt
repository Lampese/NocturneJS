///|
fn builtin_name_number(func : BuiltinFunction) -> String {
  match func {
    BuiltinFunction::NumberConstructor => "Number"
    BuiltinFunction::NumberIsNaN => "isNaN"
    BuiltinFunction::NumberIsFinite => "isFinite"
    BuiltinFunction::NumberIsInteger => "isInteger"
    BuiltinFunction::NumberIsSafeInteger => "isSafeInteger"
    BuiltinFunction::NumberToString => "toString"
    BuiltinFunction::NumberToLocaleString => "toLocaleString"
    BuiltinFunction::NumberToFixed => "toFixed"
    BuiltinFunction::NumberToExponential => "toExponential"
    BuiltinFunction::NumberToPrecision => "toPrecision"
    BuiltinFunction::NumberValueOf => "valueOf"
    _ => ""
  }
}

///|
fn builtin_length_number(func : BuiltinFunction) -> Int {
  match func {
    BuiltinFunction::NumberConstructor => 1
    BuiltinFunction::NumberIsNaN => 1
    BuiltinFunction::NumberIsFinite => 1
    BuiltinFunction::NumberIsInteger => 1
    BuiltinFunction::NumberIsSafeInteger => 1
    BuiltinFunction::NumberToString => 1
    BuiltinFunction::NumberToLocaleString => 0
    BuiltinFunction::NumberToFixed => 1
    BuiltinFunction::NumberToExponential => 1
    BuiltinFunction::NumberToPrecision => 1
    BuiltinFunction::NumberValueOf => 0
    _ => 0
  }
}
