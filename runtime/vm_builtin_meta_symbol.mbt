///|
fn builtin_name_symbol(func : BuiltinFunction) -> String {
  match func {
    BuiltinFunction::SymbolConstructor => "Symbol"
    BuiltinFunction::SymbolFor => "for"
    BuiltinFunction::SymbolKeyFor => "keyFor"
    BuiltinFunction::SymbolToString => "toString"
    BuiltinFunction::SymbolValueOf => "valueOf"
    BuiltinFunction::SymbolToPrimitive => "[Symbol.toPrimitive]"
    BuiltinFunction::SymbolDescription => "description"
    _ => ""
  }
}

///|
fn builtin_length_symbol(func : BuiltinFunction) -> Int {
  match func {
    BuiltinFunction::SymbolConstructor => 0
    BuiltinFunction::SymbolFor => 1
    BuiltinFunction::SymbolKeyFor => 1
    BuiltinFunction::SymbolToString => 0
    BuiltinFunction::SymbolValueOf => 0
    BuiltinFunction::SymbolToPrimitive => 1
    BuiltinFunction::SymbolDescription => 0
    _ => 0
  }
}
