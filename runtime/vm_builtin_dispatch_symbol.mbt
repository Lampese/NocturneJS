///|
fn call_builtin_symbol(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match builtin.kind {
    BuiltinFunction::SymbolConstructor => {
      let description = if args.is_empty() || args[0] is Undefined {
        None
      } else {
        Some(to_string_strict(args[0]))
      }
      let symbol = SymbolValue::{
        id: alloc_symbol_id(),
        description,
        key: None,
      }
      register_symbol(symbol)
      Symbol(symbol)
    }
    BuiltinFunction::SymbolFor => {
      let key_value = if args.is_empty() { Undefined } else { args[0] }
      let key = to_string_strict(key_value)
      match symbol_registry_get(key) {
        Some(symbol) => Symbol(symbol)
        None => {
          let symbol = SymbolValue::{
            id: alloc_symbol_id(),
            description: Some(key),
            key: Some(key),
          }
          symbol_registry_set(key, symbol)
          register_symbol(symbol)
          Symbol(symbol)
        }
      }
    }
    BuiltinFunction::SymbolKeyFor => {
      if args.is_empty() {
        return throw_type_error("not a symbol")
      }
      match args[0] {
        Symbol(symbol) =>
          match symbol.key {
            Some(key) => String(key)
            None => Undefined
          }
        _ => throw_type_error("not a symbol")
      }
    }
    BuiltinFunction::SymbolToString =>
      String(symbol_to_string(this_to_symbol(builtin.this_value)))
    BuiltinFunction::SymbolValueOf => Symbol(this_to_symbol(builtin.this_value))
    BuiltinFunction::SymbolToPrimitive =>
      Symbol(this_to_symbol(builtin.this_value))
    BuiltinFunction::SymbolDescription =>
      match this_to_symbol(builtin.this_value).description {
        Some(desc) => String(desc)
        None => Undefined
      }
    _ => throw_type_error("invalid builtin")
  }
}
