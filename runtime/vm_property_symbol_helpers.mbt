///|
fn own_symbol_keys_from_props(
  props : Map[String, Property],
  include_non_enum : Bool,
) -> Array[SymbolValue] {
  let symbols : Array[SymbolValue] = []
  for key, prop in props {
    if !is_symbol_prop_key(key) {
      continue
    }
    if !include_non_enum && !prop.enumerable {
      continue
    }
    match symbol_from_prop_key(key) {
      Some(symbol) => symbols.push(symbol)
      None => ()
    }
  }
  symbols
}

///|
fn own_symbol_keys(
  target : Value,
  include_non_enum : Bool,
) -> Array[SymbolValue] {
  match target {
    Object(obj) =>
      match obj.proxy_data {
        Some(_) => []
        None => own_symbol_keys_from_props(obj.props, include_non_enum)
      }
    _ =>
      match props_map_for_value(target) {
        Some(props) => own_symbol_keys_from_props(props, include_non_enum)
        None => []
      }
  }
}
