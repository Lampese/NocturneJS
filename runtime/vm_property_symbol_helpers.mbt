///|
fn own_symbol_keys_from_props(
  props : Props,
  include_non_enum : Bool,
) -> Array[SymbolValue] {
  let symbols : Array[SymbolValue] = []
  props_iter(props, fn(key, prop) {
    if !is_symbol_prop_key(key) {
      return ()
    }
    if !include_non_enum && !prop.enumerable {
      return ()
    }
    match symbol_from_prop_key(key) {
      Some(symbol) => symbols.push(symbol)
      None => ()
    }
  })
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
