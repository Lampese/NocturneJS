///|
fn copy_data_properties(target : ObjectValue, source : Value) -> Unit raise {
  if !is_object_like(source) {
    return
  }
  let from = to_object(source)
  match from {
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => {
          copy_proxy_data_properties(target, from, data)
          return
        }
        None => ()
      }
    _ => ()
  }
  let keys = own_enumerable_string_keys(from)
  for key in keys {
    let value = property_get(from, key)
    target.props.set(key, property_data(value))
  }
  let symbols = own_symbol_keys(from, false)
  for symbol in symbols {
    let key = symbol_prop_key(symbol)
    let value = property_get(from, key)
    target.props.set(key, property_data(value))
  }
}

///|
fn copy_data_properties_excluding(
  target : ObjectValue,
  source : Value,
  excluded : Array[String],
) -> Unit raise {
  if !is_object_like(source) {
    return
  }
  let from = to_object(source)
  let keys = own_property_keys_values(from)
  for key in keys {
    let key_name = match key {
      String(name) => name
      Symbol(symbol) => symbol_prop_key(symbol)
      _ => continue
    }
    if excluded.contains(key_name) {
      continue
    }
    let desc = get_own_property_descriptor(from, key_name)
    match desc {
      Undefined => ()
      _ =>
        if !is_object_like(desc) {
          let _ = throw_type_error("invalid property descriptor")

        } else {
          let enumerable_value = property_get(desc, "enumerable")
          if is_truthy(enumerable_value) {
            let value = property_get(from, key_name)
            target.props.set(key_name, property_data(value))
          }
        }
    }
  }
}
