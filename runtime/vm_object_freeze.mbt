///|
fn object_seal(target : Value, freeze_flag : Bool) -> Value raise {
  if !is_object_like(target) {
    return target
  }
  let ok = prevent_extensions_value(target)
  if !ok {
    let _ = throw_type_error("proxy preventExtensions handler returned false")
    return target
  }
  let keys = own_property_keys_values(target)
  for key in keys {
    let name = match key {
      String(prop) => prop
      Symbol(symbol) => symbol_prop_key(symbol)
      _ => continue
    }
    let current = get_own_property_descriptor(target, name)
    match current {
      Undefined => ()
      _ =>
        if !is_object_like(current) {
          let _ = throw_type_error("invalid property descriptor")

        } else {
          let seal_desc = new_object_value()
          match seal_desc {
            Object(desc_obj) => {
              desc_obj.props.set("configurable", property_data(Bool(false)))
              if freeze_flag &&
                descriptor_is_data(current) &&
                is_truthy(property_get(current, "writable")) {
                desc_obj.props.set("writable", property_data(Bool(false)))
              }
            }
            _ => ()
          }
          let _ = define_property_value(target, key, seal_desc, true)

        }
    }
  }
  target
}

///|
fn object_is_sealed(target : Value, freeze_flag : Bool) -> Bool raise {
  if !is_object_like(target) {
    return true
  }
  let keys = own_property_keys_values(target)
  for key in keys {
    let name = match key {
      String(prop) => prop
      Symbol(symbol) => symbol_prop_key(symbol)
      _ => continue
    }
    let desc = get_own_property_descriptor(target, name)
    match desc {
      Undefined => ()
      _ =>
        if !is_object_like(desc) {
          let _ = throw_type_error("invalid property descriptor")
          return false
        } else {
          if is_truthy(property_get(desc, "configurable")) {
            return false
          }
          if freeze_flag &&
            descriptor_is_data(desc) &&
            is_truthy(property_get(desc, "writable")) {
            return false
          }
        }
    }
  }
  !is_extensible_value_checked(target)
}

///|
fn object_freeze(target : Value) -> Value raise {
  object_seal(target, true)
}
