///|
fn proxy_own_keys(data : ProxyData) -> Array[String] raise {
  let values = proxy_own_keys_values(data)
  let names : Array[String] = []
  for value in values {
    match value {
      String(name) => names.push(name)
      _ => ()
    }
  }
  names
}

///|
fn proxy_own_keys_values(data : ProxyData) -> Array[Value] raise {
  proxy_check_revoked(data)
  let handler = data.handler
  let target = data.target
  let trap = property_get(handler, "ownKeys")
  match trap {
    Undefined | Null => own_property_keys_values(target)
    _ => {
      if !is_callable(trap) {
        let _ = throw_type_error("ownKeys is not callable")
        return []
      }
      let result = call_value_with_this(trap, [target], handler)
      let result_obj = to_object(result)
      let length_value = property_get(result_obj, "length")
      let length_uint = to_uint32(to_number(length_value))
      let length = UInt::reinterpret_as_int(length_uint)
      let values : Array[Value] = []
      let seen : Map[String, Bool] = Map::new()
      for i = 0; i < length; i = i + 1 {
        let entry = property_get(result_obj, "\{i}")
        match entry {
          String(name) => {
            if seen.contains(name) {
              let _ = throw_type_error("proxy: duplicate property")
              return []
            }
            seen.set(name, true)
            values.push(String(name))
          }
          Symbol(symbol) => {
            let key = symbol_prop_key(symbol)
            if seen.contains(key) {
              let _ = throw_type_error("proxy: duplicate property")
              return []
            }
            seen.set(key, true)
            values.push(Symbol(symbol))
          }
          _ => {
            let _ = throw_type_error("ownKeys must return strings or symbols")
            return []
          }
        }
      }
      let target_keys = own_property_keys_values(target)
      let target_set : Map[String, Bool] = Map::new()
      for key in target_keys {
        let key_name = match key {
          String(name) => name
          Symbol(symbol) => symbol_prop_key(symbol)
          _ => continue
        }
        target_set.set(key_name, true)
      }
      let extensible = is_extensible_value_checked(target)
      for key in target_keys {
        let key_name = match key {
          String(name) => name
          Symbol(symbol) => symbol_prop_key(symbol)
          _ => continue
        }
        let desc = get_own_property_descriptor(target, key_name)
        match desc {
          Undefined => ()
          _ =>
            if !is_object_like(desc) {
              let _ = throw_type_error("invalid property descriptor")
              return []
            } else {
              let configurable = is_truthy(property_get(desc, "configurable"))
              if !configurable || !extensible {
                if !seen.contains(key_name) {
                  let _ = throw_type_error(
                    "proxy: target property must be present in proxy ownKeys",
                  )
                  return []
                }
              }
            }
        }
      }
      if !extensible {
        for key, _ in seen {
          if !target_set.contains(key) {
            let _ = throw_type_error(
              "proxy: property not present in target were returned by non extensible proxy",
            )
            return []
          }
        }
      }
      values
    }
  }
}

///|
fn proxy_get_own_property_descriptor_info(
  data : ProxyData,
  name : String,
) -> (Value, PropertyDescInfo?) raise {
  proxy_check_revoked(data)
  let handler = data.handler
  let target = data.target
  let trap = property_get(handler, "getOwnPropertyDescriptor")
  match trap {
    Undefined | Null => (get_own_property_descriptor(target, name), None)
    _ => {
      if !is_callable(trap) {
        let _ = throw_type_error("getOwnPropertyDescriptor is not callable")
        return (Undefined, None)
      }
      let key_value = prop_key_value_from_name(name)
      let result = call_value_with_this(trap, [target, key_value], handler)
      if result is Undefined {
        let target_desc = get_own_property_descriptor(target, name)
        match target_desc {
          Undefined => ()
          _ =>
            if !is_object_like(target_desc) {
              let _ = throw_type_error("invalid property descriptor")
              return (Undefined, None)
            } else {
              let configurable = is_truthy(
                property_get(target_desc, "configurable"),
              )
              if !configurable || !is_extensible_value_checked(target) {
                let _ = throw_type_error(
                  "proxy: inconsistent getOwnPropertyDescriptor",
                )
                return (Undefined, None)
              }
            }
        }
        return (Undefined, None)
      }
      if !is_object_like(result) {
        let _ = throw_type_error("proxy: inconsistent getOwnPropertyDescriptor")
        return (Undefined, None)
      }
      let desc_info = to_property_desc_info(result)
      let target_desc = get_own_property_descriptor(target, name)
      let target_ext = is_extensible_value_checked(target)
      let target_exists = !(target_desc is Undefined)
      if target_exists {
        if !is_object_like(target_desc) {
          let _ = throw_type_error("invalid property descriptor")
          return (Undefined, None)
        }
        let target_configurable = is_truthy(
          property_get(target_desc, "configurable"),
        )
        if !target_configurable {
          if desc_info.has_configurable && desc_info.configurable {
            let _ = throw_type_error(
              "proxy: inconsistent getOwnPropertyDescriptor",
            )
            return (Undefined, None)
          }
          if desc_info.has_enumerable &&
            desc_info.enumerable !=
            is_truthy(property_get(target_desc, "enumerable")) {
            let _ = throw_type_error(
              "proxy: inconsistent getOwnPropertyDescriptor",
            )
            return (Undefined, None)
          }
          let target_is_accessor = descriptor_is_accessor(target_desc)
          let desc_has_fields = desc_info.has_value ||
            desc_info.has_writable ||
            desc_info.has_get ||
            desc_info.has_set
          let desc_is_accessor = desc_info.has_get || desc_info.has_set
          if desc_has_fields && desc_is_accessor != target_is_accessor {
            let _ = throw_type_error(
              "proxy: inconsistent getOwnPropertyDescriptor",
            )
            return (Undefined, None)
          }
          if !target_is_accessor {
            let target_writable = is_truthy(
              property_get(target_desc, "writable"),
            )
            if !target_writable {
              if desc_info.has_writable && desc_info.writable {
                let _ = throw_type_error(
                  "proxy: inconsistent getOwnPropertyDescriptor",
                )
                return (Undefined, None)
              }
            }
          }
        }
      } else if !target_ext {
        let _ = throw_type_error("proxy: inconsistent getOwnPropertyDescriptor")
        return (Undefined, None)
      }
      let result_configurable = desc_info.has_configurable &&
        desc_info.configurable
      if !result_configurable {
        if !target_exists {
          let _ = throw_type_error(
            "proxy: inconsistent getOwnPropertyDescriptor",
          )
          return (Undefined, None)
        }
        let target_configurable = is_truthy(
          property_get(target_desc, "configurable"),
        )
        if target_configurable {
          let _ = throw_type_error(
            "proxy: inconsistent getOwnPropertyDescriptor",
          )
          return (Undefined, None)
        }
        let target_is_accessor = descriptor_is_accessor(target_desc)
        if !target_is_accessor {
          let result_has_accessor = desc_info.has_get || desc_info.has_set
          if !result_has_accessor {
            let result_writable = desc_info.has_writable && desc_info.writable
            if !result_writable {
              let target_writable = is_truthy(
                property_get(target_desc, "writable"),
              )
              if target_writable {
                let _ = throw_type_error(
                  "proxy: inconsistent getOwnPropertyDescriptor",
                )
                return (Undefined, None)
              }
            }
          }
        }
      }
      (result, Some(desc_info))
    }
  }
}

///|
fn proxy_get_own_property_descriptor(
  data : ProxyData,
  name : String,
) -> Value raise {
  let (result, _) = proxy_get_own_property_descriptor_info(data, name)
  result
}

///|
fn copy_proxy_data_properties(
  target : ObjectValue,
  source : Value,
  data : ProxyData,
) -> Unit raise {
  let keys = proxy_own_keys_values(data)
  for key_value in keys {
    let name = match key_value {
      String(text) => text
      Symbol(symbol) => symbol_prop_key(symbol)
      _ => {
        let _ = throw_type_error("ownKeys must return strings or symbols")
        continue
      }
    }
    let desc = proxy_get_own_property_descriptor(data, name)
    match desc {
      Undefined => ()
      _ =>
        if !is_object_like(desc) {
          let _ = throw_type_error("invalid property descriptor")

        } else {
          let enumerable_value = property_get(desc, "enumerable")
          if is_truthy(enumerable_value) {
            let value = property_get(source, name)
            target.props.set(name, property_data(value))
          }
        }
    }
  }
}
