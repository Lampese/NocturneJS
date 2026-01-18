///|
fn proxy_get_value(
  data : ProxyData,
  name : String,
  receiver : Value,
) -> Value raise {
  proxy_check_revoked(data)
  let handler = data.handler
  let target = data.target
  let trap = property_get(handler, "get")
  match trap {
    Undefined | Null => property_get_with_receiver(target, name, receiver)
    _ => {
      if !is_callable(trap) {
        let _ = throw_type_error("get is not callable")
        return Undefined
      }
      let key_value = match symbol_from_prop_key(name) {
        Some(symbol) => Symbol(symbol)
        None => String(name)
      }
      let result = call_value_with_this(
        trap,
        [target, key_value, receiver],
        handler,
      )
      let desc = get_own_property_descriptor(target, name)
      match desc {
        Undefined => result
        _ =>
          if !is_object_like(desc) {
            let _ = throw_type_error("invalid property descriptor")
            Undefined
          } else {
            let configurable = is_truthy(property_get(desc, "configurable"))
            if !configurable {
              let is_data = has_property(desc, "value") ||
                has_property(desc, "writable")
              if is_data {
                let writable = is_truthy(property_get(desc, "writable"))
                if !writable {
                  let expected = property_get(desc, "value")
                  if !same_value(expected, result) {
                    let _ = throw_type_error("proxy: inconsistent get")
                    return Undefined
                  }
                }
              } else {
                let getter = property_get(desc, "get")
                if getter is Undefined && !(result is Undefined) {
                  let _ = throw_type_error("proxy: inconsistent get")
                  return Undefined
                }
              }
            }
            result
          }
      }
    }
  }
}

///|
fn proxy_apply_value(
  data : ProxyData,
  this_arg : Value,
  args : Array[Value],
) -> Value raise {
  proxy_check_revoked(data)
  let handler = data.handler
  let target = data.target
  let trap = property_get(handler, "apply")
  match trap {
    Undefined | Null => call_value_with_this(target, args, this_arg)
    _ => {
      if !is_callable(trap) {
        return throw_type_error("apply is not callable")
      }
      let arg_values : Array[Value?] = []
      for value in args {
        arg_values.push(Some(value))
      }
      let args_array = new_array_value(arg_values)
      call_value_with_this(trap, [target, this_arg, args_array], handler)
    }
  }
}

///|
fn proxy_construct_value(
  data : ProxyData,
  args : Array[Value],
  new_target : Value,
) -> Value raise {
  proxy_check_revoked(data)
  let handler = data.handler
  let target = data.target
  let trap = property_get(handler, "construct")
  match trap {
    Undefined | Null => call_constructor(target, args, new_target~)
    _ => {
      if !is_callable(trap) {
        return throw_type_error("construct is not callable")
      }
      let arg_values : Array[Value?] = []
      for value in args {
        arg_values.push(Some(value))
      }
      let args_array = new_array_value(arg_values)
      let result = call_value_with_this(
        trap,
        [target, args_array, new_target],
        handler,
      )
      if is_object_like(result) {
        result
      } else {
        throw_type_error("proxy construct returned non-object")
      }
    }
  }
}

///|
fn proxy_has_value(data : ProxyData, name : String) -> Bool raise {
  proxy_check_revoked(data)
  let handler = data.handler
  let target = data.target
  let trap = property_get(handler, "has")
  match trap {
    Undefined | Null => has_property_value(target, name)
    _ => {
      if !is_callable(trap) {
        let _ = throw_type_error("has is not callable")
        return false
      }
      let key_value = prop_key_value_from_name(name)
      let result = call_value_with_this(trap, [target, key_value], handler)
      let ok = is_truthy(result)
      if !ok {
        let desc = get_own_property_descriptor(target, name)
        match desc {
          Undefined => ()
          _ =>
            if !is_object_like(desc) {
              let _ = throw_type_error("invalid property descriptor")
              return false
            } else {
              let configurable = is_truthy(property_get(desc, "configurable"))
              if !configurable || !is_extensible_value_checked(target) {
                let _ = throw_type_error("proxy: inconsistent has")
                return false
              }
            }
        }
      }
      ok
    }
  }
}

///|
fn proxy_set_value(
  data : ProxyData,
  name : String,
  value : Value,
  receiver : Value,
  throw_flag : Bool,
) -> Bool raise {
  proxy_check_revoked(data)
  let handler = data.handler
  let target = data.target
  let trap = property_get(handler, "set")
  match trap {
    Undefined | Null => set_value_with_receiver(target, name, value, receiver)
    _ => {
      if !is_callable(trap) {
        let _ = throw_type_error("set is not callable")
        return false
      }
      let key_value = prop_key_value_from_name(name)
      let result = call_value_with_this(
        trap,
        [target, key_value, value, receiver],
        handler,
      )
      let ok = is_truthy(result)
      if ok {
        let desc = get_own_property_descriptor(target, name)
        match desc {
          Undefined => ()
          _ =>
            if !is_object_like(desc) {
              let _ = throw_type_error("invalid property descriptor")
              return false
            } else {
              let configurable = is_truthy(property_get(desc, "configurable"))
              if !configurable {
                if descriptor_is_data(desc) {
                  let writable = is_truthy(property_get(desc, "writable"))
                  if !writable {
                    let expected = property_get(desc, "value")
                    if !same_value(expected, value) {
                      let _ = throw_type_error("proxy: inconsistent set")
                      return false
                    }
                  }
                } else {
                  let setter = property_get(desc, "set")
                  if setter is Undefined {
                    let _ = throw_type_error("proxy: inconsistent set")
                    return false
                  }
                }
              }
            }
        }
      } else if throw_flag {
        let _ = throw_type_error("proxy: cannot set property")
        return false
      }
      ok
    }
  }
}

///|
fn proxy_define_property(
  data : ProxyData,
  name : String,
  desc_info : PropertyDescInfo,
  throw_flag : Bool,
) -> Bool raise {
  proxy_check_revoked(data)
  let handler = data.handler
  let target = data.target
  let trap = property_get(handler, "defineProperty")
  let desc_is_data = desc_info.has_value || desc_info.has_writable
  let desc_is_accessor = desc_info.has_get || desc_info.has_set
  match trap {
    Undefined | Null =>
      define_property_value(
        target,
        prop_key_value_from_name(name),
        property_descriptor_object_from_info(desc_info),
        throw_flag,
      )
    _ => {
      if !is_callable(trap) {
        let _ = throw_type_error("defineProperty is not callable")
        return false
      }
      let key_value = prop_key_value_from_name(name)
      let desc_obj = property_descriptor_object_from_info(desc_info)
      let result = call_value_with_this(
        trap,
        [target, key_value, desc_obj],
        handler,
      )
      let ok = is_truthy(result)
      if !ok {
        if throw_flag {
          let _ = throw_type_error("proxy: defineProperty exception")

        }
        return false
      }
      let target_desc = get_own_property_descriptor(target, name)
      match target_desc {
        Undefined => {
          if !is_extensible_value_checked(target) {
            let _ = throw_type_error("proxy: inconsistent defineProperty")
            return false
          }
          if desc_info.has_configurable && !desc_info.configurable {
            let _ = throw_type_error("proxy: inconsistent defineProperty")
            return false
          }
        }
        _ =>
          if !is_object_like(target_desc) {
            let _ = throw_type_error("invalid property descriptor")
            return false
          } else {
            let target_configurable = is_truthy(
              property_get(target_desc, "configurable"),
            )
            if !target_configurable {
              if desc_info.has_configurable && desc_info.configurable {
                let _ = throw_type_error("proxy: inconsistent defineProperty")
                return false
              }
              if desc_info.has_enumerable &&
                desc_info.enumerable !=
                is_truthy(property_get(target_desc, "enumerable")) {
                let _ = throw_type_error("proxy: inconsistent defineProperty")
                return false
              }
              let target_is_data = descriptor_is_data(target_desc)
              let target_is_accessor = descriptor_is_accessor(target_desc)
              if (desc_is_data || desc_is_accessor) &&
                (
                  desc_is_data != target_is_data ||
                  desc_is_accessor != target_is_accessor
                ) {
                let _ = throw_type_error("proxy: inconsistent defineProperty")
                return false
              }
              if target_is_data {
                let target_writable = is_truthy(
                  property_get(target_desc, "writable"),
                )
                if !target_writable {
                  if desc_info.has_writable && desc_info.writable {
                    let _ = throw_type_error(
                      "proxy: inconsistent defineProperty",
                    )
                    return false
                  }
                  if desc_info.has_value &&
                    !same_value(
                      desc_info.value,
                      property_get(target_desc, "value"),
                    ) {
                    let _ = throw_type_error(
                      "proxy: inconsistent defineProperty",
                    )
                    return false
                  }
                } else if desc_info.has_writable && !desc_info.writable {
                  let _ = throw_type_error("proxy: inconsistent defineProperty")
                  return false
                }
              } else {
                if desc_info.has_get &&
                  !same_value(
                    desc_info.getter,
                    property_get(target_desc, "get"),
                  ) {
                  let _ = throw_type_error("proxy: inconsistent defineProperty")
                  return false
                }
                if desc_info.has_set &&
                  !same_value(
                    desc_info.setter,
                    property_get(target_desc, "set"),
                  ) {
                  let _ = throw_type_error("proxy: inconsistent defineProperty")
                  return false
                }
              }
            } else if desc_info.has_configurable && !desc_info.configurable {
              let _ = throw_type_error("proxy: inconsistent defineProperty")
              return false
            }
          }
      }
      ok
    }
  }
}

///|
fn proxy_delete_property(data : ProxyData, name : String) -> Bool raise {
  proxy_check_revoked(data)
  let handler = data.handler
  let target = data.target
  let trap = property_get(handler, "deleteProperty")
  match trap {
    Undefined | Null => delete_property_value(target, name)
    _ => {
      if !is_callable(trap) {
        let _ = throw_type_error("deleteProperty is not callable")
        return false
      }
      let key_value = prop_key_value_from_name(name)
      let result = call_value_with_this(trap, [target, key_value], handler)
      let ok = is_truthy(result)
      if ok {
        let desc = get_own_property_descriptor(target, name)
        match desc {
          Undefined => ()
          _ =>
            if !is_object_like(desc) {
              let _ = throw_type_error("invalid property descriptor")
              return false
            } else {
              let configurable = is_truthy(property_get(desc, "configurable"))
              if !configurable || !is_extensible_value_checked(target) {
                let _ = throw_type_error("proxy: inconsistent deleteProperty")
                return false
              }
            }
        }
      }
      ok
    }
  }
}

///|
fn proxy_is_extensible(data : ProxyData) -> Bool raise {
  proxy_check_revoked(data)
  let handler = data.handler
  let target = data.target
  let trap = property_get(handler, "isExtensible")
  match trap {
    Undefined | Null => is_extensible_value_checked(target)
    _ => {
      if !is_callable(trap) {
        let _ = throw_type_error("isExtensible is not callable")
        return false
      }
      let result = call_value_with_this(trap, [target], handler)
      let ok = is_truthy(result)
      let target_ext = is_extensible_value_checked(target)
      if ok != target_ext {
        let _ = throw_type_error("proxy: inconsistent isExtensible")
        return false
      }
      ok
    }
  }
}

///|
fn proxy_prevent_extensions(data : ProxyData) -> Bool raise {
  proxy_check_revoked(data)
  let handler = data.handler
  let target = data.target
  let trap = property_get(handler, "preventExtensions")
  match trap {
    Undefined | Null => prevent_extensions_value(target)
    _ => {
      if !is_callable(trap) {
        let _ = throw_type_error("preventExtensions is not callable")
        return false
      }
      let result = call_value_with_this(trap, [target], handler)
      let ok = is_truthy(result)
      if ok && is_extensible_value_checked(target) {
        let _ = throw_type_error("proxy: inconsistent preventExtensions")
        return false
      }
      ok
    }
  }
}

///|
fn proxy_get_prototype(data : ProxyData) -> Value? raise {
  proxy_check_revoked(data)
  let handler = data.handler
  let target = data.target
  let trap = property_get(handler, "getPrototypeOf")
  match trap {
    Undefined | Null => get_proto_of_value_checked(target)
    _ => {
      if !is_callable(trap) {
        let _ = throw_type_error("getPrototypeOf is not callable")
        return None
      }
      let result = call_value_with_this(trap, [target], handler)
      let proto_value = match result {
        Null => None
        _ =>
          if is_object_like(result) {
            Some(result)
          } else {
            let _ = throw_type_error("proxy: inconsistent prototype")
            return None
          }
      }
      if !is_extensible_value_checked(target) {
        let target_proto = get_proto_of_value_checked(target)
        let target_value = match target_proto {
          Some(value) => value
          None => Null
        }
        let result_value = match proto_value {
          Some(value) => value
          None => Null
        }
        if !same_value(result_value, target_value) {
          let _ = throw_type_error("proxy: inconsistent prototype")
          return None
        }
      }
      proto_value
    }
  }
}

///|
fn proxy_set_prototype(
  data : ProxyData,
  proto : Value?,
  throw_flag : Bool,
) -> Bool raise {
  proxy_check_revoked(data)
  let handler = data.handler
  let target = data.target
  let proto_value = match proto {
    Some(value) => value
    None => Null
  }
  let trap = property_get(handler, "setPrototypeOf")
  match trap {
    Undefined | Null => set_proto_of_value(target, proto, throw_flag)
    _ => {
      if !is_callable(trap) {
        let _ = throw_type_error("setPrototypeOf is not callable")
        return false
      }
      let result = call_value_with_this(trap, [target, proto_value], handler)
      let ok = is_truthy(result)
      if !ok {
        if throw_flag {
          let _ = throw_type_error("proxy: cannot set prototype")

        }
        return false
      }
      if !is_extensible_value_checked(target) {
        let target_proto = get_proto_of_value_checked(target)
        let target_value = match target_proto {
          Some(value) => value
          None => Null
        }
        if !same_value(proto_value, target_value) {
          let _ = throw_type_error("proxy: inconsistent prototype")
          return false
        }
      }
      true
    }
  }
}
