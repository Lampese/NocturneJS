///|
fn property_get_from_props(
  props : Map[String, Property],
  name : String,
  receiver : Value,
) -> Value? raise {
  match property_get_from_props_raw(props, name, receiver) {
    Some(value) => Some(module_binding_deref(value))
    None => None
  }
}

///|
fn property_get_from_props_raw(
  props : Map[String, Property],
  name : String,
  receiver : Value,
) -> Value? raise {
  match props.get(name) {
    Some(prop) =>
      Some(
        match prop.getter {
          Some(getter) =>
            match getter {
              Undefined | Null => Undefined
              _ => call_value_with_this(getter, [], receiver)
            }
          None => prop.value
        },
      )
    None => None
  }
}

///|
fn property_get_from_chain(
  props : Map[String, Property],
  proto : Value?,
  name : String,
  receiver : Value,
) -> Value raise {
  match property_get_from_props(props, name, receiver) {
    Some(value) => value
    None =>
      match proto {
        Some(parent) => property_get_with_receiver(parent, name, receiver)
        None => Undefined
      }
  }
}

///|
fn property_get_from_chain_raw(
  props : Map[String, Property],
  proto : Value?,
  name : String,
  receiver : Value,
) -> Value raise {
  match property_get_from_props_raw(props, name, receiver) {
    Some(value) => value
    None =>
      match proto {
        Some(parent) =>
          match props_map_for_value(parent) {
            Some(parent_props) =>
              property_get_from_chain_raw(
                parent_props,
                proto_for_value(parent),
                name,
                receiver,
              )
            None => Undefined
          }
        None => Undefined
      }
  }
}

///|
fn primitive_proto(value : Value) -> Value? {
  let env = current_env()
  match value {
    String(_) =>
      match env {
        Some(env) =>
          match value_from_object(string_proto_for_env(env)) {
            Some(value) => Some(value)
            None => value_from_object(string_proto())
          }
        None => value_from_object(string_proto())
      }
    Number(_) =>
      match env {
        Some(env) =>
          match value_from_object(number_proto_for_env(env)) {
            Some(value) => Some(value)
            None => value_from_object(number_proto())
          }
        None => value_from_object(number_proto())
      }
    Bool(_) =>
      match env {
        Some(env) =>
          match value_from_object(bool_proto_for_env(env)) {
            Some(value) => Some(value)
            None => value_from_object(bool_proto())
          }
        None => value_from_object(bool_proto())
      }
    BigInt(_) =>
      match env {
        Some(env) =>
          match value_from_object(bigint_proto_for_env(env)) {
            Some(value) => Some(value)
            None => value_from_object(bigint_proto())
          }
        None => value_from_object(bigint_proto())
      }
    Symbol(_) =>
      match env {
        Some(env) =>
          match value_from_object(symbol_proto_for_env(env)) {
            Some(value) => Some(value)
            None => value_from_object(symbol_proto())
          }
        None => value_from_object(symbol_proto())
      }
    _ => None
  }
}

///|
fn set_property_for_value(
  target : Value,
  name : String,
  value : Value,
  strict : Bool,
) -> Value raise {
  if is_object_like(target) {
    let _ = property_set(target, name, value, strict)
    return value
  }
  match target {
    Null | Undefined => {
      let _ = throw_type_error("cannot set property")
      value
    }
    _ => {
      let ok = match primitive_proto(target) {
        Some(proto) => set_value_with_receiver(proto, name, value, target)
        None => false
      }
      if !ok && strict {
        let _ = throw_type_error("cannot set property")

      }
      value
    }
  }
}

///|
fn property_set(
  target : Value,
  name : String,
  value : Value,
  strict : Bool,
) -> Value raise {
  match target {
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => {
          let ok = proxy_set_value(data, name, value, target, false)
          if !ok && strict {
            return throw_type_error("proxy: cannot set property")
          }
          return value
        }
        None => ()
      }
    _ => ()
  }
  let ok = match target {
    Object(obj) =>
      if is_array_proto_object(obj) {
        array_proto_set(obj, name, value)
      } else {
        match obj.string_data {
          Some(string_value) =>
            if name == "length" {
              false
            } else {
              match parse_array_index(name) {
                Some(index) =>
                  if index >= 0 && index < string_value.length() {
                    false
                  } else {
                    set_property_with_proto(
                      obj.props,
                      obj.proto,
                      name,
                      value,
                      target,
                      obj.extensible,
                    )
                  }
                None =>
                  set_property_with_proto(
                    obj.props,
                    obj.proto,
                    name,
                    value,
                    target,
                    obj.extensible,
                  )
              }
            }
          None =>
            set_property_with_proto(
              obj.props,
              obj.proto,
              name,
              value,
              target,
              obj.extensible,
            )
        }
      }
    Function(func) => {
      if name == "prototype" && !func.props.contains(name) {
        ensure_function_prototype(func)
      }
      set_property_with_proto(
        func.props,
        func.proto,
        name,
        value,
        Value::Function(func),
        func.extensible,
      )
    }
    BoundFunction(bound) =>
      set_property_with_proto(
        bound.props,
        bound.proto,
        name,
        value,
        Value::BoundFunction(bound),
        bound.extensible,
      )
    Builtin(builtin) =>
      set_property_with_proto(
        builtin.props,
        builtin.proto,
        name,
        value,
        Value::Builtin(builtin),
        builtin.extensible,
      )
    Array(arr) => array_set(arr, name, value)
    Arguments(args) => arguments_set(args, name, value)
    _ => false
  }
  if !ok && strict {
    return throw_type_error("cannot set property")
  }
  value
}

///|
fn set_value_with_receiver(
  target : Value,
  name : String,
  value : Value,
  receiver : Value,
) -> Bool raise {
  if !is_object_like(target) {
    return false
  }
  match target {
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => return proxy_set_value(data, name, value, receiver, false)
        None => ()
      }
    _ => ()
  }
  match target {
    Object(obj) if obj.is_module_namespace => return false
    _ => ()
  }
  match target {
    Array(arr) =>
      match arr.typed_array_data {
        Some(data) =>
          match canonical_numeric_index_string(name) {
            Some(index) =>
              if same_value(target, receiver) {
                integer_indexed_element_set(data, index, value)
                return true
              } else if !typed_array_is_valid_integer_index(data, index) {
                return true
              }
            None => ()
          }
        None => ()
      }
    _ => ()
  }
  let desc = get_own_property_descriptor(target, name)
  match desc {
    Undefined =>
      match get_proto_of_value(target) {
        Some(proto) => set_value_with_receiver(proto, name, value, receiver)
        None =>
          if !is_object_like(receiver) {
            false
          } else {
            let (receiver_desc, receiver_info) = get_own_property_descriptor_with_info(
              receiver, name,
            )
            match receiver_desc {
              Undefined =>
                if !is_extensible_value(receiver) {
                  false
                } else {
                  let desc_obj = property_descriptor_object(
                    property_data(value),
                  )
                  define_property_value(
                    receiver,
                    prop_key_value_from_name(name),
                    desc_obj,
                    false,
                  )
                }
              _ =>
                if !is_object_like(receiver_desc) {
                  let _ = throw_type_error("invalid property descriptor")
                  false
                } else {
                  let info = match receiver_info {
                    Some(info) => info
                    None => to_property_desc_info(receiver_desc)
                  }
                  if info.has_get || info.has_set {
                    false
                  } else {
                    let receiver_writable = if info.has_writable {
                      info.writable
                    } else {
                      false
                    }
                    if !receiver_writable {
                      false
                    } else {
                      let desc_obj = new_object_value()
                      match desc_obj {
                        Object(obj) =>
                          props_set(obj.props, "value", property_data(value))
                        _ => ()
                      }
                      define_property_value(
                        receiver,
                        prop_key_value_from_name(name),
                        desc_obj,
                        false,
                      )
                    }
                  }
                }
            }
          }
      }
    _ =>
      if !is_object_like(desc) {
        let _ = throw_type_error("invalid property descriptor")
        false
      } else if descriptor_is_data(desc) {
        let writable = is_truthy(property_get(desc, "writable"))
        if !writable {
          return false
        }
        if !is_object_like(receiver) {
          return false
        }
        let (receiver_desc, receiver_info) = get_own_property_descriptor_with_info(
          receiver, name,
        )
        match receiver_desc {
          Undefined =>
            if !is_extensible_value(receiver) {
              false
            } else {
              let desc_obj = property_descriptor_object(property_data(value))
              define_property_value(
                receiver,
                prop_key_value_from_name(name),
                desc_obj,
                false,
              )
            }
          _ =>
            if !is_object_like(receiver_desc) {
              let _ = throw_type_error("invalid property descriptor")
              false
            } else {
              let info = match receiver_info {
                Some(info) => info
                None => to_property_desc_info(receiver_desc)
              }
              if info.has_get || info.has_set {
                false
              } else {
                let receiver_writable = if info.has_writable {
                  info.writable
                } else {
                  false
                }
                if !receiver_writable {
                  false
                } else {
                  let desc_obj = new_object_value()
                  match desc_obj {
                    Object(obj) => props_set(obj.props, "value", property_data(value))
                    _ => ()
                  }
                  define_property_value(
                    receiver,
                    prop_key_value_from_name(name),
                    desc_obj,
                    false,
                  )
                }
              }
            }
        }
      } else {
        let setter = property_get(desc, "set")
        if setter is Undefined {
          false
        } else {
          let _ = call_value_with_this(setter, [value], receiver)
          true
        }
      }
  }
}
