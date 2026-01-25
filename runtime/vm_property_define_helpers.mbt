///|
fn define_property(target : Value, key : Value, desc : Value) -> Value raise {
  let ok = define_property_value(target, key, desc, true)
  if !ok {
    return throw_type_error("cannot define property")
  }
  target
}

///|
fn module_namespace_define_property(
  obj : ObjectValue,
  name : String,
  desc_info : PropertyDescInfo,
  throw_flag : Bool,
) -> Bool raise {
  match props_get(obj.props, name) {
    None => {
      if throw_flag {
        let _ = throw_type_error("cannot define property")

      }
      false
    }
    Some(prop) => {
      if desc_info.has_get || desc_info.has_set {
        if throw_flag {
          let _ = throw_type_error("cannot redefine property")

        }
        return false
      }
      let current_writable = match module_binding_info(prop.value) {
        Some(_) => true
        None => prop.writable
      }
      if desc_info.has_configurable &&
        desc_info.configurable != prop.configurable {
        if throw_flag {
          let _ = throw_type_error("cannot redefine property")

        }
        return false
      }
      if desc_info.has_enumerable && desc_info.enumerable != prop.enumerable {
        if throw_flag {
          let _ = throw_type_error("cannot redefine property")

        }
        return false
      }
      if desc_info.has_writable && desc_info.writable != current_writable {
        if throw_flag {
          let _ = throw_type_error("cannot redefine property")

        }
        return false
      }
      if desc_info.has_value {
        let current_value = match module_binding_info(prop.value) {
          Some(_) => module_binding_deref(prop.value)
          None => prop.value
        }
        if !same_value(desc_info.value, current_value) {
          if throw_flag {
            let _ = throw_type_error("cannot redefine property")

          }
          return false
        }
      }
      true
    }
  }
}

///|
fn define_property_value(
  target : Value,
  key : Value,
  desc : Value,
  throw_flag : Bool,
) -> Bool raise {
  let name = property_key_name(key)
  if !is_object_like(target) {
    let _ = throw_type_error("not an object")
    return false
  }
  let desc_info = to_property_desc_info(desc)
  let desc_is_data = desc_info.has_value || desc_info.has_writable
  let desc_is_accessor = desc_info.has_get || desc_info.has_set
  match target {
    Object(obj) =>
      match obj.proxy_data {
        Some(data) =>
          return proxy_define_property(data, name, desc_info, throw_flag)
        None => ()
      }
    _ => ()
  }
  match target {
    Object(obj) if obj.is_module_namespace =>
      return module_namespace_define_property(obj, name, desc_info, throw_flag)
    _ => ()
  }
  let skip_validation = match target {
    Array(_) => name == "length"
    _ => false
  }
  if !skip_validation {
    let current_desc = get_own_property_descriptor(target, name)
    match current_desc {
      Undefined =>
        if !is_extensible_value(target) {
          if throw_flag {
            let _ = throw_type_error("object is not extensible")

          }
          return false
        }
      _ =>
        if !is_object_like(current_desc) {
          let _ = throw_type_error("invalid property descriptor")
          return false
        } else {
          let current_info = to_property_desc_info(current_desc)
          let current_configurable = if current_info.has_configurable {
            current_info.configurable
          } else {
            false
          }
          let current_enumerable = if current_info.has_enumerable {
            current_info.enumerable
          } else {
            false
          }
          if !current_configurable {
            if desc_info.has_configurable && desc_info.configurable {
              if throw_flag {
                let _ = throw_type_error("cannot redefine property")

              }
              return false
            }
            if desc_info.has_enumerable &&
              desc_info.enumerable != current_enumerable {
              if throw_flag {
                let _ = throw_type_error("cannot redefine property")

              }
              return false
            }
            let current_is_data = current_info.has_value ||
              current_info.has_writable
            let current_is_accessor = current_info.has_get ||
              current_info.has_set
            if (desc_is_data || desc_is_accessor) &&
              (
                desc_is_data != current_is_data ||
                desc_is_accessor != current_is_accessor
              ) {
              if throw_flag {
                let _ = throw_type_error("cannot redefine property")

              }
              return false
            }
            if current_is_data {
              let current_writable = if current_info.has_writable {
                current_info.writable
              } else {
                false
              }
              if !current_writable {
                if desc_info.has_writable && desc_info.writable {
                  if throw_flag {
                    let _ = throw_type_error("cannot redefine property")

                  }
                  return false
                }
                if desc_info.has_value &&
                  !same_value(desc_info.value, current_info.value) {
                  if throw_flag {
                    let _ = throw_type_error("cannot redefine property")

                  }
                  return false
                }
              }
            } else if current_is_accessor {
              if desc_info.has_get &&
                !same_value(desc_info.getter, current_info.getter) {
                if throw_flag {
                  let _ = throw_type_error("cannot redefine property")

                }
                return false
              }
              if desc_info.has_set &&
                !same_value(desc_info.setter, current_info.setter) {
                if throw_flag {
                  let _ = throw_type_error("cannot redefine property")

                }
                return false
              }
            }
          }
        }
    }
  }
  match target {
    Object(obj) => {
      match obj.string_data {
        Some(value) =>
          match parse_array_index(name) {
            Some(index) =>
              if index >= 0 &&
                index < value.length() &&
                !props_contains(obj.props, name) {
                props_set(obj.props, name, Property::{
                  value: String(
                    value.unsafe_substring(start=index, end=index + 1),
                  ),
                  writable: false,
                  configurable: false,
                  enumerable: true,
                  getter: None,
                  setter: None,
                })
              }
            None => ()
          }
        None => ()
      }
      define_property_in(obj.props, name, desc_info)
      true
    }
    Function(func) => {
      if name == "prototype" {
        ensure_function_prototype(func)
      }
      define_property_in(func.props, name, desc_info)
      true
    }
    BoundFunction(bound) => {
      define_property_in(bound.props, name, desc_info)
      true
    }
    Builtin(builtin) => {
      define_property_in(builtin.props, name, desc_info)
      true
    }
    Arguments(args) => {
      arguments_define_property(args, name, desc_info)
      true
    }
    Array(arr) =>
      try {
        define_array_property(arr, name, desc_info)
        true
      } catch {
        err => if throw_flag { raise err } else { false }
      }
    _ => false
  }
}

///|
fn prop_key_value_from_name(name : String) -> Value {
  match symbol_from_prop_key(name) {
    Some(symbol) => Symbol(symbol)
    None => String(name)
  }
}

///|
fn create_data_property(
  target : Value,
  name : String,
  value : Value,
) -> Bool raise {
  if !is_object_like(target) {
    let _ = throw_type_error("not an object")
    return false
  }
  match target {
    Object(obj) =>
      match obj.proxy_data {
        Some(_) => {
          let desc_obj = property_descriptor_object(property_data(value))
          let key_value = prop_key_value_from_name(name)
          return define_property_value(target, key_value, desc_obj, false)
        }
        None => ()
      }
    _ => ()
  }
  let desc = get_own_property_descriptor(target, name)
  match desc {
    Undefined => if !is_extensible_value(target) { return false }
    _ =>
      if !is_object_like(desc) {
        let _ = throw_type_error("invalid property descriptor")
        return false
      } else {
        let configurable = is_truthy(property_get(desc, "configurable"))
        if !configurable {
          return false
        }
      }
  }
  let desc_obj = property_descriptor_object(property_data(value))
  let key_value = prop_key_value_from_name(name)
  let _ = define_property(target, key_value, desc_obj)
  true
}

///|
fn create_data_property_or_throw(
  target : Value,
  name : String,
  value : Value,
) -> Unit raise {
  if !create_data_property(target, name, value) {
    let _ = throw_type_error("cannot define property")

  }
}

///|
fn define_properties_from_object(target : Value, props : Value) -> Unit raise {
  if !is_object_like(target) {
    let _ = throw_type_error("not an object")
    return
  }
  let props_obj = to_object(props)
  match props_obj {
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => {
          let keys = proxy_own_keys_values(data)
          let filtered : Array[Value] = []
          for key in keys {
            match key {
              String(name) => {
                let desc_obj = proxy_get_own_property_descriptor(data, name)
                match desc_obj {
                  Undefined => ()
                  _ =>
                    if !is_object_like(desc_obj) {
                      let _ = throw_type_error("invalid property descriptor")

                    } else if is_truthy(property_get(desc_obj, "enumerable")) {
                      filtered.push(String(name))
                    }
                }
              }
              Symbol(symbol) => {
                let key_name = symbol_prop_key(symbol)
                let desc_obj = proxy_get_own_property_descriptor(data, key_name)
                match desc_obj {
                  Undefined => ()
                  _ =>
                    if !is_object_like(desc_obj) {
                      let _ = throw_type_error("invalid property descriptor")

                    } else if is_truthy(property_get(desc_obj, "enumerable")) {
                      filtered.push(Symbol(symbol))
                    }
                }
              }
              _ => ()
            }
          }
          for key in filtered {
            match key {
              String(name) => {
                let desc = property_get(props_obj, name)
                let _ = define_property(target, String(name), desc)

              }
              Symbol(symbol) => {
                let key_name = symbol_prop_key(symbol)
                let desc = property_get(props_obj, key_name)
                let _ = define_property(target, Symbol(symbol), desc)

              }
              _ => ()
            }
          }
          return
        }
        None => ()
      }
    _ => ()
  }
  let names_value = object_keys(props_obj)
  match names_value {
    Array(arr) => {
      let len = arr.elements.length()
      for i = 0; i < len; i = i + 1 {
        match arr.elements[i] {
          Some(String(name)) => {
            let desc = property_get(props_obj, name)
            let _ = define_property(target, String(name), desc)

          }
          _ => ()
        }
      }
    }
    _ => ()
  }
  let symbols = own_symbol_keys(props_obj, false)
  for symbol in symbols {
    let name = symbol_prop_key(symbol)
    let desc = property_get(props_obj, name)
    let _ = define_property(target, Symbol(symbol), desc)

  }
}

///|
fn arguments_define_property(
  args : ArgumentsValue,
  name : String,
  desc_info : PropertyDescInfo,
) -> Unit raise {
  match parse_array_index(name) {
    Some(index) =>
      if index >= 0 && index < args.elements.length() {
        if !props_contains(args.props, name) {
          let current = arguments_index_value(args, index)
          props_set(args.props, name, property_data(current))
        }
        let mut mapped = false
        if index < args.mapped.length() &&
          index < args.params.length() &&
          args.mapped[index] {
          mapped = true
        }
        if mapped &&
          !desc_info.has_value &&
          !(desc_info.has_get || desc_info.has_set) {
          let current = arguments_index_value(args, index)
          match props_get(args.props, name) {
            Some(prop) =>
              if prop.getter is None && prop.setter is None {
                props_set(args.props, name, Property::{
                  value: current,
                  writable: prop.writable,
                  configurable: prop.configurable,
                  enumerable: prop.enumerable,
                  getter: None,
                  setter: None,
                })
              }
            None => ()
          }
        }
        define_property_in(args.props, name, desc_info)
        if mapped {
          if desc_info.has_value && !(desc_info.has_get || desc_info.has_set) {
            env_set_local(args.env, args.params[index], desc_info.value)
          }
          if desc_info.has_get ||
            desc_info.has_set ||
            (desc_info.has_writable && !desc_info.writable) {
            args.mapped[index] = false
          }
        }
        rc_replace_optional_value(args.elements[index], None)
        args.elements[index] = None
      } else {
        define_property_in(args.props, name, desc_info)
      }
    None => define_property_in(args.props, name, desc_info)
  }
}

///|
fn define_array_property(
  arr : ArrayValue,
  name : String,
  desc_info : PropertyDescInfo,
) -> Unit raise {
  match arr.typed_array_data {
    Some(data) =>
      match canonical_numeric_index_string(name) {
        Some(index) => {
          if desc_info.has_get || desc_info.has_set {
            let _ = throw_type_error("invalid property descriptor")
            return
          }
          if desc_info.has_configurable && !desc_info.configurable {
            let _ = throw_type_error("cannot define property")
            return
          }
          if desc_info.has_enumerable && !desc_info.enumerable {
            let _ = throw_type_error("cannot define property")
            return
          }
          if desc_info.has_writable && !desc_info.writable {
            let _ = throw_type_error("cannot define property")
            return
          }
          if !typed_array_is_valid_integer_index(data, index) {
            let _ = throw_type_error("cannot define property")
            return
          }
          if desc_info.has_value {
            integer_indexed_element_set(data, index, desc_info.value)
          }
          return
        }
        None => {
          define_property_in(arr.props, name, desc_info)
          return
        }
      }
    None => ()
  }
  if name == "length" {
    let mut target_len : Int64? = None
    if desc_info.has_value {
      let new_len_uint = to_uint32(to_number(desc_info.value))
      let number_len = to_number(desc_info.value)
      let new_len_num = UInt::to_double(new_len_uint)
      if Double::is_nan(number_len) || new_len_num != number_len {
        let _ = throw_range_error("invalid array length")
        return
      }
      target_len = Some(Double::to_int64(new_len_num))
    }
    if desc_info.has_get || desc_info.has_set {
      let _ = throw_type_error("invalid property descriptor")
      return
    }
    if desc_info.has_configurable && desc_info.configurable {
      let _ = throw_type_error("cannot redefine property")
      return
    }
    if desc_info.has_enumerable && desc_info.enumerable {
      let _ = throw_type_error("cannot redefine property")
      return
    }
    let mut current_writable = true
    match props_get(arr.props, "length") {
      Some(prop) => current_writable = prop.writable
      None => ()
    }
    if !current_writable && desc_info.has_writable && desc_info.writable {
      let _ = throw_type_error("cannot redefine property")
      return
    }
    let mut writable = current_writable
    if desc_info.has_writable {
      writable = desc_info.writable
    }
    match target_len {
      Some(new_len) => {
        let ok = array_set_length_int64(arr, new_len, true)
        if !ok {
          if desc_info.has_writable && !desc_info.writable {
            let len_value = Number(
              Int64::to_double(array_length_from_props(arr)),
            )
            props_set(arr.props, "length", Property::{
              value: len_value,
              writable: false,
              configurable: false,
              enumerable: false,
              getter: None,
              setter: None,
            })
          }
          let _ = throw_type_error("cannot redefine property")
          return
        }
      }
      None => ()
    }
    let len_value = Number(Int64::to_double(array_length_from_props(arr)))
    props_set(arr.props, "length", Property::{
      value: len_value,
      writable,
      configurable: false,
      enumerable: false,
      getter: None,
      setter: None,
    })
    return
  }
  match array_index_from_name(name) {
    Some(index64) => {
      if index64 >= 0L {
        let current_len = array_length_from_props(arr)
        match props_get(arr.props, "length") {
          Some(prop) =>
            if !prop.writable && index64 >= current_len {
              let _ = throw_type_error("cannot define property")
              return
            }
          None => ()
        }
        let elements_len = arr.elements.length()
        if index64 <= Int64::from_int(elements_len) {
          let index = Int64::to_int(index64)
          if index == elements_len {
            arr.elements.push(None)
          }
        }
        let current_len = array_length_from_props(arr)
        let elem_len = Int64::from_int(arr.elements.length())
        let mut new_len = if elem_len > current_len {
          elem_len
        } else {
          current_len
        }
        let index_len = index64 + 1L
        if index_len > new_len {
          new_len = index_len
        }
        set_length_prop(arr.props, Number(Int64::to_double(new_len)))
      }
      if arr.typed_array_data is None &&
        !props_contains(arr.props, name) &&
        index64 >= 0L &&
        index64 < Int64::from_int(arr.elements.length()) {
        let index = Int64::to_int(index64)
        match arr.elements[index] {
          Some(value) => props_set(arr.props, name, property_data(value))
          None => ()
        }
      }
      define_property_in(arr.props, name, desc_info)
    }
    None => define_property_in(arr.props, name, desc_info)
  }
}

///|
fn define_property_in(
  props : Props,
  name : String,
  desc_info : PropertyDescInfo,
) -> Unit {
  let mut value = Undefined
  let mut writable = false
  let mut configurable = false
  let mut enumerable = false
  let mut getter : Value? = None
  let mut setter : Value? = None
  let mut has_value = false
  let mut has_writable = false
  let mut has_get = false
  let mut has_set = false
  match props_get(props, name) {
    Some(existing) => {
      value = existing.value
      writable = existing.writable
      configurable = existing.configurable
      enumerable = existing.enumerable
      getter = existing.getter
      setter = existing.setter
    }
    None => ()
  }
  if desc_info.has_value {
    value = desc_info.value
    has_value = true
  }
  if desc_info.has_writable {
    writable = desc_info.writable
    has_writable = true
  }
  if desc_info.has_configurable {
    configurable = desc_info.configurable
  }
  if desc_info.has_enumerable {
    enumerable = desc_info.enumerable
  }
  if desc_info.has_get {
    has_get = true
    getter = Some(desc_info.getter)
  }
  if desc_info.has_set {
    has_set = true
    setter = Some(desc_info.setter)
  }
  let is_accessor = has_get || has_set
  let is_data = has_value || has_writable
  if is_data && !is_accessor {
    getter = None
    setter = None
  }
  if is_accessor && !is_data {
    value = Undefined
    writable = false
  }
  props_set(props, name, Property::{
    value,
    writable,
    configurable,
    enumerable,
    getter,
    setter,
  })
}
