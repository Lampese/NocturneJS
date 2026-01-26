///|
fn has_own_property(value : Value, name : String) -> Bool raise {
  match value {
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => {
          let desc = proxy_get_own_property_descriptor(data, name)
          match desc {
            Undefined => false
            _ =>
              if !is_object_like(desc) {
                let _ = throw_type_error("invalid property descriptor")
                false
              } else {
                true
              }
          }
        }
        None => {
          if obj.is_module_namespace {
            let desc = get_own_property_descriptor(value, name)
            match desc {
              Undefined => false
              _ =>
                if !is_object_like(desc) {
                  let _ = throw_type_error("invalid property descriptor")
                  false
                } else {
                  true
                }
            }
          } else {
          if props_contains(obj.props, name) {
            return true
          }
          match obj.string_data {
            Some(text) =>
              if name == "length" {
                true
              } else {
                match parse_array_index(name) {
                  Some(index) => index >= 0 && index < text.length()
                  None => false
                }
              }
            None => false
          }
          }
        }
      }
    Function(func) => {
      if name == "prototype" && !props_contains(func.props, name) {
        ensure_function_prototype(func)
      }
      props_contains(func.props, name)
    }
    BoundFunction(bound) => props_contains(bound.props, name)
    Builtin(builtin) => props_contains(builtin.props, name)
    Array(arr) =>
      match arr.typed_array_data {
        Some(data) =>
          match canonical_numeric_index_string(name) {
            Some(index) => typed_array_is_valid_integer_index(data, index)
            None => props_contains(arr.props, name)
          }
        None =>
          if props_contains(arr.props, name) {
            true
          } else {
            match parse_array_index(name) {
              Some(index) =>
                index >= 0 &&
                index < arr.elements.length() &&
                arr.elements[index] is Some(_)
              None => false
            }
          }
      }
    Arguments(args) =>
      match parse_array_index(name) {
        Some(index) => arguments_has_index(args, index)
        None => props_contains(args.props, name)
      }
    _ => false
  }
}

///|
fn own_property_is_enumerable(value : Value, name : String) -> Bool raise {
  match value {
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => {
          let desc = proxy_get_own_property_descriptor(data, name)
          match desc {
            Undefined => false
            _ =>
              if !is_object_like(desc) {
                let _ = throw_type_error("invalid property descriptor")
                false
              } else {
                is_truthy(property_get(desc, "enumerable"))
              }
          }
        }
        None => {
          if obj.is_module_namespace {
            let desc = get_own_property_descriptor(value, name)
            match desc {
              Undefined => false
              _ =>
                if !is_object_like(desc) {
                  let _ = throw_type_error("invalid property descriptor")
                  false
                } else {
                  is_truthy(property_get(desc, "enumerable"))
                }
            }
          } else {
          match props_get(obj.props, name) {
            Some(prop) => return prop.enumerable
            None => ()
          }
          match obj.string_data {
            Some(text) =>
              if name == "length" {
                false
              } else {
                match parse_array_index(name) {
                  Some(index) => index >= 0 && index < text.length()
                  None => false
                }
              }
            None => false
          }
          }
        }
      }
    Function(func) =>
      match props_get(func.props, name) {
        Some(prop) => prop.enumerable
        None => false
      }
    BoundFunction(bound) =>
      match props_get(bound.props, name) {
        Some(prop) => prop.enumerable
        None => false
      }
    Builtin(builtin) =>
      match props_get(builtin.props, name) {
        Some(prop) => prop.enumerable
        None => false
      }
    Array(arr) =>
      match arr.typed_array_data {
        Some(data) =>
          match canonical_numeric_index_string(name) {
            Some(index) => typed_array_is_valid_integer_index(data, index)
            None =>
              match props_get(arr.props, name) {
                Some(prop) => prop.enumerable
                None => false
              }
          }
        None =>
          match props_get(arr.props, name) {
            Some(prop) => prop.enumerable
            None =>
              match parse_array_index(name) {
                Some(index) =>
                  index >= 0 &&
                  index < arr.elements.length() &&
                  arr.elements[index] is Some(_)
                None => false
              }
          }
      }
    Arguments(args) =>
      match props_get(args.props, name) {
        Some(prop) => prop.enumerable
        None =>
          match parse_array_index(name) {
            Some(index) => arguments_has_index(args, index)
            None => false
          }
      }
    _ => false
  }
}
