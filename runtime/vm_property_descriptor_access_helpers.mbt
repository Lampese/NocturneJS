///|
fn get_own_property_descriptor(target : Value, name : String) -> Value raise {
  match target {
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => proxy_get_own_property_descriptor(data, name)
        None =>
          match obj.props.get(name) {
            Some(prop) =>
              if obj.is_module_namespace {
                match module_binding_info(prop.value) {
                  Some(_) =>
                    property_descriptor_object(Property::{
                      value: module_binding_deref(prop.value),
                      writable: true,
                      configurable: prop.configurable,
                      enumerable: prop.enumerable,
                      getter: prop.getter,
                      setter: prop.setter,
                    })
                  None => property_descriptor_object(prop)
                }
              } else {
                property_descriptor_object(prop)
              }
            None =>
              match obj.string_data {
                Some(value) =>
                  if name == "length" {
                    property_descriptor_object(
                      property_data_const(
                        Number(Double::from_int(value.length())),
                      ),
                    )
                  } else {
                    match parse_array_index(name) {
                      Some(index) =>
                        if index >= 0 && index < value.length() {
                          property_descriptor_object(Property::{
                            value: String(
                              value.unsafe_substring(start=index, end=index + 1),
                            ),
                            writable: false,
                            configurable: false,
                            enumerable: true,
                            getter: None,
                            setter: None,
                          })
                        } else {
                          Undefined
                        }
                      None => Undefined
                    }
                  }
                None => Undefined
              }
          }
      }
    Function(func) => {
      if name == "prototype" && !func.props.contains(name) {
        ensure_function_prototype(func)
      }
      match func.props.get(name) {
        Some(prop) => property_descriptor_object(prop)
        None => Undefined
      }
    }
    BoundFunction(bound) =>
      match bound.props.get(name) {
        Some(prop) => property_descriptor_object(prop)
        None => Undefined
      }
    Builtin(builtin) =>
      match builtin.props.get(name) {
        Some(prop) => property_descriptor_object(prop)
        None => Undefined
      }
    Array(arr) =>
      match arr.typed_array_data {
        Some(data) =>
          match canonical_numeric_index_string(name) {
            Some(index) =>
              if typed_array_is_valid_integer_index(data, index) {
                let value = typed_array_get_index(data, Double::to_int(index))
                property_descriptor_object(Property::{
                  value,
                  writable: true,
                  configurable: true,
                  enumerable: true,
                  getter: None,
                  setter: None,
                })
              } else {
                Undefined
              }
            None =>
              match arr.props.get(name) {
                Some(prop) => property_descriptor_object(prop)
                None => Undefined
              }
          }
        None =>
          match arr.props.get(name) {
            Some(prop) => property_descriptor_object(prop)
            None =>
              match parse_array_index(name) {
                Some(index) =>
                  if index >= 0 && index < arr.elements.length() {
                    match arr.elements[index] {
                      Some(value) =>
                        property_descriptor_object(property_data(value))
                      None => Undefined
                    }
                  } else {
                    Undefined
                  }
                None => Undefined
              }
          }
      }
    Arguments(args) =>
      match args.props.get(name) {
        Some(prop) =>
          match parse_array_index(name) {
            Some(index) =>
              if index < args.mapped.length() &&
                index < args.params.length() &&
                args.mapped[index] &&
                prop.getter is None &&
                prop.setter is None {
                let value = arguments_index_value(args, index)
                property_descriptor_object(Property::{
                  value,
                  writable: prop.writable,
                  configurable: prop.configurable,
                  enumerable: prop.enumerable,
                  getter: None,
                  setter: None,
                })
              } else {
                property_descriptor_object(prop)
              }
            None => property_descriptor_object(prop)
          }
        None =>
          match parse_array_index(name) {
            Some(index) =>
              if arguments_has_index(args, index) {
                property_descriptor_object(
                  property_data(arguments_index_value(args, index)),
                )
              } else {
                Undefined
              }
            None => Undefined
          }
      }
    _ => Undefined
  }
}

///|
fn get_own_property_descriptor_with_info(
  target : Value,
  name : String,
) -> (Value, PropertyDescInfo?) raise {
  match target {
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => proxy_get_own_property_descriptor_info(data, name)
        None => {
          let desc = get_own_property_descriptor(target, name)
          match desc {
            Undefined => (Undefined, None)
            _ =>
              if !is_object_like(desc) {
                (desc, None)
              } else {
                (desc, Some(to_property_desc_info(desc)))
              }
          }
        }
      }
    _ => {
      let desc = get_own_property_descriptor(target, name)
      match desc {
        Undefined => (Undefined, None)
        _ =>
          if !is_object_like(desc) {
            (desc, None)
          } else {
            (desc, Some(to_property_desc_info(desc)))
          }
      }
    }
  }
}
