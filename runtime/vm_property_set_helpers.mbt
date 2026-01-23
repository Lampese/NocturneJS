///|
fn set_property_in(
  props : Map[String, Property],
  name : String,
  value : Value,
  receiver : Value,
) -> Bool raise {
  match props.get(name) {
    Some(prop) =>
      match prop.setter {
        Some(setter) =>
          match setter {
            Undefined | Null => false
            _ => {
              let _ = call_value_with_this(setter, [value], receiver)
              true
            }
          }
        None =>
          if prop.writable {
            props_set(props, name, Property::{
              value,
              writable: prop.writable,
              configurable: prop.configurable,
              enumerable: prop.enumerable,
              getter: prop.getter,
              setter: prop.setter,
            })
            true
          } else {
            false
          }
      }
    None => {
      props_set(props, name, property_data(value))
      true
    }
  }
}

///|
fn set_property_with_proto(
  props : Map[String, Property],
  proto : Value?,
  name : String,
  value : Value,
  receiver : Value,
  extensible : Bool,
) -> Bool raise {
  let _ = extensible
  if props.contains(name) {
    return set_property_in(props, name, value, receiver)
  }
  match proto {
    Some(parent) => set_value_with_receiver(parent, name, value, receiver)
    None =>
      if !is_object_like(receiver) {
        false
      } else {
        let receiver_desc = get_own_property_descriptor(receiver, name)
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
            } else if descriptor_is_accessor(receiver_desc) {
              false
            } else {
              let receiver_writable = is_truthy(
                property_get(receiver_desc, "writable"),
              )
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
  }
}
