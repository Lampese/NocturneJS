///|
fn define_private_property(
  target : Value,
  key : String,
  prop : Property,
) -> Unit raise {
  match props_map_for_value(target) {
    Some(props) =>
      if props.contains(key) {
        let _ = throw_type_error("private class field already exists")

      } else {
        props_set(props, key, prop)
      }
    None => {
      let _ = throw_type_error("invalid private access")

    }
  }
}

///|
fn private_property_get(
  target : Value,
  key : String,
  name : String,
) -> Value raise {
  match props_map_for_value(target) {
    Some(props) =>
      match props.get(key) {
        Some(prop) =>
          if prop.getter is Some(_) || prop.setter is Some(_) {
            match prop.getter {
              Some(getter) =>
                match getter {
                  Undefined | Null =>
                    throw_type_error(
                      "private class field '#" +
                      name +
                      "' does not have a getter",
                    )
                  _ => call_value_with_this(getter, [], target)
                }
              None =>
                throw_type_error(
                  "private class field '#" + name + "' does not have a getter",
                )
            }
          } else {
            module_binding_deref(prop.value)
          }
        None =>
          throw_type_error("private class field '#" + name + "' does not exist")
      }
    None =>
      throw_type_error("private class field '#" + name + "' does not exist")
  }
}

///|
fn private_property_set(
  target : Value,
  key : String,
  name : String,
  value : Value,
) -> Unit raise {
  match props_map_for_value(target) {
    Some(props) =>
      match props.get(key) {
        Some(prop) =>
          if prop.getter is Some(_) || prop.setter is Some(_) {
            match prop.setter {
              Some(setter) =>
                match setter {
                  Undefined | Null => {
                    let _ = throw_type_error(
                      "private class field '#" + name + "' is not writable",
                    )

                  }
                  _ => {
                    let _ = call_value_with_this(setter, [value], target)

                  }
                }
              None => {
                let _ = throw_type_error(
                  "private class field '#" + name + "' is not writable",
                )

              }
            }
          } else if prop.writable {
            props_set(props, key, Property::{
              value,
              writable: prop.writable,
              configurable: prop.configurable,
              enumerable: prop.enumerable,
              getter: prop.getter,
              setter: prop.setter,
            })
          } else {
            let _ = throw_type_error(
              "private class field '#" + name + "' is not writable",
            )

          }
        None => {
          let _ = throw_type_error(
            "private class field '#" + name + "' does not exist",
          )

        }
      }
    None => {
      let _ = throw_type_error(
        "private class field '#" + name + "' does not exist",
      )

    }
  }
}
