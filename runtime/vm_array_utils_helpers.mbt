///|
fn array_has_property(arr : ArrayValue, name : String) -> Bool raise {
  if name == "length" {
    return true
  }
  match arr.typed_array_data {
    Some(data) =>
      match canonical_numeric_index_string(name) {
        Some(index) => return typed_array_is_valid_integer_index(data, index)
        None => {
          if arr.props.contains(name) {
            return true
          }
          match arr.proto {
            Some(proto) => return has_property(proto, name)
            None => return false
          }
        }
      }
    None => ()
  }
  if arr.props.contains(name) {
    return true
  }
  match parse_array_index(name) {
    Some(index) =>
      if index >= 0 && index < arr.elements.length() {
        match arr.elements[index] {
          Some(_) => true
          None =>
            match arr.proto {
              Some(proto) => has_property(proto, name)
              None => false
            }
        }
      } else {
        match arr.proto {
          Some(proto) => has_property(proto, name)
          None => false
        }
      }
    None =>
      match arr.proto {
        Some(proto) => has_property(proto, name)
        None => false
      }
  }
}
