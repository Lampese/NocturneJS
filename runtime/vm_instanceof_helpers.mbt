///|
fn ordinary_instanceof(left : Value, right : Value) -> Bool raise {
  if !is_object_like(left) {
    return false
  }
  match right {
    BoundFunction(bound) => return ordinary_instanceof(left, bound.target)
    _ => ()
  }
  let proto_value = property_get(right, "prototype")
  if !is_object_like(proto_value) {
    let _ = throw_type_error("operand 'prototype' property is not an object")
    return false
  }
  let proto_id = match value_id(proto_value) {
    Some(id) => id
    None => {
      let _ = throw_type_error("operand 'prototype' property is not an object")
      return false
    }
  }
  let mut current = get_proto_of_value_checked(left)
  for {
    match current {
      Some(obj) =>
        if value_id(obj) is Some(current_id) && current_id == proto_id {
          return true
        } else {
          current = get_proto_of_value_checked(obj)
          continue
        }
      None => return false
    }
  }
  false
}
