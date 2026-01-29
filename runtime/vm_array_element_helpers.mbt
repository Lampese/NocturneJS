///|
fn array_get_with_receiver(
  arr : ArrayValue,
  name : String,
  receiver : Value,
) -> Value raise {
  match arr.typed_array_data {
    Some(data) =>
      return typed_array_get_with_receiver(arr, data, name, receiver)
    None => ()
  }
  if name == "length" {
    return array_length_value_from_props(arr, receiver)
  }
  match parse_array_index(name) {
    Some(index) =>
      if index >= 0 {
        if index < arr.elements.length() {
          match property_get_from_props(arr.props, name, receiver) {
            Some(value) => return value
            None => ()
          }
          match arr.elements[index] {
            Some(value) => return value
            None => ()
          }
        } else {
          match property_get_from_props(arr.props, name, receiver) {
            Some(value) => return value
            None => ()
          }
        }
        match arr.proto {
          Some(proto) =>
            return property_get_with_receiver(proto, name, receiver)
          None => return Undefined
        }
      } else {
        return Undefined
      }
    None => {
      match property_get_from_props(arr.props, name, receiver) {
        Some(value) => return value
        None => ()
      }
      match arr.proto {
        Some(proto) => return property_get_with_receiver(proto, name, receiver)
        None => return Undefined
      }
    }
  }
  Undefined
}

///|
fn typed_array_get_with_receiver(
  arr : ArrayValue,
  data : TypedArrayData,
  name : String,
  receiver : Value,
) -> Value raise {
  match canonical_numeric_index_string(name) {
    Some(index) =>
      if typed_array_is_valid_integer_index(data, index) {
        typed_array_get_index(data, Double::to_int(index))
      } else {
        Undefined
      }
    None => {
      match property_get_from_props(arr.props, name, receiver) {
        Some(value) => return value
        None => ()
      }
      match arr.proto {
        Some(proto) => property_get_with_receiver(proto, name, receiver)
        None => Undefined
      }
    }
  }
}

///|
fn arguments_has_index(args : ArgumentsValue, index : Int) -> Bool {
  if index < 0 {
    return false
  }
  let name = Int::to_string(index)
  if props_contains(args.props, name) {
    return true
  }
  if index >= args.elements.length() {
    return false
  }
  if index < args.mapped.length() && args.mapped[index] {
    return true
  }
  args.elements[index] is Some(_)
}

///|
fn arguments_index_value(args : ArgumentsValue, index : Int) -> Value {
  if index < 0 || index >= args.elements.length() {
    return Undefined
  }
  if index < args.mapped.length() &&
    index < args.params.length() &&
    args.mapped[index] {
    let name = args.params[index]
    match args.env.bindings.get(name) {
      Some(value) => return value
      None => return Undefined
    }
  }
  let name = Int::to_string(index)
  match props_get(args.props, name) {
    Some(prop) => prop.value
    None =>
      match args.elements[index] {
        Some(value) => value
        None => Undefined
      }
  }
}

///|
fn arguments_get_with_receiver(
  args : ArgumentsValue,
  name : String,
  receiver : Value,
) -> Value raise {
  match parse_array_index(name) {
    Some(index) =>
      if index >= 0 {
        if index < args.elements.length() {
          if index < args.mapped.length() &&
            index < args.params.length() &&
            args.mapped[index] {
            let param_name = args.params[index]
            match args.env.bindings.get(param_name) {
              Some(value) => return value
              None => return Undefined
            }
          }
          match property_get_from_props(args.props, name, receiver) {
            Some(value) => return value
            None => ()
          }
          match args.elements[index] {
            Some(value) => return value
            None => ()
          }
        }
        match property_get_from_props(args.props, name, receiver) {
          Some(value) => return value
          None => ()
        }
      }
    None => ()
  }
  match property_get_from_props(args.props, name, receiver) {
    Some(value) => value
    None =>
      match args.proto {
        Some(proto) => property_get_with_receiver(proto, name, receiver)
        None => Undefined
      }
  }
}

///|
fn array_set_index_fast(
  arr : ArrayValue,
  index64 : Int64,
  value : Value,
) -> Bool? raise {
  let max_dense_index = 2147483646
  if index64 < 0L || index64 > Int64::from_int(max_dense_index) {
    return None
  }
  if !array_can_fast_index_get(arr) {
    return None
  }
  let current_len = array_length_from_props(arr)
  match props_get(arr.props, "length") {
    Some(prop) =>
      if !prop.writable && index64 >= current_len {
        return Some(false)
      }
    None => ()
  }
  let index = Int64::to_int(index64)
  let elements_len = arr.elements.length()
  if index > elements_len {
    return None
  }
  if !arr.extensible {
    if index >= elements_len {
      return Some(false)
    }
    match arr.elements[index] {
      Some(_) => ()
      None => return Some(false)
    }
  }
  if index < elements_len {
    rc_replace_optional_value(arr.elements[index], Some(value))
    arr.elements[index] = Some(value)
  } else {
    while arr.elements.length() < index {
      arr.elements.push(None)
    }
    rc_incref_value(value)
    arr.elements.push(Some(value))
  }
  let elem_len = Int64::from_int(arr.elements.length())
  let new_len = if elem_len > current_len { elem_len } else { current_len }
  set_length_prop(arr.props, Number(Int64::to_double(new_len)))
  Some(true)
}

///|
fn arguments_set_index_fast(
  args : ArgumentsValue,
  index64 : Int64,
  value : Value,
) -> Bool? raise {
  let max_int = 2147483647
  if index64 < 0L || index64 > Int64::from_int(max_int) {
    return None
  }
  if props_has_index_props(args.props) {
    return None
  }
  if proto_chain_has_index_props(args.proto) {
    return None
  }
  let index = Int64::to_int(index64)
  if index >= args.elements.length() {
    return None
  }
  if !args.extensible && !arguments_has_index(args, index) {
    return Some(false)
  }
  Some(arguments_set_index(args, index, value))
}
