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
  if args.props.contains(name) {
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
  match args.props.get(name) {
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
