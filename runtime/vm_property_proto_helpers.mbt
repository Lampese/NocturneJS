///|
fn has_property(value : Value, name : String) -> Bool raise {
  match value {
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => proxy_has_value(data, name)
        None =>
          match obj.string_data {
            Some(string_value) => {
              if name == "length" {
                return true
              }
              match parse_array_index(name) {
                Some(index) =>
                  if index >= 0 && index < string_value.length() {
                    return true
                  }
                None => ()
              }
              has_property_in_chain(obj.props, obj.proto, name)
            }
            None => has_property_in_chain(obj.props, obj.proto, name)
          }
      }
    Function(func) => {
      if name == "prototype" && !func.props.contains(name) {
        ensure_function_prototype(func)
      }
      has_property_in_chain(func.props, func.proto, name)
    }
    BoundFunction(bound) =>
      has_property_in_chain(bound.props, bound.proto, name)
    Builtin(builtin) =>
      has_property_in_chain(builtin.props, builtin.proto, name)
    Array(arr) => array_has_property(arr, name)
    Arguments(args) => arguments_has_property(args, name)
    String(value) => {
      if name == "length" {
        return true
      }
      match parse_array_index(name) {
        Some(index) => if index >= 0 && index < value.length() { return true }
        None => ()
      }
      match string_proto() {
        Some(proto) => has_property_in_chain(proto.props, proto.proto, name)
        None => false
      }
    }
    Number(_) =>
      match number_proto() {
        Some(proto) => has_property_in_chain(proto.props, proto.proto, name)
        None => false
      }
    BigInt(_) =>
      match bigint_proto() {
        Some(proto) => has_property_in_chain(proto.props, proto.proto, name)
        None => false
      }
    _ => false
  }
}

///|
fn has_property_value(value : Value, name : String) -> Bool raise {
  match value {
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => proxy_has_value(data, name)
        None => has_property(value, name)
      }
    _ => has_property(value, name)
  }
}

///|
fn arguments_has_property(args : ArgumentsValue, name : String) -> Bool raise {
  match parse_array_index(name) {
    Some(index) =>
      if index >= 0 && index < args.elements.length() {
        return arguments_has_index(args, index)
      }
    None => ()
  }
  if args.props.contains(name) {
    return true
  }
  match args.proto {
    Some(proto) => has_property(proto, name)
    None => false
  }
}

///|
fn has_property_in_chain(
  props : Map[String, Property],
  proto : Value?,
  name : String,
) -> Bool raise {
  if props.contains(name) {
    return true
  }
  match proto {
    Some(parent) => has_property(parent, name)
    None => false
  }
}

///|
fn get_proto_of_value(value : Value) -> Value? {
  match value {
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => get_proto_of_value(data.target)
        None => obj.proto
      }
    Function(func) => func.proto
    BoundFunction(bound) => bound.proto
    Builtin(builtin) => builtin.proto
    Array(arr) => arr.proto
    Arguments(args) => args.proto
    _ => None
  }
}

///|
fn get_proto_of_value_checked(value : Value) -> Value? raise {
  match value {
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => proxy_get_prototype(data)
        None => obj.proto
      }
    Function(func) => func.proto
    BoundFunction(bound) => bound.proto
    Builtin(builtin) => builtin.proto
    Array(arr) => arr.proto
    Arguments(args) => args.proto
    _ => None
  }
}

///|
fn same_proto(left : Value?, right : Value?) -> Bool {
  match (left, right) {
    (None, None) => true
    (Some(left_val), Some(right_val)) =>
      match (value_id(left_val), value_id(right_val)) {
        (Some(left_id), Some(right_id)) => left_id == right_id
        _ => false
      }
    _ => false
  }
}

///|
fn proto_chain_contains(start : Value?, target_id : Int) -> Bool {
  let seen : Map[Int, Bool] = Map::new()
  fn walk(current : Value?) -> Bool {
    match current {
      None => false
      Some(value) => {
        match value {
          Object(obj) => if obj.proxy_data is Some(_) { return false }
          _ => ()
        }
        match value_id(value) {
          None => false
          Some(id) => {
            if seen.contains(id) {
              return false
            }
            if id == target_id {
              return true
            }
            seen.set(id, true)
            walk(get_proto_of_value(value))
          }
        }
      }
    }
  }

  walk(start)
}

///|
fn set_proto_of_object(
  obj : ObjectValue,
  proto : Value?,
  throw_flag : Bool,
) -> Bool raise {
  if same_proto(obj.proto, proto) {
    return true
  }
  if obj.immutable_proto {
    if throw_flag {
      let _ = throw_type_error("prototype is immutable")

    }
    return false
  }
  if !obj.extensible {
    if throw_flag {
      let _ = throw_type_error("object is not extensible")

    }
    return false
  }
  if proto_chain_contains(proto, obj.id) {
    if throw_flag {
      let _ = throw_type_error("circular prototype chain")

    }
    return false
  }
  obj.proto = proto
  true
}

///|
fn set_proto_of_value(
  value : Value,
  proto : Value?,
  throw_flag : Bool,
) -> Bool raise {
  match value {
    Null | Undefined => {
      if throw_flag {
        let _ = throw_type_error("not an object")

      }
      false
    }
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => proxy_set_prototype(data, proto, throw_flag)
        None => set_proto_of_object(obj, proto, throw_flag)
      }
    Function(func) => {
      if same_proto(func.proto, proto) {
        return true
      }
      if !func.extensible {
        if throw_flag {
          let _ = throw_type_error("object is not extensible")

        }
        return false
      }
      func.proto = proto
      true
    }
    BoundFunction(bound) => {
      if same_proto(bound.proto, proto) {
        return true
      }
      if !bound.extensible {
        if throw_flag {
          let _ = throw_type_error("object is not extensible")

        }
        return false
      }
      bound.proto = proto
      true
    }
    Builtin(builtin) => {
      if same_proto(builtin.proto, proto) {
        return true
      }
      if !builtin.extensible {
        if throw_flag {
          let _ = throw_type_error("object is not extensible")

        }
        return false
      }
      builtin.proto = proto
      true
    }
    Array(arr) => {
      if same_proto(arr.proto, proto) {
        return true
      }
      if !arr.extensible {
        if throw_flag {
          let _ = throw_type_error("object is not extensible")

        }
        return false
      }
      arr.proto = proto
      true
    }
    Arguments(args) => {
      if same_proto(args.proto, proto) {
        return true
      }
      if !args.extensible {
        if throw_flag {
          let _ = throw_type_error("object is not extensible")

        }
        return false
      }
      args.proto = proto
      true
    }
    _ => true
  }
}
