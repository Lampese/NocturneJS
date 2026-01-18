///|
fn property_get_with_receiver(
  target : Value,
  name : String,
  receiver : Value,
) -> Value raise {
  match target {
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => proxy_get_value(data, name, receiver)
        None => string_object_get(obj, name, receiver)
      }
    Function(func) => function_property_get_with_receiver(func, name, receiver)
    BoundFunction(bound) =>
      property_get_from_chain(bound.props, bound.proto, name, receiver)
    Builtin(builtin) =>
      property_get_from_chain(builtin.props, builtin.proto, name, receiver)
    Array(arr) => array_get_with_receiver(arr, name, receiver)
    Arguments(args) => arguments_get_with_receiver(args, name, receiver)
    String(value) => string_property_get(value, name)
    Number(value) => number_property_get(value, name)
    BigInt(value) => bigint_property_get(value, name)
    _ => Undefined
  }
}

///|
fn property_get(target : Value, name : String) -> Value raise {
  property_get_with_receiver(target, name, target)
}

///|
fn proto_for_value(target : Value) -> Value? {
  match target {
    Object(obj) => obj.proto
    Function(func) => func.proto
    BoundFunction(bound) => bound.proto
    Builtin(builtin) => builtin.proto
    Array(arr) => arr.proto
    Arguments(args) => args.proto
    _ => None
  }
}

///|
fn property_get_raw(target : Value, name : String) -> Value raise {
  match props_map_for_value(target) {
    Some(props) =>
      property_get_from_chain_raw(props, proto_for_value(target), name, target)
    None => property_get(target, name)
  }
}
