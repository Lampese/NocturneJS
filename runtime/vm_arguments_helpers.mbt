///|
fn arguments_set(
  args : ArgumentsValue,
  name : String,
  value : Value,
) -> Bool raise {
  let receiver = Arguments(args)
  if name == "length" || name == "callee" {
    return set_property_in(args.props, name, value, receiver)
  }
  match parse_array_index(name) {
    Some(index) => {
      if index < 0 {
        return false
      }
      if !args.extensible && !arguments_has_index(args, index) {
        return false
      }
      if props_contains(args.props, name) &&
        !(index < args.mapped.length() && args.mapped[index]) {
        return set_property_in(args.props, name, value, receiver)
      }
      arguments_set_index(args, index, value)
    }
    None =>
      if args.extensible || props_contains(args.props, name) {
        set_property_in(args.props, name, value, receiver)
      } else {
        false
      }
  }
}

///|
fn arguments_set_index(
  args : ArgumentsValue,
  index : Int,
  value : Value,
) -> Bool raise {
  if index < 0 {
    return false
  }
  let len = args.elements.length()
  if index < len {
    if index < args.mapped.length() &&
      index < args.params.length() &&
      args.mapped[index] {
      env_set_local(args.env, args.params[index], value)
    }
    rc_replace_optional_value(args.elements[index], Some(value))
    args.elements[index] = Some(value)
    true
  } else {
    set_property_in(args.props, Int::to_string(index), value, Arguments(args))
  }
}
