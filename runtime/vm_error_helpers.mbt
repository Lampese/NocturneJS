///|
fn new_error_value(
  proto : ObjectValue?,
  message : String,
  set_message? : Bool = true,
) -> Value {
  let obj = new_object_value_with_proto(value_from_object(proto))
  match obj {
    Object(value) => {
      value.is_error = true
      if set_message {
        props_set(value.props, "message", property_data_non_enum(String(message)))
      }
      match current_source() {
        Some(source) => {
          let positions = expr_pos_stack()
          if !positions.is_empty() {
            let (stack, line, column) = build_stack_from_positions(
              message, source, positions,
            )
            props_set(value.props, 
              "lineNumber",
              property_data(Number(Double::from_int(line))),
            )
            props_set(value.props, 
              "columnNumber",
              property_data(Number(Double::from_int(column))),
            )
            props_set(value.props, "stack", property_data(String(stack)))
          } else {
            let funcs = current_function_stack()
            if !funcs.is_empty() {
              let lines : Array[String] = []
              let header = if message.is_empty() { "Error" } else { message }
              lines.push(header)
              let mut i = funcs.length()
              while i > 0 {
                i = i - 1
                let func = funcs[i]
                let name = match func.name {
                  Some(value) => if value.is_empty() { "<anonymous>" } else { value }
                  None => "<anonymous>"
                }
                lines.push("at " + name)
              }
              props_set(value.props, "stack", property_data(String(lines.join("\n"))))
            }
          }
        }
        None => ()
      }
    }
    _ => ()
  }
  obj
}

///|
fn error_proto_from_target(
  target : Value,
  fallback : ObjectValue?,
) -> ObjectValue? raise {
  let fallback_value = value_from_object(fallback)
  let proto_value = constructor_proto_from_target(target, fallback_value)
  match proto_value {
    Some(Object(obj)) => Some(obj)
    _ => fallback
  }
}

///|
fn new_error_value_with_pos(
  proto : ObjectValue?,
  message : String,
  line : Int,
  column : Int,
) -> Value {
  let obj = new_object_value_with_proto(value_from_object(proto))
  match obj {
    Object(value) => {
      value.is_error = true
      props_set(value.props, "message", property_data_non_enum(String(message)))
      props_set(value.props, 
        "lineNumber",
        property_data(Number(Double::from_int(line))),
      )
      props_set(value.props, 
        "columnNumber",
        property_data(Number(Double::from_int(column))),
      )
      let stack = "\{message}:\{line}:\{column}"
      props_set(value.props, "stack", property_data(String(stack)))
    }
    _ => ()
  }
  obj
}

///|
priv struct ErrorMessageInfo {
  message : String
  has_message : Bool
}

///|
fn error_message_from_args(
  args : Array[Value],
  index? : Int = 0,
) -> ErrorMessageInfo raise {
  if index >= args.length() {
    return ErrorMessageInfo::{ message: "", has_message: false }
  }
  let value = args[index]
  if value is Undefined {
    return ErrorMessageInfo::{ message: "", has_message: false }
  }
  ErrorMessageInfo::{ message: to_string_strict(value), has_message: true }
}

///|
fn install_error_cause(obj : ObjectValue, options : Value) -> Unit raise {
  if !is_object_like(options) {
    return
  }
  if has_property_value(options, "cause") {
    let cause = property_get(options, "cause")
    props_set(obj.props, "cause", property_data_non_enum(cause))
  }
}

///|
fn new_aggregate_error_value(args : Array[Value]) -> Value raise {
  let errors_proto = match current_env() {
    Some(env) =>
      match value_from_object(array_proto_for_env(env)) {
        Some(value) => Some(value)
        None => value_from_object(array_proto())
      }
    None => value_from_object(array_proto())
  }
  new_aggregate_error_value_with_proto(
    args,
    aggregate_error_proto(),
    errors_proto,
  )
}

///|
fn new_aggregate_error_value_with_proto(
  args : Array[Value],
  proto : ObjectValue?,
  errors_proto : Value?,
) -> Value raise {
  let errors_value = if args.is_empty() { Undefined } else { args[0] }
  let message_info = error_message_from_args(args, index=1)
  let list = build_spread_list(errors_value)
  let elements : Array[Value?] = []
  for value in list {
    elements.push(Some(value))
  }
  let errors_array = new_array_value_with_proto(elements, proto=errors_proto)
  let obj = new_error_value(
    proto,
    message_info.message,
    set_message=message_info.has_message,
  )
  match obj {
    Object(value) =>
      props_set(value.props, "errors", property_data_non_enum(errors_array))
    _ => ()
  }
  obj
}

///|
fn throw_type_error_for_env(env : Env?, message : String) -> Value raise {
  let proto = match env {
    Some(env) =>
      match type_error_proto_for_env(env) {
        Some(obj) => Some(obj)
        None => type_error_proto()
      }
    None => type_error_proto()
  }
  raise ThrowSignal(new_error_value(proto, message))
}

///|
fn throw_type_error(message : String) -> Value raise {
  throw_type_error_for_env(current_env(), message)
}

///|
fn type_error_value(message : String) -> Value {
  let proto = match current_env() {
    Some(env) =>
      match type_error_proto_for_env(env) {
        Some(obj) => Some(obj)
        None => type_error_proto()
      }
    None => type_error_proto()
  }
  new_error_value(proto, message)
}

///|
fn throw_reference_error(message : String) -> Value raise {
  let proto = match current_env() {
    Some(env) =>
      match reference_error_proto_for_env(env) {
        Some(obj) => Some(obj)
        None => reference_error_proto()
      }
    None => reference_error_proto()
  }
  raise ThrowSignal(new_error_value(proto, message))
}

///|
fn throw_syntax_error(message : String) -> Value raise {
  let proto = match current_env() {
    Some(env) =>
      match syntax_error_proto_for_env(env) {
        Some(obj) => Some(obj)
        None => syntax_error_proto()
      }
    None => syntax_error_proto()
  }
  raise ThrowSignal(new_error_value(proto, message))
}

///|
fn throw_range_error(message : String) -> Value raise {
  let proto = match current_env() {
    Some(env) =>
      match range_error_proto_for_env(env) {
        Some(obj) => Some(obj)
        None => range_error_proto()
      }
    None => range_error_proto()
  }
  raise ThrowSignal(new_error_value(proto, message))
}

///|
fn throw_uri_error(message : String) -> Value raise {
  let proto = match current_env() {
    Some(env) =>
      match uri_error_proto_for_env(env) {
        Some(obj) => Some(obj)
        None => uri_error_proto()
      }
    None => uri_error_proto()
  }
  raise ThrowSignal(new_error_value(proto, message))
}
