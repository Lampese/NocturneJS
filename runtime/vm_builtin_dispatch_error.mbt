///|
fn call_builtin_error(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match builtin.kind {
    BuiltinFunction::ErrorToString => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      if !is_object_like(target) {
        return throw_type_error("not an object")
      }
      let obj = to_object(target)
      let name_value = property_get(obj, "name")
      let name = if name_value is Undefined {
        "Error"
      } else {
        to_string_strict(name_value)
      }
      let message_value = property_get(obj, "message")
      let message = if message_value is Undefined {
        ""
      } else {
        to_string_strict(message_value)
      }
      if name.is_empty() {
        String(message)
      } else if message.is_empty() {
        String(name)
      } else {
        String(name + ": " + message)
      }
    }
    BuiltinFunction::ErrorIsError =>
      if args.is_empty() {
        Bool(false)
      } else {
        match args[0] {
          Object(obj) => Bool(obj.is_error)
          _ => Bool(false)
        }
      }
    BuiltinFunction::ErrorConstructor => {
      let message_info = error_message_from_args(args)
      let fallback = match current_env() {
        Some(env) =>
          match error_proto_for_env_name(env, "Error") {
            Some(proto) => Some(proto)
            None => error_proto()
          }
        None => error_proto()
      }
      let proto = error_proto_from_target(Builtin(builtin), fallback)
      let obj = new_error_value(
        proto,
        message_info.message,
        set_message=message_info.has_message,
      )
      if args.length() > 1 {
        match obj {
          Object(value) => install_error_cause(value, args[1])
          _ => ()
        }
      }
      obj
    }
    BuiltinFunction::AggregateErrorConstructor => {
      let fallback = match current_env() {
        Some(env) =>
          match error_proto_for_env_name(env, "AggregateError") {
            Some(proto) => Some(proto)
            None => aggregate_error_proto()
          }
        None => aggregate_error_proto()
      }
      let proto = error_proto_from_target(Builtin(builtin), fallback)
      let target_env = function_realm_env_checked(Builtin(builtin))
      let errors_proto = match
        value_from_object(array_proto_for_env(target_env)) {
        Some(value) => Some(value)
        None => value_from_object(array_proto())
      }
      let obj = new_aggregate_error_value_with_proto(args, proto, errors_proto)
      if args.length() > 2 {
        match obj {
          Object(value) => install_error_cause(value, args[2])
          _ => ()
        }
      }
      obj
    }
    BuiltinFunction::EvalErrorConstructor => {
      let message_info = error_message_from_args(args)
      let fallback = match current_env() {
        Some(env) =>
          match error_proto_for_env_name(env, "EvalError") {
            Some(proto) => Some(proto)
            None => eval_error_proto()
          }
        None => eval_error_proto()
      }
      let proto = error_proto_from_target(Builtin(builtin), fallback)
      let obj = new_error_value(
        proto,
        message_info.message,
        set_message=message_info.has_message,
      )
      if args.length() > 1 {
        match obj {
          Object(value) => install_error_cause(value, args[1])
          _ => ()
        }
      }
      obj
    }
    BuiltinFunction::SyntaxErrorConstructor => {
      let message_info = error_message_from_args(args)
      let fallback = match current_env() {
        Some(env) =>
          match syntax_error_proto_for_env(env) {
            Some(proto) => Some(proto)
            None => syntax_error_proto()
          }
        None => syntax_error_proto()
      }
      let proto = error_proto_from_target(Builtin(builtin), fallback)
      let obj = new_error_value(
        proto,
        message_info.message,
        set_message=message_info.has_message,
      )
      if args.length() > 1 {
        match obj {
          Object(value) => install_error_cause(value, args[1])
          _ => ()
        }
      }
      obj
    }
    BuiltinFunction::RangeErrorConstructor => {
      let message_info = error_message_from_args(args)
      let fallback = match current_env() {
        Some(env) =>
          match range_error_proto_for_env(env) {
            Some(proto) => Some(proto)
            None => range_error_proto()
          }
        None => range_error_proto()
      }
      let proto = error_proto_from_target(Builtin(builtin), fallback)
      let obj = new_error_value(
        proto,
        message_info.message,
        set_message=message_info.has_message,
      )
      if args.length() > 1 {
        match obj {
          Object(value) => install_error_cause(value, args[1])
          _ => ()
        }
      }
      obj
    }
    BuiltinFunction::TypeErrorConstructor => {
      let message_info = error_message_from_args(args)
      let fallback = match current_env() {
        Some(env) =>
          match type_error_proto_for_env(env) {
            Some(proto) => Some(proto)
            None => type_error_proto()
          }
        None => type_error_proto()
      }
      let proto = error_proto_from_target(Builtin(builtin), fallback)
      let obj = new_error_value(
        proto,
        message_info.message,
        set_message=message_info.has_message,
      )
      if args.length() > 1 {
        match obj {
          Object(value) => install_error_cause(value, args[1])
          _ => ()
        }
      }
      obj
    }
    BuiltinFunction::ReferenceErrorConstructor => {
      let message_info = error_message_from_args(args)
      let fallback = match current_env() {
        Some(env) =>
          match reference_error_proto_for_env(env) {
            Some(proto) => Some(proto)
            None => reference_error_proto()
          }
        None => reference_error_proto()
      }
      let proto = error_proto_from_target(Builtin(builtin), fallback)
      let obj = new_error_value(
        proto,
        message_info.message,
        set_message=message_info.has_message,
      )
      if args.length() > 1 {
        match obj {
          Object(value) => install_error_cause(value, args[1])
          _ => ()
        }
      }
      obj
    }
    BuiltinFunction::UriErrorConstructor => {
      let message_info = error_message_from_args(args)
      let fallback = match current_env() {
        Some(env) =>
          match uri_error_proto_for_env(env) {
            Some(proto) => Some(proto)
            None => uri_error_proto()
          }
        None => uri_error_proto()
      }
      let proto = error_proto_from_target(Builtin(builtin), fallback)
      let obj = new_error_value(
        proto,
        message_info.message,
        set_message=message_info.has_message,
      )
      if args.length() > 1 {
        match obj {
          Object(value) => install_error_cause(value, args[1])
          _ => ()
        }
      }
      obj
    }
    BuiltinFunction::ThrowTypeError => {
      if args.is_empty() {
        match builtin.this_value {
          Some(Function(func)) =>
            if !func.is_strict && func.is_constructor {
              return Undefined
            }
          _ => ()
        }
      }
      throw_type_error_for_env(builtin.realm_env, "invalid property access")
    }
    _ => throw_type_error("invalid builtin")
  }
}
