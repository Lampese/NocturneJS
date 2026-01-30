///|
fn resolve_super_base() -> Value raise {
  match current_super_function() {
    Some(func) =>
      match func.home_object {
        Some(home) =>
          match get_proto_of_value(home) {
            Some(base) => base
            None => Null
          }
        None =>
          match func.super_constructor {
            Some(value) => value
            None => throw_reference_error("super is not defined")
          }
      }
    None => throw_reference_error("super is not defined")
  }
}

///|
fn super_base_and_receiver(env : Env) -> (Value, Value) raise {
  let receiver = env_get(env, "this")
  let base = resolve_super_base()
  (base, receiver)
}

///|
fn super_set_property(
  env : Env,
  base : Value,
  receiver : Value,
  name : String,
  value : Value,
) -> Unit raise {
  let ok = match base {
    Object(obj) =>
      set_property_with_proto(
        obj.props,
        obj.proto,
        name,
        value,
        receiver,
        obj.extensible,
      )
    Function(func) =>
      set_property_with_proto(
        func.props,
        func.proto,
        name,
        value,
        receiver,
        func.extensible,
      )
    BoundFunction(bound) =>
      set_property_with_proto(
        bound.props,
        bound.proto,
        name,
        value,
        receiver,
        bound.extensible,
      )
    Builtin(builtin) =>
      set_property_with_proto(
        builtin.props,
        builtin.proto,
        name,
        value,
        receiver,
        builtin.extensible,
      )
    _ => false
  }
  if !ok && env.strict {
    let _ = throw_type_error("cannot set property")

  }
}

///|
fn resolve_super_constructor() -> Value raise {
  match current_super_function() {
    Some(func) =>
      if func.is_class_constructor && func.super_constructor is Some(_) {
        match get_proto_of_value(Value::Function(func)) {
          Some(value) => value
          None => Null
        }
      } else {
        match func.super_constructor {
          Some(value) => value
          None => throw_reference_error("super is not defined")
        }
      }
    None => throw_reference_error("super is not defined")
  }
}

///|
fn super_call_with_ctor(
  env : Env,
  super_ctor : Value,
  argv : Array[Value],
) -> Value raise {
  if !is_constructor_value(super_ctor) {
    return throw_not_constructor(super_ctor)
  }
  let super_env = find_this_env(env)
  let already_initialized = !super_env.this_uninitialized
  let result = call_constructor(
    super_ctor,
    argv,
    new_target=super_env.new_target,
  )
  if already_initialized {
    let _ = throw_reference_error("this is already initialized")

  }
  env_set_local(super_env, "this", result)
  if super_env.this_uninitialized {
    super_env.this_uninitialized = false
    match current_super_function() {
      Some(func) => init_instance_fields(func, super_env, result)
      None => ()
    }
  }
  result
}
