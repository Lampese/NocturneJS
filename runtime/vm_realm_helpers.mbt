///|
fn clone_global_object(obj : ObjectValue, env : Env) -> ObjectValue {
  let props : Map[String, Property] = Map::new()
  for name, prop in obj.props {
    let value = clone_realm_value(prop.value, env)
    let getter = match prop.getter {
      Some(getter_value) => Some(clone_realm_value(getter_value, env))
      None => None
    }
    let setter = match prop.setter {
      Some(setter_value) => Some(clone_realm_value(setter_value, env))
      None => None
    }
    props_set(props, name, Property::{
      value,
      writable: prop.writable,
      configurable: prop.configurable,
      enumerable: prop.enumerable,
      getter,
      setter,
    })
  }
  let clone = register_object_value(ObjectValue::{
    id: alloc_id(),
    props,
    proto: obj.proto,
    string_data: None,
    number_data: None,
    bool_data: None,
    bigint_data: None,
    symbol_data: None,
    regexp_data: None,
    generator_data: None,
    async_generator_data: None,
    proxy_data: None,
    map_data: None,
    set_data: None,
    map_iter_data: None,
    set_iter_data: None,
    iterator_wrap_data: None,
    iterator_concat_data: None,
    iterator_helper_data: None,
    weakmap_data: None,
    weakset_data: None,
    weakref_data: None,
    finreg_data: None,
    date_data: None,
    array_buffer_data: None,
    dataview_data: None,
    is_html_dda: false,
    is_error: false,
    is_module_namespace: false,
    realm_env: Some(env),
    immutable_proto: false,
    extensible: obj.extensible,
  })
  rc_incref_optional_value(clone.proto)
  rc_incref_optional_env(clone.realm_env)
  clone
}

///|
fn clone_object_value_for_env(
  obj : ObjectValue,
  env : Env,
  constructor_override : Value?,
) -> ObjectValue {
  let props : Map[String, Property] = Map::new()
  for name, prop in obj.props {
    let value = if name == "constructor" {
      match constructor_override {
        Some(ctor_value) => ctor_value
        None => clone_realm_value(prop.value, env)
      }
    } else {
      clone_realm_value(prop.value, env)
    }
    let getter = match prop.getter {
      Some(getter_value) => Some(clone_realm_value(getter_value, env))
      None => None
    }
    let setter = match prop.setter {
      Some(setter_value) => Some(clone_realm_value(setter_value, env))
      None => None
    }
    props_set(props, name, Property::{
      value,
      writable: prop.writable,
      configurable: prop.configurable,
      enumerable: prop.enumerable,
      getter,
      setter,
    })
  }
  let clone = register_object_value(ObjectValue::{
    id: alloc_id(),
    props,
    proto: obj.proto,
    string_data: obj.string_data,
    number_data: obj.number_data,
    bool_data: obj.bool_data,
    bigint_data: obj.bigint_data,
    symbol_data: obj.symbol_data,
    regexp_data: obj.regexp_data,
    generator_data: obj.generator_data,
    async_generator_data: obj.async_generator_data,
    proxy_data: obj.proxy_data,
    map_data: obj.map_data,
    set_data: obj.set_data,
    map_iter_data: obj.map_iter_data,
    set_iter_data: obj.set_iter_data,
    iterator_wrap_data: obj.iterator_wrap_data,
    iterator_concat_data: obj.iterator_concat_data,
    iterator_helper_data: obj.iterator_helper_data,
    weakmap_data: obj.weakmap_data,
    weakset_data: obj.weakset_data,
    weakref_data: obj.weakref_data,
    finreg_data: obj.finreg_data,
    date_data: obj.date_data,
    array_buffer_data: obj.array_buffer_data,
    dataview_data: obj.dataview_data,
    is_html_dda: obj.is_html_dda,
    is_error: obj.is_error,
    is_module_namespace: obj.is_module_namespace,
    realm_env: Some(env),
    immutable_proto: obj.immutable_proto,
    extensible: obj.extensible,
  })
  rc_incref_optional_value(clone.proto)
  rc_incref_optional_env(clone.realm_env)
  match clone.symbol_data {
    Some(symbol) => rc_incref_value(Symbol(symbol))
    None => ()
  }
  match clone.generator_data {
    Some(data) => {
      rc_incref_env(data.env)
      rc_incref_value(Function(data.func))
      rc_incref_values(data.roots)
    }
    None => ()
  }
  match clone.async_generator_data {
    Some(data) =>
      for request in data.queue {
        rc_incref_gen_resume(request.resume_state)
        rc_incref_value(request.promise)
        rc_incref_value(request.resolve)
        rc_incref_value(request.reject)
      }
    None => ()
  }
  match clone.proxy_data {
    Some(data) => {
      rc_incref_value(data.target)
      rc_incref_value(data.handler)
    }
    None => ()
  }
  match clone.map_data {
    Some(data) => rc_incref_map_entries(data.entries)
    None => ()
  }
  match clone.set_data {
    Some(data) => rc_incref_optional_values(data.entries)
    None => ()
  }
  match clone.map_iter_data {
    Some(data) => rc_incref_map_entries(data.entries)
    None => ()
  }
  match clone.set_iter_data {
    Some(data) => rc_incref_optional_values(data.entries)
    None => ()
  }
  match clone.iterator_wrap_data {
    Some(data) => {
      rc_incref_value(data.wrapped_iter)
      rc_incref_value(data.wrapped_next)
    }
    None => ()
  }
  match clone.iterator_concat_data {
    Some(data) => {
      rc_incref_value(data.iter)
      rc_incref_value(data.next)
      for entry in data.values {
        let (key, value) = entry
        rc_incref_value(key)
        rc_incref_value(value)
      }
    }
    None => ()
  }
  match clone.iterator_helper_data {
    Some(data) => {
      rc_incref_value(data.obj)
      rc_incref_value(data.next)
      rc_incref_value(data.func)
      rc_incref_value(data.inner)
      rc_incref_value(data.inner_next)
    }
    None => ()
  }
  match clone.weakmap_data {
    Some(data) =>
      for entry in data.entries {
        let (_, value) = entry
        rc_incref_value(value)
      }
    None => ()
  }
  match clone.finreg_data {
    Some(data) => {
      rc_incref_env(data.realm_env)
      rc_incref_value(data.callback)
      for entry in data.entries {
        rc_incref_value(entry.held_value)
      }
    }
    None => ()
  }
  match clone.dataview_data {
    Some(data) => rc_incref_value(Object(data.buffer))
    None => ()
  }
  clone
}

///|
fn clone_function_value_for_env(
  func : FunctionValue,
  env : Env,
  constructor_override : Value?,
  proto_override : Value?,
) -> FunctionValue {
  let props : Map[String, Property] = Map::new()
  for name, prop in func.props {
    let value = if name == "constructor" {
      match constructor_override {
        Some(ctor_value) => ctor_value
        None => clone_realm_value(prop.value, env)
      }
    } else {
      clone_realm_value(prop.value, env)
    }
    let getter = match prop.getter {
      Some(getter_value) => Some(clone_realm_value(getter_value, env))
      None => None
    }
    let setter = match prop.setter {
      Some(setter_value) => Some(clone_realm_value(setter_value, env))
      None => None
    }
    props_set(props, name, Property::{
      value,
      writable: prop.writable,
      configurable: prop.configurable,
      enumerable: prop.enumerable,
      getter,
      setter,
    })
  }
  let proto = match proto_override {
    Some(value) => Some(value)
    None => func.proto
  }
  let clone = register_function_value(FunctionValue::{
    name: func.name,
    source: func.source,
    source_root: func.source_root,
    source_path: func.source_path,
    line_number: func.line_number,
    column_number: func.column_number,
    local_name_binding: func.local_name_binding,
    params: func.params,
    param_inits: func.param_inits,
    body: func.body,
    env,
    id: alloc_id(),
    props,
    proto,
    home_object: func.home_object,
    super_constructor: func.super_constructor,
    instance_fields: func.instance_fields,
    private_instance_fields: func.private_instance_fields,
    private_instance_props: func.private_instance_props,
    is_constructor: func.is_constructor,
    is_class_constructor: func.is_class_constructor,
    is_default_constructor: func.is_default_constructor,
    extensible: func.extensible,
    is_strict: func.is_strict,
    is_generator: func.is_generator,
    is_async: func.is_async,
    is_arrow: func.is_arrow,
    in_class_field_init: func.in_class_field_init,
  })
  rc_incref_env(env)
  rc_incref_optional_value(clone.proto)
  rc_incref_optional_value(clone.home_object)
  rc_incref_optional_value(clone.super_constructor)
  clone
}

///|
fn clone_realm_value(value : Value, env : Env) -> Value {
  match value {
    Builtin(builtin) => Builtin(clone_builtin_value_for_env(builtin, env))
    _ => value
  }
}

///|
fn clone_builtin_value_for_env(
  builtin : BuiltinValue,
  env : Env,
) -> BuiltinValue {
  let props : Map[String, Property] = Map::new()
  let proto = match builtin.kind {
    BuiltinFunction::GeneratorFunctionConstructor
    | BuiltinFunction::AsyncGeneratorFunctionConstructor
    | BuiltinFunction::AsyncFunctionConstructor =>
      match function_ctor_for_env(env) {
        Some(value) => Some(value)
        None => builtin.proto
      }
    BuiltinFunction::Uint8ArrayConstructor
    | BuiltinFunction::Int8ArrayConstructor
    | BuiltinFunction::Uint8ClampedArrayConstructor
    | BuiltinFunction::Uint16ArrayConstructor
    | BuiltinFunction::Int16ArrayConstructor
    | BuiltinFunction::Uint32ArrayConstructor
    | BuiltinFunction::Int32ArrayConstructor
    | BuiltinFunction::BigInt64ArrayConstructor
    | BuiltinFunction::BigUint64ArrayConstructor
    | BuiltinFunction::Float16ArrayConstructor
    | BuiltinFunction::Float32ArrayConstructor
    | BuiltinFunction::Float64ArrayConstructor =>
      match typed_array_ctor_for_env(env) {
        Some(value) => Some(value)
        None =>
          match function_proto_for_env(env) {
            Some(value) => Some(value)
            None => builtin.proto
          }
      }
    _ =>
      match function_proto_for_env(env) {
        Some(value) => Some(value)
        None => builtin.proto
      }
  }
  let clone = register_builtin_value(BuiltinValue::{
    kind: builtin.kind,
    id: alloc_id(),
    props,
    this_value: builtin.this_value,
    proto,
    extensible: builtin.extensible,
    is_html_dda: builtin.is_html_dda,
    realm_env: Some(env),
  })
  rc_incref_optional_value(clone.this_value)
  rc_incref_optional_value(clone.proto)
  rc_incref_optional_env(clone.realm_env)
  let clone_value = Builtin(clone)
  for name, prop in builtin.props {
    if name == "prototype" {
      match prop.value {
        Object(obj) => {
          let proto_obj = clone_object_value_for_env(
            obj,
            env,
            Some(clone_value),
          )
          let proto_value = Object(proto_obj)
          props_set(clone.props, name, Property::{
            value: proto_value,
            writable: prop.writable,
            configurable: prop.configurable,
            enumerable: prop.enumerable,
            getter: match prop.getter {
              Some(getter_value) => Some(clone_realm_value(getter_value, env))
              None => None
            },
            setter: match prop.setter {
              Some(setter_value) => Some(clone_realm_value(setter_value, env))
              None => None
            },
          })
          continue
        }
        Function(func) => {
          let proto_override = match object_proto_for_env(env) {
            Some(obj) => Some(Object(obj))
            None => None
          }
          let proto_func = clone_function_value_for_env(
            func,
            env,
            Some(clone_value),
            proto_override,
          )
          let proto_value = Function(proto_func)
          props_set(clone.props, name, Property::{
            value: proto_value,
            writable: prop.writable,
            configurable: prop.configurable,
            enumerable: prop.enumerable,
            getter: match prop.getter {
              Some(getter_value) => Some(clone_realm_value(getter_value, env))
              None => None
            },
            setter: match prop.setter {
              Some(setter_value) => Some(clone_realm_value(setter_value, env))
              None => None
            },
          })
          continue
        }
        _ => ()
      }
    }
    props_set(clone.props, name, Property::{
      value: clone_realm_value(prop.value, env),
      writable: prop.writable,
      configurable: prop.configurable,
      enumerable: prop.enumerable,
      getter: match prop.getter {
        Some(getter_value) => Some(clone_realm_value(getter_value, env))
        None => None
      },
      setter: match prop.setter {
        Some(setter_value) => Some(clone_realm_value(setter_value, env))
        None => None
      },
    })
  }
  clone
}

///|
fn builtin_proto_for_env(
  kind : BuiltinFunction,
  env : Env,
  fallback : Value?,
) -> Value? {
  match kind {
    BuiltinFunction::GeneratorFunctionConstructor
    | BuiltinFunction::AsyncGeneratorFunctionConstructor
    | BuiltinFunction::AsyncFunctionConstructor =>
      match function_ctor_for_env(env) {
        Some(value) => Some(value)
        None => fallback
      }
    BuiltinFunction::Uint8ArrayConstructor
    | BuiltinFunction::Int8ArrayConstructor
    | BuiltinFunction::Uint8ClampedArrayConstructor
    | BuiltinFunction::Uint16ArrayConstructor
    | BuiltinFunction::Int16ArrayConstructor
    | BuiltinFunction::Uint32ArrayConstructor
    | BuiltinFunction::Int32ArrayConstructor
    | BuiltinFunction::BigInt64ArrayConstructor
    | BuiltinFunction::BigUint64ArrayConstructor
    | BuiltinFunction::Float16ArrayConstructor
    | BuiltinFunction::Float32ArrayConstructor
    | BuiltinFunction::Float64ArrayConstructor =>
      match typed_array_ctor_for_env(env) {
        Some(value) => Some(value)
        None =>
          match function_proto_for_env(env) {
            Some(value) => Some(value)
            None => fallback
          }
      }
    _ =>
      match function_proto_for_env(env) {
        Some(value) => Some(value)
        None => fallback
      }
  }
}

///|
fn fixup_realm_value(
  value : Value,
  env : Env,
  seen_objects : Map[Int, Bool],
  seen_builtins : Map[Int, Bool],
) -> Unit {
  match value {
    Builtin(builtin) => {
      if seen_builtins.contains(builtin.id) {
        return
      }
      seen_builtins.set(builtin.id, true)
      match builtin.realm_env {
        Some(realm_env) => if realm_env.id != env.id { return }
        None => ()
      }
      let next_proto = builtin_proto_for_env(builtin.kind, env, builtin.proto)
      rc_replace_optional_value(builtin.proto, next_proto)
      builtin.proto = next_proto
      match builtin.this_value {
        Some(inner) =>
          fixup_realm_value(inner, env, seen_objects, seen_builtins)
        None => ()
      }
      for _, prop in builtin.props {
        fixup_realm_value(prop.value, env, seen_objects, seen_builtins)
        match prop.getter {
          Some(getter_value) =>
            fixup_realm_value(getter_value, env, seen_objects, seen_builtins)
          None => ()
        }
        match prop.setter {
          Some(setter_value) =>
            fixup_realm_value(setter_value, env, seen_objects, seen_builtins)
          None => ()
        }
      }
    }
    Function(func) => {
      if seen_objects.contains(func.id) {
        return
      }
      seen_objects.set(func.id, true)
      match func.proto {
        Some(proto_value) =>
          fixup_realm_value(proto_value, env, seen_objects, seen_builtins)
        None => ()
      }
      for _, prop in func.props {
        fixup_realm_value(prop.value, env, seen_objects, seen_builtins)
        match prop.getter {
          Some(getter_value) =>
            fixup_realm_value(getter_value, env, seen_objects, seen_builtins)
          None => ()
        }
        match prop.setter {
          Some(setter_value) =>
            fixup_realm_value(setter_value, env, seen_objects, seen_builtins)
          None => ()
        }
      }
    }
    BoundFunction(bound) => {
      if seen_objects.contains(bound.id) {
        return
      }
      seen_objects.set(bound.id, true)
      match bound.proto {
        Some(proto_value) =>
          fixup_realm_value(proto_value, env, seen_objects, seen_builtins)
        None => ()
      }
      fixup_realm_value(bound.target, env, seen_objects, seen_builtins)
      for value in bound.bound_args {
        fixup_realm_value(value, env, seen_objects, seen_builtins)
      }
      for _, prop in bound.props {
        fixup_realm_value(prop.value, env, seen_objects, seen_builtins)
        match prop.getter {
          Some(getter_value) =>
            fixup_realm_value(getter_value, env, seen_objects, seen_builtins)
          None => ()
        }
        match prop.setter {
          Some(setter_value) =>
            fixup_realm_value(setter_value, env, seen_objects, seen_builtins)
          None => ()
        }
      }
    }
    Object(obj) => {
      if seen_objects.contains(obj.id) {
        return
      }
      seen_objects.set(obj.id, true)
      match obj.proto {
        Some(proto_value) =>
          fixup_realm_value(proto_value, env, seen_objects, seen_builtins)
        None => ()
      }
      for _, prop in obj.props {
        fixup_realm_value(prop.value, env, seen_objects, seen_builtins)
        match prop.getter {
          Some(getter_value) =>
            fixup_realm_value(getter_value, env, seen_objects, seen_builtins)
          None => ()
        }
        match prop.setter {
          Some(setter_value) =>
            fixup_realm_value(setter_value, env, seen_objects, seen_builtins)
          None => ()
        }
      }
    }
    Array(arr) => {
      if seen_objects.contains(arr.id) {
        return
      }
      seen_objects.set(arr.id, true)
      match arr.proto {
        Some(proto_value) =>
          fixup_realm_value(proto_value, env, seen_objects, seen_builtins)
        None => ()
      }
      for entry in arr.elements {
        match entry {
          Some(inner) =>
            fixup_realm_value(inner, env, seen_objects, seen_builtins)
          None => ()
        }
      }
      for _, prop in arr.props {
        fixup_realm_value(prop.value, env, seen_objects, seen_builtins)
        match prop.getter {
          Some(getter_value) =>
            fixup_realm_value(getter_value, env, seen_objects, seen_builtins)
          None => ()
        }
        match prop.setter {
          Some(setter_value) =>
            fixup_realm_value(setter_value, env, seen_objects, seen_builtins)
          None => ()
        }
      }
    }
    Arguments(args) => {
      if seen_objects.contains(args.id) {
        return
      }
      seen_objects.set(args.id, true)
      match args.proto {
        Some(proto_value) =>
          fixup_realm_value(proto_value, env, seen_objects, seen_builtins)
        None => ()
      }
      for entry in args.elements {
        match entry {
          Some(inner) =>
            fixup_realm_value(inner, env, seen_objects, seen_builtins)
          None => ()
        }
      }
      for _, prop in args.props {
        fixup_realm_value(prop.value, env, seen_objects, seen_builtins)
        match prop.getter {
          Some(getter_value) =>
            fixup_realm_value(getter_value, env, seen_objects, seen_builtins)
          None => ()
        }
        match prop.setter {
          Some(setter_value) =>
            fixup_realm_value(setter_value, env, seen_objects, seen_builtins)
          None => ()
        }
      }
    }
    _ => ()
  }
}

///|
fn fixup_realm_builtins(env : Env) -> Unit {
  let seen_objects : Map[Int, Bool] = Map::new()
  let seen_builtins : Map[Int, Bool] = Map::new()
  match global_object_value(env) {
    Undefined => ()
    value => fixup_realm_value(value, env, seen_objects, seen_builtins)
  }
}

///|
fn new_realm_env(base_env : Env) -> Env {
  let env = Env::new(None)
  let global_obj = match global_object(base_env) {
    Some(obj) => clone_global_object(obj, env)
    None => {
      let obj = new_object_struct(value_from_object(object_proto()))
      rc_replace_optional_env(obj.realm_env, Some(env))
      obj.realm_env = Some(env)
      obj
    }
  }
  let global_value = Object(global_obj)
  env_bindings_set(env, "this", global_value)
  props_set(global_obj.props, "globalThis", property_data(global_value))
  match function_proto_for_env(env) {
    Some(Function(func)) =>
      match object_proto_for_env(env) {
        Some(obj) => {
          rc_replace_optional_value(func.proto, Some(Object(obj)))
          func.proto = Some(Object(obj))
        }
        None => ()
      }
    _ => ()
  }
  fixup_realm_builtins(env)
  register_promise_ctor_from_global(env)
  env
}

///|
fn realm_env_from_value(value : Value?) -> Env {
  match value {
    Some(Object(obj)) =>
      match obj.realm_env {
        Some(env) => env
        None =>
          match root_env() {
            Some(env) => env
            None => Env::new(None)
          }
      }
    _ =>
      match root_env() {
        Some(env) => env
        None => Env::new(None)
      }
  }
}
