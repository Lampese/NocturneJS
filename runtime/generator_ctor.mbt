///|
let generator_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let generator_function_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let async_function_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let async_generator_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let async_generator_function_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let generator_function_ctor_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let async_generator_function_ctor_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let generator_proto_env_ref : Ref[Map[Int, ObjectValue]] = Ref::new(Map::new())

///|
let async_generator_proto_env_ref : Ref[Map[Int, ObjectValue]] = Ref::new(
  Map::new(),
)

///|
let generator_function_ctor_env_ref : Ref[Map[Int, BuiltinValue]] = Ref::new(
  Map::new(),
)

///|
let async_generator_function_ctor_env_ref : Ref[Map[Int, BuiltinValue]] = Ref::new(
  Map::new(),
)

///|
let async_function_ctor_env_ref : Ref[Map[Int, BuiltinValue]] = Ref::new(
  Map::new(),
)

///|
fn generator_proto_env_clear() -> Unit {
  generator_proto_env_ref.update(fn(table) {
    for _, obj in table {
      rc_decref_value(Object(obj))
    }
    Map::new()
  })
}

///|
fn async_generator_proto_env_clear() -> Unit {
  async_generator_proto_env_ref.update(fn(table) {
    for _, obj in table {
      rc_decref_value(Object(obj))
    }
    Map::new()
  })
}

///|
fn generator_function_ctor_env_clear() -> Unit {
  generator_function_ctor_env_ref.update(fn(table) {
    for _, ctor in table {
      rc_decref_value(Builtin(ctor))
    }
    Map::new()
  })
}

///|
fn async_generator_function_ctor_env_clear() -> Unit {
  async_generator_function_ctor_env_ref.update(fn(table) {
    for _, ctor in table {
      rc_decref_value(Builtin(ctor))
    }
    Map::new()
  })
}

///|
fn async_function_ctor_env_clear() -> Unit {
  async_function_ctor_env_ref.update(fn(table) {
    for _, ctor in table {
      rc_decref_value(Builtin(ctor))
    }
    Map::new()
  })
}

///|
fn generator_proto() -> ObjectValue? {
  get_proto_ref(generator_proto_ref)
}

///|
fn generator_function_proto() -> ObjectValue? {
  get_proto_ref(generator_function_proto_ref)
}

///|
fn async_function_proto() -> ObjectValue? {
  get_proto_ref(async_function_proto_ref)
}

///|
fn async_generator_proto() -> ObjectValue? {
  get_proto_ref(async_generator_proto_ref)
}

///|
fn async_generator_function_proto() -> ObjectValue? {
  get_proto_ref(async_generator_function_proto_ref)
}

///|
fn generator_function_ctor_for_env(env : Env) -> BuiltinValue? {
  let mut value : BuiltinValue? = None
  let realm_id = match global_object(env) {
    Some(obj) => obj.id
    None => env.id
  }
  generator_function_ctor_env_ref.update(fn(table) {
    match table.get(realm_id) {
      Some(ctor) => {
        value = Some(ctor)
        table
      }
      None =>
        match generator_function_proto() {
          Some(proto_obj) =>
            match props_get(proto_obj.props, "constructor") {
                Some(prop) =>
                  match prop.value {
                    Builtin(builtin) => {
                      let clone = clone_builtin_value_for_env(builtin, env)
                      rc_incref_value(Builtin(clone))
                      table.set(realm_id, clone)
                      value = Some(clone)
                      table
                    }
                  _ => table
                }
              None => table
            }
          None => table
        }
    }
  })
  value
}

///|
fn generator_function_proto_for_env(env : Env) -> ObjectValue? {
  match generator_function_ctor_for_env(env) {
    Some(ctor) =>
      match props_get(ctor.props, "prototype") {
        Some(proto_prop) =>
          match proto_prop.value {
            Object(proto_obj) => {
              let _ = generator_proto_for_env_with_ctor(env, Some(proto_obj))
              Some(proto_obj)
            }
            _ => None
          }
        None => None
      }
    None => None
  }
}

///|
fn async_function_ctor_for_env(env : Env) -> BuiltinValue? {
  let mut value : BuiltinValue? = None
  let realm_id = match global_object(env) {
    Some(obj) => obj.id
    None => env.id
  }
  async_function_ctor_env_ref.update(fn(table) {
    match table.get(realm_id) {
      Some(ctor) => {
        value = Some(ctor)
        table
      }
      None =>
        match async_function_proto() {
          Some(proto_obj) =>
            match props_get(proto_obj.props, "constructor") {
              Some(prop) =>
                  match prop.value {
                    Builtin(builtin) => {
                      let clone = clone_builtin_value_for_env(builtin, env)
                      rc_incref_value(Builtin(clone))
                      table.set(realm_id, clone)
                      value = Some(clone)
                      table
                    }
                  _ => table
                }
              None => table
            }
          None => table
        }
    }
  })
  value
}

///|
fn async_function_proto_for_env(env : Env) -> ObjectValue? {
  match async_function_ctor_for_env(env) {
    Some(ctor) =>
      match props_get(ctor.props, "prototype") {
        Some(proto_prop) =>
          match proto_prop.value {
            Object(proto_obj) => Some(proto_obj)
            _ => None
          }
        None => None
      }
    None => None
  }
}

///|
fn async_generator_function_ctor_for_env(env : Env) -> BuiltinValue? {
  let mut value : BuiltinValue? = None
  let realm_id = match global_object(env) {
    Some(obj) => obj.id
    None => env.id
  }
  async_generator_function_ctor_env_ref.update(fn(table) {
    match table.get(realm_id) {
      Some(ctor) => {
        value = Some(ctor)
        table
      }
      None =>
        match async_generator_function_proto() {
          Some(proto_obj) =>
            match props_get(proto_obj.props, "constructor") {
              Some(prop) =>
                  match prop.value {
                    Builtin(builtin) => {
                      let clone = clone_builtin_value_for_env(builtin, env)
                      rc_incref_value(Builtin(clone))
                      table.set(realm_id, clone)
                      value = Some(clone)
                      table
                    }
                  _ => table
                }
              None => table
            }
          None => table
        }
    }
  })
  value
}

///|
fn async_generator_function_proto_for_env(env : Env) -> ObjectValue? {
  match async_generator_function_ctor_for_env(env) {
    Some(ctor) =>
      match props_get(ctor.props, "prototype") {
        Some(proto_prop) =>
          match proto_prop.value {
            Object(proto_obj) => {
              let _ = async_generator_proto_for_env_with_ctor(
                env,
                Some(proto_obj),
              )
              Some(proto_obj)
            }
            _ => None
          }
        None => None
      }
    None => None
  }
}

///|
fn async_iterator_proto_for_env(env : Env) -> ObjectValue? {
  let mut value : ObjectValue? = None
  let realm_id = match global_object(env) {
    Some(obj) => obj.id
    None => env.id
  }
  async_iterator_proto_env_ref.update(fn(table) {
    match table.get(realm_id) {
      Some(obj) => {
        value = Some(obj)
        table
      }
      None =>
        match async_iterator_proto() {
          Some(obj) => {
            let clone = clone_object_value_for_env(obj, env, None)
            let proto_value = match object_proto_for_env(env) {
              Some(obj_proto) => Some(Object(obj_proto))
              None => value_from_object(object_proto())
            }
            rc_replace_optional_value(clone.proto, proto_value)
            clone.proto = proto_value
            rc_incref_value(Object(clone))
            table.set(realm_id, clone)
            value = Some(clone)
            table
          }
          None => table
        }
    }
  })
  value
}

///|
fn set_function_proto_prototype(
  func_proto : ObjectValue,
  proto_obj : ObjectValue,
) -> Unit {
  let proto_prop = match props_get(func_proto.props, "prototype") {
    Some(prop) =>
      Property::{
        value: Object(proto_obj),
        writable: prop.writable,
        configurable: prop.configurable,
        enumerable: prop.enumerable,
        getter: prop.getter,
        setter: prop.setter,
      }
    None => property_data_readonly(Object(proto_obj))
  }
  props_set(func_proto.props, "prototype", proto_prop)
}

///|
fn generator_proto_for_env_with_ctor(
  env : Env,
  func_proto : ObjectValue?,
) -> ObjectValue? {
  let mut value : ObjectValue? = None
  let realm_id = match global_object(env) {
    Some(obj) => obj.id
    None => env.id
  }
  generator_proto_env_ref.update(fn(table) {
    match table.get(realm_id) {
      Some(obj) => {
        value = Some(obj)
        table
      }
      None =>
        match generator_proto() {
          Some(obj) => {
            let ctor_override = match func_proto {
              Some(proto_obj) => Some(Object(proto_obj))
              None => value_from_object(generator_function_proto())
            }
            let clone = clone_object_value_for_env(obj, env, ctor_override)
            let proto_value = match iterator_proto_for_env(env) {
              Some(iter_proto) => Some(Object(iter_proto))
              None => value_from_object(iterator_proto())
            }
            rc_replace_optional_value(clone.proto, proto_value)
            clone.proto = proto_value
            rc_incref_value(Object(clone))
            table.set(realm_id, clone)
            value = Some(clone)
            table
          }
          None => table
        }
    }
  })
  match (value, func_proto) {
    (Some(gen_proto), Some(func_proto_obj)) =>
      set_function_proto_prototype(func_proto_obj, gen_proto)
    _ => ()
  }
  value
}

///|
fn generator_proto_for_env(env : Env) -> ObjectValue? {
  match generator_function_proto_for_env(env) {
    Some(func_proto) => generator_proto_for_env_with_ctor(env, Some(func_proto))
    None => generator_proto_for_env_with_ctor(env, None)
  }
}

///|
fn async_generator_proto_for_env_with_ctor(
  env : Env,
  func_proto : ObjectValue?,
) -> ObjectValue? {
  let mut value : ObjectValue? = None
  let realm_id = match global_object(env) {
    Some(obj) => obj.id
    None => env.id
  }
  async_generator_proto_env_ref.update(fn(table) {
    match table.get(realm_id) {
      Some(obj) => {
        value = Some(obj)
        table
      }
      None =>
        match async_generator_proto() {
          Some(obj) => {
            let ctor_override = match func_proto {
              Some(proto_obj) => Some(Object(proto_obj))
              None => value_from_object(async_generator_function_proto())
            }
            let clone = clone_object_value_for_env(obj, env, ctor_override)
            let proto_value = match async_iterator_proto_for_env(env) {
              Some(iter_proto) => Some(Object(iter_proto))
              None => value_from_object(async_iterator_proto())
            }
            rc_replace_optional_value(clone.proto, proto_value)
            clone.proto = proto_value
            rc_incref_value(Object(clone))
            table.set(realm_id, clone)
            value = Some(clone)
            table
          }
          None => table
        }
    }
  })
  match (value, func_proto) {
    (Some(gen_proto), Some(func_proto_obj)) =>
      set_function_proto_prototype(func_proto_obj, gen_proto)
    _ => ()
  }
  value
}

///|
fn async_generator_proto_for_env(env : Env) -> ObjectValue? {
  match async_generator_function_proto_for_env(env) {
    Some(func_proto) =>
      async_generator_proto_for_env_with_ctor(env, Some(func_proto))
    None => async_generator_proto_for_env_with_ctor(env, None)
  }
}
