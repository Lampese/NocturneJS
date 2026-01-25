///|
let promise_ctor_env_ref : Ref[Map[Int, BuiltinValue]] = Ref::new(Map::new())

///|
let promise_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
fn promise_proto() -> ObjectValue? {
  get_proto_ref(promise_proto_ref)
}

///|
fn promise_proto_for_env(env : Env) -> ObjectValue? {
  match global_object(env) {
    Some(global_obj) =>
      match props_get(global_obj.props, "Promise") {
        Some(prop) =>
          match prop.value {
            Builtin(builtin) =>
              match props_get(builtin.props, "prototype") {
                Some(proto_prop) =>
                  match proto_prop.value {
                    Object(proto_obj) => Some(proto_obj)
                    _ => None
                  }
                None => None
              }
            Function(func) =>
              match props_get(func.props, "prototype") {
                Some(proto_prop) =>
                  match proto_prop.value {
                    Object(proto_obj) => Some(proto_obj)
                    _ => None
                  }
                None => None
              }
            _ => None
          }
        None => None
      }
    None => None
  }
}

///|
fn promise_ctor_env_id(env : Env) -> Int {
  match global_object(env) {
    Some(obj) => obj.id
    None => env.id
  }
}

///|
fn register_promise_ctor_for_env(env : Env, ctor : BuiltinValue) -> Unit {
  let realm_id = promise_ctor_env_id(env)
  promise_ctor_env_ref.update(fn(table) {
    match table.get(realm_id) {
      Some(existing) => rc_decref_value(Builtin(existing))
      None => ()
    }
    rc_incref_value(Builtin(ctor))
    table.set(realm_id, ctor)
    table
  })
}

///|
fn promise_ctor_env_clear() -> Unit {
  promise_ctor_env_ref.update(fn(table) {
    for _, ctor in table {
      rc_decref_value(Builtin(ctor))
    }
    Map::new()
  })
}

///|
fn promise_ctor_for_env(env : Env) -> Value? {
  let mut value : BuiltinValue? = None
  let realm_id = promise_ctor_env_id(env)
  promise_ctor_env_ref.update(fn(table) {
    match table.get(realm_id) {
      Some(ctor) => {
        value = Some(ctor)
        table
      }
      None => table
    }
  })
  match value {
    Some(ctor) => Some(Builtin(ctor))
    None => None
  }
}

///|
fn promise_ctor_from_global_env(env : Env) -> Value? {
  match global_object(env) {
    Some(obj) =>
      match props_get(obj.props, "Promise") {
        Some(prop) => Some(prop.value)
        None => None
      }
    None => None
  }
}

///|
fn register_promise_ctor_from_global(env : Env) -> Unit {
  match promise_ctor_from_global_env(env) {
    Some(Builtin(builtin)) => register_promise_ctor_for_env(env, builtin)
    _ => ()
  }
}

///|
fn promise_ctor_value_for_env(env : Env) -> Value {
  match promise_ctor_for_env(env) {
    Some(value) => value
    None =>
      match promise_ctor_from_global_env(env) {
        Some(value) => value
        None => new_builtin_value(BuiltinFunction::PromiseConstructor)
      }
  }
}

///|
fn promise_ctor_value_for_current_env() -> Value {
  match current_env() {
    Some(env) => promise_ctor_value_for_env(env)
    None =>
      match root_env() {
        Some(env) => promise_ctor_value_for_env(env)
        None => new_builtin_value(BuiltinFunction::PromiseConstructor)
      }
  }
}
