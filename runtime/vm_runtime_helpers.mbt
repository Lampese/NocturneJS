///|
fn alloc_id() -> Int {
  let mut id = 0
  id_ref.update(fn(current) {
    id = current
    current + 1
  })
  id
}

///|
fn set_proto_ref(cell : Ref[ObjectValue?], value : ObjectValue?) -> Unit {
  cell.update(fn(_) { value })
}

///|
fn get_proto_ref(cell : Ref[ObjectValue?]) -> ObjectValue? {
  let mut value : ObjectValue? = None
  cell.update(fn(current) {
    value = current
    current
  })
  value
}

///|
fn set_value_ref(cell : Ref[Value?], value : Value?) -> Unit {
  cell.update(fn(_) { value })
}

///|
fn get_value_ref(cell : Ref[Value?]) -> Value? {
  let mut value : Value? = None
  cell.update(fn(current) {
    value = current
    current
  })
  value
}

///|
fn realm_env_from_env(env : Env) -> Env {
  match env.parent {
    Some(parent) => realm_env_from_env(parent)
    None => env
  }
}

///|
fn new_throw_type_error_value_for_env(env : Env) -> Value {
  let props = props_new()
  props_set(props, "length", property_data_const(Number(Double::from_int(0))))
  props_set(props, "name", property_data_const(String("")))
  let proto = match function_proto_for_env(env) {
    Some(value) => Some(value)
    None => function_proto()
  }
  let builtin = register_builtin_value(BuiltinValue::{
    kind: BuiltinFunction::ThrowTypeError,
    id: alloc_id(),
    props,
    this_value: None,
    proto,
    extensible: false,
    is_html_dda: false,
    realm_env: Some(env),
  })
  rc_incref_optional_value(proto)
  rc_incref_optional_env(Some(env))
  Builtin(builtin)
}

///|
fn throw_type_error_value_for_env(env : Env) -> Value {
  let realm_env = realm_env_from_env(env)
  let mut value : Value? = None
  throw_type_error_ref.update(fn(table) {
    value = table.get(realm_env.id)
    table
  })
  match value {
    Some(existing) => existing
    None => {
      let created = new_throw_type_error_value_for_env(realm_env)
      throw_type_error_set(realm_env.id, created)
      created
    }
  }
}

///|
fn template_registry_for_env(env : Env) -> Map[Int, Value] {
  let realm_env = realm_env_from_env(env)
  let mut registry : Map[Int, Value]? = None
  template_registry_ref.update(fn(table) {
    registry = table.get(realm_env.id)
    table
  })
  match registry {
    Some(existing) => existing
    None => {
      let created = Map::new()
      template_registry_ref.update(fn(table) {
        table.set(realm_env.id, created)
        table
      })
      created
    }
  }
}

///|
fn throw_type_error_set(env_id : Int, value : Value) -> Unit {
  throw_type_error_ref.update(fn(table) {
    match table.get(env_id) {
      Some(existing) => rc_decref_value(existing)
      None => ()
    }
    rc_incref_value(value)
    table.set(env_id, value)
    table
  })
}

///|
fn throw_type_error_clear() -> Unit {
  throw_type_error_ref.update(fn(table) {
    for _, value in table {
      rc_decref_value(value)
    }
    Map::new()
  })
}

///|
fn template_registry_set(
  registry : Map[Int, Value],
  site_id : Int,
  value : Value,
) -> Unit {
  match registry.get(site_id) {
    Some(existing) => rc_decref_value(existing)
    None => ()
  }
  rc_incref_value(value)
  registry.set(site_id, value)
}

///|
fn template_registry_clear() -> Unit {
  template_registry_ref.update(fn(table) {
    for _, registry in table {
      for _, value in registry {
        rc_decref_value(value)
      }
    }
    Map::new()
  })
}

///|
fn value_from_object(obj : ObjectValue?) -> Value? {
  match obj {
    Some(value) => Some(Object(value))
    None => None
  }
}
