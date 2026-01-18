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
  let props = Map::new()
  props.set("length", property_data_const(Number(Double::from_int(0))))
  props.set("name", property_data_const(String("")))
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
      throw_type_error_ref.update(fn(table) {
        table.set(realm_env.id, created)
        table
      })
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
fn value_from_object(obj : ObjectValue?) -> Value? {
  match obj {
    Some(value) => Some(Object(value))
    None => None
  }
}
