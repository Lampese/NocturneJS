///|
fn rc_meta_register(id : Int) -> Unit {
  gc_rc_meta_ref().update(fn(table) {
    if !table.contains(id) {
      table.set(id, RcMeta::{ ref_count: 0 })
    }
    table
  })
}

///|
fn rc_meta_remove(id : Int) -> Unit {
  gc_rc_meta_ref().update(fn(table) {
    let _ = table.remove(id)
    table
  })
}

///|
fn rc_meta_incref(id : Int) -> Unit {
  gc_rc_meta_ref().update(fn(table) {
    match table.get(id) {
      Some(meta) => meta.ref_count = meta.ref_count + 1
      None => table.set(id, RcMeta::{ ref_count: 1 })
    }
    table
  })
}

///|
fn rc_meta_decref(id : Int) -> Unit {
  gc_rc_meta_ref().update(fn(table) {
    match table.get(id) {
      Some(meta) =>
        if meta.ref_count > 0 {
          meta.ref_count = meta.ref_count - 1
        }
      None => ()
    }
    table
  })
}

///|
fn rc_incref_value(value : Value) -> Unit {
  match value_id(value) {
    Some(id) => rc_meta_incref(id)
    None => ()
  }
}

///|
fn rc_decref_value(value : Value) -> Unit {
  match value_id(value) {
    Some(id) => rc_meta_decref(id)
    None => ()
  }
}

///|
fn rc_incref_env(env : Env) -> Unit {
  rc_meta_incref(env.id)
}

///|
fn rc_decref_env(env : Env) -> Unit {
  rc_meta_decref(env.id)
}

///|
fn rc_incref_optional_value(value : Value?) -> Unit {
  match value {
    Some(inner) => rc_incref_value(inner)
    None => ()
  }
}

///|
fn rc_decref_optional_value(value : Value?) -> Unit {
  match value {
    Some(inner) => rc_decref_value(inner)
    None => ()
  }
}

///|
fn rc_incref_optional_env(env : Env?) -> Unit {
  match env {
    Some(inner) => rc_incref_env(inner)
    None => ()
  }
}

///|
fn rc_decref_optional_env(env : Env?) -> Unit {
  match env {
    Some(inner) => rc_decref_env(inner)
    None => ()
  }
}

///|
fn rc_replace_value(old_value : Value, new_value : Value) -> Unit {
  let old_id = value_id(old_value)
  let new_id = value_id(new_value)
  if old_id == new_id {
    return ()
  }
  rc_decref_value(old_value)
  rc_incref_value(new_value)
}

///|
fn rc_replace_optional_value(
  old_value : Value?,
  new_value : Value?,
) -> Unit {
  match (old_value, new_value) {
    (None, None) => ()
    (Some(old), Some(new)) => rc_replace_value(old, new)
    (Some(old), None) => rc_decref_value(old)
    (None, Some(new)) => rc_incref_value(new)
  }
}

///|
fn rc_replace_optional_env(old_env : Env?, new_env : Env?) -> Unit {
  match (old_env, new_env) {
    (None, None) => ()
    (Some(old), Some(new)) =>
      if old.id != new.id {
        rc_decref_env(old)
        rc_incref_env(new)
      }
    (Some(old), None) => rc_decref_env(old)
    (None, Some(new)) => rc_incref_env(new)
  }
}

///|
fn rc_incref_property(name : String, prop : Property) -> Unit {
  if is_symbol_prop_key(name) {
    match symbol_from_prop_key(name) {
      Some(symbol) => rc_incref_value(Symbol(symbol))
      None => ()
    }
  }
  rc_incref_value(prop.value)
  rc_incref_optional_value(prop.getter)
  rc_incref_optional_value(prop.setter)
}

///|
fn rc_decref_property(name : String, prop : Property) -> Unit {
  if is_symbol_prop_key(name) {
    match symbol_from_prop_key(name) {
      Some(symbol) => rc_decref_value(Symbol(symbol))
      None => ()
    }
  }
  rc_decref_value(prop.value)
  rc_decref_optional_value(prop.getter)
  rc_decref_optional_value(prop.setter)
}

///|
fn props_set(
  props : Map[String, Property],
  name : String,
  prop : Property,
) -> Unit {
  match props.get(name) {
    Some(existing) => rc_decref_property(name, existing)
    None => ()
  }
  rc_incref_property(name, prop)
  props.set(name, prop)
}

///|
fn props_remove(props : Map[String, Property], name : String) -> Unit {
  match props.get(name) {
    Some(existing) => rc_decref_property(name, existing)
    None => ()
  }
  let _ = props.remove(name)
}

///|
fn props_clear(props : Map[String, Property]) -> Unit {
  for name, prop in props {
    rc_decref_property(name, prop)
  }
  props.clear()
}

///|
fn rc_truncate_optional_values(values : Array[Value?], new_len : Int) -> Unit {
  let mut index = new_len
  while index < values.length() {
    rc_decref_optional_value(values[index])
    index = index + 1
  }
  values.truncate(new_len)
}

///|
fn rc_clear_optional_values(values : Array[Value?]) -> Unit {
  for entry in values {
    rc_decref_optional_value(entry)
  }
  values.clear()
}

///|
fn rc_clear_values(values : Array[Value]) -> Unit {
  for entry in values {
    rc_decref_value(entry)
  }
  values.clear()
}

///|
fn rc_incref_optional_values(values : Array[Value?]) -> Unit {
  for entry in values {
    rc_incref_optional_value(entry)
  }
}

///|
fn rc_decref_optional_values(values : Array[Value?]) -> Unit {
  for entry in values {
    rc_decref_optional_value(entry)
  }
}

///|
fn rc_incref_values(values : Array[Value]) -> Unit {
  for entry in values {
    rc_incref_value(entry)
  }
}

///|
fn rc_decref_values(values : Array[Value]) -> Unit {
  for entry in values {
    rc_decref_value(entry)
  }
}

///|
fn rc_incref_map_entries(entries : Array[(Value, Value)?]) -> Unit {
  for entry in entries {
    match entry {
      Some((key, value)) => {
        rc_incref_value(key)
        rc_incref_value(value)
      }
      None => ()
    }
  }
}

///|
fn rc_decref_map_entries(entries : Array[(Value, Value)?]) -> Unit {
  for entry in entries {
    match entry {
      Some((key, value)) => {
        rc_decref_value(key)
        rc_decref_value(value)
      }
      None => ()
    }
  }
}
