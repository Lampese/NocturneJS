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
  props : Props,
  name : String,
  prop : Property,
) -> Unit {
  props.version = props.version + 1
  match props.repr {
    PropsRepr::Shape(shape, slots) => {
      if !shape.entry_map.contains(name) &&
        shape.entries.length() >= PROPS_DICT_THRESHOLD {
        props_convert_to_dict(props)
        props_set(props, name, prop)
        return ()
      }
      let new_slot = slot_from_property(prop)
      match shape.entry_map.get(name) {
        Some(index) =>
          if index >= 0 && index < slots.length() {
            let entry = shape.entries[index]
            let mut next_shape = shape
            if entry.writable != prop.writable ||
              entry.enumerable != prop.enumerable ||
              entry.configurable != prop.configurable {
              next_shape =
                shape_transition_update(
                  shape,
                  name,
                  prop.writable,
                  prop.enumerable,
                  prop.configurable,
                )
            }
            let old_prop = property_from_entry_slot(entry, slots[index])
            rc_decref_property(name, old_prop)
            rc_incref_property(name, prop)
            slots[index] = new_slot
            props.repr = PropsRepr::Shape(next_shape, slots)
          } else {
            rc_incref_property(name, prop)
            while slots.length() < index {
              slots.push(PropSlot::Data(Undefined))
            }
            slots.push(new_slot)
            props.repr = PropsRepr::Shape(shape, slots)
          }
        None => {
          let next_shape =
            shape_transition_add(
              shape,
              name,
              prop.writable,
              prop.enumerable,
              prop.configurable,
            )
          rc_incref_property(name, prop)
          slots.push(new_slot)
          props.repr = PropsRepr::Shape(next_shape, slots)
        }
      }
    }
    PropsRepr::Dict(dict) => {
      match dict.map.get(name) {
        Some(existing) => rc_decref_property(name, existing)
        None => dict.order.push(name)
      }
      rc_incref_property(name, prop)
      dict.map.set(name, prop)
      props.repr = PropsRepr::Dict(dict)
    }
  }
}

///|
fn props_remove(props : Props, name : String) -> Unit {
  props.version = props.version + 1
  match props.repr {
    PropsRepr::Shape(shape, slots) =>
      match shape.entry_map.get(name) {
        Some(index) =>
          if index >= 0 && index < slots.length() {
            let entry = shape.entries[index]
            let old_prop = property_from_entry_slot(entry, slots[index])
            rc_decref_property(name, old_prop)
            let _ = slots.remove(index)
            let next_shape = shape_transition_remove(shape, name)
            props.repr = PropsRepr::Shape(next_shape, slots)
          } else {
            let next_shape = shape_transition_remove(shape, name)
            props.repr = PropsRepr::Shape(next_shape, slots)
          }
        None => ()
      }
    PropsRepr::Dict(dict) => {
      match dict.map.get(name) {
        Some(existing) => rc_decref_property(name, existing)
        None => ()
      }
      let _ = dict.map.remove(name)
      let mut i = 0
      while i < dict.order.length() {
        if dict.order[i] == name {
          let _ = dict.order.remove(i)
          break
        }
        i = i + 1
      }
      if dict.map.is_empty() {
        props_reset_empty(props)
      } else {
        props.repr = PropsRepr::Dict(dict)
      }
    }
  }
}

///|
fn props_clear(props : Props) -> Unit {
  if props_is_empty(props) {
    return ()
  }
  props_iter(props, fn(name, prop) {
    rc_decref_property(name, prop)
  })
  props_reset_empty(props)
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
