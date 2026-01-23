///|
fn weakmap_key_id(key : Value) -> Int? {
  value_id(key)
}

///|
fn weakmap_find_index(data : WeakMapData, key : Value) -> Int? {
  match weakmap_key_id(key) {
    Some(id) => data.index.get(id)
    None => None
  }
}

///|
fn weakmap_remove_index(data : WeakMapData, index : Int) -> Unit {
  let len = data.entries.length()
  if index < 0 || index >= len {
    return ()
  }
  let (removed_key, removed_value) = data.entries[index]
  rc_decref_value(removed_value)
  match weakmap_key_id(removed_key) {
    Some(id) => data.index.remove(id)
    None => ()
  }
  let last_index = len - 1
  if index == last_index {
    let _ = data.entries.remove(last_index)
    return ()
  }
  let last_entry = data.entries[last_index]
  data.entries[index] = last_entry
  let _ = data.entries.remove(last_index)
  let (last_key, _) = last_entry
  match weakmap_key_id(last_key) {
    Some(id) => data.index.set(id, index)
    None => ()
  }
}

///|
fn weakmap_get_value(data : WeakMapData, key : Value) -> Value {
  match weakmap_find_index(data, key) {
    Some(index) => {
      let (_, value) = data.entries[index]
      value
    }
    None => Undefined
  }
}

///|
fn weakmap_has_key(data : WeakMapData, key : Value) -> Bool {
  match weakmap_find_index(data, key) {
    Some(_) => true
    None => false
  }
}

///|
fn weakset_find_index(entries : Array[Value], key : Value) -> Int? {
  let mut index = 0
  for entry in entries {
    if same_value_zero(entry, key) {
      return Some(index)
    }
    index = index + 1
  }
  None
}

///|
fn weakmap_set_value(data : WeakMapData, key : Value, value : Value) -> Unit {
  match weakmap_key_id(key) {
    Some(id) =>
      match data.index.get(id) {
        Some(index) => {
          let (_, old_value) = data.entries[index]
          rc_replace_value(old_value, value)
          data.entries[index] = (key, value)
        }
        None => {
          rc_incref_value(value)
          data.entries.push((key, value))
          data.index.set(id, data.entries.length() - 1)
        }
      }
    None => ()
  }
}

///|
fn weakmap_get_or_insert(
  data : WeakMapData,
  key : Value,
  value_or_callback : Value,
  computed : Bool,
) -> Value raise {
  if computed && !is_callable(value_or_callback) {
    return throw_type_error("not a function")
  }
  match weakmap_find_index(data, key) {
    Some(index) => {
      let (_, value) = data.entries[index]
      value
    }
    None => {
      let value = if computed {
        let result = call_value_with_this(value_or_callback, [key], Undefined)
        let _ = weakmap_delete_key(data, key)
        result
      } else {
        value_or_callback
      }
      weakmap_set_value(data, key, value)
      value
    }
  }
}

///|
fn weakmap_delete_key(data : WeakMapData, key : Value) -> Bool {
  match weakmap_find_index(data, key) {
    Some(index) => {
      weakmap_remove_index(data, index)
      true
    }
    None => false
  }
}

///|
fn weakset_has_value(data : WeakSetData, value : Value) -> Bool {
  match weakset_find_index(data.entries, value) {
    Some(_) => true
    None => false
  }
}

///|
fn weakset_add_value(data : WeakSetData, value : Value) -> Unit {
  match weakset_find_index(data.entries, value) {
    Some(_) => ()
    None => data.entries.push(value)
  }
}

///|
fn weakset_delete_value(data : WeakSetData, value : Value) -> Bool {
  match weakset_find_index(data.entries, value) {
    Some(index) => {
      let _ = data.entries.remove(index)
      true
    }
    None => false
  }
}
