///|
fn map_find_index(entries : Array[(Value, Value)?], key : Value) -> Int? {
  let mut index = 0
  for entry in entries {
    match entry {
      Some((k, _)) => if same_value_zero(k, key) { return Some(index) }
      None => ()
    }
    index = index + 1
  }
  None
}

///|
fn set_find_index(entries : Array[Value?], key : Value) -> Int? {
  let mut index = 0
  for entry in entries {
    match entry {
      Some(value) => if same_value_zero(value, key) { return Some(index) }
      None => ()
    }
    index = index + 1
  }
  None
}

///|
fn map_get_value(data : MapData, key : Value) -> Value {
  let normalized = map_normalize_key(key)
  match map_find_index(data.entries, normalized) {
    Some(index) =>
      match data.entries[index] {
        Some((_, value)) => value
        None => Undefined
      }
    None => Undefined
  }
}

///|
fn map_has_key(data : MapData, key : Value) -> Bool {
  let normalized = map_normalize_key(key)
  match map_find_index(data.entries, normalized) {
    Some(_) => true
    None => false
  }
}

///|
fn map_set_value(data : MapData, key : Value, value : Value) -> Unit {
  let normalized = map_normalize_key(key)
  match map_find_index(data.entries, normalized) {
    Some(index) => data.entries[index] = Some((normalized, value))
    None => data.entries.push(Some((normalized, value)))
  }
}

///|
fn map_delete_key(data : MapData, key : Value) -> Bool {
  let normalized = map_normalize_key(key)
  match map_find_index(data.entries, normalized) {
    Some(index) => {
      data.entries[index] = None
      true
    }
    None => false
  }
}

///|
fn map_get_or_insert(
  data : MapData,
  key : Value,
  value_or_callback : Value,
  computed : Bool,
) -> Value raise {
  if computed && !is_callable(value_or_callback) {
    return throw_type_error("not a function")
  }
  let normalized = map_normalize_key(key)
  match map_find_index(data.entries, normalized) {
    Some(index) =>
      match data.entries[index] {
        Some((_, value)) => value
        None => Undefined
      }
    None => {
      let value = if computed {
        let result = call_value_with_this(
          value_or_callback,
          [normalized],
          Undefined,
        )
        let _ = map_delete_key(data, normalized)
        result
      } else {
        value_or_callback
      }
      map_set_value(data, normalized, value)
      value
    }
  }
}

///|
fn map_entry_count(data : MapData) -> Int {
  let mut count = 0
  for entry in data.entries {
    match entry {
      Some(_) => count = count + 1
      None => ()
    }
  }
  count
}

///|
fn set_has_value(data : SetData, value : Value) -> Bool {
  let normalized = map_normalize_key(value)
  match set_find_index(data.entries, normalized) {
    Some(_) => true
    None => false
  }
}

///|
fn set_add_value(data : SetData, value : Value) -> Unit {
  let normalized = map_normalize_key(value)
  match set_find_index(data.entries, normalized) {
    Some(_) => ()
    None => data.entries.push(Some(normalized))
  }
}

///|
fn set_delete_value(data : SetData, value : Value) -> Bool {
  let normalized = map_normalize_key(value)
  match set_find_index(data.entries, normalized) {
    Some(index) => {
      data.entries[index] = None
      true
    }
    None => false
  }
}

///|
fn set_entry_count(data : SetData) -> Int {
  let mut count = 0
  for entry in data.entries {
    match entry {
      Some(_) => count = count + 1
      None => ()
    }
  }
  count
}
