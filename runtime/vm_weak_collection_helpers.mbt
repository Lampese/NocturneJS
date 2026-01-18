///|
fn weakmap_find_index(entries : Array[(Value, Value)], key : Value) -> Int? {
  let mut index = 0
  for entry in entries {
    let (k, _) = entry
    if same_value_zero(k, key) {
      return Some(index)
    }
    index = index + 1
  }
  None
}

///|
fn weakmap_get_value(data : WeakMapData, key : Value) -> Value {
  match weakmap_find_index(data.entries, key) {
    Some(index) => {
      let (_, value) = data.entries[index]
      value
    }
    None => Undefined
  }
}

///|
fn weakmap_has_key(data : WeakMapData, key : Value) -> Bool {
  match weakmap_find_index(data.entries, key) {
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
  match weakmap_find_index(data.entries, key) {
    Some(index) => data.entries[index] = (key, value)
    None => data.entries.push((key, value))
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
  match weakmap_find_index(data.entries, key) {
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
  match weakmap_find_index(data.entries, key) {
    Some(index) => {
      let _ = data.entries.remove(index)
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
