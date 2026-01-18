///|
fn init_map_or_set_from_iterable(
  obj : Value,
  iterable : Value,
  is_set : Bool,
) -> Unit raise {
  if is_nullish(iterable) {
    return
  }
  let adder_name = if is_set { "add" } else { "set" }
  let adder = property_get(obj, adder_name)
  if !is_callable(adder) {
    let _ = throw_type_error("set/add is not a function")
    return
  }
  let (iterator, next_method) = get_iterator_from_value(iterable)
  while true {
    let (done, item) = iterator_step_value(iterator, next_method)
    if done {
      break
    }
    try {
      if is_set {
        let _ = call_value_with_this(adder, [item], obj)

      } else {
        if !is_object_like(item) {
          let _ = throw_type_error("not an object")

        }
        let key = property_get(item, "0")
        let value = property_get(item, "1")
        let _ = call_value_with_this(adder, [key, value], obj)

      }
    } catch {
      err => {
        iterator_close_on_error(iterator)
        raise err
      }
    }
  }
}

///|
fn get_set_record(obj : Value) -> (Int64, Value, Value) raise {
  let obj_value = to_object(obj)
  let mut size_value : Int64? = None
  match obj_value {
    Object(set_obj) =>
      match set_obj.set_data {
        Some(data) => size_value = Some(Int64::from_int(set_entry_count(data)))
        None => ()
      }
    _ => ()
  }
  let size = match size_value {
    Some(value) => value
    None => {
      let size_prop = property_get(obj_value, "size")
      let size_prim = if is_object_like(size_prop) {
        to_primitive(size_prop, false)
      } else {
        size_prop
      }
      let size_num = match size_prim {
        BigInt(_) => {
          let _ = throw_type_error("cannot convert a BigInt value to a number")
          0.0
        }
        _ => to_number(size_prim)
      }
      if Double::is_nan(size_num) {
        let _ = throw_type_error(".size is not a number")

      }
      if size_num < 0.0 {
        let _ = throw_range_error(".size must be positive")

      }
      let max_double = 9.223372036854776e18
      let max_value : Int64 = 9223372036854775807
      if size_num >= max_double {
        max_value
      } else {
        Double::to_int64(Double::trunc(size_num))
      }
    }
  }
  let has = property_get(obj_value, "has")
  match has {
    Undefined => {
      let _ = throw_type_error(".has is undefined")
      return (0L, Undefined, Undefined)
    }
    _ => ()
  }
  if !is_callable(has) {
    let _ = throw_type_error(".has is not a function")
    return (0L, Undefined, Undefined)
  }
  let keys = property_get(obj_value, "keys")
  match keys {
    Undefined => {
      let _ = throw_type_error(".keys is undefined")
      return (0L, Undefined, Undefined)
    }
    _ => ()
  }
  if !is_callable(keys) {
    let _ = throw_type_error(".keys is not a function")
    return (0L, Undefined, Undefined)
  }
  (size, has, keys)
}

///|
fn iterator_from_keys(obj : Value, keys : Value) -> (Value, Value) raise {
  let iterator = call_value_with_this(keys, [], obj)
  let next_method = property_get(iterator, "next")
  if !is_callable(next_method) {
    let _ = throw_type_error("not a function")

  }
  (iterator, next_method)
}

///|
fn set_copy_value(this_value : Value?) -> Value raise {
  let (_, data) = require_set(this_value)
  let new_value = new_set_value()
  match new_value {
    Object(obj) =>
      match obj.set_data {
        Some(new_data) =>
          for entry in data.entries {
            match entry {
              Some(value) => new_data.entries.push(Some(value))
              None => ()
            }
          }
        None => ()
      }
    _ => ()
  }
  new_value
}

///|
fn set_is_disjoint_from(this_value : Value?, other : Value) -> Value raise {
  let (_, data) = require_set(this_value)
  let (size, has, keys) = get_set_record(other)
  let this_size = Int64::from_int(set_entry_count(data))
  let mut found = false
  if this_size <= size {
    let mut index = 0
    while index < data.entries.length() {
      match data.entries[index] {
        Some(value) => {
          let result = call_value_with_this(has, [value], other)
          if is_truthy(result) {
            found = true
            break
          }
        }
        None => ()
      }
      index = index + 1
    }
  } else {
    let (iterator, next_method) = iterator_from_keys(other, keys)
    let mut should_close = false
    while true {
      let (done, item) = iterator_step_value(iterator, next_method)
      if done {
        break
      }
      if set_has_value(data, item) {
        found = true
        should_close = true
        break
      }
    }
    if should_close {
      iterator_close(iterator)
    }
  }
  Bool(!found)
}

///|
fn set_is_subset_of(this_value : Value?, other : Value) -> Value raise {
  let (_, data) = require_set(this_value)
  let (size, has, _) = get_set_record(other)
  let this_size = Int64::from_int(set_entry_count(data))
  if this_size > size {
    return Bool(false)
  }
  let mut ok = true
  let mut index = 0
  while index < data.entries.length() {
    match data.entries[index] {
      Some(value) => {
        let result = call_value_with_this(has, [value], other)
        if !is_truthy(result) {
          ok = false
          break
        }
      }
      None => ()
    }
    index = index + 1
  }
  Bool(ok)
}

///|
fn set_is_superset_of(this_value : Value?, other : Value) -> Value raise {
  let (_, data) = require_set(this_value)
  let (size, _, keys) = get_set_record(other)
  let this_size = Int64::from_int(set_entry_count(data))
  if this_size < size {
    return Bool(false)
  }
  let (iterator, next_method) = iterator_from_keys(other, keys)
  let mut ok = true
  let mut should_close = false
  while true {
    let (done, item) = iterator_step_value(iterator, next_method)
    if done {
      break
    }
    if !set_has_value(data, item) {
      ok = false
      should_close = true
      break
    }
  }
  if should_close {
    iterator_close(iterator)
  }
  Bool(ok)
}

///|
fn set_intersection(this_value : Value?, other : Value) -> Value raise {
  let (_, data) = require_set(this_value)
  let (size, has, keys) = get_set_record(other)
  let this_size = Int64::from_int(set_entry_count(data))
  let new_value = new_set_value()
  let (_, new_data) = require_set(Some(new_value))
  if this_size > size {
    let (iterator, next_method) = iterator_from_keys(other, keys)
    while true {
      let (done, item) = iterator_step_value(iterator, next_method)
      if done {
        break
      }
      if set_has_value(data, item) {
        set_add_value(new_data, item)
      }
    }
  } else {
    let mut index = 0
    while index < data.entries.length() {
      match data.entries[index] {
        Some(value) => {
          let result = call_value_with_this(has, [value], other)
          if is_truthy(result) {
            set_add_value(new_data, value)
          }
        }
        None => ()
      }
      index = index + 1
    }
  }
  new_value
}

///|
fn set_difference(this_value : Value?, other : Value) -> Value raise {
  let (_, data) = require_set(this_value)
  let (size, has, keys) = get_set_record(other)
  let this_size = Int64::from_int(set_entry_count(data))
  let new_value = set_copy_value(this_value)
  let (_, new_data) = require_set(Some(new_value))
  if this_size <= size {
    let mut index = 0
    while index < new_data.entries.length() {
      match new_data.entries[index] {
        Some(value) => {
          let result = call_value_with_this(has, [value], other)
          if is_truthy(result) {
            let _ = set_delete_value(new_data, value)

          }
        }
        None => ()
      }
      index = index + 1
    }
  } else {
    let (iterator, next_method) = iterator_from_keys(other, keys)
    while true {
      let (done, item) = iterator_step_value(iterator, next_method)
      if done {
        break
      }
      let _ = set_delete_value(new_data, item)

    }
  }
  new_value
}

///|
fn set_symmetric_difference(this_value : Value?, other : Value) -> Value raise {
  let (_, data) = require_set(this_value)
  let (_, _, keys) = get_set_record(other)
  let (iterator, next_method) = iterator_from_keys(other, keys)
  let new_value = set_copy_value(this_value)
  let (_, new_data) = require_set(Some(new_value))
  while true {
    let (done, item) = iterator_step_value(iterator, next_method)
    if done {
      break
    }
    let present_in_this = set_has_value(data, item)
    let present_in_new = set_has_value(new_data, item)
    if present_in_this {
      let _ = set_delete_value(new_data, item)

    } else if present_in_new {
      ()
    } else {
      set_add_value(new_data, item)
    }
  }
  new_value
}

///|
fn set_union(this_value : Value?, other : Value) -> Value raise {
  let (_, _, keys) = get_set_record(other)
  let (iterator, next_method) = iterator_from_keys(other, keys)
  let new_value = set_copy_value(this_value)
  let (_, new_data) = require_set(Some(new_value))
  while true {
    let (done, item) = iterator_step_value(iterator, next_method)
    if done {
      break
    }
    set_add_value(new_data, item)
  }
  new_value
}
