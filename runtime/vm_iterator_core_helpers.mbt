///|
fn prop_is_builtin_iter(prop : Property, kind : BuiltinFunction) -> Bool {
  if prop.getter is None && prop.setter is None {
    match prop.value {
      Builtin(builtin) => builtin.kind == kind
      _ => false
    }
  } else {
    false
  }
}

///|
fn props_has_builtin_iter(
  props : Props,
  key : String,
  kind : BuiltinFunction,
) -> Bool? {
  match props_get(props, key) {
    Some(prop) => Some(prop_is_builtin_iter(prop, kind))
    None => None
  }
}

///|
fn proto_has_builtin_iter(
  proto : Value?,
  key : String,
  kind : BuiltinFunction,
) -> Bool {
  match proto {
    Some(Object(proto_obj)) =>
      match props_has_builtin_iter(proto_obj.props, key, kind) {
        Some(ok) => ok
        None => false
      }
    _ => false
  }
}

///|
fn proto_chain_has_builtin_iter(
  proto : Value?,
  key : String,
  kind : BuiltinFunction,
) -> Bool {
  match proto {
    Some(Object(proto_obj)) =>
      match props_has_builtin_iter(proto_obj.props, key, kind) {
        Some(ok) => ok
        None =>
          match proto_obj.proto {
            Some(_) => proto_has_builtin_iter(proto_obj.proto, key, kind)
            None => false
          }
      }
    _ => false
  }
}

///|
fn fast_iterator_for_value(value : Value) -> (Value, Value)? raise {
  let iter_key = match symbol_iterator_key() {
    Some(key) => key
    None => return None
  }
  match value {
    Array(arr) => {
      let mut own_ok : Bool? = None
      if arr.typed_array_data is Some(_) {
        match props_has_builtin_iter(
          arr.props,
          iter_key,
          BuiltinFunction::TypedArrayValues,
        ) {
          Some(ok) =>
            if ok {
              own_ok = Some(true)
            } else {
              return None
            }
          None => ()
        }
        if own_ok is None &&
          !proto_chain_has_builtin_iter(
            arr.proto,
            iter_key,
            BuiltinFunction::TypedArrayValues,
          ) {
          return None
        }
        let iterator = new_array_iterator(Array(arr), 1)
        let next_method = property_get(iterator, "next")
        return Some((iterator, next_method))
      }
      match props_has_builtin_iter(arr.props, iter_key, BuiltinFunction::ArrayValues) {
        Some(ok) =>
          if ok {
            own_ok = Some(true)
          } else {
            return None
          }
        None => ()
      }
      if own_ok is None &&
        !proto_chain_has_builtin_iter(
          arr.proto,
          iter_key,
          BuiltinFunction::ArrayValues,
        ) {
        return None
      }
      let iterator = new_array_iterator(Array(arr), 1)
      let next_method = property_get(iterator, "next")
      Some((iterator, next_method))
    }
    String(_) => {
      let proto = match current_env() {
        Some(env) => string_proto_for_env(env)
        None => string_proto()
      }
      match proto {
        Some(proto_obj) =>
          match props_has_builtin_iter(proto_obj.props, iter_key, BuiltinFunction::StringIterator) {
            Some(true) => {
              let text = to_string_strict(value)
              let iterator = new_string_iterator(String(text))
              let next_method = property_get(iterator, "next")
              Some((iterator, next_method))
            }
            _ => None
          }
        None => None
      }
    }
    Object(obj) =>
      match obj.string_data {
        Some(_) => {
          match props_has_builtin_iter(obj.props, iter_key, BuiltinFunction::StringIterator) {
            Some(true) => ()
            Some(false) => return None
            None =>
              if !proto_chain_has_builtin_iter(
                obj.proto,
                iter_key,
                BuiltinFunction::StringIterator,
              ) {
                return None
              }
          }
          let text = to_string_strict(value)
          let iterator = new_string_iterator(String(text))
          let next_method = property_get(iterator, "next")
          Some((iterator, next_method))
        }
        None => None
      }
    _ => None
  }
}

///|
fn get_iterator_from_value(value : Value) -> (Value, Value) raise {
  match fast_iterator_for_value(value) {
    Some(result) => return result
    None => ()
  }
  let obj = to_object(value)
  let iter_method = match symbol_iterator_key() {
    Some(key) => property_get(obj, key)
    None => Undefined
  }
  match iter_method {
    Undefined | Null => {
      let _ = throw_type_error("value is not iterable")
      return (Undefined, Undefined)
    }
    _ =>
      if !is_callable(iter_method) {
        let _ = throw_type_error("value is not iterable")
        return (Undefined, Undefined)
      }
  }
  let iterator = call_value_with_this(iter_method, [], value)
  if !is_object_like(iterator) {
    let _ = throw_type_error("not an object")
    return (Undefined, Undefined)
  }
  let next_method = property_get(iterator, "next")
  (iterator, next_method)
}

///|
fn get_iterator_flattenable(value : Value) -> (Value, Value) raise {
  if !is_object_like(value) {
    let _ = throw_type_error("not an object")
    return (Undefined, Undefined)
  }
  let iter_method = match symbol_iterator_key() {
    Some(key) => property_get(value, key)
    None => Undefined
  }
  match iter_method {
    Undefined | Null => {
      let next_method = property_get(value, "next")
      (value, next_method)
    }
    _ => {
      if !is_callable(iter_method) {
        let _ = throw_type_error("value is not iterable")
        return (Undefined, Undefined)
      }
      let iterator = call_value_with_this(iter_method, [], value)
      if !is_object_like(iterator) {
        let _ = throw_type_error("not an object")
        return (Undefined, Undefined)
      }
      let next_method = property_get(iterator, "next")
      if !is_callable(next_method) {
        let _ = throw_type_error("not a function")
        return (Undefined, Undefined)
      }
      (iterator, next_method)
    }
  }
}

///|
fn iterator_step_value(
  iterator : Value,
  next_method : Value,
) -> (Bool, Value) raise {
  let result = call_value_with_this(next_method, [], iterator)
  if !is_object_like(result) {
    let _ = throw_type_error("iterator must return an object")
    return (true, Undefined)
  }
  let done = is_truthy(property_get(result, "done"))
  if done {
    return (true, Undefined)
  }
  let value = property_get(result, "value")
  (false, value)
}

///|
fn iterator_close(iterator : Value) -> Unit raise {
  let return_method = property_get(iterator, "return")
  match return_method {
    Undefined | Null => ()
    _ => {
      if !is_callable(return_method) {
        let _ = throw_type_error("not a function")
        return
      }
      let result = call_value_with_this(return_method, [], iterator)
      if !is_object_like(result) {
        let _ = throw_type_error("iterator must return an object")

      }
    }
  }
}

///|
fn iterator_close_on_error(iterator : Value) -> Unit {
  iterator_close(iterator) catch {
    _ => ()
  }
}

///|
fn get_async_iterator_from_value(value : Value) -> (Value, Value) raise {
  let obj = to_object(value)
  let async_method = match symbol_async_iterator_key() {
    Some(key) => property_get(obj, key)
    None => Undefined
  }
  match async_method {
    Undefined | Null => {
      let (sync_iter, sync_next) = get_iterator_from_value(value)
      let iterator = new_async_from_sync_iterator(sync_iter, sync_next)
      let next_method = property_get(iterator, "next")
      (iterator, next_method)
    }
    _ => {
      if !is_callable(async_method) {
        let _ = throw_type_error("not a function")
        return (Undefined, Undefined)
      }
      let iterator = call_value_with_this(async_method, [], value)
      if !is_object_like(iterator) {
        let _ = throw_type_error("not an object")
        return (Undefined, Undefined)
      }
      let next_method = property_get(iterator, "next")
      (iterator, next_method)
    }
  }
}

///|
fn async_iterator_step_value(
  iterator : Value,
  next_method : Value,
) -> (Bool, Value) raise {
  let result = call_value_with_this(next_method, [], iterator)
  let awaited = await_value(result)
  if !is_object_like(awaited) {
    let _ = throw_type_error("iterator must return an object")
    return (true, Undefined)
  }
  let done = is_truthy(property_get(awaited, "done"))
  if done {
    return (true, Undefined)
  }
  let value = property_get(awaited, "value")
  (false, value)
}

///|
fn async_iterator_close(iterator : Value) -> Unit raise {
  let return_method = property_get(iterator, "return")
  match return_method {
    Undefined | Null => ()
    _ => {
      if !is_callable(return_method) {
        let _ = throw_type_error("not a function")
        return
      }
      let result = call_value_with_this(return_method, [], iterator)
      let awaited = await_value(result)
      if !is_object_like(awaited) {
        let _ = throw_type_error("iterator must return an object")

      }
    }
  }
}

///|
fn async_iterator_close_on_error(iterator : Value) -> Unit {
  async_iterator_close(iterator) catch {
    _ => ()
  }
}
