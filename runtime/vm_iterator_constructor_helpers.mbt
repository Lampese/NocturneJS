///|
fn iterator_constructor_value() -> Value raise {
  match current_env() {
    Some(env) =>
      match global_object(env) {
        Some(obj) => property_get(Object(obj), "Iterator")
        None => Undefined
      }
    None =>
      match root_env() {
        Some(env) =>
          match global_object(env) {
            Some(obj) => property_get(Object(obj), "Iterator")
            None => Undefined
          }
        None => Undefined
      }
  }
}

///|
fn iterator_from_value(value : Value) -> Value raise {
  let obj = match value {
    String(_) => value
    _ =>
      if is_object_like(value) {
        value
      } else {
        return throw_type_error("Iterator.from called on non-object")
      }
  }
  let iter_method = match symbol_iterator_key() {
    Some(key) => property_get(obj, key)
    None => Undefined
  }
  let iterator = match iter_method {
    Undefined | Null => obj
    _ => {
      if !is_callable(iter_method) {
        return throw_type_error("not a function")
      }
      call_value_with_this(iter_method, [], obj)
    }
  }
  if !is_object_like(iterator) {
    return throw_type_error("not an object")
  }
  let next_method = property_get(iterator, "next")
  let iterator_ctor = iterator_constructor_value()
  if is_object_like(iterator_ctor) &&
    ordinary_instanceof(iterator, iterator_ctor) {
    return iterator
  }
  let wrapper = new_object_value_with_proto(
    value_from_object(iterator_wrap_proto()),
  )
  match wrapper {
    Object(obj) =>
      obj.iterator_wrap_data = Some(IteratorWrapData::{
        wrapped_iter: iterator,
        wrapped_next: next_method,
      })
    _ => ()
  }
  wrapper
}

///|
fn iterator_concat_value(args : Array[Value]) -> Value raise {
  let values : Array[(Value, Value)] = []
  for value in args {
    if !is_object_like(value) {
      return throw_type_error("not an object")
    }
    let iter_method = match symbol_iterator_key() {
      Some(key) => property_get(value, key)
      None => Undefined
    }
    match iter_method {
      Undefined | Null => return throw_type_error("not iterable")
      _ =>
        if !is_callable(iter_method) {
          return throw_type_error("not a function")
        }
    }
    values.push((value, iter_method))
  }
  let iterator = new_object_value_with_proto(
    value_from_object(iterator_concat_proto()),
  )
  match iterator {
    Object(obj) =>
      obj.iterator_concat_data = Some(IteratorConcatData::{
        index: 0,
        count: values.length(),
        running: false,
        iter: Undefined,
        next: Undefined,
        values,
      })
    _ => ()
  }
  iterator
}
