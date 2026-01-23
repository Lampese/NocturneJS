///|
fn iterator_result(value : Value, done : Bool) -> Value {
  let obj = new_object_value()
  match obj {
    Object(result) => {
      props_set(result.props, "value", property_data(value))
      props_set(result.props, "done", property_data(Bool(done)))
    }
    _ => ()
  }
  obj
}

///|
let iterator_helper_kind_drop = 0

///|
let iterator_helper_kind_filter = 1

///|
let iterator_helper_kind_flat_map = 2

///|
let iterator_helper_kind_map = 3

///|
let iterator_helper_kind_take = 4

///|
let iterator_proto_kind_every = 0

///|
let iterator_proto_kind_find = 1

///|
let iterator_proto_kind_for_each = 2

///|
let iterator_proto_kind_some = 3

///|
let async_from_sync_iter_kind_next = 0

///|
let async_from_sync_iter_kind_return = 1

///|
let async_from_sync_iter_kind_throw = 2

///|
fn iterator_return_self(this_value : Value?) -> Value {
  match this_value {
    Some(value) => value
    None => Undefined
  }
}

///|
fn iterator_wrap_next(this_value : Value?) -> Value raise {
  let (_, data) = require_iterator_wrap(this_value)
  let result = call_value_with_this(data.wrapped_next, [], data.wrapped_iter)
  if !is_object_like(result) {
    let _ = throw_type_error("iterator must return an object")
    return iterator_result(Undefined, true)
  }
  result
}

///|
fn iterator_wrap_return(this_value : Value?) -> Value raise {
  let (_, data) = require_iterator_wrap(this_value)
  let iter = data.wrapped_iter
  let return_method = property_get(iter, "return")
  match return_method {
    Undefined | Null => iterator_result(Undefined, true)
    _ => {
      if !is_callable(return_method) {
        return throw_type_error("not a function")
      }
      let result = call_value_with_this(return_method, [], iter)
      if !is_object_like(result) {
        return throw_type_error("iterator must return an object")
      }
      result
    }
  }
}

///|
fn iterator_concat_next(this_value : Value?) -> Value raise {
  let (_, data) = require_iterator_concat(this_value)
  if data.running {
    return throw_type_error("cannot invoke a running iterator")
  }
  data.running = true
  try {
    for {
      if data.index >= data.count {
        data.running = false
        return iterator_result(Undefined, true)
      }
      if data.next is Undefined {
        let (iterable, iter_method) = data.values[data.index]
        let iter = call_value_with_this(iter_method, [], iterable)
        if !is_object_like(iter) {
          let _ = throw_type_error("not an object")
          data.running = false
          return iterator_result(Undefined, true)
        }
        let next_method = property_get(iter, "next")
        if !is_callable(next_method) {
          let _ = throw_type_error("not a function")
          data.running = false
          return iterator_result(Undefined, true)
        }
        rc_replace_value(data.iter, iter)
        data.iter = iter
        rc_replace_value(data.next, next_method)
        data.next = next_method
      }
      let (done, value) = iterator_step_value(data.iter, data.next)
      if done {
        data.index = data.index + 1
        rc_replace_value(data.iter, Undefined)
        data.iter = Undefined
        rc_replace_value(data.next, Undefined)
        data.next = Undefined
        continue
      }
      data.running = false
      return iterator_result(value, false)
    }
    data.running = false
    iterator_result(Undefined, true)
  } catch {
    err => {
      data.running = false
      raise err
    }
  }
}

///|
fn iterator_concat_return(this_value : Value?) -> Value raise {
  let (_, data) = require_iterator_concat(this_value)
  if data.running {
    return throw_type_error("cannot invoke a running iterator")
  }
  data.running = true
  try {
    let iter = data.iter
    data.index = data.count
    rc_replace_value(data.iter, Undefined)
    data.iter = Undefined
    rc_replace_value(data.next, Undefined)
    data.next = Undefined
    if iter is Undefined {
      data.running = false
      return iterator_result(Undefined, true)
    }
    let return_method = property_get(iter, "return")
    match return_method {
      Undefined | Null => {
        data.running = false
        iterator_result(Undefined, true)
      }
      _ => {
        if !is_callable(return_method) {
          data.running = false
          return throw_type_error("not a function")
        }
        let result = call_value_with_this(return_method, [], iter)
        if !is_object_like(result) {
          data.running = false
          return throw_type_error("iterator must return an object")
        }
        data.running = false
        result
      }
    }
  } catch {
    err => {
      data.running = false
      raise err
    }
  }
}
