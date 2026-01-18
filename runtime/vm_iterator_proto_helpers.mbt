///|
fn iterator_helper_create(
  kind : Int,
  this_value : Value?,
  args : Array[Value],
) -> Value raise {
  let target = match this_value {
    Some(value) => value
    None => Undefined
  }
  if !is_object_like(target) {
    return throw_type_error("not an object")
  }
  let mut func = Undefined
  let mut count = 0L
  match kind {
    _ if kind == iterator_helper_kind_drop || kind == iterator_helper_kind_take => {
      let limit_value = if args.is_empty() { Undefined } else { args[0] }
      let num = to_number(limit_value) catch {
        err => {
          iterator_close_on_error(target)
          raise err
        }
      }
      if Double::is_nan(num) {
        iterator_close_on_error(target)
        return throw_range_error("must be positive")
      }
      let integer = if Double::is_inf(num) { num } else { Double::trunc(num) }
      if integer < 0.0 {
        iterator_close_on_error(target)
        return throw_range_error("must be positive")
      }
      count = to_length_int64(integer)
    }
    _ => {
      func = if args.is_empty() { Undefined } else { args[0] }
      if !is_callable(func) {
        iterator_close_on_error(target)
        return throw_type_error("not a function")
      }
    }
  }
  let next_method = property_get(target, "next")
  let iterator = new_object_value_with_proto(
    value_from_object(iterator_helper_proto()),
  )
  match iterator {
    Object(obj) =>
      obj.iterator_helper_data = Some(IteratorHelperData::{
        obj: target,
        next: next_method,
        func,
        inner: Undefined,
        inner_next: Undefined,
        count,
        kind,
        executing: false,
        done: false,
      })
    _ => ()
  }
  iterator
}

///|
fn iterator_helper_next(this_value : Value?) -> Value raise {
  let (_, data) = require_iterator_helper(this_value)
  if data.executing {
    return throw_type_error("cannot invoke a running iterator")
  }
  if data.done {
    return iterator_result(Undefined, true)
  }
  data.executing = true
  if !is_callable(data.next) {
    data.executing = false
    return throw_type_error("not a function")
  }
  try {
    match data.kind {
      _ if data.kind == iterator_helper_kind_drop => {
        while data.count > 0L {
          let (done, _) = iterator_step_value(data.obj, data.next)
          if done {
            data.done = true
            data.executing = false
            return iterator_result(Undefined, true)
          }
          data.count = data.count - 1L
        }
        let (done, value) = iterator_step_value(data.obj, data.next)
        if done {
          data.done = true
          data.executing = false
          return iterator_result(Undefined, true)
        }
        data.executing = false
        return iterator_result(value, false)
      }
      _ if data.kind == iterator_helper_kind_take => {
        if data.count <= 0L {
          data.done = true
          iterator_close(data.obj)
          data.executing = false
          return iterator_result(Undefined, true)
        }
        let (done, value) = iterator_step_value(data.obj, data.next)
        if done {
          data.done = true
          data.executing = false
          return iterator_result(Undefined, true)
        }
        data.count = data.count - 1L
        data.executing = false
        return iterator_result(value, false)
      }
      _ if data.kind == iterator_helper_kind_filter => {
        for {
          let (done, value) = iterator_step_value(data.obj, data.next)
          if done {
            data.done = true
            data.executing = false
            return iterator_result(Undefined, true)
          }
          let index = data.count
          data.count = data.count + 1L
          let selected = call_value_with_this(
            data.func,
            [value, Number(Int64::to_double(index))],
            Undefined,
          ) catch {
            err => {
              iterator_close_on_error(data.obj)
              raise err
            }
          }
          if is_truthy(selected) {
            data.executing = false
            return iterator_result(value, false)
          }
        }
        data.executing = false
        iterator_result(Undefined, true)
      }
      _ if data.kind == iterator_helper_kind_map => {
        let (done, value) = iterator_step_value(data.obj, data.next)
        if done {
          data.done = true
          data.executing = false
          return iterator_result(Undefined, true)
        }
        let index = data.count
        data.count = data.count + 1L
        let mapped = call_value_with_this(
          data.func,
          [value, Number(Int64::to_double(index))],
          Undefined,
        ) catch {
          err => {
            iterator_close_on_error(data.obj)
            raise err
          }
        }
        data.executing = false
        return iterator_result(mapped, false)
      }
      _ if data.kind == iterator_helper_kind_flat_map => {
        for {
          if !(data.inner is Undefined) {
            let (done, value) = iterator_step_value(data.inner, data.inner_next) catch {
              err => {
                iterator_close_on_error(data.obj)
                raise err
              }
            }
            if done {
              data.inner = Undefined
              data.inner_next = Undefined
              continue
            }
            data.executing = false
            return iterator_result(value, false)
          }
          let (done, value) = iterator_step_value(data.obj, data.next)
          if done {
            data.done = true
            data.executing = false
            return iterator_result(Undefined, true)
          }
          let index = data.count
          data.count = data.count + 1L
          let mapped = call_value_with_this(
            data.func,
            [value, Number(Int64::to_double(index))],
            Undefined,
          ) catch {
            err => {
              iterator_close_on_error(data.obj)
              raise err
            }
          }
          let (inner_iter, inner_next) = get_iterator_flattenable(mapped) catch {
            err => {
              iterator_close_on_error(data.obj)
              raise err
            }
          }
          data.inner = inner_iter
          data.inner_next = inner_next
        }
        data.executing = false
        iterator_result(Undefined, true)
      }
      _ => {
        data.executing = false
        iterator_result(Undefined, true)
      }
    }
  } catch {
    err => {
      data.executing = false
      raise err
    }
  }
}

///|
fn iterator_helper_return(this_value : Value?) -> Value raise {
  let (_, data) = require_iterator_helper(this_value)
  if data.executing {
    return throw_type_error("cannot invoke a running iterator")
  }
  data.executing = true
  try {
    if data.done {
      data.executing = false
      return iterator_result(Undefined, true)
    }
    data.done = true
    let inner = data.inner
    data.inner = Undefined
    data.inner_next = Undefined
    if is_object_like(inner) {
      iterator_close(inner)
    }
    iterator_close(data.obj)
    data.executing = false
    iterator_result(Undefined, true)
  } catch {
    err => {
      data.executing = false
      raise err
    }
  }
}

///|
fn iterator_proto_iterate(
  kind : Int,
  this_value : Value?,
  args : Array[Value],
) -> Value raise {
  let iter = match this_value {
    Some(value) => value
    None => Undefined
  }
  if !is_object_like(iter) {
    return throw_type_error("not an object")
  }
  let callback = if args.is_empty() { Undefined } else { args[0] }
  if !is_callable(callback) {
    iterator_close_on_error(iter)
    return throw_type_error("not a function")
  }
  let next_method = property_get(iter, "next")
  if !is_callable(next_method) {
    return throw_type_error("not a function")
  }
  let mut index = 0L
  for {
    let (done, value) = iterator_step_value(iter, next_method)
    if done {
      return match kind {
        _ if kind == iterator_proto_kind_every => Bool(true)
        _ if kind == iterator_proto_kind_some => Bool(false)
        _ if kind == iterator_proto_kind_find => Undefined
        _ => Undefined
      }
    }
    let result = call_value_with_this(
      callback,
      [value, Number(Int64::to_double(index))],
      Undefined,
    ) catch {
      err => {
        iterator_close_on_error(iter)
        raise err
      }
    }
    index = index + 1L
    match kind {
      _ if kind == iterator_proto_kind_every =>
        if !is_truthy(result) {
          iterator_close(iter)
          return Bool(false)
        }
      _ if kind == iterator_proto_kind_some =>
        if is_truthy(result) {
          iterator_close(iter)
          return Bool(true)
        }
      _ if kind == iterator_proto_kind_find =>
        if is_truthy(result) {
          iterator_close(iter)
          return value
        }
      _ => ()
    }
  }
  Undefined
}

///|
fn iterator_proto_reduce(
  this_value : Value?,
  args : Array[Value],
) -> Value raise {
  let iter = match this_value {
    Some(value) => value
    None => Undefined
  }
  if !is_object_like(iter) {
    return throw_type_error("not an object")
  }
  let reducer = if args.is_empty() { Undefined } else { args[0] }
  if !is_callable(reducer) {
    iterator_close_on_error(iter)
    return throw_type_error("not a function")
  }
  let next_method = property_get(iter, "next")
  if !is_callable(next_method) {
    return throw_type_error("not a function")
  }
  let mut index = 0L
  let mut accumulator = Undefined
  if args.length() > 1 {
    accumulator = args[1]
  } else {
    let (done, value) = iterator_step_value(iter, next_method)
    if done {
      return throw_type_error("empty iterator")
    }
    accumulator = value
    index = 1L
  }
  for {
    let (done, value) = iterator_step_value(iter, next_method)
    if done {
      return accumulator
    }
    let result = call_value_with_this(
      reducer,
      [accumulator, value, Number(Int64::to_double(index))],
      Undefined,
    ) catch {
      err => {
        iterator_close_on_error(iter)
        raise err
      }
    }
    accumulator = result
    index = index + 1L
  }
  accumulator
}

///|
fn iterator_proto_to_array(this_value : Value?) -> Value raise {
  let iter = match this_value {
    Some(value) => value
    None => Undefined
  }
  if !is_object_like(iter) {
    return throw_type_error("not an object")
  }
  let next_method = property_get(iter, "next")
  if !is_callable(next_method) {
    return throw_type_error("not a function")
  }
  let array_proto_value = match current_env() {
    Some(env) =>
      match value_from_object(array_proto_for_env(env)) {
        Some(value) => Some(value)
        None => value_from_object(array_proto())
      }
    None => value_from_object(array_proto())
  }
  let values : Array[Value?] = []
  for {
    let (done, value) = iterator_step_value(iter, next_method)
    if done {
      return new_array_value_with_proto(values, proto=array_proto_value)
    }
    values.push(Some(value))
  }
  new_array_value_with_proto(values, proto=array_proto_value)
}

///|
fn iterator_constructor_getter(this_value : Value?) -> Value raise {
  let _ = this_value
  iterator_constructor_value()
}

///|
fn iterator_constructor_setter(
  this_value : Value?,
  value : Value,
) -> Value raise {
  let target = match this_value {
    Some(value) => to_object(value)
    None => {
      let _ = throw_type_error("not an object")
      return Undefined
    }
  }
  if !is_object_like(value) {
    return throw_type_error("not an object")
  }
  let desc = new_object_value()
  match desc {
    Object(obj) => {
      obj.props.set("value", property_data(value))
      obj.props.set("writable", property_data(Bool(true)))
      obj.props.set("enumerable", property_data(Bool(false)))
      obj.props.set("configurable", property_data(Bool(true)))
    }
    _ => ()
  }
  let _ = define_property_value(target, String("constructor"), desc, true)
  value
}

///|
fn iterator_to_string_tag_getter(this_value : Value?) -> Value {
  let _ = this_value
  String("Iterator")
}

///|
fn iterator_to_string_tag_setter(
  this_value : Value?,
  value : Value,
) -> Value raise {
  let target = match this_value {
    Some(value) => value
    None => Undefined
  }
  if !is_object_like(target) {
    return throw_type_error("not an object")
  }
  match iterator_proto() {
    Some(proto) =>
      match target {
        Object(obj) =>
          if obj.id == proto.id {
            return throw_type_error("invalid receiver")
          }
        _ => ()
      }
    None => ()
  }
  match symbol_to_string_tag_key() {
    Some(key) => {
      let desc = get_own_property_descriptor(target, key)
      match desc {
        Undefined => {
          let _ = create_data_property_or_throw(target, key, value)

        }
        _ => {
          let _ = property_set(target, key, value, true)

        }
      }
    }
    None => ()
  }
  Undefined
}
