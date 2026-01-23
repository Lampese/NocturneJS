///|
fn new_async_from_sync_iterator(
  sync_iter : Value,
  next_method : Value,
) -> Value {
  let iterator = new_object_value_with_proto(
    value_from_object(async_from_sync_iterator_proto()),
  )
  match iterator {
    Object(iter_obj) => {
      props_set(iter_obj.props, 
        async_from_sync_iter_target_key,
        property_data_non_enum(sync_iter),
      )
      props_set(iter_obj.props, 
        async_from_sync_iter_next_key,
        property_data_non_enum(next_method),
      )
    }
    _ => ()
  }
  iterator
}

///|
fn require_async_from_sync_iterator(
  this_value : Value?,
) -> (ObjectValue, Value, Value) raise {
  match this_value {
    Some(Object(obj)) => {
      let (sync_iter, ok) = iterator_internal_value(
        obj, async_from_sync_iter_target_key, "not an Async-from-Sync Iterator",
      )
      if !ok {
        return (obj, Undefined, Undefined)
      }
      let (next_method, ok) = iterator_internal_value(
        obj, async_from_sync_iter_next_key, "not an Async-from-Sync Iterator",
      )
      if !ok {
        return (obj, Undefined, Undefined)
      }
      (obj, sync_iter, next_method)
    }
    _ => {
      let _ = throw_type_error("not an Async-from-Sync Iterator")
      (new_object_struct(None), Undefined, Undefined)
    }
  }
}

///|
fn new_async_from_sync_iterator_unwrap(done : Bool) -> Value {
  let func = new_builtin_value(BuiltinFunction::AsyncFromSyncIteratorUnwrap)
  match func {
    Builtin(builtin) =>
      async_from_sync_unwrap_data_set(builtin.id, AsyncFromSyncIteratorUnwrapData::{
        done,
      })
    _ => ()
  }
  func
}

///|
fn new_async_from_sync_iterator_close_wrap(sync_iter : Value) -> Value {
  let func = new_builtin_value(BuiltinFunction::AsyncFromSyncIteratorCloseWrap)
  match func {
    Builtin(builtin) =>
      async_from_sync_close_wrap_data_set(builtin.id, AsyncFromSyncIteratorCloseWrapData::{
        sync_iter,
      })
    _ => ()
  }
  func
}

///|
fn async_from_sync_iterator_unwrap(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value {
  match async_from_sync_unwrap_data_get(builtin.id) {
    Some(data) => {
      let value = if args.is_empty() { Undefined } else { args[0] }
      iterator_result(value, data.done)
    }
    None => Undefined
  }
}

///|
fn async_from_sync_iterator_close_wrap(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match async_from_sync_close_wrap_data_get(builtin.id) {
    Some(data) => {
      let reason = if args.is_empty() { Undefined } else { args[0] }
      iterator_close_on_error(data.sync_iter)
      raise ThrowSignal(reason)
    }
    None => throw_type_error("not an Async-from-Sync Iterator")
  }
}

///|
fn async_from_sync_iterator_step(
  this_value : Value?,
  args : Array[Value],
  kind : Int,
) -> Value raise {
  let (promise, resolve, reject) = new_promise_capability(Undefined)
  try {
    let (_, sync_iter, next_method) = require_async_from_sync_iterator(
      this_value,
    )
    let arg = if args.is_empty() { Undefined } else { args[0] }
    let mut method_value = Undefined
    if kind == async_from_sync_iter_kind_next {
      method_value = next_method
    } else if kind == async_from_sync_iter_kind_return {
      method_value = property_get(sync_iter, "return")
      if method_value is Undefined || method_value is Null {
        let result = iterator_result(arg, true)
        let _ = call_value_with_this(resolve, [result], Undefined)
        return promise
      }
    } else {
      method_value = property_get(sync_iter, "throw")
      if method_value is Undefined || method_value is Null {
        iterator_close(sync_iter)
        let err = type_error_value("throw is not a method")
        let _ = call_value_with_this(reject, [err], Undefined)
        return promise
      }
    }
    if !is_callable(method_value) {
      let _ = throw_type_error("not a function")
      return promise
    }
    let argv = if args.is_empty() { [] } else { [arg] }
    let result = call_value_with_this(method_value, argv, sync_iter)
    if !is_object_like(result) {
      let _ = throw_type_error("iterator must return an object")
      return promise
    }
    let done = is_truthy(property_get(result, "done"))
    let value = property_get(result, "value")
    let promise_ctor = promise_ctor_value_for_current_env()
    let value_promise = promise_resolve_value(promise_ctor, value, false) catch {
      ThrowSignal(err) => {
        if !done && kind != async_from_sync_iter_kind_return {
          iterator_close_on_error(sync_iter)
        }
        let _ = call_value_with_this(reject, [err], Undefined)
        return promise
      }
      err => raise err
    }
    match promise_data_from_value(value_promise) {
      Some(data) => {
        let on_fulfilled = new_async_from_sync_iterator_unwrap(done)
        let on_rejected = if done || kind == async_from_sync_iter_kind_return {
          Undefined
        } else {
          new_async_from_sync_iterator_close_wrap(sync_iter)
        }
        perform_promise_then(
          value_promise, data, on_fulfilled, on_rejected, resolve, reject,
        )
      }
      None => {
        let _ = throw_type_error("not a Promise")

      }
    }
    promise
  } catch {
    ThrowSignal(err) => {
      let _ = call_value_with_this(reject, [err], Undefined)
      promise
    }
    err => raise err
  }
}
