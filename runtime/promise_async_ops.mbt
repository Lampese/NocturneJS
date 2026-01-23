///|
fn new_promise_value_with_proto(proto : Value?) -> Value {
  let value = new_object_value_with_proto(proto)
  match value {
    Object(obj) => {
      let data = PromiseData::{
        state: PromiseState::Pending,
        value: Undefined,
        fulfill_reactions: [],
        reject_reactions: [],
      }
      promise_table_set(obj.id, data)
    }
    _ => ()
  }
  value
}

///|
fn promise_settle(
  data : PromiseData,
  state : PromiseState,
  value : Value,
) -> Unit {
  match data.state {
    PromiseState::Pending => ()
    _ => return
  }
  data.state = state
  rc_replace_value(data.value, value)
  data.value = value
  let fulfill_items = data.fulfill_reactions
  let reject_items = data.reject_reactions
  data.fulfill_reactions = []
  data.reject_reactions = []
  let reactions = if state is PromiseState::Fulfilled {
    fulfill_items
  } else {
    reject_items
  }
  let discarded = if state is PromiseState::Fulfilled {
    reject_items
  } else {
    fulfill_items
  }
  for reaction in reactions {
    enqueue_job(new_builtin_value(BuiltinFunction::PromiseReactionJob), [
      reaction.resolve,
      reaction.reject,
      reaction.handler,
      Bool(state is PromiseState::Rejected),
      value,
    ])
    rc_decref_promise_reaction(reaction)
  }
  for reaction in discarded {
    rc_decref_promise_reaction(reaction)
  }
}

///|
fn perform_promise_then(
  promise : Value,
  data : PromiseData,
  on_fulfilled : Value,
  on_rejected : Value,
  resolve : Value,
  reject : Value,
) -> Unit {
  let fulfill_handler = if is_callable(on_fulfilled) {
    on_fulfilled
  } else {
    Undefined
  }
  let reject_handler = if is_callable(on_rejected) {
    on_rejected
  } else {
    Undefined
  }
  match data.state {
    PromiseState::Pending => {
      let fulfill_reaction = PromiseReaction::{
        resolve,
        reject,
        handler: fulfill_handler,
      }
      rc_incref_promise_reaction(fulfill_reaction)
      data.fulfill_reactions.push(fulfill_reaction)
      let reject_reaction = PromiseReaction::{
        resolve,
        reject,
        handler: reject_handler,
      }
      rc_incref_promise_reaction(reject_reaction)
      data.reject_reactions.push(reject_reaction)
    }
    PromiseState::Fulfilled =>
      enqueue_job(new_builtin_value(BuiltinFunction::PromiseReactionJob), [
        resolve,
        reject,
        fulfill_handler,
        Bool(false),
        data.value,
      ])
    PromiseState::Rejected =>
      enqueue_job(new_builtin_value(BuiltinFunction::PromiseReactionJob), [
        resolve,
        reject,
        reject_handler,
        Bool(true),
        data.value,
      ])
  }
  let _ = promise

}

///|
fn promise_all_remaining_add(
  remaining : PromiseAllRemaining,
  addend : Int,
) -> Bool {
  remaining.count = remaining.count + addend
  remaining.count == 0
}

///|
fn promise_species_constructor(promise : Value) -> Value raise {
  let default_ctor = promise_ctor_value_for_current_env()
  let ctor = property_get(promise, "constructor")
  match ctor {
    Undefined => default_ctor
    _ =>
      if !is_object_like(ctor) {
        throw_type_error("constructor is not an object")
      } else {
        match symbol_species_key() {
          Some(key) =>
            match property_get(ctor, key) {
              Undefined | Null => default_ctor
              species =>
                if !is_constructor_value(species) {
                  throw_type_error("not a constructor")
                } else {
                  species
                }
            }
          None => default_ctor
        }
      }
  }
}

///|
fn create_promise_resolving_functions(promise : Value) -> (Value, Value) {
  let shared = PromiseResolveShared::{ already_resolved: false }
  let resolve = new_builtin_value(BuiltinFunction::PromiseResolveFunction)
  let reject = new_builtin_value(BuiltinFunction::PromiseRejectFunction)
  match resolve {
    Builtin(builtin) =>
      promise_resolve_func_data_set(builtin.id, PromiseResolveFunctionData::{
        promise,
        shared,
        is_reject: false,
      })
    _ => ()
  }
  match reject {
    Builtin(builtin) =>
      promise_resolve_func_data_set(builtin.id, PromiseResolveFunctionData::{
        promise,
        shared,
        is_reject: true,
      })
    _ => ()
  }
  (resolve, reject)
}

///|
fn new_promise_capability_executor() -> Value {
  let executor = new_builtin_value(BuiltinFunction::PromiseCapabilityExecutor)
  match executor {
    Builtin(builtin) =>
      promise_executor_data_set(builtin.id, PromiseExecutorData::{
        resolve: None,
        reject: None,
      })
    _ => ()
  }
  executor
}

///|
fn new_promise_capability(ctor : Value) -> (Value, Value, Value) raise {
  let executor = new_promise_capability_executor()
  let promise = if ctor is Undefined {
    let default_ctor = promise_ctor_value_for_current_env()
    call_constructor(default_ctor, [executor])
  } else {
    call_constructor(ctor, [executor])
  }
  let mut resolve_value = Undefined
  let mut reject_value = Undefined
  match executor {
    Builtin(builtin) =>
      match promise_executor_data_get(builtin.id) {
        Some(data) => {
          match data.resolve {
            Some(value) => resolve_value = value
            None => ()
          }
          match data.reject {
            Some(value) => reject_value = value
            None => ()
          }
        }
        None => ()
      }
    _ => ()
  }
  if !is_callable(resolve_value) || !is_callable(reject_value) {
    let _ = throw_type_error("executor not called")

  }
  (promise, resolve_value, reject_value)
}

///|
fn promise_resolve_value(
  this_value : Value,
  value : Value,
  is_reject : Bool,
) -> Value raise {
  if !is_object_like(this_value) {
    return throw_type_error("not an object")
  }
  if !is_reject {
    match promise_data_from_value(value) {
      Some(_) => {
        let ctor = property_get(value, "constructor")
        if strict_eq(ctor, this_value) {
          return value
        }
      }
      None => ()
    }
  }
  let (promise, resolve, reject) = new_promise_capability(this_value)
  let target = if is_reject { reject } else { resolve }
  let _ = call_value_with_this(target, [value], Undefined)
  promise
}

///|
fn new_promise_all_element_function(data : PromiseAllElementData) -> Value {
  let func = new_builtin_value(BuiltinFunction::PromiseAllResolveElement)
  match func {
    Builtin(builtin) => promise_all_element_data_set(builtin.id, data)
    _ => ()
  }
  func
}

///|
fn new_promise_then_finally_function(data : PromiseThenFinallyData) -> Value {
  let func = new_builtin_value(BuiltinFunction::PromiseThenFinally)
  match func {
    Builtin(builtin) => promise_then_finally_data_set(builtin.id, data)
    _ => ()
  }
  func
}

///|
fn new_promise_finally_value_thunk(value : Value) -> Value {
  let func = new_builtin_value(BuiltinFunction::PromiseFinallyValueThunk)
  match func {
    Builtin(builtin) => promise_finally_value_data_set(builtin.id, value)
    _ => ()
  }
  func
}

///|
fn new_promise_finally_thrower(value : Value) -> Value {
  let func = new_builtin_value(BuiltinFunction::PromiseFinallyThrower)
  match func {
    Builtin(builtin) => promise_finally_value_data_set(builtin.id, value)
    _ => ()
  }
  func
}

///|
fn set_array_index(values : Value, index : Int, value : Value) -> Unit raise {
  let name = Int::to_string(index)
  create_data_property_or_throw(values, name, value)
}

///|
fn promise_invoke_then(
  promise : Value,
  on_fulfilled : Value,
  on_rejected : Value,
) -> Value raise {
  let then_method = property_get(promise, "then")
  if !is_callable(then_method) {
    return throw_type_error("not a function")
  }
  call_value_with_this(then_method, [on_fulfilled, on_rejected], promise)
}

///|
fn promise_then_value(promise : Value, value : Value) -> Value raise {
  let on_fulfilled = new_promise_finally_value_thunk(value)
  promise_invoke_then(promise, on_fulfilled, Undefined)
}

///|
fn promise_all_value(
  this_value : Value,
  iterable : Value,
  mode : Int,
) -> Value raise {
  if !is_object_like(this_value) {
    return throw_type_error("not an object")
  }
  let (result_promise, resolve, reject) = new_promise_capability(this_value)
  let mut promise_resolve = Undefined
  try {
    promise_resolve = property_get(this_value, "resolve")
    if !is_callable(promise_resolve) {
      let error = type_error_value("not a function")
      let _ = call_value_with_this(reject, [error], Undefined)
      return result_promise
    }
  } catch {
    ThrowSignal(value) => {
      let _ = call_value_with_this(reject, [value], Undefined)
      return result_promise
    }
    err => raise err
  }
  let mut iterator = Undefined
  let mut next_method = Undefined
  try {
    let (iter_value, next_value) = get_iterator_from_value(iterable)
    iterator = iter_value
    next_method = next_value
  } catch {
    ThrowSignal(value) => {
      let _ = call_value_with_this(reject, [value], Undefined)
      return result_promise
    }
    err => raise err
  }
  let values = new_array_value([])
  let remaining = PromiseAllRemaining::{ count: 1 }
  let mut index = 0
  try {
    while true {
      let mut step = Undefined
      try {
        step = call_value_with_this(next_method, [], iterator)
      } catch {
        ThrowSignal(value) => {
          let _ = call_value_with_this(reject, [value], Undefined)
          return result_promise
        }
        err => raise err
      }
      let step_obj = to_object(step)
      let mut done = false
      try {
        done = is_truthy(property_get(step_obj, "done"))
      } catch {
        ThrowSignal(value) => {
          let _ = call_value_with_this(reject, [value], Undefined)
          return result_promise
        }
        err => raise err
      }
      if done {
        break
      }
      let mut item = Undefined
      try {
        item = property_get(step_obj, "value")
      } catch {
        ThrowSignal(value) => {
          let _ = call_value_with_this(reject, [value], Undefined)
          return result_promise
        }
        err => raise err
      }
      try {
        let next_promise = call_value_with_this(
          promise_resolve,
          [item],
          this_value,
        )
        let shared = PromiseAllElementShared::{ already_called: false }
        let result_resolve = if mode == 2 { reject } else { resolve }
        let resolve_data = PromiseAllElementData::{
          shared,
          index,
          values,
          result_resolve,
          remaining,
          mode,
          is_reject: false,
        }
        let mut resolve_element = new_promise_all_element_function(resolve_data)
        let mut reject_element : Value = Undefined
        if mode == 1 {
          let reject_data = PromiseAllElementData::{
            shared,
            index,
            values,
            result_resolve: resolve,
            remaining,
            mode,
            is_reject: true,
          }
          reject_element = new_promise_all_element_function(reject_data)
        } else if mode == 2 {
          let _ = set_array_index(values, index, Undefined)
          reject_element = resolve_element
          resolve_element = resolve
        } else {
          reject_element = reject
        }
        let _ = promise_all_remaining_add(remaining, 1)
        let _ = promise_invoke_then(
          next_promise, resolve_element, reject_element,
        )
        index = index + 1
      } catch {
        ThrowSignal(value) => {
          iterator_close_on_error(iterator)
          let _ = call_value_with_this(reject, [value], Undefined)
          return result_promise
        }
        err => {
          iterator_close_on_error(iterator)
          raise err
        }
      }
    }
  } catch {
    ThrowSignal(value) => {
      iterator_close_on_error(iterator)
      let _ = call_value_with_this(reject, [value], Undefined)
      return result_promise
    }
    err => {
      iterator_close_on_error(iterator)
      raise err
    }
  }
  if promise_all_remaining_add(remaining, -1) {
    try {
      if mode == 2 {
        let error = new_aggregate_error_value([values])
        let _ = call_value_with_this(reject, [error], Undefined)

      } else {
        let _ = call_value_with_this(resolve, [values], Undefined)

      }
    } catch {
      ThrowSignal(value) => {
        let _ = call_value_with_this(reject, [value], Undefined)

      }
      err => raise err
    }
  }
  result_promise
}

///|
fn promise_race_value(this_value : Value, iterable : Value) -> Value raise {
  if !is_object_like(this_value) {
    return throw_type_error("not an object")
  }
  let (result_promise, resolve, reject) = new_promise_capability(this_value)
  let mut promise_resolve = Undefined
  try {
    promise_resolve = property_get(this_value, "resolve")
    if !is_callable(promise_resolve) {
      let error = type_error_value("not a function")
      let _ = call_value_with_this(reject, [error], Undefined)
      return result_promise
    }
  } catch {
    ThrowSignal(value) => {
      let _ = call_value_with_this(reject, [value], Undefined)
      return result_promise
    }
    err => raise err
  }
  let mut iterator = Undefined
  let mut next_method = Undefined
  try {
    let (iter_value, next_value) = get_iterator_from_value(iterable)
    iterator = iter_value
    next_method = next_value
  } catch {
    ThrowSignal(value) => {
      let _ = call_value_with_this(reject, [value], Undefined)
      return result_promise
    }
    err => raise err
  }
  try {
    while true {
      let mut step = Undefined
      try {
        step = call_value_with_this(next_method, [], iterator)
      } catch {
        ThrowSignal(value) => {
          let _ = call_value_with_this(reject, [value], Undefined)
          return result_promise
        }
        err => raise err
      }
      let step_obj = to_object(step)
      let mut done = false
      try {
        done = is_truthy(property_get(step_obj, "done"))
      } catch {
        ThrowSignal(value) => {
          let _ = call_value_with_this(reject, [value], Undefined)
          return result_promise
        }
        err => raise err
      }
      if done {
        break
      }
      let mut item = Undefined
      try {
        item = property_get(step_obj, "value")
      } catch {
        ThrowSignal(value) => {
          let _ = call_value_with_this(reject, [value], Undefined)
          return result_promise
        }
        err => raise err
      }
      try {
        let next_promise = call_value_with_this(
          promise_resolve,
          [item],
          this_value,
        )
        let _ = promise_invoke_then(next_promise, resolve, reject)

      } catch {
        ThrowSignal(value) => {
          iterator_close_on_error(iterator)
          let _ = call_value_with_this(reject, [value], Undefined)
          return result_promise
        }
        err => {
          iterator_close_on_error(iterator)
          raise err
        }
      }
    }
  } catch {
    ThrowSignal(value) => {
      iterator_close_on_error(iterator)
      let _ = call_value_with_this(reject, [value], Undefined)
      return result_promise
    }
    err => {
      iterator_close_on_error(iterator)
      raise err
    }
  }
  result_promise
}

///|
fn promise_try_value(this_value : Value, args : Array[Value]) -> Value raise {
  if !is_object_like(this_value) {
    return throw_type_error("not an object")
  }
  let (result_promise, resolve, reject) = new_promise_capability(this_value)
  let call_args : Array[Value] = []
  let func = if args.is_empty() { Undefined } else { args[0] }
  let mut i = 1
  while i < args.length() {
    call_args.push(args[i])
    i = i + 1
  }
  let mut is_reject = false
  let mut result = Undefined
  try {
    result = call_value_with_this(func, call_args, Undefined)
  } catch {
    ThrowSignal(value) => {
      is_reject = true
      result = value
    }
    err => raise err
  }
  let target = if is_reject { reject } else { resolve }
  let _ = call_value_with_this(target, [result], Undefined)
  result_promise
}

///|
fn promise_with_resolvers_value(this_value : Value) -> Value raise {
  if !is_object_like(this_value) {
    return throw_type_error("not an object")
  }
  let (promise, resolve, reject) = new_promise_capability(this_value)
  let obj = new_object_value()
  create_data_property_or_throw(obj, "promise", promise)
  create_data_property_or_throw(obj, "resolve", resolve)
  create_data_property_or_throw(obj, "reject", reject)
  obj
}
