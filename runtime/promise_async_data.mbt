///|
priv enum PromiseState {
  Pending
  Fulfilled
  Rejected
}

///|
priv struct PromiseReaction {
  resolve : Value
  reject : Value
  handler : Value
}

///|
priv struct PromiseData {
  mut state : PromiseState
  mut value : Value
  mut fulfill_reactions : Array[PromiseReaction]
  mut reject_reactions : Array[PromiseReaction]
}

///|
fn rc_incref_promise_reaction(reaction : PromiseReaction) -> Unit {
  rc_incref_value(reaction.resolve)
  rc_incref_value(reaction.reject)
  rc_incref_value(reaction.handler)
}

///|
fn rc_decref_promise_reaction(reaction : PromiseReaction) -> Unit {
  rc_decref_value(reaction.resolve)
  rc_decref_value(reaction.reject)
  rc_decref_value(reaction.handler)
}

///|
fn rc_incref_promise_data(data : PromiseData) -> Unit {
  rc_incref_value(data.value)
  for reaction in data.fulfill_reactions {
    rc_incref_promise_reaction(reaction)
  }
  for reaction in data.reject_reactions {
    rc_incref_promise_reaction(reaction)
  }
}

///|
fn rc_decref_promise_data(data : PromiseData) -> Unit {
  rc_decref_value(data.value)
  for reaction in data.fulfill_reactions {
    rc_decref_promise_reaction(reaction)
  }
  for reaction in data.reject_reactions {
    rc_decref_promise_reaction(reaction)
  }
}

///|
let promise_table_ref : Ref[Map[Int, PromiseData]] = Ref::new(Map::new())

///|
priv struct PromiseResolveShared {
  mut already_resolved : Bool
}

///|
priv struct PromiseResolveFunctionData {
  promise : Value
  shared : PromiseResolveShared
  is_reject : Bool
}

///|
let promise_resolve_func_data_ref : Ref[Map[Int, PromiseResolveFunctionData]] = Ref::new(
  Map::new(),
)

///|
priv struct PromiseExecutorData {
  mut resolve : Value?
  mut reject : Value?
}

///|
let promise_executor_data_ref : Ref[Map[Int, PromiseExecutorData]] = Ref::new(
  Map::new(),
)

///|
priv struct PromiseAllRemaining {
  mut count : Int
}

///|
priv struct PromiseAllElementShared {
  mut already_called : Bool
}

///|
priv struct PromiseAllElementData {
  shared : PromiseAllElementShared
  index : Int
  values : Value
  result_resolve : Value
  remaining : PromiseAllRemaining
  mode : Int
  is_reject : Bool
}

///|
let promise_all_element_data_ref : Ref[Map[Int, PromiseAllElementData]] = Ref::new(
  Map::new(),
)

///|
priv struct PromiseThenFinallyData {
  ctor : Value
  on_finally : Value
  is_reject : Bool
}

///|
let promise_then_finally_data_ref : Ref[Map[Int, PromiseThenFinallyData]] = Ref::new(
  Map::new(),
)

///|
priv struct AsyncFromSyncIteratorUnwrapData {
  done : Bool
}

///|
let async_from_sync_unwrap_data_ref : Ref[
  Map[Int, AsyncFromSyncIteratorUnwrapData],
] = Ref::new(Map::new())

///|
priv struct AsyncFromSyncIteratorCloseWrapData {
  sync_iter : Value
}

///|
let async_from_sync_close_wrap_data_ref : Ref[
  Map[Int, AsyncFromSyncIteratorCloseWrapData],
] = Ref::new(Map::new())

///|
priv struct AsyncGeneratorResolveFunctionData {
  generator : Value
  resume_next : Bool
  is_reject : Bool
}

///|
let async_generator_resolve_func_data_ref : Ref[
  Map[Int, AsyncGeneratorResolveFunctionData],
] = Ref::new(Map::new())

///|
priv struct AsyncFunctionResolveFunctionData {
  generator : Value
  is_reject : Bool
}

///|
let async_function_resolve_func_data_ref : Ref[
  Map[Int, AsyncFunctionResolveFunctionData],
] = Ref::new(Map::new())

///|
priv struct AsyncFunctionData {
  resolve : Value
  reject : Value
}

///|
let async_function_data_ref : Ref[Map[Int, AsyncFunctionData]] = Ref::new(
  Map::new(),
)

///|
let promise_finally_value_data_ref : Ref[Map[Int, Value]] = Ref::new(Map::new())

///|
fn promise_table_set(id : Int, data : PromiseData) -> Unit {
  promise_table_ref.update(fn(table) {
    match table.get(id) {
      Some(old) => rc_decref_promise_data(old)
      None => ()
    }
    rc_incref_promise_data(data)
    table.set(id, data)
    table
  })
}

///|
fn promise_table_remove(id : Int) -> Unit {
  promise_table_ref.update(fn(table) {
    match table.get(id) {
      Some(data) => rc_decref_promise_data(data)
      None => ()
    }
    let _ = table.remove(id)
    table
  })
}

///|
fn promise_table_clear() -> Unit {
  promise_table_ref.update(fn(table) {
    for _, data in table {
      rc_decref_promise_data(data)
    }
    Map::new()
  })
}

///|
fn promise_table_get(id : Int) -> PromiseData? {
  let mut result : PromiseData? = None
  promise_table_ref.update(fn(table) {
    result = table.get(id)
    table
  })
  result
}

///|
fn promise_data_from_value(value : Value) -> PromiseData? {
  match value {
    Object(obj) => promise_table_get(obj.id)
    _ => None
  }
}

///|
fn require_promise_data(this_value : Value?) -> PromiseData raise {
  match this_value {
    Some(Object(obj)) =>
      match promise_table_get(obj.id) {
        Some(data) => data
        None => {
          let _ = throw_type_error("not a Promise")
          PromiseData::{
            state: PromiseState::Pending,
            value: Undefined,
            fulfill_reactions: [],
            reject_reactions: [],
          }
        }
      }
    _ => {
      let _ = throw_type_error("not a Promise")
      PromiseData::{
        state: PromiseState::Pending,
        value: Undefined,
        fulfill_reactions: [],
        reject_reactions: [],
      }
    }
  }
}

///|
fn promise_resolve_func_data_get(id : Int) -> PromiseResolveFunctionData? {
  let mut value : PromiseResolveFunctionData? = None
  promise_resolve_func_data_ref.update(fn(table) {
    value = table.get(id)
    table
  })
  value
}

///|
fn promise_resolve_func_data_set(
  id : Int,
  data : PromiseResolveFunctionData,
) -> Unit {
  promise_resolve_func_data_ref.update(fn(table) {
    match table.get(id) {
      Some(old) => rc_decref_value(old.promise)
      None => ()
    }
    rc_incref_value(data.promise)
    table.set(id, data)
    table
  })
}

///|
fn promise_resolve_func_data_clear(id : Int) -> Unit {
  promise_resolve_func_data_ref.update(fn(table) {
    match table.get(id) {
      Some(data) => rc_decref_value(data.promise)
      None => ()
    }
    let _ = table.remove(id)
    table
  })
}

///|
fn promise_resolve_func_data_clear_all() -> Unit {
  promise_resolve_func_data_ref.update(fn(table) {
    for _, data in table {
      rc_decref_value(data.promise)
    }
    Map::new()
  })
}

///|
fn async_generator_resolve_func_data_get(
  id : Int,
) -> AsyncGeneratorResolveFunctionData? {
  let mut value : AsyncGeneratorResolveFunctionData? = None
  async_generator_resolve_func_data_ref.update(fn(table) {
    value = table.get(id)
    table
  })
  value
}

///|
fn async_generator_resolve_func_data_set(
  id : Int,
  data : AsyncGeneratorResolveFunctionData,
) -> Unit {
  async_generator_resolve_func_data_ref.update(fn(table) {
    match table.get(id) {
      Some(old) => rc_decref_value(old.generator)
      None => ()
    }
    rc_incref_value(data.generator)
    table.set(id, data)
    table
  })
}

///|
fn async_generator_resolve_func_data_clear(id : Int) -> Unit {
  async_generator_resolve_func_data_ref.update(fn(table) {
    match table.get(id) {
      Some(data) => rc_decref_value(data.generator)
      None => ()
    }
    let _ = table.remove(id)
    table
  })
}

///|
fn async_generator_resolve_func_data_clear_all() -> Unit {
  async_generator_resolve_func_data_ref.update(fn(table) {
    for _, data in table {
      rc_decref_value(data.generator)
    }
    Map::new()
  })
}

///|
fn async_function_resolve_func_data_get(
  id : Int,
) -> AsyncFunctionResolveFunctionData? {
  let mut value : AsyncFunctionResolveFunctionData? = None
  async_function_resolve_func_data_ref.update(fn(table) {
    value = table.get(id)
    table
  })
  value
}

///|
fn async_function_resolve_func_data_set(
  id : Int,
  data : AsyncFunctionResolveFunctionData,
) -> Unit {
  async_function_resolve_func_data_ref.update(fn(table) {
    match table.get(id) {
      Some(old) => rc_decref_value(old.generator)
      None => ()
    }
    rc_incref_value(data.generator)
    table.set(id, data)
    table
  })
}

///|
fn async_function_resolve_func_data_clear(id : Int) -> Unit {
  async_function_resolve_func_data_ref.update(fn(table) {
    match table.get(id) {
      Some(data) => rc_decref_value(data.generator)
      None => ()
    }
    let _ = table.remove(id)
    table
  })
}

///|
fn async_function_resolve_func_data_clear_all() -> Unit {
  async_function_resolve_func_data_ref.update(fn(table) {
    for _, data in table {
      rc_decref_value(data.generator)
    }
    Map::new()
  })
}

///|
fn async_function_data_get(id : Int) -> AsyncFunctionData? {
  let mut value : AsyncFunctionData? = None
  async_function_data_ref.update(fn(table) {
    value = table.get(id)
    table
  })
  value
}

///|
fn async_function_data_set(id : Int, data : AsyncFunctionData) -> Unit {
  async_function_data_ref.update(fn(table) {
    match table.get(id) {
      Some(old) => {
        rc_decref_value(old.resolve)
        rc_decref_value(old.reject)
      }
      None => ()
    }
    rc_incref_value(data.resolve)
    rc_incref_value(data.reject)
    table.set(id, data)
    table
  })
}

///|
fn async_function_data_clear(id : Int) -> Unit {
  async_function_data_ref.update(fn(table) {
    match table.get(id) {
      Some(data) => {
        rc_decref_value(data.resolve)
        rc_decref_value(data.reject)
      }
      None => ()
    }
    let _ = table.remove(id)
    table
  })
}

///|
fn async_function_data_clear_all() -> Unit {
  async_function_data_ref.update(fn(table) {
    for _, data in table {
      rc_decref_value(data.resolve)
      rc_decref_value(data.reject)
    }
    Map::new()
  })
}

///|
fn promise_executor_data_get(id : Int) -> PromiseExecutorData? {
  let mut value : PromiseExecutorData? = None
  promise_executor_data_ref.update(fn(table) {
    value = table.get(id)
    table
  })
  value
}

///|
fn promise_executor_data_set(id : Int, data : PromiseExecutorData) -> Unit {
  promise_executor_data_ref.update(fn(table) {
    match table.get(id) {
      Some(old) => {
        rc_decref_optional_value(old.resolve)
        rc_decref_optional_value(old.reject)
      }
      None => ()
    }
    rc_incref_optional_value(data.resolve)
    rc_incref_optional_value(data.reject)
    table.set(id, data)
    table
  })
}

///|
fn promise_executor_data_clear(id : Int) -> Unit {
  promise_executor_data_ref.update(fn(table) {
    match table.get(id) {
      Some(data) => {
        rc_decref_optional_value(data.resolve)
        rc_decref_optional_value(data.reject)
      }
      None => ()
    }
    let _ = table.remove(id)
    table
  })
}

///|
fn promise_executor_data_clear_all() -> Unit {
  promise_executor_data_ref.update(fn(table) {
    for _, data in table {
      rc_decref_optional_value(data.resolve)
      rc_decref_optional_value(data.reject)
    }
    Map::new()
  })
}

///|
fn promise_all_element_data_get(id : Int) -> PromiseAllElementData? {
  let mut value : PromiseAllElementData? = None
  promise_all_element_data_ref.update(fn(table) {
    value = table.get(id)
    table
  })
  value
}

///|
fn promise_all_element_data_set(id : Int, data : PromiseAllElementData) -> Unit {
  promise_all_element_data_ref.update(fn(table) {
    match table.get(id) {
      Some(old) => {
        rc_decref_value(old.values)
        rc_decref_value(old.result_resolve)
      }
      None => ()
    }
    rc_incref_value(data.values)
    rc_incref_value(data.result_resolve)
    table.set(id, data)
    table
  })
}

///|
fn promise_all_element_data_clear(id : Int) -> Unit {
  promise_all_element_data_ref.update(fn(table) {
    match table.get(id) {
      Some(data) => {
        rc_decref_value(data.values)
        rc_decref_value(data.result_resolve)
      }
      None => ()
    }
    let _ = table.remove(id)
    table
  })
}

///|
fn promise_all_element_data_clear_all() -> Unit {
  promise_all_element_data_ref.update(fn(table) {
    for _, data in table {
      rc_decref_value(data.values)
      rc_decref_value(data.result_resolve)
    }
    Map::new()
  })
}

///|
fn promise_then_finally_data_get(id : Int) -> PromiseThenFinallyData? {
  let mut value : PromiseThenFinallyData? = None
  promise_then_finally_data_ref.update(fn(table) {
    value = table.get(id)
    table
  })
  value
}

///|
fn promise_then_finally_data_set(
  id : Int,
  data : PromiseThenFinallyData,
) -> Unit {
  promise_then_finally_data_ref.update(fn(table) {
    match table.get(id) {
      Some(old) => {
        rc_decref_value(old.ctor)
        rc_decref_value(old.on_finally)
      }
      None => ()
    }
    rc_incref_value(data.ctor)
    rc_incref_value(data.on_finally)
    table.set(id, data)
    table
  })
}

///|
fn promise_then_finally_data_clear(id : Int) -> Unit {
  promise_then_finally_data_ref.update(fn(table) {
    match table.get(id) {
      Some(data) => {
        rc_decref_value(data.ctor)
        rc_decref_value(data.on_finally)
      }
      None => ()
    }
    let _ = table.remove(id)
    table
  })
}

///|
fn promise_then_finally_data_clear_all() -> Unit {
  promise_then_finally_data_ref.update(fn(table) {
    for _, data in table {
      rc_decref_value(data.ctor)
      rc_decref_value(data.on_finally)
    }
    Map::new()
  })
}

///|
fn async_from_sync_unwrap_data_get(
  id : Int,
) -> AsyncFromSyncIteratorUnwrapData? {
  let mut value : AsyncFromSyncIteratorUnwrapData? = None
  async_from_sync_unwrap_data_ref.update(fn(table) {
    value = table.get(id)
    table
  })
  value
}

///|
fn async_from_sync_unwrap_data_set(
  id : Int,
  data : AsyncFromSyncIteratorUnwrapData,
) -> Unit {
  async_from_sync_unwrap_data_ref.update(fn(table) {
    table.set(id, data)
    table
  })
}

///|
fn async_from_sync_close_wrap_data_get(
  id : Int,
) -> AsyncFromSyncIteratorCloseWrapData? {
  let mut value : AsyncFromSyncIteratorCloseWrapData? = None
  async_from_sync_close_wrap_data_ref.update(fn(table) {
    value = table.get(id)
    table
  })
  value
}

///|
fn async_from_sync_close_wrap_data_set(
  id : Int,
  data : AsyncFromSyncIteratorCloseWrapData,
) -> Unit {
  async_from_sync_close_wrap_data_ref.update(fn(table) {
    match table.get(id) {
      Some(old) => rc_decref_value(old.sync_iter)
      None => ()
    }
    rc_incref_value(data.sync_iter)
    table.set(id, data)
    table
  })
}

///|
fn async_from_sync_close_wrap_data_clear(id : Int) -> Unit {
  async_from_sync_close_wrap_data_ref.update(fn(table) {
    match table.get(id) {
      Some(data) => rc_decref_value(data.sync_iter)
      None => ()
    }
    let _ = table.remove(id)
    table
  })
}

///|
fn async_from_sync_close_wrap_data_clear_all() -> Unit {
  async_from_sync_close_wrap_data_ref.update(fn(table) {
    for _, data in table {
      rc_decref_value(data.sync_iter)
    }
    Map::new()
  })
}

///|
fn promise_finally_value_data_get(id : Int) -> Value? {
  let mut value : Value? = None
  promise_finally_value_data_ref.update(fn(table) {
    value = table.get(id)
    table
  })
  value
}

///|
fn promise_finally_value_data_set(id : Int, value : Value) -> Unit {
  promise_finally_value_data_ref.update(fn(table) {
    match table.get(id) {
      Some(old) => rc_decref_value(old)
      None => ()
    }
    rc_incref_value(value)
    table.set(id, value)
    table
  })
}

///|
fn promise_finally_value_data_clear(id : Int) -> Unit {
  promise_finally_value_data_ref.update(fn(table) {
    match table.get(id) {
      Some(value) => rc_decref_value(value)
      None => ()
    }
    let _ = table.remove(id)
    table
  })
}

///|
fn promise_finally_value_data_clear_all() -> Unit {
  promise_finally_value_data_ref.update(fn(table) {
    for _, value in table {
      rc_decref_value(value)
    }
    Map::new()
  })
}
