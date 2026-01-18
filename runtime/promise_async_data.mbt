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
  mut is_handled : Bool
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
            is_handled: false,
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
        is_handled: false,
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
    table.set(id, data)
    table
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
    table.set(id, data)
    table
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
    table.set(id, data)
    table
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
    table.set(id, data)
    table
  })
}

///|
fn async_function_data_clear(id : Int) -> Unit {
  async_function_data_ref.update(fn(table) {
    let _ = table.remove(id)
    table
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
    table.set(id, data)
    table
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
    table.set(id, data)
    table
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
    table.set(id, data)
    table
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
    table.set(id, data)
    table
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
    table.set(id, value)
    table
  })
}
