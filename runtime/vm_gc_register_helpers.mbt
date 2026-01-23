///|
fn value_id(value : Value) -> Int? {
  match value {
    Object(obj) => Some(obj.id)
    Function(func) => Some(func.id)
    BoundFunction(bound) => Some(bound.id)
    Builtin(builtin) => Some(builtin.id)
    Array(arr) => Some(arr.id)
    Arguments(args) => Some(args.id)
    Symbol(symbol) => Some(symbol.id)
    _ => None
  }
}

///|
fn register_env(env : Env) -> Unit {
  gc_registry_ref().update(fn(registry) {
    if !registry.envs.contains(env.id) {
      registry.envs.set(env.id, env)
      rc_meta_register(env.id)
      gc_alloc_count_ref().update(fn(count) { count + 1 })
    }
    registry
  })
  gc_request()
}

///|
fn register_value(value : Value) -> Unit {
  match value_id(value) {
    Some(id) =>
      gc_registry_ref().update(fn(registry) {
        if !registry.values.contains(id) {
          registry.values.set(id, value)
          rc_meta_register(id)
          gc_alloc_count_ref().update(fn(count) { count + 1 })
        }
        registry
      })
    None => ()
  }
  gc_request()
}

///|
fn register_object_value(obj : ObjectValue) -> ObjectValue {
  register_value(Object(obj))
  obj
}

///|
fn register_array_value(arr : ArrayValue) -> ArrayValue {
  register_value(Array(arr))
  arr
}

///|
fn register_function_value(func : FunctionValue) -> FunctionValue {
  register_value(Function(func))
  func
}

///|
fn register_bound_function_value(
  bound : BoundFunctionValue,
) -> BoundFunctionValue {
  register_value(BoundFunction(bound))
  bound
}

///|
fn register_builtin_value(builtin : BuiltinValue) -> BuiltinValue {
  register_value(Builtin(builtin))
  builtin
}

///|
fn register_arguments_value(args : ArgumentsValue) -> ArgumentsValue {
  register_value(Arguments(args))
  args
}

///|
fn gc_prepare_meta(state : GcState) -> Unit {
  let mut rc_table : Map[Int, RcMeta] = Map::new()
  gc_rc_meta_ref().update(fn(table) {
    rc_table = table
    table
  })
  gc_registry_ref().update(fn(registry) {
    registry.values.each(fn(id, _) {
      let mut ref_count = 0
      match rc_table.get(id) {
        Some(meta) => ref_count = meta.ref_count
        None => ()
      }
      state.meta.set(id, GcMeta::{
        ref_count,
        mark: false,
        in_tmp: false,
      })
    })
    registry.envs.each(fn(id, _) {
      let mut ref_count = 0
      match rc_table.get(id) {
        Some(meta) => ref_count = meta.ref_count
        None => ()
      }
      state.meta.set(id, GcMeta::{
        ref_count,
        mark: false,
        in_tmp: false,
      })
    })
    registry
  })
}

///|
fn gc_meta_incref(state : GcState, id : Int) -> Unit {
  match state.meta.get(id) {
    Some(meta) => meta.ref_count = meta.ref_count + 1
    None => ()
  }
}

///|
fn gc_meta_decref(state : GcState, id : Int) -> Unit {
  match state.meta.get(id) {
    Some(meta) =>
      if meta.ref_count > 0 {
        meta.ref_count = meta.ref_count - 1
        if meta.ref_count == 0 && meta.mark && !meta.in_tmp {
          meta.in_tmp = true
        }
      }
    None => ()
  }
}
