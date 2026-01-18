///|
fn gc_release_env(env : Env) -> Unit {
  env.bindings.clear()
  env.readonly_bindings.clear()
  env.deletable_bindings.clear()
  env.private_bindings.clear()
  env.param_binding_names.clear()
  env.uninitialized_bindings.clear()
  env.with_object = None
  env.var_object = None
  env.new_target = Undefined
  env.var_env = None
}

///|
fn gc_release_props(props : Map[String, Property]) -> Unit {
  props.clear()
}

///|
fn gc_release_object_tables(obj_id : Int) -> Unit {
  promise_table_ref.update(fn(table) {
    let _ = table.remove(obj_id)
    table
  })
  async_function_data_ref.update(fn(table) {
    let _ = table.remove(obj_id)
    table
  })
}

///|
fn gc_release_builtin_tables(builtin_id : Int) -> Unit {
  promise_resolve_func_data_ref.update(fn(table) {
    let _ = table.remove(builtin_id)
    table
  })
  promise_executor_data_ref.update(fn(table) {
    let _ = table.remove(builtin_id)
    table
  })
  promise_all_element_data_ref.update(fn(table) {
    let _ = table.remove(builtin_id)
    table
  })
  promise_then_finally_data_ref.update(fn(table) {
    let _ = table.remove(builtin_id)
    table
  })
  async_from_sync_unwrap_data_ref.update(fn(table) {
    let _ = table.remove(builtin_id)
    table
  })
  async_from_sync_close_wrap_data_ref.update(fn(table) {
    let _ = table.remove(builtin_id)
    table
  })
  async_generator_resolve_func_data_ref.update(fn(table) {
    let _ = table.remove(builtin_id)
    table
  })
  async_function_resolve_func_data_ref.update(fn(table) {
    let _ = table.remove(builtin_id)
    table
  })
  promise_finally_value_data_ref.update(fn(table) {
    let _ = table.remove(builtin_id)
    table
  })
}

///|
fn gc_release_object(obj : ObjectValue) -> Unit {
  gc_release_object_tables(obj.id)
  gc_release_props(obj.props)
  obj.proto = None
  obj.realm_env = None
  match obj.generator_data {
    Some(data) => data.roots.clear()
    None => ()
  }
  obj.generator_data = None
  obj.async_generator_data = None
  obj.map_iter_data = None
  obj.set_iter_data = None
  obj.iterator_wrap_data = None
  obj.iterator_concat_data = None
  obj.iterator_helper_data = None
  obj.weakref_data = None
  obj.date_data = None
  obj.array_buffer_data = None
  obj.dataview_data = None
  obj.regexp_data = None
  match obj.proxy_data {
    Some(data) => {
      data.target = Undefined
      data.handler = Undefined
    }
    None => ()
  }
  obj.proxy_data = None
  match obj.map_data {
    Some(data) => data.entries.clear()
    None => ()
  }
  obj.map_data = None
  match obj.set_data {
    Some(data) => data.entries.clear()
    None => ()
  }
  obj.set_data = None
  match obj.weakmap_data {
    Some(data) => data.entries.clear()
    None => ()
  }
  match obj.weakset_data {
    Some(data) => data.entries.clear()
    None => ()
  }
  match obj.finreg_data {
    Some(data) => data.entries.clear()
    None => ()
  }
  obj.weakmap_data = None
  obj.weakset_data = None
  obj.finreg_data = None
}

///|
fn gc_release_function(func : FunctionValue) -> Unit {
  gc_release_props(func.props)
  gc_release_props(func.private_instance_props)
  // Avoid clearing shared AST-backed arrays (params/body/fields) since they are
  // reused across FunctionValue instances and clones.
  func.proto = None
  func.home_object = None
  func.super_constructor = None
  func.source = None
}

///|
fn gc_release_bound_function(bound : BoundFunctionValue) -> Unit {
  gc_release_props(bound.props)
  bound.target = Undefined
  bound.bound_this = Undefined
  bound.bound_args.clear()
  bound.proto = None
}

///|
fn gc_release_builtin(builtin : BuiltinValue) -> Unit {
  gc_release_builtin_tables(builtin.id)
  gc_release_props(builtin.props)
  builtin.this_value = None
  builtin.proto = None
  builtin.realm_env = None
}

///|
fn gc_release_array(arr : ArrayValue) -> Unit {
  gc_release_props(arr.props)
  arr.elements.clear()
  arr.proto = None
  arr.typed_array_data = None
}

///|
fn gc_release_arguments(args : ArgumentsValue) -> Unit {
  gc_release_props(args.props)
  args.elements.clear()
  args.mapped.clear()
  args.params.clear()
  args.proto = None
}

///|
fn gc_release_value(value : Value) -> Unit {
  match value {
    Object(obj) => gc_release_object(obj)
    Function(func) => gc_release_function(func)
    BoundFunction(bound) => gc_release_bound_function(bound)
    Builtin(builtin) => gc_release_builtin(builtin)
    Array(arr) => gc_release_array(arr)
    Arguments(args) => gc_release_arguments(args)
    Symbol(symbol) =>
      symbol_table_ref.update(fn(current) {
        let _ = current.remove(symbol.id)
        current
      })
    _ => ()
  }
}

///|
fn gc_release_all() -> Unit {
  gc_registry_ref().update(fn(registry) {
    registry.values.each(fn(_, value) { gc_release_value(value) })
    registry.envs.each(fn(_, env) { gc_release_env(env) })
    GcRegistry::{ envs: Map::new(), values: Map::new() }
  })
  gc_alloc_count_ref().update(fn(_) { 0 })
}
