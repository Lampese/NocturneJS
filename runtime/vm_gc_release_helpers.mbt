///|
fn gc_release_env(env : Env) -> Unit {
  rc_decref_optional_env(env.parent)
  for _, value in env.bindings {
    rc_decref_value(value)
  }
  env.bindings.clear()
  env.readonly_bindings.clear()
  env.deletable_bindings.clear()
  env.private_bindings.clear()
  env.param_binding_names.clear()
  env.uninitialized_bindings.clear()
  rc_replace_optional_value(env.with_object, None)
  env.with_object = None
  rc_replace_optional_value(env.var_object, None)
  env.var_object = None
  rc_replace_value(env.new_target, Undefined)
  env.new_target = Undefined
  rc_replace_optional_env(env.var_env, None)
  env.var_env = None
}

///|
fn gc_release_props(props : Props) -> Unit {
  props_clear(props)
}

///|

///|
fn gc_release_object_tables(obj_id : Int) -> Unit {
  promise_table_remove(obj_id)
  async_function_data_clear(obj_id)
}

///|
fn gc_release_builtin_tables(builtin_id : Int) -> Unit {
  promise_resolve_func_data_clear(builtin_id)
  promise_executor_data_clear(builtin_id)
  promise_all_element_data_clear(builtin_id)
  promise_then_finally_data_clear(builtin_id)
  async_from_sync_unwrap_data_ref.update(fn(table) {
    let _ = table.remove(builtin_id)
    table
  })
  async_from_sync_close_wrap_data_clear(builtin_id)
  async_generator_resolve_func_data_clear(builtin_id)
  async_function_resolve_func_data_clear(builtin_id)
  promise_finally_value_data_clear(builtin_id)
}

///|
fn gc_release_object(obj : ObjectValue) -> Unit {
  let is_date = obj.date_data is Some(_)
  gc_release_object_tables(obj.id)
  gc_release_props(obj.props)
  rc_replace_optional_value(obj.proto, None)
  obj.proto = None
  rc_replace_optional_env(obj.realm_env, None)
  obj.realm_env = None
  match obj.symbol_data {
    Some(symbol) => rc_decref_value(Symbol(symbol))
    None => ()
  }
  match obj.generator_data {
    Some(data) => {
      rc_decref_env(data.env)
      rc_decref_value(Function(data.func))
      rc_clear_values(data.roots)
    }
    None => ()
  }
  obj.generator_data = None
  match obj.async_generator_data {
    Some(data) =>
      for request in data.queue {
        match request.resume_state {
          GenResume::Next(value) => rc_decref_value(value)
          GenResume::Return(value) => rc_decref_value(value)
          GenResume::Throw(value) => rc_decref_value(value)
        }
        rc_decref_value(request.promise)
        rc_decref_value(request.resolve)
        rc_decref_value(request.reject)
      }
    None => ()
  }
  obj.async_generator_data = None
  match obj.map_iter_data {
    Some(data) => rc_decref_map_entries(data.entries)
    None => ()
  }
  obj.map_iter_data = None
  match obj.set_iter_data {
    Some(data) => rc_decref_optional_values(data.entries)
    None => ()
  }
  obj.set_iter_data = None
  match obj.iterator_wrap_data {
    Some(data) => {
      rc_decref_value(data.wrapped_iter)
      rc_decref_value(data.wrapped_next)
    }
    None => ()
  }
  obj.iterator_wrap_data = None
  match obj.iterator_concat_data {
    Some(data) => {
      rc_decref_value(data.iter)
      rc_decref_value(data.next)
      for pair in data.values {
        let (value, next_value) = pair
        rc_decref_value(value)
        rc_decref_value(next_value)
      }
    }
    None => ()
  }
  obj.iterator_concat_data = None
  match obj.iterator_helper_data {
    Some(data) => {
      rc_decref_value(data.obj)
      rc_decref_value(data.next)
      rc_decref_value(data.func)
      rc_decref_value(data.inner)
      rc_decref_value(data.inner_next)
    }
    None => ()
  }
  obj.iterator_helper_data = None
  obj.weakref_data = None
  obj.date_data = None
  obj.array_buffer_data = None
  match obj.dataview_data {
    Some(data) => rc_decref_value(Object(data.buffer))
    None => ()
  }
  obj.dataview_data = None
  obj.regexp_data = None
  match obj.proxy_data {
    Some(data) => {
      rc_decref_value(data.target)
      rc_decref_value(data.handler)
      data.target = Undefined
      data.handler = Undefined
    }
    None => ()
  }
  obj.proxy_data = None
  match obj.map_data {
    Some(data) => {
      rc_decref_map_entries(data.entries)
      data.entries.clear()
    }
    None => ()
  }
  obj.map_data = None
  match obj.set_data {
    Some(data) => rc_clear_optional_values(data.entries)
    None => ()
  }
  obj.set_data = None
  match obj.weakmap_data {
    Some(data) => {
      for entry in data.entries {
        let (_, value) = entry
        rc_decref_value(value)
      }
      data.entries.clear()
      data.index.clear()
    }
    None => ()
  }
  match obj.weakset_data {
    Some(data) => data.entries.clear()
    None => ()
  }
  match obj.finreg_data {
    Some(data) => {
      rc_decref_env(data.realm_env)
      rc_decref_value(data.callback)
      for entry in data.entries {
        rc_decref_value(entry.held_value)
      }
      data.entries.clear()
    }
    None => ()
  }
  obj.weakmap_data = None
  obj.weakset_data = None
  obj.finreg_data = None
  if is_date {
    date_pool_put(obj)
  }
}

///|
fn gc_release_function(func : FunctionValue) -> Unit {
  gc_release_props(func.props)
  gc_release_props(func.private_instance_props)
  bytecode_cache_remove(func.id)
  // Avoid clearing shared AST-backed arrays (params/body/fields) since they are
  // reused across FunctionValue instances and clones.
  rc_replace_optional_value(func.proto, None)
  func.proto = None
  rc_replace_optional_value(func.home_object, None)
  func.home_object = None
  rc_replace_optional_value(func.super_constructor, None)
  func.super_constructor = None
  rc_decref_env(func.env)
  func.source = None
}

///|
fn gc_release_bound_function(bound : BoundFunctionValue) -> Unit {
  gc_release_props(bound.props)
  rc_replace_value(bound.target, Undefined)
  bound.target = Undefined
  rc_replace_value(bound.bound_this, Undefined)
  bound.bound_this = Undefined
  rc_clear_values(bound.bound_args)
  rc_replace_optional_value(bound.proto, None)
  bound.proto = None
}

///|
fn gc_release_builtin(builtin : BuiltinValue) -> Unit {
  gc_release_builtin_tables(builtin.id)
  gc_release_props(builtin.props)
  rc_replace_optional_value(builtin.this_value, None)
  builtin.this_value = None
  rc_replace_optional_value(builtin.proto, None)
  builtin.proto = None
  rc_replace_optional_env(builtin.realm_env, None)
  builtin.realm_env = None
}

///|
fn gc_release_array(arr : ArrayValue) -> Unit {
  gc_release_props(arr.props)
  rc_clear_optional_values(arr.elements)
  rc_replace_optional_value(arr.proto, None)
  arr.proto = None
  match arr.typed_array_data {
    Some(data) => rc_decref_value(Object(data.buffer))
    None => ()
  }
  arr.typed_array_data = None
}

///|
fn gc_release_arguments(args : ArgumentsValue) -> Unit {
  gc_release_props(args.props)
  rc_clear_optional_values(args.elements)
  args.mapped.clear()
  args.params.clear()
  rc_replace_optional_value(args.proto, None)
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
    Symbol(symbol) => symbol_table_remove(symbol.id)
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
  gc_rc_meta_ref().update(fn(_) { Map::new() })
}
