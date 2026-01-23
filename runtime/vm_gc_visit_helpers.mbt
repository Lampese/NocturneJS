///|
fn is_live_value(state : GcState, value : Value) -> Bool {
  match value {
    Undefined => true
    _ =>
      match value_id(value) {
        Some(id) =>
          match state.meta.get(id) {
            Some(meta) => meta.ref_count > 0
            None => false
          }
        None => false
      }
  }
}

///|
fn is_live_value_now(value : Value) -> Bool {
  let state = new_gc_state()
  gc_prepare_meta(state)
  gc_count_roots(state)
  gc_count_internal_refs(state)
  is_live_value(state, value)
}

///|
fn gc_visit_props(
  props : Map[String, Property],
  visit_value : (Value) -> Unit,
) -> Unit {
  for key, prop in props {
    if is_symbol_prop_key(key) {
      match symbol_from_prop_key(key) {
        Some(symbol) => visit_value(Symbol(symbol))
        None => ()
      }
    }
    visit_value(prop.value)
    match prop.getter {
      Some(getter) => visit_value(getter)
      None => ()
    }
    match prop.setter {
      Some(setter) => visit_value(setter)
      None => ()
    }
  }
}

///|
fn gc_visit_proto(proto : Value?, visit_value : (Value) -> Unit) -> Unit {
  match proto {
    Some(value) => visit_value(value)
    None => ()
  }
}

///|
fn gc_visit_env_children(
  env : Env,
  visit_value : (Value) -> Unit,
  visit_env : (Env) -> Unit,
) -> Unit {
  for _, value in env.bindings {
    visit_value(value)
  }
  match env.with_object {
    Some(value) => visit_value(value)
    None => ()
  }
  match env.var_object {
    Some(value) => visit_value(value)
    None => ()
  }
  visit_value(env.new_target)
  match env.var_env {
    Some(var_env) => visit_env(var_env)
    None => ()
  }
  match env.parent {
    Some(parent) => visit_env(parent)
    None => ()
  }
}

///|
fn gc_visit_promise_data(obj_id : Int, visit_value : (Value) -> Unit) -> Unit {
  match promise_table_get(obj_id) {
    Some(data) => {
      visit_value(data.value)
      for reaction in data.fulfill_reactions {
        visit_value(reaction.resolve)
        visit_value(reaction.reject)
        visit_value(reaction.handler)
      }
      for reaction in data.reject_reactions {
        visit_value(reaction.resolve)
        visit_value(reaction.reject)
        visit_value(reaction.handler)
      }
    }
    None => ()
  }
}

///|
fn gc_visit_async_function_data(
  obj_id : Int,
  visit_value : (Value) -> Unit,
) -> Unit {
  match async_function_data_get(obj_id) {
    Some(data) => {
      visit_value(data.resolve)
      visit_value(data.reject)
    }
    None => ()
  }
}

///|
fn gc_visit_object_children(
  state : GcState,
  obj : ObjectValue,
  visit_value : (Value) -> Unit,
  visit_env : (Env) -> Unit,
  collect_weak : Bool,
) -> Unit {
  gc_visit_proto(obj.proto, visit_value)
  gc_visit_props(obj.props, visit_value)
  match obj.symbol_data {
    Some(symbol) => visit_value(Symbol(symbol))
    None => ()
  }
  match obj.realm_env {
    Some(env) => visit_env(env)
    None => ()
  }
  match obj.generator_data {
    Some(data) => {
      visit_env(data.env)
      visit_value(Function(data.func))
      for value in data.roots {
        visit_value(value)
      }
    }
    None => ()
  }
  match obj.async_generator_data {
    Some(data) =>
      for request in data.queue {
        match request.resume_state {
          GenResume::Next(value) => visit_value(value)
          GenResume::Return(value) => visit_value(value)
          GenResume::Throw(value) => visit_value(value)
        }
        visit_value(request.promise)
        visit_value(request.resolve)
        visit_value(request.reject)
      }
    None => ()
  }
  match obj.proxy_data {
    Some(data) => {
      visit_value(data.target)
      visit_value(data.handler)
    }
    None => ()
  }
  match obj.map_data {
    Some(data) =>
      for entry in data.entries {
        match entry {
          Some((key, value)) => {
            visit_value(key)
            visit_value(value)
          }
          None => ()
        }
      }
    None => ()
  }
  match obj.set_data {
    Some(data) =>
      for value in data.entries {
        match value {
          Some(entry) => visit_value(entry)
          None => ()
        }
      }
    None => ()
  }
  match obj.map_iter_data {
    Some(data) =>
      for entry in data.entries {
        match entry {
          Some((key, value)) => {
            visit_value(key)
            visit_value(value)
          }
          None => ()
        }
      }
    None => ()
  }
  match obj.set_iter_data {
    Some(data) =>
      for value in data.entries {
        match value {
          Some(entry) => visit_value(entry)
          None => ()
        }
      }
    None => ()
  }
  match obj.iterator_wrap_data {
    Some(data) => {
      visit_value(data.wrapped_iter)
      visit_value(data.wrapped_next)
    }
    None => ()
  }
  match obj.iterator_concat_data {
    Some(data) => {
      visit_value(data.iter)
      visit_value(data.next)
      for pair in data.values {
        let (value, next_value) = pair
        visit_value(value)
        visit_value(next_value)
      }
    }
    None => ()
  }
  match obj.iterator_helper_data {
    Some(data) => {
      visit_value(data.obj)
      visit_value(data.next)
      visit_value(data.func)
      visit_value(data.inner)
      visit_value(data.inner_next)
    }
    None => ()
  }
  match obj.weakref_data {
    Some(_) => if collect_weak { state.weakrefs.push(obj) }
    None => ()
  }
  match obj.weakmap_data {
    Some(data) => {
      if collect_weak {
        state.weakmaps.push(obj)
      }
      for entry in data.entries {
        let (_, value) = entry
        visit_value(value)
      }
    }
    None => ()
  }
  match obj.weakset_data {
    Some(_) => if collect_weak { state.weaksets.push(obj) }
    None => ()
  }
  match obj.finreg_data {
    Some(data) => {
      visit_env(data.realm_env)
      visit_value(data.callback)
      for entry in data.entries {
        visit_value(entry.held_value)
      }
      if collect_weak {
        state.finregs.push(obj)
      }
    }
    None => ()
  }
  match obj.dataview_data {
    Some(data) => visit_value(Object(data.buffer))
    None => ()
  }
  gc_visit_promise_data(obj.id, visit_value)
  gc_visit_async_function_data(obj.id, visit_value)
}

///|
fn gc_visit_function_children(
  func : FunctionValue,
  visit_value : (Value) -> Unit,
  visit_env : (Env) -> Unit,
) -> Unit {
  gc_visit_proto(func.proto, visit_value)
  match func.home_object {
    Some(value) => visit_value(value)
    None => ()
  }
  gc_visit_props(func.props, visit_value)
  gc_visit_props(func.private_instance_props, visit_value)
  visit_env(func.env)
  match func.super_constructor {
    Some(value) => visit_value(value)
    None => ()
  }
}

///|
fn gc_visit_bound_function_children(
  bound : BoundFunctionValue,
  visit_value : (Value) -> Unit,
) -> Unit {
  gc_visit_proto(bound.proto, visit_value)
  gc_visit_props(bound.props, visit_value)
  visit_value(bound.target)
  visit_value(bound.bound_this)
  for value in bound.bound_args {
    visit_value(value)
  }
}

///|
fn gc_visit_builtin_extra_children(
  builtin : BuiltinValue,
  visit_value : (Value) -> Unit,
) -> Unit {
  match builtin.kind {
    BuiltinFunction::PromiseResolveFunction
    | BuiltinFunction::PromiseRejectFunction =>
      match promise_resolve_func_data_get(builtin.id) {
        Some(data) => visit_value(data.promise)
        None => ()
      }
    BuiltinFunction::PromiseCapabilityExecutor =>
      match promise_executor_data_get(builtin.id) {
        Some(data) => {
          match data.resolve {
            Some(value) => visit_value(value)
            None => ()
          }
          match data.reject {
            Some(value) => visit_value(value)
            None => ()
          }
        }
        None => ()
      }
    BuiltinFunction::PromiseAllResolveElement =>
      match promise_all_element_data_get(builtin.id) {
        Some(data) => {
          visit_value(data.values)
          visit_value(data.result_resolve)
        }
        None => ()
      }
    BuiltinFunction::PromiseThenFinally =>
      match promise_then_finally_data_get(builtin.id) {
        Some(data) => {
          visit_value(data.ctor)
          visit_value(data.on_finally)
        }
        None => ()
      }
    BuiltinFunction::PromiseFinallyValueThunk
    | BuiltinFunction::PromiseFinallyThrower =>
      match promise_finally_value_data_get(builtin.id) {
        Some(value) => visit_value(value)
        None => ()
      }
    BuiltinFunction::AsyncFromSyncIteratorCloseWrap =>
      match async_from_sync_close_wrap_data_get(builtin.id) {
        Some(data) => visit_value(data.sync_iter)
        None => ()
      }
    BuiltinFunction::AsyncGeneratorResolveFunction =>
      match async_generator_resolve_func_data_get(builtin.id) {
        Some(data) => visit_value(data.generator)
        None => ()
      }
    BuiltinFunction::AsyncFunctionResolveFunction =>
      match async_function_resolve_func_data_get(builtin.id) {
        Some(data) => visit_value(data.generator)
        None => ()
      }
    _ => ()
  }
}

///|
fn gc_visit_builtin_children(
  builtin : BuiltinValue,
  visit_value : (Value) -> Unit,
  visit_env : (Env) -> Unit,
) -> Unit {
  gc_visit_proto(builtin.proto, visit_value)
  gc_visit_props(builtin.props, visit_value)
  match builtin.this_value {
    Some(value) => visit_value(value)
    None => ()
  }
  match builtin.realm_env {
    Some(env) => visit_env(env)
    None => ()
  }
  gc_visit_builtin_extra_children(builtin, visit_value)
}

///|
fn gc_visit_array_children(
  arr : ArrayValue,
  visit_value : (Value) -> Unit,
) -> Unit {
  gc_visit_proto(arr.proto, visit_value)
  gc_visit_props(arr.props, visit_value)
  for entry in arr.elements {
    match entry {
      Some(value) => visit_value(value)
      None => ()
    }
  }
  match arr.typed_array_data {
    Some(data) => visit_value(Object(data.buffer))
    None => ()
  }
}

///|
fn gc_visit_arguments_children(
  args : ArgumentsValue,
  visit_value : (Value) -> Unit,
  visit_env : (Env) -> Unit,
) -> Unit {
  gc_visit_proto(args.proto, visit_value)
  gc_visit_props(args.props, visit_value)
  visit_env(args.env)
  for entry in args.elements {
    match entry {
      Some(value) => visit_value(value)
      None => ()
    }
  }
}

///|
fn gc_visit_value_children(
  state : GcState,
  value : Value,
  visit_value : (Value) -> Unit,
  visit_env : (Env) -> Unit,
  collect_weak : Bool,
) -> Unit {
  match value {
    Object(obj) =>
      gc_visit_object_children(state, obj, visit_value, visit_env, collect_weak)
    Function(func) => gc_visit_function_children(func, visit_value, visit_env)
    BoundFunction(bound) => gc_visit_bound_function_children(bound, visit_value)
    Builtin(builtin) =>
      gc_visit_builtin_children(builtin, visit_value, visit_env)
    Array(arr) => gc_visit_array_children(arr, visit_value)
    Arguments(args) => gc_visit_arguments_children(args, visit_value, visit_env)
    _ => ()
  }
}

///|
fn gc_count_roots(state : GcState) -> Unit {
  match root_env() {
    Some(env) => gc_incref_env(state, env)
    None => ()
  }
  for env in env_stack() {
    gc_incref_env(state, env)
  }
  for func in current_function_stack() {
    gc_incref_value(state, Function(func))
  }
  gc_value_root_ref().update(fn(stack) {
    for value in stack {
      gc_incref_value(state, value)
    }
    stack
  })
  bytecode_visit_roots(fn(value) { gc_incref_value(state, value) })
  gc_incref_object_ref(state, object_proto_ref)
  gc_incref_value_ref(state, function_proto_ref)
  gc_incref_object_ref(state, array_proto_ref)
  gc_incref_object_ref(state, array_buffer_proto_ref)
  gc_incref_object_ref(state, shared_array_buffer_proto_ref)
  gc_incref_object_ref(state, dataview_proto_ref)
  gc_incref_object_ref(state, typed_array_proto_ref)
  gc_incref_object_ref(state, uint8_array_proto_ref)
  gc_incref_object_ref(state, int8_array_proto_ref)
  gc_incref_object_ref(state, uint8_clamped_array_proto_ref)
  gc_incref_object_ref(state, uint16_array_proto_ref)
  gc_incref_object_ref(state, int16_array_proto_ref)
  gc_incref_object_ref(state, uint32_array_proto_ref)
  gc_incref_object_ref(state, int32_array_proto_ref)
  gc_incref_object_ref(state, bigint64_array_proto_ref)
  gc_incref_object_ref(state, biguint64_array_proto_ref)
  gc_incref_object_ref(state, float16_array_proto_ref)
  gc_incref_object_ref(state, float32_array_proto_ref)
  gc_incref_object_ref(state, float64_array_proto_ref)
  gc_incref_object_ref(state, string_proto_ref)
  gc_incref_object_ref(state, bool_proto_ref)
  gc_incref_object_ref(state, number_proto_ref)
  gc_incref_object_ref(state, bigint_proto_ref)
  gc_incref_object_ref(state, symbol_proto_ref)
  gc_incref_object_ref(state, regexp_proto_ref)
  gc_incref_object_ref(state, date_proto_ref)
  gc_incref_object_ref(state, generator_proto_ref)
  gc_incref_object_ref(state, generator_function_proto_ref)
  gc_incref_object_ref(state, async_function_proto_ref)
  gc_incref_object_ref(state, generator_function_ctor_ref)
  gc_incref_object_ref(state, async_generator_proto_ref)
  gc_incref_object_ref(state, async_generator_function_proto_ref)
  gc_incref_object_ref(state, async_generator_function_ctor_ref)
  gc_incref_object_ref(state, iterator_proto_ref)
  gc_incref_object_ref(state, async_iterator_proto_ref)
  gc_incref_object_ref(state, async_from_sync_iterator_proto_ref)
  gc_incref_object_ref(state, iterator_concat_proto_ref)
  gc_incref_object_ref(state, iterator_helper_proto_ref)
  gc_incref_object_ref(state, iterator_wrap_proto_ref)
  gc_incref_object_ref(state, array_iterator_proto_ref)
  gc_incref_object_ref(state, string_iterator_proto_ref)
  gc_incref_object_ref(state, regexp_string_iterator_proto_ref)
  gc_incref_object_ref(state, map_iterator_proto_ref)
  gc_incref_object_ref(state, set_iterator_proto_ref)
  gc_incref_object_ref(state, map_proto_ref)
  gc_incref_object_ref(state, set_proto_ref_cell)
  gc_incref_object_ref(state, weakmap_proto_ref)
  gc_incref_object_ref(state, weakset_proto_ref_cell)
  gc_incref_object_ref(state, weakref_proto_ref)
  gc_incref_object_ref(state, finreg_proto_ref)
  gc_incref_object_ref(state, promise_proto_ref)
  gc_incref_object_ref(state, error_proto_ref)
  gc_incref_object_ref(state, aggregate_error_proto_ref)
  gc_incref_object_ref(state, eval_error_proto_ref)
  gc_incref_object_ref(state, syntax_error_proto_ref)
  gc_incref_object_ref(state, range_error_proto_ref)
  gc_incref_object_ref(state, type_error_proto_ref)
  gc_incref_object_ref(state, reference_error_proto_ref)
  gc_incref_object_ref(state, uri_error_proto_ref)
  gc_incref_symbol_ref(state, symbol_iterator_ref)
  gc_incref_symbol_ref(state, symbol_to_string_tag_ref)
  gc_incref_symbol_ref(state, symbol_async_iterator_ref)
  gc_incref_symbol_ref(state, symbol_has_instance_ref)
  gc_incref_symbol_ref(state, symbol_is_concat_spreadable_ref)
  gc_incref_symbol_ref(state, symbol_match_ref)
  gc_incref_symbol_ref(state, symbol_match_all_ref)
  gc_incref_symbol_ref(state, symbol_replace_ref)
  gc_incref_symbol_ref(state, symbol_search_ref)
  gc_incref_symbol_ref(state, symbol_species_ref)
  gc_incref_symbol_ref(state, symbol_split_ref)
  gc_incref_symbol_ref(state, symbol_to_primitive_ref)
  gc_incref_symbol_ref(state, symbol_unscopables_ref)
  symbol_registry_ref.update(fn(current) {
    for _, symbol in current {
      gc_incref_symbol(state, symbol)
    }
    current
  })
  throw_type_error_ref.update(fn(table) {
    for _, value in table {
      gc_incref_value(state, value)
    }
    table
  })
  template_registry_ref.update(fn(table) {
    for _, registry in table {
      for _, value in registry {
        gc_incref_value(state, value)
      }
    }
    table
  })
  generator_proto_env_ref.update(fn(table) {
    for _, obj in table {
      gc_incref_value(state, Object(obj))
    }
    table
  })
  async_generator_proto_env_ref.update(fn(table) {
    for _, obj in table {
      gc_incref_value(state, Object(obj))
    }
    table
  })
  generator_function_ctor_env_ref.update(fn(table) {
    for _, builtin in table {
      gc_incref_value(state, Builtin(builtin))
    }
    table
  })
  async_generator_function_ctor_env_ref.update(fn(table) {
    for _, builtin in table {
      gc_incref_value(state, Builtin(builtin))
    }
    table
  })
  async_function_ctor_env_ref.update(fn(table) {
    for _, builtin in table {
      gc_incref_value(state, Builtin(builtin))
    }
    table
  })
  async_iterator_proto_env_ref.update(fn(table) {
    for _, obj in table {
      gc_incref_value(state, Object(obj))
    }
    table
  })
  job_queue_ref.update(fn(queue) {
    for job in queue {
      gc_incref_value(state, job.func)
      for value in job.args {
        gc_incref_value(state, value)
      }
      match job.env {
        Some(env) => gc_incref_env(state, env)
        None => ()
      }
    }
    queue
  })
  timer_queue_ref.update(fn(queue) {
    for entry in queue {
      gc_incref_value(state, entry.callback)
    }
    queue
  })
  worker_table_ref.update(fn(table) {
    for _, worker in table {
      gc_incref_env(state, worker.env)
      gc_incref_value(state, Object(worker.worker_obj))
      gc_incref_value(state, Object(worker.parent_obj))
      for value in worker.queue_to_main {
        gc_incref_value(state, value)
      }
      for value in worker.queue_to_worker {
        gc_incref_value(state, value)
      }
    }
    table
  })
  agent_table_ref.update(fn(table) {
    for _, agent in table {
      gc_incref_env(state, agent.env)
      match agent.broadcast_callback {
        Some(callback) => gc_incref_value(state, callback)
        None => ()
      }
    }
    table
  })
  module_table_ref.update(fn(table) {
    for _, record in table {
      gc_incref_value(state, Object(record.exports))
      match record.module_ns {
        Some(obj) => gc_incref_value(state, Object(obj))
        None => ()
      }
      match record.meta_obj {
        Some(obj) => gc_incref_value(state, Object(obj))
        None => ()
      }
      match record.eval_promise {
        Some(value) => gc_incref_value(state, value)
        None => ()
      }
    }
    table
  })
  module_exports_stack_ref.update(fn(stack) {
    for obj in stack {
      gc_incref_value(state, Object(obj))
    }
    stack
  })
  module_eval_info_ref.update(fn(table) {
    for _, info in table {
      match info.eval_exception {
        Some(value) => gc_incref_value(state, value)
        None => ()
      }
      match info.resolve {
        Some(value) => gc_incref_value(state, value)
        None => ()
      }
      match info.reject {
        Some(value) => gc_incref_value(state, value)
        None => ()
      }
    }
    table
  })
  module_import_dynamic_job_data_ref.update(fn(table) {
    for _, data in table {
      gc_incref_value(state, data.resolve)
      gc_incref_value(state, data.reject)
      gc_incref_value(state, data.attributes)
    }
    table
  })
  module_builtin_namespace_ref.update(fn(table) {
    for _, ns_map in table {
      for _, obj in ns_map {
        gc_incref_value(state, Object(obj))
      }
    }
    table
  })
  module_env_exports_ref.update(fn(table) {
    for _, obj in table {
      gc_incref_value(state, Object(obj))
    }
    table
  })
  module_env_ref.update(fn(table) {
    for _, env in table {
      gc_incref_env(state, env)
    }
    table
  })
}
