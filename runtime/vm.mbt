///|
pub struct Env {
  id : Int
  parent : Env?
  bindings : @hashmap.HashMap[String, Value]
  readonly_bindings : @hashmap.HashMap[String, Bool]
  deletable_bindings : @hashmap.HashMap[String, Bool]
  private_bindings : @hashmap.HashMap[String, String]
  mut strict : Bool
  mut with_object : Value?
  mut var_object : Value?
  mut new_target : Value
  mut allow_new_target : Bool
  mut allow_super : Bool
  mut allow_super_call : Bool
  mut this_uninitialized : Bool
  mut is_param_env : Bool
  mut is_catch_env : Bool
  param_binding_names : @hashmap.HashMap[String, Bool]
  uninitialized_bindings : @hashmap.HashMap[String, Bool]
  mut var_env : Env?
}

///|
pub impl Show for Env with output(self, logger) {
  let _ = self
  logger.write_string("Env")
}

///|
pub struct Vm {
  env : Env
  runtime : RuntimeState
} derive(Show)

///|
priv suberror ReturnSignal {
  ReturnSignal(Value)
}

///|
priv suberror BreakSignal {
  BreakSignal((String?, Value?))
}

///|
priv suberror ContinueSignal {
  ContinueSignal((String?, Value?))
}

///|
priv suberror ThrowSignal {
  ThrowSignal(Value)
}

///|
let id_ref : Ref[Int] = Ref::new(1)

///|
let throw_type_error_ref : Ref[Map[Int, Value]] = Ref::new(Map::new())

///|
let template_registry_ref : Ref[Map[Int, Map[Int, Value]]] = Ref::new(
  Map::new(),
)

///|
let js_string_len_max : Int = (1 << 30) - 1

///|
let max_array_index : Int64 = 4294967294

///|
let max_array_length : Int64 = max_array_index + 1L

///|

///|
pub fn Env::new(parent : Env?) -> Env {
  let strict = match parent {
    Some(parent_env) => parent_env.strict
    None => false
  }
  let new_target = match parent {
    Some(parent_env) => parent_env.new_target
    None => Undefined
  }
  let allow_new_target = match parent {
    Some(parent_env) => parent_env.allow_new_target
    None => false
  }
  let allow_super = match parent {
    Some(parent_env) => parent_env.allow_super
    None => false
  }
  let allow_super_call = match parent {
    Some(parent_env) => parent_env.allow_super_call
    None => false
  }
  let env = Env::{
    id: alloc_id(),
    parent,
    bindings: @hashmap.new(),
    readonly_bindings: @hashmap.new(),
    deletable_bindings: @hashmap.new(),
    private_bindings: @hashmap.new(),
    strict,
    with_object: None,
    var_object: None,
    new_target,
    allow_new_target,
    allow_super,
    allow_super_call,
    this_uninitialized: false,
    is_param_env: false,
    is_catch_env: false,
    param_binding_names: @hashmap.new(),
    uninitialized_bindings: @hashmap.new(),
    var_env: None,
  }
  register_env(env)
  env
}

///|
pub fn reset_runtime_state() -> Unit {
  run_gc_without_weak()
  gc_release_all()
  gc_alloc_count_ref().update(fn(_) { 0 })
  gc_threshold_ref().update(fn(_) { gc_default_threshold })
  gc_value_root_ref().update(fn(_) { [] })
  gc_pending_ref().update(fn(_) { false })
  id_ref.update(fn(_) { 1 })
  symbol_registry_ref.update(fn(_) { Map::new() })
  symbol_table_ref.update(fn(_) { Map::new() })
  symbol_iterator_ref.update(fn(_) { None })
  symbol_to_string_tag_ref.update(fn(_) { None })
  symbol_async_iterator_ref.update(fn(_) { None })
  symbol_has_instance_ref.update(fn(_) { None })
  symbol_is_concat_spreadable_ref.update(fn(_) { None })
  symbol_match_ref.update(fn(_) { None })
  symbol_match_all_ref.update(fn(_) { None })
  symbol_replace_ref.update(fn(_) { None })
  symbol_search_ref.update(fn(_) { None })
  symbol_species_ref.update(fn(_) { None })
  symbol_split_ref.update(fn(_) { None })
  symbol_to_primitive_ref.update(fn(_) { None })
  symbol_unscopables_ref.update(fn(_) { None })
  throw_type_error_ref.update(fn(_) { Map::new() })
  template_registry_ref.update(fn(_) { Map::new() })
  annex_b_var_func_decls_ref.update(fn(_) { [] })
  top_level_func_decls_ref.update(fn(_) { [] })
  object_proto_ref.update(fn(_) { None })
  function_proto_ref.update(fn(_) { None })
  array_proto_ref.update(fn(_) { None })
  string_proto_ref.update(fn(_) { None })
  dataview_proto_ref.update(fn(_) { None })
  typed_array_proto_ref.update(fn(_) { None })
  uint8_array_proto_ref.update(fn(_) { None })
  int8_array_proto_ref.update(fn(_) { None })
  uint8_clamped_array_proto_ref.update(fn(_) { None })
  uint16_array_proto_ref.update(fn(_) { None })
  int16_array_proto_ref.update(fn(_) { None })
  uint32_array_proto_ref.update(fn(_) { None })
  int32_array_proto_ref.update(fn(_) { None })
  bigint64_array_proto_ref.update(fn(_) { None })
  biguint64_array_proto_ref.update(fn(_) { None })
  float16_array_proto_ref.update(fn(_) { None })
  float32_array_proto_ref.update(fn(_) { None })
  float64_array_proto_ref.update(fn(_) { None })
  bool_proto_ref.update(fn(_) { None })
  number_proto_ref.update(fn(_) { None })
  bigint_proto_ref.update(fn(_) { None })
  symbol_proto_ref.update(fn(_) { None })
  regexp_proto_ref.update(fn(_) { None })
  date_proto_ref.update(fn(_) { None })
  generator_proto_ref.update(fn(_) { None })
  generator_function_proto_ref.update(fn(_) { None })
  async_function_proto_ref.update(fn(_) { None })
  generator_function_ctor_ref.update(fn(_) { None })
  async_generator_proto_ref.update(fn(_) { None })
  async_generator_function_proto_ref.update(fn(_) { None })
  async_generator_function_ctor_ref.update(fn(_) { None })
  iterator_proto_ref.update(fn(_) { None })
  async_iterator_proto_ref.update(fn(_) { None })
  async_from_sync_iterator_proto_ref.update(fn(_) { None })
  generator_proto_env_ref.update(fn(_) { Map::new() })
  async_generator_proto_env_ref.update(fn(_) { Map::new() })
  generator_function_ctor_env_ref.update(fn(_) { Map::new() })
  async_generator_function_ctor_env_ref.update(fn(_) { Map::new() })
  async_function_ctor_env_ref.update(fn(_) { Map::new() })
  promise_ctor_env_ref.update(fn(_) { Map::new() })
  async_iterator_proto_env_ref.update(fn(_) { Map::new() })
  iterator_concat_proto_ref.update(fn(_) { None })
  iterator_helper_proto_ref.update(fn(_) { None })
  iterator_wrap_proto_ref.update(fn(_) { None })
  array_iterator_proto_ref.update(fn(_) { None })
  string_iterator_proto_ref.update(fn(_) { None })
  regexp_string_iterator_proto_ref.update(fn(_) { None })
  map_iterator_proto_ref.update(fn(_) { None })
  set_iterator_proto_ref.update(fn(_) { None })
  map_proto_ref.update(fn(_) { None })
  set_proto_ref_cell.update(fn(_) { None })
  weakmap_proto_ref.update(fn(_) { None })
  weakset_proto_ref_cell.update(fn(_) { None })
  weakref_proto_ref.update(fn(_) { None })
  finreg_proto_ref.update(fn(_) { None })
  promise_proto_ref.update(fn(_) { None })
  error_proto_ref.update(fn(_) { None })
  aggregate_error_proto_ref.update(fn(_) { None })
  eval_error_proto_ref.update(fn(_) { None })
  syntax_error_proto_ref.update(fn(_) { None })
  range_error_proto_ref.update(fn(_) { None })
  type_error_proto_ref.update(fn(_) { None })
  reference_error_proto_ref.update(fn(_) { None })
  uri_error_proto_ref.update(fn(_) { None })
  root_env_ref.update(fn(_) { None })
  env_stack_ref.update(fn(_) { [] })
  source_stack_ref.update(fn(_) { [] })
  expr_pos_stack_ref.update(fn(_) { [] })
  source_path_stack_ref.update(fn(_) { [] })
  math_rand_state_ref.update(fn(_) { 0x6a09e667f3bcc909UL })
  current_function_stack_ref.update(fn(_) { [] })
  current_generator_data_ref.update(fn(_) { None })
  print_log_ref().update(fn(_) { [] })
  async_done_ref().update(fn(_) { 0 })
  promise_table_ref.update(fn(_) { Map::new() })
  promise_resolve_func_data_ref.update(fn(_) { Map::new() })
  promise_executor_data_ref.update(fn(_) { Map::new() })
  promise_all_element_data_ref.update(fn(_) { Map::new() })
  promise_then_finally_data_ref.update(fn(_) { Map::new() })
  async_from_sync_unwrap_data_ref.update(fn(_) { Map::new() })
  async_from_sync_close_wrap_data_ref.update(fn(_) { Map::new() })
  async_generator_resolve_func_data_ref.update(fn(_) { Map::new() })
  async_function_resolve_func_data_ref.update(fn(_) { Map::new() })
  async_function_data_ref.update(fn(_) { Map::new() })
  promise_finally_value_data_ref.update(fn(_) { Map::new() })
  job_queue_ref.update(fn(_) { [] })
  timer_queue_ref.update(fn(_) { [] })
  next_timer_id_ref.update(fn(_) { 1 })
  worker_table_ref.update(fn(_) { Map::new() })
  next_worker_id_ref.update(fn(_) { 1 })
  agent_table_ref.update(fn(_) { Map::new() })
  agent_report_queue_ref.update(fn(_) { [] })
  agent_lock_queue_ref.update(fn(_) { [] })
  agent_spin_load_ref.update(fn(_) { Map::new() })
  next_agent_id_ref.update(fn(_) { 1 })
  current_agent_id_ref.update(fn(_) { None })
  agent_clock_ref.update(fn(_) { 0 })
  can_block_ref().update(fn(_) { true })
  atomics_waiter_table_ref.update(fn(_) { Map::new() })
  atomics_waiter_list_ref.update(fn(_) { [] })
  atomics_waiter_next_id_ref.update(fn(_) { 1 })
  atomics_waiter_stack_ref.update(fn(_) { Map::new() })
  atomics_notify_time_ref.update(fn(_) { Map::new() })
  module_table_ref.update(fn(_) { Map::new() })
  module_exports_stack_ref.update(fn(_) { [] })
  module_export_info_ref.update(fn(_) { Map::new() })
  module_eval_info_ref.update(fn(_) { Map::new() })
  module_async_eval_timestamp_ref.update(fn(_) { 1 })
  module_async_callback_data_ref.update(fn(_) { Map::new() })
  module_import_dynamic_job_data_ref.update(fn(_) { Map::new() })
  module_builtin_namespace_ref.update(fn(_) { Map::new() })
  module_env_stack_ref.update(fn(_) { [] })
  module_import_stack_ref.update(fn(_) { [] })
  module_env_exports_ref.update(fn(_) { Map::new() })
  module_env_exports_by_obj_ref.update(fn(_) { Map::new() })
  module_env_export_names_ref.update(fn(_) { Map::new() })
  module_env_export_uninitialized_ref.update(fn(_) { Map::new() })
  module_env_ref.update(fn(_) { Map::new() })
}

///|
pub fn set_can_block(flag : Bool) -> Unit {
  can_block_ref().update(fn(_) { flag })
}

///|
fn can_block() -> Bool {
  let mut value = true
  can_block_ref().update(fn(current) {
    value = current
    current
  })
  value
}

///|
pub fn Vm::new() -> Vm {
  let runtime = RuntimeState::new()
  let _ = set_current_runtime_state(runtime)
  let env = Env::new(None)
  set_root_env(env)
  let (
    object_proto_obj,
    object_proto_value,
    function_proto_value,
    generator_function_proto_obj,
    async_generator_function_proto_obj,
    async_function_proto_obj,
    array_proto_obj,
    array_proto_value,
    array_values_func,
    array_buffer_proto_obj,
    array_buffer_proto_value,
    shared_array_buffer_proto_obj,
    shared_array_buffer_proto_value,
    dataview_proto_obj,
    dataview_proto_value,
  ) = init_core_protos(env)
  let (
    string_proto_obj,
    string_proto_value,
    number_proto_obj,
    number_proto_value,
    bool_proto_obj,
    bool_proto_value,
    bigint_proto_obj,
    bigint_proto_value,
    symbol_proto_obj,
    symbol_proto_value,
    regexp_proto_obj,
    regexp_proto_value,
    date_proto_obj,
    date_proto_value,
  ) = init_primitive_protos(object_proto_obj)
  let (
    iterator_proto_obj,
    map_proto_obj,
    map_proto_value,
    map_entries_func,
    set_proto_obj,
    set_proto_value,
    set_values_func,
    weakmap_proto_obj,
    weakmap_proto_value,
    weakset_proto_obj,
    weakset_proto_value,
    weakref_proto_obj,
    weakref_proto_value,
    finreg_proto_obj,
    finreg_proto_value,
  ) = init_iterator_and_collection_protos(
    object_proto_obj, generator_function_proto_obj, async_generator_function_proto_obj,
  )
  let (promise_proto_obj, promise_proto_value) = init_promise_proto(
    object_proto_obj,
  )
  let (
    error_ctor,
    aggregate_error_ctor,
    eval_error_ctor,
    syntax_error_ctor,
    range_error_ctor,
    type_error_ctor,
    reference_error_ctor,
    uri_error_ctor,
  ) = init_error_suite(object_proto_obj)
  let global = init_global_object(env, object_proto_obj)
  let (
    object_ctor,
    function_ctor,
    string_ctor,
    number_ctor,
    bool_ctor,
    bigint_ctor,
    symbol_ctor,
    iterator_ctor,
    parse_int_func,
    parse_float_func,
    species_key,
    regexp_ctor,
    date_ctor,
    map_ctor,
    set_ctor,
    weakmap_ctor,
    weakset_ctor,
    weakref_ctor,
    finreg_ctor,
    promise_ctor,
    proxy_ctor,
    array_ctor,
    array_buffer_ctor,
    shared_array_buffer_ctor,
    dataview_ctor,
  ) = init_ctor_sets(
    env, object_proto_obj, object_proto_value, function_proto_value, generator_function_proto_obj,
    async_generator_function_proto_obj, async_function_proto_obj, string_proto_obj,
    string_proto_value, number_proto_obj, number_proto_value, bool_proto_obj, bool_proto_value,
    bigint_proto_obj, bigint_proto_value, symbol_proto_obj, symbol_proto_value, iterator_proto_obj,
    array_proto_obj, array_proto_value, array_values_func, array_buffer_proto_obj,
    array_buffer_proto_value, shared_array_buffer_proto_obj, shared_array_buffer_proto_value,
    dataview_proto_obj, dataview_proto_value, regexp_proto_obj, regexp_proto_value,
    date_proto_obj, date_proto_value, map_proto_obj, map_proto_value, map_entries_func,
    set_proto_obj, set_proto_value, set_values_func, weakmap_proto_obj, weakmap_proto_value,
    weakset_proto_obj, weakset_proto_value, weakref_proto_obj, weakref_proto_value,
    finreg_proto_obj, finreg_proto_value, promise_proto_obj, promise_proto_value,
  )
  let (
    typed_array_ctor,
    uint8_array_ctor,
    int8_array_ctor,
    uint8_clamped_array_ctor,
    uint16_array_ctor,
    int16_array_ctor,
    uint32_array_ctor,
    int32_array_ctor,
    bigint64_array_ctor,
    biguint64_array_ctor,
    float16_array_ctor,
    float32_array_ctor,
    float64_array_ctor,
  ) = init_typed_array_family(object_proto_obj, array_proto_obj, species_key)
  let (json_obj, reflect_obj, atomics_obj, math_obj) = init_standard_objects(
    object_proto_obj,
  )
  let (std_obj, os_obj, bjson_obj) = init_host_objects(object_proto_obj)
  init_global_bindings(
    env, global, object_ctor, function_ctor, array_ctor, array_buffer_ctor, shared_array_buffer_ctor,
    dataview_ctor, typed_array_ctor, uint8_array_ctor, int8_array_ctor, uint8_clamped_array_ctor,
    uint16_array_ctor, int16_array_ctor, uint32_array_ctor, int32_array_ctor, bigint64_array_ctor,
    biguint64_array_ctor, float16_array_ctor, float32_array_ctor, float64_array_ctor,
    string_ctor, number_ctor, bool_ctor, bigint_ctor, symbol_ctor, iterator_ctor,
    regexp_ctor, date_ctor, map_ctor, set_ctor, weakmap_ctor, weakset_ctor, weakref_ctor,
    finreg_ctor, promise_ctor, proxy_ctor, error_ctor, aggregate_error_ctor, eval_error_ctor,
    syntax_error_ctor, range_error_ctor, type_error_ctor, reference_error_ctor, uri_error_ctor,
    json_obj, reflect_obj, atomics_obj, math_obj, parse_int_func, parse_float_func,
    std_obj, os_obj, bjson_obj,
  )
  let _ = install_test262_helpers(env)
  Vm::{ env, runtime }
}

///|
pub fn Vm::run_gc(self : Vm) -> Unit {
  let prev = set_current_runtime_state(self.runtime)
  run_gc()
  restore_runtime_state(prev)
}

///|
pub fn Vm::set_gc_threshold(self : Vm, threshold : Int) -> Unit {
  self.runtime.gc_threshold_ref.update(fn(_) { threshold })
}

///|
pub fn Vm::set_gc_debug(self : Vm, enabled : Bool) -> Unit {
  self.runtime.gc_debug_ref.update(fn(_) { enabled })
}

///|
pub fn Vm::set_can_block(self : Vm, flag : Bool) -> Unit {
  self.runtime.can_block_ref.update(fn(_) { flag })
}

///|
pub fn Vm::reset_runtime_state(self : Vm) -> Unit {
  let prev = set_current_runtime_state(self.runtime)
  reset_runtime_state()
  restore_runtime_state(prev)
}

///|
pub fn Vm::eval(self : Vm, source : String) -> Value raise {
  let prev_runtime = set_current_runtime_state(self.runtime)
  try {
    let script = parse_script_with_pos(source)
    let result = eval_script(self.env, script)
    run_pending_timers()
    run_pending_workers()
    gc_maybe_collect_with_root(result)
    result
  } catch {
    err => {
      restore_runtime_state(prev_runtime)
      raise err
    }
  } noraise {
    value => {
      restore_runtime_state(prev_runtime)
      value
    }
  }
}

///|
pub fn Vm::eval_with_path(
  self : Vm,
  source : String,
  path : String,
) -> Value raise {
  let prev_runtime = set_current_runtime_state(self.runtime)
  try {
    let script = parse_script_with_pos(source)
    let full_path = resolve_path(path)
    let result = if script.is_module {
      let _ = eval_module_script(script, full_path)
      let promise = module_evaluate(full_path)
      let _ = await_promise_completion(promise)
      Undefined
    } else {
      with_source_path(full_path, fn() raise { eval_script(self.env, script) })
    }
    run_pending_timers()
    run_pending_workers()
    gc_maybe_collect_with_root(result)
    result
  } catch {
    err => {
      restore_runtime_state(prev_runtime)
      raise err
    }
  } noraise {
    value => {
      restore_runtime_state(prev_runtime)
      value
    }
  }
}

///|
pub fn Vm::eval_with_path_mode(
  self : Vm,
  source : String,
  path : String,
  force_module : Bool,
  force_strict : Bool,
) -> Value raise {
  let prev_runtime = set_current_runtime_state(self.runtime)
  try {
    let script = parse_script_with_pos_mode(source, force_module, force_strict)
    validate_context_in_script(script, false, false, false)
    let full_path = resolve_path(path)
    let result = if script.is_module {
      let _ = eval_module_script(script, full_path)
      let promise = module_evaluate(full_path)
      let _ = await_promise_completion(promise)
      Undefined
    } else if force_strict {
      let old_strict = self.env.strict
      self.env.strict = true
      try
        with_source_path(full_path, fn() raise { eval_script(self.env, script) })
      catch {
        err => {
          self.env.strict = old_strict
          raise err
        }
      } noraise {
        value => {
          self.env.strict = old_strict
          value
        }
      }
    } else {
      with_source_path(full_path, fn() raise { eval_script(self.env, script) })
    }
    run_pending_jobs()
    run_pending_timers()
    run_pending_jobs()
    run_pending_workers()
    run_pending_jobs()
    gc_maybe_collect_with_root(result)
    result
  } catch {
    err => {
      restore_runtime_state(prev_runtime)
      raise err
    }
  } noraise {
    value => {
      restore_runtime_state(prev_runtime)
      value
    }
  }
}

///|
pub fn Vm::eval_result(
  self : Vm,
  source : String,
) -> Result[Value, Value] raise {
  Ok(self.eval(source)) catch {
    ThrowSignal(value) => Err(value)
    err => raise err
  }
}

///|
pub fn Vm::eval_result_with_path(
  self : Vm,
  source : String,
  path : String,
) -> Result[Value, Value] raise {
  Ok(self.eval_with_path(source, path)) catch {
    ThrowSignal(value) => Err(value)
    err => raise err
  }
}

///|
pub fn Vm::eval_result_with_path_mode(
  self : Vm,
  source : String,
  path : String,
  force_module : Bool,
  force_strict : Bool,
) -> Result[Value, Value] raise {
  Ok(self.eval_with_path_mode(source, path, force_module, force_strict)) catch {
    ThrowSignal(value) => Err(value)
    err => raise err
  }
}
