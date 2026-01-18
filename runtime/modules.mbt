///|
priv enum ModuleState {
  Loading
  Loaded
}

///|
priv enum ModuleEvalStatus {
  NotEvaluated
  Evaluating
  EvaluatingAsync
  Evaluated
}

///|
priv struct ModuleRecord {
  exports : ObjectValue
  state : ModuleState
  module_ns : ObjectValue?
  meta_obj : ObjectValue?
  eval_promise : Value?
}

///|
priv struct ModuleEvalInfo {
  script : @engine.Script
  env_id : Int
  deps : Array[String]
  has_tla : Bool
  mut status : ModuleEvalStatus
  mut dfs_index : Int
  mut dfs_ancestor_index : Int
  mut pending_async_deps : Int
  async_parents : Array[String]
  mut async_timestamp : Int64
  mut async_evaluation : Bool
  mut cycle_root : String?
  mut eval_has_exception : Bool
  mut eval_exception : Value?
  mut resolve : Value?
  mut reject : Value?
}

///|
priv struct ModuleExportInfo {
  local_exports : Map[String, String]
  indirect_exports : Map[String, (String, String)]
  star_exports : Array[String]
}

///|
priv struct ModuleResolveExportResult {
  status : Int
  path : String?
  local_name : String?
}

///|
priv struct ModuleImportDynamicJobData {
  resolve : Value
  reject : Value
  specifier : String
  attributes : Value
  base_path : String?
}

///|
let module_table_ref : Ref[Map[String, ModuleRecord]] = Ref::new(Map::new())

///|
let module_exports_stack_ref : Ref[Array[ObjectValue]] = Ref::new([])

///|
let module_export_info_ref : Ref[Map[String, ModuleExportInfo]] = Ref::new(
  Map::new(),
)

///|
let module_eval_info_ref : Ref[Map[String, ModuleEvalInfo]] = Ref::new(
  Map::new(),
)

///|
let module_async_eval_timestamp_ref : Ref[Int64] = Ref::new(1)

///|
let module_async_callback_data_ref : Ref[Map[Int, String]] = Ref::new(
  Map::new(),
)

///|
let module_import_dynamic_job_data_ref : Ref[
  Map[Int, ModuleImportDynamicJobData],
] = Ref::new(Map::new())

///|
let module_builtin_namespace_ref : Ref[Map[Int, Map[String, ObjectValue]]] = Ref::new(
  Map::new(),
)

///|
let module_env_stack_ref : Ref[Array[Int]] = Ref::new([])

///|
let module_import_stack_ref : Ref[Array[Array[String]]] = Ref::new([])

///|
let module_env_exports_ref : Ref[Map[Int, ObjectValue]] = Ref::new(Map::new())

///|
let module_env_exports_by_obj_ref : Ref[Map[Int, Int]] = Ref::new(Map::new())

///|
let module_env_export_names_ref : Ref[Map[Int, Map[String, Array[String]]]] = Ref::new(
  Map::new(),
)

///|
let module_env_export_uninitialized_ref : Ref[Map[Int, Map[String, Bool]]] = Ref::new(
  Map::new(),
)

///|
let module_env_ref : Ref[Map[Int, Env]] = Ref::new(Map::new())

///|
fn push_module_exports(exports : ObjectValue) -> Unit {
  module_exports_stack_ref.update(fn(current) {
    current.push(exports)
    current
  })
}

///|
fn pop_module_exports() -> Unit {
  module_exports_stack_ref.update(fn(current) {
    if !current.is_empty() {
      let _ = current.pop()

    }
    current
  })
}

///|
fn current_module_exports() -> ObjectValue? {
  let mut value : ObjectValue? = None
  module_exports_stack_ref.update(fn(current) {
    if !current.is_empty() {
      value = Some(current[current.length() - 1])
    }
    current
  })
  value
}

///|
fn with_module_exports(
  exports : ObjectValue,
  f : () -> Value raise,
) -> Value raise {
  push_module_exports(exports)
  try {
    let result = f()
    pop_module_exports()
    result
  } catch {
    err => {
      pop_module_exports()
      raise err
    }
  }
}

///|
fn with_module_exports_step(
  exports : ObjectValue,
  f : () -> GenStep raise,
) -> GenStep raise {
  push_module_exports(exports)
  try {
    let result = f()
    pop_module_exports()
    result
  } catch {
    err => {
      pop_module_exports()
      raise err
    }
  }
}

///|
fn push_module_env_id(id : Int) -> Unit {
  module_env_stack_ref.update(fn(current) {
    current.push(id)
    current
  })
}

///|
fn pop_module_env_id() -> Unit {
  module_env_stack_ref.update(fn(current) {
    if !current.is_empty() {
      let _ = current.pop()

    }
    current
  })
}

///|
fn current_module_env_id() -> Int? {
  let mut value : Int? = None
  module_env_stack_ref.update(fn(current) {
    if !current.is_empty() {
      value = Some(current[current.length() - 1])
    }
    current
  })
  value
}

///|
fn push_module_imports() -> Unit {
  module_import_stack_ref.update(fn(current) {
    current.push([])
    current
  })
}

///|
fn pop_module_imports() -> Array[String] {
  let mut value : Array[String] = []
  module_import_stack_ref.update(fn(current) {
    if !current.is_empty() {
      value = current.pop().unwrap_or([])
    }
    current
  })
  value
}

///|
fn record_module_import(cache_key : String) -> Unit {
  module_import_stack_ref.update(fn(current) {
    if !current.is_empty() {
      current[current.length() - 1].push(cache_key)
    }
    current
  })
}

///|
fn with_module_env_id(env_id : Int, f : () -> Value raise) -> Value raise {
  push_module_env_id(env_id)
  try {
    let result = f()
    pop_module_env_id()
    result
  } catch {
    err => {
      pop_module_env_id()
      raise err
    }
  }
}

///|
fn with_module_env_id_step(
  env_id : Int,
  f : () -> GenStep raise,
) -> GenStep raise {
  push_module_env_id(env_id)
  try {
    let result = f()
    pop_module_env_id()
    result
  } catch {
    err => {
      pop_module_env_id()
      raise err
    }
  }
}

///|
fn module_env_exports_set(env_id : Int, exports : ObjectValue) -> Unit {
  module_env_exports_ref.update(fn(table) {
    table.set(env_id, exports)
    table
  })
  module_env_exports_by_obj_ref.update(fn(table) {
    table.set(exports.id, env_id)
    table
  })
}

///|
fn module_env_exports_remove(env_id : Int) -> Unit {
  let mut exports_id : Int? = None
  module_env_exports_ref.update(fn(table) {
    match table.get(env_id) {
      Some(exports) => exports_id = Some(exports.id)
      None => ()
    }
    table.remove(env_id)
    table
  })
  module_env_exports_by_obj_ref.update(fn(table) {
    match exports_id {
      Some(id) => table.remove(id)
      None => ()
    }
    table
  })
}

///|
fn module_env_exports_get(env_id : Int) -> ObjectValue? {
  let mut value : ObjectValue? = None
  module_env_exports_ref.update(fn(table) {
    value = table.get(env_id)
    table
  })
  value
}

///|
fn module_env_id_for_exports_obj(exports : ObjectValue) -> Int? {
  let mut value : Int? = None
  module_env_exports_by_obj_ref.update(fn(table) {
    value = table.get(exports.id)
    table
  })
  value
}

///|
fn module_env_set(env : Env) -> Unit {
  module_env_ref.update(fn(table) {
    table.set(env.id, env)
    table
  })
}

///|
fn module_env_get(env_id : Int) -> Env? {
  let mut value : Env? = None
  module_env_ref.update(fn(table) {
    value = table.get(env_id)
    table
  })
  value
}

///|
fn module_env_remove(env_id : Int) -> Unit {
  module_env_ref.update(fn(table) {
    table.remove(env_id)
    table
  })
}

///|
fn sync_module_env(env : Env) -> Unit {
  let mut should_sync = false
  module_env_ref.update(fn(table) {
    should_sync = table.contains(env.id)
    table
  })
  if should_sync {
    module_env_set(env)
  }
}

///|
fn module_env_export_names_add(
  env_id : Int,
  binding : String,
  export_name : String,
) -> Unit {
  module_env_export_names_ref.update(fn(table) {
    let names = match table.get(env_id) {
      Some(existing) => existing
      None => Map::new()
    }
    let exports = match names.get(binding) {
      Some(existing) => existing
      None => []
    }
    if !exports.contains(export_name) {
      exports.push(export_name)
    }
    names.set(binding, exports)
    table.set(env_id, names)
    table
  })
}

///|
fn module_env_export_uninitialized_add(
  env_id : Int,
  export_name : String,
) -> Unit {
  module_env_export_uninitialized_ref.update(fn(table) {
    let names = match table.get(env_id) {
      Some(existing) => existing
      None => Map::new()
    }
    names.set(export_name, true)
    table.set(env_id, names)
    table
  })
}

///|
fn module_env_export_uninitialized_has(
  env_id : Int,
  export_name : String,
) -> Bool {
  let mut result = false
  module_env_export_uninitialized_ref.update(fn(table) {
    match table.get(env_id) {
      Some(names) => result = names.contains(export_name)
      None => ()
    }
    table
  })
  result
}

///|
fn module_env_export_uninitialized_remove(
  env_id : Int,
  export_name : String,
) -> Unit {
  module_env_export_uninitialized_ref.update(fn(table) {
    match table.get(env_id) {
      Some(names) => {
        names.remove(export_name)
        if names.is_empty() {
          table.remove(env_id)
        } else {
          table.set(env_id, names)
        }
      }
      None => ()
    }
    table
  })
}

///|
fn module_env_export_names_get(
  env_id : Int,
  binding : String,
) -> Array[String]? {
  let mut result : Array[String]? = None
  module_env_export_names_ref.update(fn(table) {
    match table.get(env_id) {
      Some(names) => result = names.get(binding)
      None => ()
    }
    table
  })
  result
}

///|
fn module_env_export_binding_for_name(
  env_id : Int,
  export_name : String,
) -> String? {
  let mut result : String? = None
  module_env_export_names_ref.update(fn(table) {
    match table.get(env_id) {
      Some(names) =>
        for binding, exports in names {
          if exports.contains(export_name) {
            result = Some(binding)
            break
          }
        }
      None => ()
    }
    table
  })
  result
}

///|
fn module_env_export_names_all(env_id : Int) -> Map[String, Array[String]]? {
  let mut result : Map[String, Array[String]]? = None
  module_env_export_names_ref.update(fn(table) {
    result = table.get(env_id)
    table
  })
  result
}

///|
fn module_env_export_names_remove(env_id : Int) -> Unit {
  module_env_export_names_ref.update(fn(table) {
    table.remove(env_id)
    table
  })
  module_env_export_uninitialized_ref.update(fn(table) {
    table.remove(env_id)
    table
  })
}

///|
fn update_module_export_binding(
  env : Env,
  binding : String,
  value : Value,
) -> Unit {
  match module_env_exports_get(env.id) {
    Some(exports) =>
      match module_env_export_names_get(env.id, binding) {
        Some(names) =>
          for export_name in names {
            match exports.props.get(export_name) {
              Some(prop) =>
                exports.props.set(export_name, Property::{
                  value,
                  writable: prop.writable,
                  configurable: prop.configurable,
                  enumerable: prop.enumerable,
                  getter: prop.getter,
                  setter: prop.setter,
                })
              None => ()
            }
          }
        None => ()
      }
    None => ()
  }
}

///|
fn module_record_get(path : String) -> ModuleRecord? {
  let mut value : ModuleRecord? = None
  module_table_ref.update(fn(table) {
    value = table.get(path)
    table
  })
  value
}

///|
fn module_record_set(path : String, record : ModuleRecord) -> Unit {
  module_table_ref.update(fn(table) {
    table.set(path, record)
    table
  })
}

///|
fn module_eval_promise_get(path : String) -> Value? {
  match module_record_get(path) {
    Some(record) => record.eval_promise
    None => None
  }
}

///|
fn module_eval_promise_set(path : String, promise : Value?) -> Unit {
  match module_record_get(path) {
    Some(record) =>
      module_record_set(path, ModuleRecord::{
        exports: record.exports,
        state: record.state,
        module_ns: record.module_ns,
        meta_obj: record.meta_obj,
        eval_promise: promise,
      })
    None => ()
  }
}

///|
fn module_eval_info_get(path : String) -> ModuleEvalInfo? {
  let mut value : ModuleEvalInfo? = None
  module_eval_info_ref.update(fn(table) {
    value = table.get(path)
    table
  })
  value
}

///|
fn module_eval_info_set(path : String, info : ModuleEvalInfo) -> Unit {
  module_eval_info_ref.update(fn(table) {
    table.set(path, info)
    table
  })
}

///|
fn module_eval_info_remove(path : String) -> Unit {
  module_eval_info_ref.update(fn(table) {
    table.remove(path)
    table
  })
}

///|
fn module_export_info_get(path : String) -> ModuleExportInfo? {
  let mut value : ModuleExportInfo? = None
  module_export_info_ref.update(fn(table) {
    value = table.get(path)
    table
  })
  value
}

///|
fn module_export_info_set(path : String, info : ModuleExportInfo) -> Unit {
  module_export_info_ref.update(fn(table) {
    table.set(path, info)
    table
  })
}

///|
fn module_export_info_remove(path : String) -> Unit {
  module_export_info_ref.update(fn(table) {
    table.remove(path)
    table
  })
}

///|
fn module_record_remove(path : String) -> Unit {
  module_table_ref.update(fn(table) {
    table.remove(path)
    table
  })
  module_export_info_remove(path)
}

///|
fn module_builtin_namespace_get(env_id : Int, name : String) -> ObjectValue? {
  let mut value : ObjectValue? = None
  module_builtin_namespace_ref.update(fn(table) {
    match table.get(env_id) {
      Some(namespaces) => value = namespaces.get(name)
      None => ()
    }
    table
  })
  value
}

///|
fn module_builtin_namespace_set(
  env_id : Int,
  name : String,
  value : ObjectValue,
) -> Unit {
  module_builtin_namespace_ref.update(fn(table) {
    let namespaces = match table.get(env_id) {
      Some(existing) => existing
      None => Map::new()
    }
    namespaces.set(name, value)
    table.set(env_id, namespaces)
    table
  })
}
