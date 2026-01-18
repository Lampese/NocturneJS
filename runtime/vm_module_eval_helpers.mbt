///|
fn module_eval_next_timestamp() -> Int64 {
  let mut value : Int64 = 0
  module_async_eval_timestamp_ref.update(fn(current) {
    value = current
    current + 1
  })
  value
}

///|
fn module_is_cycle_root(path : String, info : ModuleEvalInfo) -> Bool {
  match info.cycle_root {
    Some(root) => root == path
    None => true
  }
}

///|
fn module_eval_resolvers_set(
  path : String,
  resolve : Value,
  reject : Value,
) -> Unit {
  match module_eval_info_get(path) {
    Some(info) => {
      let updated = info
      updated.resolve = Some(resolve)
      updated.reject = Some(reject)
      module_eval_info_set(path, updated)
    }
    None => ()
  }
}

///|
fn module_eval_promise_init(path : String) -> Value raise {
  match module_eval_promise_get(path) {
    Some(promise) => promise
    None => {
      let (promise, resolve, reject) = new_promise_capability(Undefined)
      module_eval_promise_set(path, Some(promise))
      module_eval_resolvers_set(path, resolve, reject)
      promise
    }
  }
}

///|
fn module_eval_resolve(path : String) -> Unit raise {
  match module_eval_info_get(path) {
    Some(info) =>
      if module_is_cycle_root(path, info) {
        match info.resolve {
          Some(resolve) => {
            let _ = call_value_with_this(resolve, [Undefined], Undefined)

          }
          None => ()
        }
      }
    None => ()
  }
}

///|
fn module_eval_reject(path : String, error : Value) -> Unit raise {
  match module_eval_info_get(path) {
    Some(info) =>
      if module_is_cycle_root(path, info) {
        match info.reject {
          Some(reject) => {
            let _ = call_value_with_this(reject, [error], Undefined)

          }
          None => ()
        }
      }
    None => ()
  }
}

///|
fn module_execute_sync(path : String) -> Unit raise {
  let info = match module_eval_info_get(path) {
    Some(value) => value
    None => return
  }
  let env = match module_env_get(info.env_id) {
    Some(value) => value
    None => return
  }
  let exports_obj = match module_record_get(path) {
    Some(record) => record.exports
    None => new_object_struct(None)
  }
  let promise = with_source_path(path, fn() raise {
    with_source_value(info.script.source, fn() raise {
      module_async_eval(
        env,
        info.script.body,
        Undefined,
        path,
        info.script.source,
        exports_obj,
      )
    })
  })
  match promise_data_from_value(promise) {
    Some(data) =>
      match data.state {
        PromiseState::Fulfilled => ()
        PromiseState::Rejected => raise ThrowSignal(data.value)
        PromiseState::Pending => {
          let _ = throw_type_error("promise is pending")
          ()
        }
      }
    None => ()
  }
}

///|
fn module_async_callback_data_set(id : Int, path : String) -> Unit {
  module_async_callback_data_ref.update(fn(table) {
    table.set(id, path)
    table
  })
}

///|
fn module_async_callback_data_get(id : Int) -> String? {
  let mut value : String? = None
  module_async_callback_data_ref.update(fn(table) {
    value = table.get(id)
    table
  })
  value
}

///|
fn module_import_dynamic_job_data_set(
  id : Int,
  data : ModuleImportDynamicJobData,
) -> Unit {
  module_import_dynamic_job_data_ref.update(fn(table) {
    table.set(id, data)
    table
  })
}

///|
fn module_import_dynamic_job_data_take(id : Int) -> ModuleImportDynamicJobData? {
  let mut value : ModuleImportDynamicJobData? = None
  module_import_dynamic_job_data_ref.update(fn(table) {
    value = table.get(id)
    match value {
      Some(_) => {
        let _ = table.remove(id)

      }
      None => ()
    }
    table
  })
  value
}

///|
fn new_module_async_callback(path : String, is_reject : Bool) -> Value {
  let func = if is_reject {
    new_builtin_value(BuiltinFunction::ModuleAsyncRejected)
  } else {
    new_builtin_value(BuiltinFunction::ModuleAsyncFulfilled)
  }
  match func {
    Builtin(builtin) => module_async_callback_data_set(builtin.id, path)
    _ => ()
  }
  func
}

///|
fn module_execute_async(path : String) -> Unit raise {
  let info = match module_eval_info_get(path) {
    Some(value) => value
    None => return
  }
  let env = match module_env_get(info.env_id) {
    Some(value) => value
    None => return
  }
  let exports_obj = match module_record_get(path) {
    Some(record) => record.exports
    None => new_object_struct(None)
  }
  let promise = with_source_path(path, fn() raise {
    with_source_value(info.script.source, fn() raise {
      module_async_eval(
        env,
        info.script.body,
        Undefined,
        path,
        info.script.source,
        exports_obj,
      )
    })
  })
  let on_fulfilled = new_module_async_callback(path, false)
  let on_rejected = new_module_async_callback(path, true)
  let _ = promise_invoke_then(promise, on_fulfilled, on_rejected)

}

///|
fn module_exec_list_contains(list : Array[String], path : String) -> Bool {
  for entry in list {
    if entry == path {
      return true
    }
  }
  false
}

///|
fn module_exec_list_sort(list : Array[String]) -> Unit {
  let len = list.length()
  let mut i = 0
  while i < len {
    let mut j = i + 1
    while j < len {
      let mut ts_i : Int64 = 0
      let mut ts_j : Int64 = 0
      match module_eval_info_get(list[i]) {
        Some(info) => ts_i = info.async_timestamp
        None => ()
      }
      match module_eval_info_get(list[j]) {
        Some(info) => ts_j = info.async_timestamp
        None => ()
      }
      if ts_j < ts_i {
        let tmp = list[i]
        list[i] = list[j]
        list[j] = tmp
      }
      j = j + 1
    }
    i = i + 1
  }
}

///|
fn module_gather_available_ancestors(
  path : String,
  exec_list : Array[String],
) -> Unit raise {
  let info = match module_eval_info_get(path) {
    Some(value) => value
    None => return
  }
  for parent in info.async_parents {
    let parent_info = match module_eval_info_get(parent) {
      Some(value) => value
      None => continue
    }
    if !(parent_info.status is ModuleEvalStatus::EvaluatingAsync) {
      continue
    }
    if parent_info.eval_has_exception {
      continue
    }
    parent_info.pending_async_deps = parent_info.pending_async_deps - 1
    if parent_info.pending_async_deps == 0 {
      if !module_exec_list_contains(exec_list, parent) {
        exec_list.push(parent)
      }
      if !parent_info.has_tla {
        module_eval_info_set(parent, parent_info)
        module_gather_available_ancestors(parent, exec_list)
        continue
      }
    }
    module_eval_info_set(parent, parent_info)
  }
}

///|
fn module_async_evaluation_rejected(path : String, error : Value) -> Unit raise {
  let info = match module_eval_info_get(path) {
    Some(value) => value
    None => return
  }
  if info.status is ModuleEvalStatus::Evaluated {
    return
  }
  info.eval_has_exception = true
  info.eval_exception = Some(error)
  info.status = ModuleEvalStatus::Evaluated
  info.async_evaluation = false
  module_eval_info_set(path, info)
  module_eval_reject(path, error)
  for parent in info.async_parents {
    module_async_evaluation_rejected(parent, error)
  }
}

///|
fn module_async_evaluation_fulfilled(path : String) -> Unit raise {
  let info = match module_eval_info_get(path) {
    Some(value) => value
    None => return
  }
  if info.status is ModuleEvalStatus::Evaluated {
    return
  }
  info.async_evaluation = false
  info.status = ModuleEvalStatus::Evaluated
  module_eval_info_set(path, info)
  module_eval_resolve(path)
  let exec_list : Array[String] = []
  module_gather_available_ancestors(path, exec_list)
  module_exec_list_sort(exec_list)
  for entry in exec_list {
    let info = match module_eval_info_get(entry) {
      Some(value) => value
      None => continue
    }
    if info.status is ModuleEvalStatus::Evaluated && info.eval_has_exception {
      continue
    }
    if info.has_tla {
      module_execute_async(entry) catch {
        ThrowSignal(err) => module_async_evaluation_rejected(entry, err)
        err => raise err
      }
    } else {
      try {
        module_execute_sync(entry)
        let updated = info
        updated.async_evaluation = false
        updated.status = ModuleEvalStatus::Evaluated
        module_eval_info_set(entry, updated)
        module_eval_resolve(entry)
      } catch {
        ThrowSignal(err) => module_async_evaluation_rejected(entry, err)
        err => raise err
      }
    }
  }
}

///|
fn module_inner_evaluation(
  path : String,
  index : Int,
  stack : Array[String],
) -> Int raise {
  let mut info = match module_eval_info_get(path) {
    Some(value) => value
    None => return index
  }
  match info.status {
    ModuleEvalStatus::EvaluatingAsync | ModuleEvalStatus::Evaluated =>
      if info.eval_has_exception {
        match info.eval_exception {
          Some(value) => raise ThrowSignal(value)
          None => raise ThrowSignal(type_error_value("module failed"))
        }
      } else {
        return index
      }
    ModuleEvalStatus::Evaluating => return index
    ModuleEvalStatus::NotEvaluated => ()
  }
  info.status = ModuleEvalStatus::Evaluating
  info.dfs_index = index
  info.dfs_ancestor_index = index
  info.pending_async_deps = 0
  let mut next_index = index + 1
  stack.push(path)
  module_eval_info_set(path, info)
  for dep in info.deps {
    next_index = module_inner_evaluation(dep, next_index, stack)
    let dep_info = match module_eval_info_get(dep) {
      Some(value) => value
      None => continue
    }
    let mut dep_root = dep
    if dep_info.status is ModuleEvalStatus::Evaluating {
      let updated = match module_eval_info_get(path) {
        Some(value) => value
        None => continue
      }
      updated.dfs_ancestor_index = if updated.dfs_ancestor_index <
        dep_info.dfs_ancestor_index {
        updated.dfs_ancestor_index
      } else {
        dep_info.dfs_ancestor_index
      }
      module_eval_info_set(path, updated)
    } else {
      dep_root = match dep_info.cycle_root {
        Some(value) => value
        None => dep
      }
      match module_eval_info_get(dep_root) {
        Some(root_info) =>
          if root_info.eval_has_exception {
            match root_info.eval_exception {
              Some(value) => raise ThrowSignal(value)
              None => raise ThrowSignal(type_error_value("module failed"))
            }
          }
        None => ()
      }
    }
    let dep_root_info = match module_eval_info_get(dep_root) {
      Some(value) => value
      None => continue
    }
    if dep_root_info.async_evaluation {
      let updated = match module_eval_info_get(path) {
        Some(value) => value
        None => continue
      }
      updated.pending_async_deps = updated.pending_async_deps + 1
      module_eval_info_set(path, updated)
      dep_root_info.async_parents.push(path)
      module_eval_info_set(dep_root, dep_root_info)
    }
  }
  info = match module_eval_info_get(path) {
    Some(value) => value
    None => return next_index
  }
  if info.pending_async_deps > 0 {
    info.async_evaluation = true
    info.async_timestamp = module_eval_next_timestamp()
    module_eval_info_set(path, info)
  } else if info.has_tla {
    info.async_evaluation = true
    info.async_timestamp = module_eval_next_timestamp()
    module_eval_info_set(path, info)
    module_execute_async(path)
  } else {
    module_eval_info_set(path, info)
    module_execute_sync(path)
  }
  info = match module_eval_info_get(path) {
    Some(value) => value
    None => return next_index
  }
  if info.dfs_index == info.dfs_ancestor_index {
    while true {
      if stack.is_empty() {
        break
      }
      let current = stack.pop().unwrap_or(path)
      let entry_info = match module_eval_info_get(current) {
        Some(value) => value
        None => continue
      }
      if !entry_info.async_evaluation {
        entry_info.status = ModuleEvalStatus::Evaluated
      } else {
        entry_info.status = ModuleEvalStatus::EvaluatingAsync
      }
      entry_info.cycle_root = Some(path)
      module_eval_info_set(current, entry_info)
      match module_eval_promise_get(path) {
        Some(promise) => module_eval_promise_set(current, Some(promise))
        None => ()
      }
      if current == path {
        break
      }
    }
  }
  next_index
}

///|
fn module_evaluate(path : String) -> Value raise {
  let info = module_eval_info_get(path)
  match info {
    None => {
      let (promise, resolve, _) = new_promise_capability(Undefined)
      module_eval_promise_set(path, Some(promise))
      let _ = call_value_with_this(resolve, [Undefined], Undefined)
      promise
    }
    Some(info) => {
      let root_path = if info.status is ModuleEvalStatus::EvaluatingAsync ||
        info.status is ModuleEvalStatus::Evaluated {
        match info.cycle_root {
          Some(value) => value
          None => path
        }
      } else {
        path
      }
      let promise = module_eval_promise_init(root_path)
      let stack : Array[String] = []
      try {
        let _ = module_inner_evaluation(root_path, 0, stack)

      } catch {
        ThrowSignal(err) => {
          for entry in stack {
            let entry_info = match module_eval_info_get(entry) {
              Some(value) => value
              None => continue
            }
            entry_info.status = ModuleEvalStatus::Evaluated
            entry_info.eval_has_exception = true
            entry_info.eval_exception = Some(err)
            entry_info.cycle_root = Some(root_path)
            module_eval_info_set(entry, entry_info)
            match module_eval_promise_get(root_path) {
              Some(promise) => module_eval_promise_set(entry, Some(promise))
              None => ()
            }
          }
          module_eval_reject(root_path, err)
          return promise
        }
        err => raise err
      }
      match module_eval_info_get(root_path) {
        Some(root_info) =>
          if !root_info.async_evaluation && !root_info.eval_has_exception {
            module_eval_resolve(root_path)
          }
        None => ()
      }
      promise
    }
  }
}

///|
fn await_promise_completion(promise : Value) -> Value raise {
  while true {
    match promise_data_from_value(promise) {
      Some(data) =>
        match data.state {
          PromiseState::Fulfilled => return data.value
          PromiseState::Rejected => raise ThrowSignal(data.value)
          PromiseState::Pending => {
            let mut did_work = false
            if has_pending_jobs() {
              run_pending_jobs()
              did_work = true
            }
            if has_pending_timers() {
              run_pending_timers()
              did_work = true
            }
            if has_pending_jobs() {
              run_pending_jobs()
              did_work = true
            }
            if has_pending_workers() {
              run_pending_workers()
              did_work = true
            }
            if has_pending_jobs() {
              run_pending_jobs()
              did_work = true
            }
            if !did_work {
              let _ = throw_type_error("promise is pending")
              return promise
            }
          }
        }
      None => return promise
    }
  }
  promise
}
