///|
fn eval_module_script(
  script : @engine.Script,
  full_path : String,
) -> ObjectValue raise {
  validate_context_in_script(script, false, false, false)
  let is_main = match current_module_env_id() {
    Some(_) => false
    None => true
  }
  let meta_obj = module_meta_object(full_path, is_main)
  let exports_value = new_object_value()
  let exports_obj = match exports_value {
    Object(obj) => obj
    _ => new_object_struct(None)
  }
  module_record_set(full_path, ModuleRecord::{
    exports: exports_obj,
    state: ModuleState::Loading,
    module_ns: None,
    meta_obj: Some(meta_obj),
    eval_promise: None,
  })
  module_export_info_set(full_path, module_export_info_from_script(script))
  let module_env = match root_env() {
    Some(env) => Env::new(Some(env))
    None => Env::new(None)
  }
  module_env.strict = true
  module_env.bindings.set("this", Undefined)
  module_env.var_env = Some(module_env)
  module_env_exports_set(module_env.id, exports_obj)
  module_env_set(module_env)
  let mut deps : Array[String] = []
  let has_tla = module_has_top_level_await(script)
  let result = try {
    let _ = with_source_path(full_path, fn() raise {
      with_module_exports(exports_obj, fn() raise {
        with_module_env_id(module_env.id, fn() raise {
          predeclare_module_exports(module_env.id, exports_obj, script)
          push_module_imports()
          try {
            preload_module_imports(module_env, script)
            deps = pop_module_imports()
          } catch {
            err => {
              let _ = pop_module_imports()
              raise err
            }
          }
          with_source_value(script.source, fn() raise {
            hoist_decls(module_env, script.body, false, false)
            if eval_body_has_lexical_decls(script.body) {
              eval_predeclare_lexical_decls(module_env, script.body)
            }
            Undefined
          })
        })
      })
    })
    match module_env_export_names_all(module_env.id) {
      Some(names) =>
        for binding, _ in names {
          match module_env.bindings.get(binding) {
            Some(_) => ()
            None => {
              let _ = throw_syntax_error(
                "exported variable '\{binding}' does not exist",
              )

            }
          }
        }
      None => ()
    }
    exports_obj.extensible = false
    let mut module_ns : ObjectValue? = None
    match module_namespace_from_value(Object(exports_obj)) {
      Some(namespace_obj) => module_ns = Some(namespace_obj)
      None => ()
    }
    module_record_set(full_path, ModuleRecord::{
      exports: exports_obj,
      state: ModuleState::Loaded,
      module_ns,
      meta_obj: Some(meta_obj),
      eval_promise: None,
    })
    module_eval_info_set(full_path, ModuleEvalInfo::{
      script,
      env_id: module_env.id,
      deps,
      has_tla,
      status: ModuleEvalStatus::NotEvaluated,
      dfs_index: 0,
      dfs_ancestor_index: 0,
      pending_async_deps: 0,
      async_parents: [],
      async_timestamp: 0,
      async_evaluation: false,
      cycle_root: None,
      eval_has_exception: false,
      eval_exception: None,
      resolve: None,
      reject: None,
    })
    exports_obj
  } catch {
    err => {
      module_record_remove(full_path)
      module_eval_info_remove(full_path)
      module_env_exports_remove(module_env.id)
      module_env_export_names_remove(module_env.id)
      module_env_remove(module_env.id)
      raise err
    }
  }
  result
}

///|
fn module_has_top_level_await(script : @engine.Script) -> Bool {
  stmts_contain_yield(script.body)
}

///|
fn module_syntax_error_value(env : Env, message : String) -> Value {
  let proto = match syntax_error_proto_for_env(env) {
    Some(value) => Some(value)
    None => syntax_error_proto()
  }
  new_error_value(proto, message)
}
