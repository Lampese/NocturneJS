///|
fn call_builtin_module(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match builtin.kind {
    BuiltinFunction::ModuleImport => {
      if args.is_empty() {
        return throw_type_error("missing module specifier")
      }
      let specifier = to_string_value(args[0])
      let attributes = if args.length() > 1 {
        module_attributes_from_value(args[1])
      } else {
        Undefined
      }
      module_import_from_specifier(specifier, attributes, true)
    }
    BuiltinFunction::ModuleImportDynamic => {
      if args.is_empty() {
        return throw_type_error("missing module specifier")
      }
      let (promise, resolve, reject) = new_promise_capability(Undefined)
      let mut base_path : String? = None
      match current_source_path() {
        Some(path) => base_path = Some(path)
        None =>
          match current_function() {
            Some(func) =>
              match func.source_path {
                Some(path) => base_path = Some(path)
                None => ()
              }
            None => ()
          }
      }
      try {
        let specifier = to_string_strict(args[0])
        let options = if args.length() > 1 { args[1] } else { Undefined }
        let attributes = module_attributes_from_options(options)
        let job = new_builtin_value(BuiltinFunction::ModuleImportDynamicJob)
        match job {
          Builtin(builtin) =>
            module_import_dynamic_job_data_set(builtin.id, ModuleImportDynamicJobData::{
              resolve,
              reject,
              specifier,
              attributes,
              base_path,
            })
          _ => ()
        }
        enqueue_job(job, [])
        promise
      } catch {
        ThrowSignal(value) => {
          let _ = call_value_with_this(reject, [value], Undefined)
          promise
        }
        err => raise err
      }
    }
    BuiltinFunction::ModuleImportDynamicJob => {
      match module_import_dynamic_job_data_take(builtin.id) {
        Some(data) => {
          try {
            let module_value = match data.base_path {
              Some(path) =>
                with_source_path(path, fn() raise {
                  module_import_from_specifier(
                    data.specifier,
                    data.attributes,
                    false,
                  )
                })
              None =>
                module_import_from_specifier(
                  data.specifier,
                  data.attributes,
                  false,
                )
            }
            let cache_key = match data.base_path {
              Some(path) =>
                with_source_path_string_option(path, fn() raise {
                  module_cache_key_from_specifier(
                    data.specifier,
                    data.attributes,
                  )
                })
              None =>
                module_cache_key_from_specifier(data.specifier, data.attributes)
            }
            match cache_key {
              Some(key) => {
                let eval_promise = module_evaluate(key)
                let chained = promise_then_value(eval_promise, module_value)
                let _ = promise_invoke_then(chained, data.resolve, data.reject)

              }
              None => {
                let _ = call_value_with_this(
                  data.resolve,
                  [module_value],
                  Undefined,
                )

              }
            }
          } catch {
            ThrowSignal(value) => {
              let _ = call_value_with_this(data.reject, [value], Undefined)

            }
            err => {
              module_import_dynamic_job_data_release(data)
              raise err
            }
          }
          module_import_dynamic_job_data_release(data)
        }
        None => ()
      }
      Undefined
    }
    BuiltinFunction::ModuleImportBinding => {
      if args.length() < 2 {
        return throw_type_error("missing import binding")
      }
      let specifier = to_string_value(args[0])
      let name = to_string_value(args[1])
      let attributes = if args.length() > 2 {
        module_attributes_from_value(args[2])
      } else {
        Undefined
      }
      if name != "default" {
        match module_builtin_value(specifier) {
          Some(_) => ()
          None => {
            let full_path = resolve_path_from_source(specifier)
            let mut json_kind = module_test_json(attributes)
            if json_kind == 0 && full_path.has_suffix(".json") {
              json_kind = 1
            }
            if json_kind > 0 {
              return throw_syntax_error(
                "JSON modules do not support named imports",
              )
            }
          }
        }
      }
      let module_value = module_import_from_specifier(
        specifier, attributes, true,
      )
      let cache_key = module_cache_key_from_specifier(specifier, attributes)
      let export_target = match cache_key {
        Some(key) =>
          match module_record_get(key) {
            Some(record) => Object(record.exports)
            None => module_value
          }
        None => module_value
      }
      let mut resolved = false
      match cache_key {
        Some(key) =>
          match module_export_info_get(key) {
            Some(_) => {
              let result = module_resolve_export_name(key, name)
              if result.status == 0 {
                return throw_syntax_error("module export '\{name}' not found")
              } else if result.status == 2 {
                return throw_syntax_error("ambiguous export '\{name}'")
              }
              resolved = true
            }
            None => ()
          }
        None => ()
      }
      if !resolved && !has_property_value(export_target, name) {
        return throw_syntax_error("module export '\{name}' not found")
      }
      new_module_binding_value(export_target, name)
    }
    BuiltinFunction::ModuleBinding => {
      if args.is_empty() {
        return throw_type_error("missing binding name")
      }
      let name = to_string_value(args[0])
      match current_module_env_id() {
        Some(env_id) =>
          match module_env_get(env_id) {
            Some(env) =>
              match env.bindings.get(name) {
                Some(value) => value
                None => Undefined
              }
            None => throw_type_error("binding not in module")
          }
        None => throw_type_error("binding not in module")
      }
    }
    BuiltinFunction::ModuleExport => {
      if args.length() < 2 {
        return throw_type_error("missing export")
      }
      let name = to_string_value(args[0])
      let value = args[1]
      let binding_name = if args.length() > 2 {
        match args[2] {
          Null | Undefined => None
          _ => Some(to_string_value(args[2]))
        }
      } else {
        None
      }
      match current_module_exports() {
        Some(exports) => {
          let mut export_value = value
          if binding_name is None && name == "default" {
            set_anonymous_function_name(export_value, "default", None)
          }
          match current_module_env_id() {
            Some(env_id) => module_env_export_uninitialized_remove(env_id, name)
            None => ()
          }
          match (current_module_env_id(), binding_name) {
            (Some(env_id), Some(binding)) => {
              module_env_export_names_add(env_id, binding, name)
              match module_env_get(env_id) {
                Some(env) =>
                  match env.bindings.get(binding) {
                    Some(raw_value) =>
                      match module_binding_info(raw_value) {
                        Some(_) => export_value = raw_value
                        None => ()
                      }
                    None => ()
                  }
                None => ()
              }
            }
            _ => ()
          }
          props_set(exports.props, name, Property::{
            value: export_value,
            writable: false,
            configurable: false,
            enumerable: true,
            getter: None,
            setter: None,
          })
          match current_source_path() {
            Some(path) => module_namespace_add_export(path, exports, name)
            None => ()
          }
          export_value
        }
        None => throw_type_error("export not in module")
      }
    }
    BuiltinFunction::ModuleExportStar => {
      if args.is_empty() {
        return throw_type_error("missing module specifier")
      }
      let specifier = to_string_value(args[0])
      let attributes = if args.length() > 1 {
        module_attributes_from_value(args[1])
      } else {
        Undefined
      }
      let module_value = module_import_from_specifier(
        specifier, attributes, true,
      )
      let module_obj = to_object(module_value)
      let props = props_map_for_value(module_obj)
      match current_module_exports() {
        Some(exports) => {
          let mut module_path : String? = None
          match current_source_path() {
            Some(path) => module_path = Some(path)
            None => ()
          }
          match props {
            Some(props) =>
              props_iter_raise(props, fn(name, prop) raise {
                if name == "default" || !prop.enumerable {
                  return ()
                }
                let mut is_ambiguous = false
                match module_path {
                  Some(path) =>
                    match module_export_info_get(path) {
                      Some(_) => {
                        let result = module_resolve_export_name(path, name)
                        if result.status == 2 {
                          is_ambiguous = true
                        }
                      }
                      None => ()
                    }
                  None => ()
                }
                if is_ambiguous {
                  if props_contains(exports.props, name) {
                    props_remove(exports.props, name)
                  }
                  match module_path {
                    Some(path) => module_namespace_remove_export(path, name)
                    None => ()
                  }
                  return ()
                }
                if props_contains(exports.props, name) {
                  return ()
                }
                let value = new_module_binding_value(module_obj, name)
                props_set(exports.props, name, Property::{
                  value,
                  writable: false,
                  configurable: false,
                  enumerable: true,
                  getter: None,
                  setter: None,
                })
                match module_path {
                  Some(path) => module_namespace_add_export(path, exports, name)
                  None => ()
                }
              })
            None => ()
          }
          module_obj
        }
        None => throw_type_error("export not in module")
      }
    }
    BuiltinFunction::ModuleAsyncFulfilled => {
      match module_async_callback_data_get(builtin.id) {
        Some(path) => module_async_evaluation_fulfilled(path)
        None => ()
      }
      Undefined
    }
    BuiltinFunction::ModuleAsyncRejected => {
      let error = if args.is_empty() { Undefined } else { args[0] }
      match module_async_callback_data_get(builtin.id) {
        Some(path) => module_async_evaluation_rejected(path, error)
        None => ()
      }
      Undefined
    }
    _ => throw_type_error("invalid builtin")
  }
}
