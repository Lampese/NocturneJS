///|
fn new_module_binding_value(module_value : Value, name : String) -> Value raise {
  let target = to_object(module_value)
  let binding_value = new_object_value_with_proto(
    value_from_object(object_proto()),
  )
  match binding_value {
    Object(obj) => {
      obj.props.set(module_binding_target_key, property_data_non_enum(target))
      obj.props.set(
        module_binding_name_key,
        property_data_non_enum(String(name)),
      )
      obj.extensible = false
    }
    _ => ()
  }
  binding_value
}

///|
fn module_binding_info(value : Value) -> (Value, String, Int)? {
  match value {
    Object(obj) =>
      match
        (
          obj.props.get(module_binding_target_key),
          obj.props.get(module_binding_name_key),
        ) {
        (Some(target_prop), Some(name_prop)) =>
          match name_prop.value {
            String(name) => Some((target_prop.value, name, obj.id))
            _ => None
          }
        _ => None
      }
    _ => None
  }
}

///|
fn module_binding_deref(value : Value) -> Value raise {
  let mut current = value
  let seen : Map[Int, Bool] = Map::new()
  while true {
    match module_binding_info(current) {
      Some((target, name, binding_id)) => {
        if seen.contains(binding_id) {
          return Undefined
        }
        seen.set(binding_id, true)
        match target {
          Object(obj) =>
            match module_env_id_for_exports_obj(obj) {
              Some(env_id) =>
                match module_env_get(env_id) {
                  Some(env) =>
                    match module_env_export_binding_for_name(env_id, name) {
                      Some(binding) =>
                        if env.uninitialized_bindings.contains(binding) {
                          let _ = throw_reference_error("undefined variable")
                          return Undefined
                        }
                      None =>
                        if module_env_export_uninitialized_has(env_id, name) {
                          let _ = throw_reference_error("undefined variable")
                          return Undefined
                        }
                    }
                  None => ()
                }
              None => ()
            }
          _ => ()
        }
        current = property_get_raw(target, name)
      }
      None => return current
    }
  } else {
    Undefined
  }
}

///|
fn module_meta_object(path : String, is_main : Bool) -> ObjectValue {
  let meta_value = new_object_value_with_proto(None)
  let meta_obj = match meta_value {
    Object(obj) => obj
    _ => new_object_struct(None)
  }
  let url = if path.contains(":") { path } else { "file://" + path }
  meta_obj.props.set("url", property_data(String(url)))
  meta_obj.props.set("main", property_data(Bool(is_main)))
  meta_obj
}

///|
fn import_meta_value() -> Value raise {
  let mut path : String? = None
  match current_function() {
    Some(func) => path = func.source_path
    None =>
      match current_source_path() {
        Some(value) => path = Some(value)
        None => ()
      }
  }
  match path {
    Some(path) =>
      match module_record_get(path) {
        Some(record) =>
          match record.meta_obj {
            Some(meta_obj) => Object(meta_obj)
            None => {
              let meta_obj = module_meta_object(path, false)
              module_record_set(path, ModuleRecord::{
                exports: record.exports,
                state: record.state,
                module_ns: record.module_ns,
                meta_obj: Some(meta_obj),
                eval_promise: record.eval_promise,
              })
              Object(meta_obj)
            }
          }
        None => throw_type_error("import.meta not supported in this context")
      }
    None => throw_type_error("import.meta not supported in this context")
  }
}
