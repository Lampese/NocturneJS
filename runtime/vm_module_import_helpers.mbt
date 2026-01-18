///|
fn module_import_from_specifier(
  specifier : String,
  attributes : Value,
  record_import : Bool,
) -> Value raise {
  match module_builtin_value(specifier) {
    Some(value) => value
    None => {
      let full_path = resolve_path_from_source(specifier)
      let mut json_kind = module_test_json(attributes)
      if json_kind == 0 && full_path.has_suffix(".json") {
        json_kind = 1
      }
      let cache_key = if json_kind == 0 {
        full_path
      } else if json_kind == 1 {
        full_path + "|json"
      } else {
        full_path + "|json5"
      }
      if record_import {
        record_module_import(cache_key)
      }
      match module_record_get(cache_key) {
        Some(record) =>
          match record.state {
            ModuleState::Loaded =>
              match record.module_ns {
                Some(namespace_obj) => Object(namespace_obj)
                None =>
                  match module_namespace_from_value(Object(record.exports)) {
                    Some(namespace_obj) => {
                      module_record_set(cache_key, ModuleRecord::{
                        exports: record.exports,
                        state: record.state,
                        module_ns: Some(namespace_obj),
                        meta_obj: record.meta_obj,
                        eval_promise: record.eval_promise,
                      })
                      Object(namespace_obj)
                    }
                    None => Object(record.exports)
                  }
              }
            ModuleState::Loading =>
              match record.module_ns {
                Some(namespace_obj) => Object(namespace_obj)
                None =>
                  match module_namespace_from_value(Object(record.exports)) {
                    Some(namespace_obj) => {
                      module_record_set(cache_key, ModuleRecord::{
                        exports: record.exports,
                        state: record.state,
                        module_ns: Some(namespace_obj),
                        meta_obj: record.meta_obj,
                        eval_promise: record.eval_promise,
                      })
                      Object(namespace_obj)
                    }
                    None => Object(record.exports)
                  }
              }
          }
        None =>
          if json_kind > 0 {
            let source = @fs.read_file_to_string(full_path) catch {
              _ => {
                let _ = throw_reference_error("could not load '\{specifier}'")
                ""
              }
            }
            let json_value = if json_kind == 2 {
              std_parse_ext_json(source)
            } else {
              json_parse_value(source)
            }
            let exports_value = new_object_value()
            let exports_obj = match exports_value {
              Object(obj) => obj
              _ => new_object_struct(None)
            }
            exports_obj.props.set("default", property_data(json_value))
            exports_obj.extensible = false
            let mut module_ns : ObjectValue? = None
            match module_namespace_from_value(Object(exports_obj)) {
              Some(namespace_obj) => module_ns = Some(namespace_obj)
              None => ()
            }
            module_record_set(cache_key, ModuleRecord::{
              exports: exports_obj,
              state: ModuleState::Loaded,
              module_ns,
              meta_obj: None,
              eval_promise: None,
            })
            match module_ns {
              Some(namespace_obj) => Object(namespace_obj)
              None => Object(exports_obj)
            }
          } else {
            let source = @fs.read_file_to_string(full_path) catch {
              _ => {
                let _ = throw_reference_error("could not load '\{specifier}'")
                ""
              }
            }
            let exports_obj = eval_module_source(source, full_path)
            match module_namespace_from_value(Object(exports_obj)) {
              Some(namespace_obj) => {
                match module_record_get(cache_key) {
                  Some(record) =>
                    module_record_set(cache_key, ModuleRecord::{
                      exports: record.exports,
                      state: record.state,
                      module_ns: Some(namespace_obj),
                      meta_obj: record.meta_obj,
                      eval_promise: record.eval_promise,
                    })
                  None => ()
                }
                Object(namespace_obj)
              }
              None => Object(exports_obj)
            }
          }
      }
    }
  }
}

///|
fn module_cache_key_from_specifier(
  specifier : String,
  attributes : Value,
) -> String? raise {
  match module_builtin_value(specifier) {
    Some(_) => None
    None => {
      let full_path = resolve_path_from_source(specifier)
      let mut json_kind = module_test_json(attributes)
      if json_kind == 0 && full_path.has_suffix(".json") {
        json_kind = 1
      }
      let cache_key = if json_kind == 0 {
        full_path
      } else if json_kind == 1 {
        full_path + "|json"
      } else {
        full_path + "|json5"
      }
      Some(cache_key)
    }
  }
}

///|
fn resolve_module_specifier_from(
  base_path : String,
  specifier : String,
) -> String {
  push_source_path(base_path)
  let full_path = resolve_path_from_source(specifier)
  pop_source_path()
  full_path
}

///|
fn module_resolve_export_name(
  path : String,
  export_name : String,
) -> ModuleResolveExportResult raise {
  let resolve_set : Map[String, Bool] = Map::new()
  module_resolve_export_name_inner(path, export_name, resolve_set)
}

///|
fn module_resolve_export_name_inner(
  path : String,
  export_name : String,
  resolve_set : Map[String, Bool],
) -> ModuleResolveExportResult raise {
  let key = path + "\u{00}" + export_name
  if resolve_set.contains(key) {
    return ModuleResolveExportResult::{
      status: 0,
      path: None,
      local_name: None,
    }
  }
  resolve_set.set(key, true)
  let info = match module_export_info_get(path) {
    Some(value) => value
    None =>
      return ModuleResolveExportResult::{
        status: 0,
        path: None,
        local_name: None,
      }
  }
  match info.local_exports.get(export_name) {
    Some(binding) => {
      let resolved_name = if binding == "" { export_name } else { binding }
      return ModuleResolveExportResult::{
        status: 1,
        path: Some(path),
        local_name: Some(resolved_name),
      }
    }
    None => ()
  }
  match info.indirect_exports.get(export_name) {
    Some((specifier, import_name)) => {
      let target_path = resolve_module_specifier_from(path, specifier)
      if import_name == "*" {
        return ModuleResolveExportResult::{
          status: 1,
          path: Some(path),
          local_name: Some("*"),
        }
      }
      return module_resolve_export_name_inner(
        target_path, import_name, resolve_set,
      )
    }
    None => ()
  }
  if export_name == "default" {
    return ModuleResolveExportResult::{
      status: 0,
      path: None,
      local_name: None,
    }
  }
  let mut star_resolution : ModuleResolveExportResult = ModuleResolveExportResult::{
    status: 0,
    path: None,
    local_name: None,
  }
  for specifier in info.star_exports {
    let target_path = resolve_module_specifier_from(path, specifier)
    let result = module_resolve_export_name_inner(
      target_path, export_name, resolve_set,
    )
    if result.status == 2 {
      return result
    }
    if result.status == 1 {
      if star_resolution.status == 0 {
        star_resolution = result
      } else {
        let mut same_binding = false
        match (star_resolution.path, result.path) {
          (Some(resolved_path), Some(next_path)) =>
            match (star_resolution.local_name, result.local_name) {
              (Some(resolved_name), Some(next_name)) =>
                same_binding = resolved_path == next_path &&
                  resolved_name == next_name
              _ => ()
            }
          _ => ()
        }
        if !same_binding {
          return ModuleResolveExportResult::{
            status: 2,
            path: None,
            local_name: None,
          }
        }
      }
    }
  }
  star_resolution
}

///|
fn module_namespace_from_value(value : Value) -> ObjectValue? raise {
  let target = to_object(value)
  let namespace_value = new_object_value_with_proto(None)
  match namespace_value {
    Object(namespace_obj) => {
      namespace_obj.is_module_namespace = true
      namespace_obj.immutable_proto = true
      match props_map_for_value(target) {
        Some(props) => {
          let names = module_namespace_prop_keys(props, false)
          for name in names {
            let prop_value = new_module_binding_value(target, name)
            namespace_obj.props.set(name, Property::{
              value: prop_value,
              writable: false,
              configurable: false,
              enumerable: true,
              getter: None,
              setter: None,
            })
          }
        }
        None => ()
      }
      match symbol_to_string_tag_key() {
        Some(key) =>
          namespace_obj.props.set(key, property_data_const(String("Module")))
        None => ()
      }
      namespace_obj.extensible = false
      Some(namespace_obj)
    }
    _ => None
  }
}

///|
fn module_namespace_add_export(
  path : String,
  exports : ObjectValue,
  name : String,
) -> Unit raise {
  match module_record_get(path) {
    Some(record) =>
      match record.module_ns {
        Some(namespace_obj) =>
          if !namespace_obj.props.contains(name) {
            let prop_value = new_module_binding_value(Object(exports), name)
            namespace_obj.props.set(name, Property::{
              value: prop_value,
              writable: false,
              configurable: false,
              enumerable: true,
              getter: None,
              setter: None,
            })
          }
        None => ()
      }
    None => ()
  }
}

///|
fn module_namespace_remove_export(path : String, name : String) -> Unit {
  match module_record_get(path) {
    Some(record) =>
      match record.module_ns {
        Some(namespace_obj) =>
          if namespace_obj.props.contains(name) {
            namespace_obj.props.remove(name)
          }
        None => ()
      }
    None => ()
  }
}

///|
fn module_builtin_value(name : String) -> Value? raise {
  let mapped = if name == "std" ||
    name == "os" ||
    name == "bjson" ||
    name == "bjson.so" {
    name
  } else if name.has_suffix("/bjson.so") {
    "bjson.so"
  } else {
    name
  }
  let root_id = match root_env() {
    Some(env) => Some(env.id)
    None => None
  }
  match root_id {
    Some(env_id) =>
      match module_builtin_namespace_get(env_id, mapped) {
        Some(obj) => Some(Object(obj))
        None =>
          match mapped {
            "std" | "os" | "bjson" | "bjson.so" => {
              let base_value = if mapped == "bjson.so" {
                global_property_value("bjson")
              } else {
                global_property_value(mapped)
              }
              match base_value {
                Some(value) =>
                  match module_namespace_from_value(value) {
                    Some(namespace_obj) => {
                      module_builtin_namespace_set(
                        env_id, mapped, namespace_obj,
                      )
                      Some(Object(namespace_obj))
                    }
                    None => None
                  }
                None => None
              }
            }
            _ => None
          }
      }
    None =>
      match mapped {
        "std" | "os" | "bjson" | "bjson.so" => {
          let base_value = if mapped == "bjson.so" {
            global_property_value("bjson")
          } else {
            global_property_value(mapped)
          }
          match base_value {
            Some(value) =>
              match module_namespace_from_value(value) {
                Some(namespace_obj) => Some(Object(namespace_obj))
                None => None
              }
            None => None
          }
        }
        _ => None
      }
  }
}

///|
fn module_check_attributes(attributes : Value) -> Unit {
  let _ = attributes

}

///|
fn module_attributes_from_object(attributes_obj : Value) -> Value raise {
  let attrs_value = new_object_value_with_proto(None)
  let keys = own_enumerable_string_keys(attributes_obj)
  for key in keys {
    let value = property_get(attributes_obj, key)
    match value {
      String(_) => ()
      _ => {
        let _ = throw_type_error("module attribute values must be strings")

      }
    }
    match attrs_value {
      Object(obj) => obj.props.set(key, property_data(value))
      _ => ()
    }
  }
  module_check_attributes(attrs_value)
  attrs_value
}

///|
fn module_attributes_from_value(attributes : Value) -> Value raise {
  match attributes {
    Undefined | Null => Undefined
    _ =>
      if !is_object_like(attributes) {
        throw_type_error("module attributes must be an object")
      } else {
        module_attributes_from_object(attributes)
      }
  }
}

///|
fn module_attributes_from_options(options : Value) -> Value raise {
  if options is Undefined {
    return Undefined
  }
  if !is_object_like(options) {
    return throw_type_error("options must be an object")
  }
  let with_value = property_get(options, "with")
  if with_value is Undefined {
    return Undefined
  }
  if !is_object_like(with_value) {
    return throw_type_error("options.with must be an object")
  }
  module_attributes_from_object(with_value)
}

///|
fn module_test_json(attributes : Value) -> Int raise {
  if attributes is Undefined {
    return 0
  }
  let type_value = property_get(attributes, "type")
  match type_value {
    String(text) =>
      if text == "json" {
        1
      } else if text == "json5" {
        2
      } else {
        0
      }
    _ => 0
  }
}

///|
fn eval_module_source(source : String, full_path : String) -> ObjectValue raise {
  let script = parse_script_with_pos_mode(source, true, false)
  eval_module_script(script, full_path)
}
