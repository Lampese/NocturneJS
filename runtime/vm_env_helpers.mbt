///|

///|
fn env_bindings_set(env : Env, name : String, value : Value) -> Unit {
  match env.bindings.get(name) {
    Some(existing) => rc_replace_value(existing, value)
    None => rc_incref_value(value)
  }
  env.bindings.set(name, value)
}

///|
fn env_bindings_remove(env : Env, name : String) -> Unit {
  match env.bindings.get(name) {
    Some(existing) => rc_decref_value(existing)
    None => ()
  }
  let _ = env.bindings.remove(name)
}

///|
fn is_unscopable(obj : Value, name : String) -> Bool raise {
  match symbol_unscopables_key() {
    Some(key) =>
      match property_get(obj, key) {
        Undefined | Null => false
        target =>
          if is_object_like(target) {
            is_truthy(property_get(target, name))
          } else {
            false
          }
      }
    None => false
  }
}

///|
priv enum BindingTarget {
  WithObject(Value)
  VarObject(Value)
  Env(Env)
  Unbound
}

///|
fn resolve_binding_target(env : Env, name : String) -> BindingTarget raise {
  match env.with_object {
    Some(obj) =>
      if has_property_value(obj, name) && !is_unscopable(obj, name) {
        return BindingTarget::WithObject(obj)
      }
    None => ()
  }
  match env.bindings.get(name) {
    Some(_) => BindingTarget::Env(env)
    None => {
      match env.var_object {
        Some(obj) =>
          if has_property_value(obj, name) {
            return BindingTarget::VarObject(obj)
          }
        None => ()
      }
      match env.parent {
        Some(parent) => resolve_binding_target(parent, name)
        None => BindingTarget::Unbound
      }
    }
  }
}

///|
fn env_get_binding_value(env : Env, name : String) -> Value raise {
  match env.bindings.get(name) {
    Some(value) =>
      if env.uninitialized_bindings.contains(name) {
        throw_reference_error("undefined variable")
      } else if name == "this" && env.this_uninitialized {
        throw_reference_error("this is not initialized")
      } else {
        module_binding_deref(value)
      }
    None => throw_reference_error("undefined variable")
  }
}

///|
fn get_binding_value_from_target(
  env : Env,
  name : String,
  target : BindingTarget,
  strict : Bool,
) -> Value raise {
  match target {
    BindingTarget::WithObject(obj) =>
      if has_property_value(obj, name) {
        property_get(obj, name)
      } else if strict {
        throw_reference_error("undefined variable")
      } else {
        Undefined
      }
    BindingTarget::VarObject(obj) =>
      if has_property_value(obj, name) {
        property_get(obj, name)
      } else if strict {
        throw_reference_error("undefined variable")
      } else {
        Undefined
      }
    BindingTarget::Env(target_env) => env_get_binding_value(target_env, name)
    BindingTarget::Unbound =>
      match global_object(env) {
        Some(obj) =>
          if has_property_in_chain(obj.props, obj.proto, name) {
            property_get_from_chain(
              obj.props,
              obj.proto,
              name,
              Value::Object(obj),
            )
          } else {
            throw_reference_error("undefined variable")
          }
        None => throw_reference_error("undefined variable")
      }
  }
}

///|
fn assign_binding_value_to_target(
  env : Env,
  name : String,
  target : BindingTarget,
  value : Value,
  strict : Bool,
) -> Unit raise {
  match target {
    BindingTarget::WithObject(obj) =>
      if has_property_value(obj, name) {
        let _ = property_set(obj, name, value, strict)

      } else if strict {
        let _ = throw_reference_error("undefined variable")

      } else {
        let _ = property_set(obj, name, value, false)

      }
    BindingTarget::VarObject(obj) =>
      if has_property_value(obj, name) {
        let _ = property_set(obj, name, value, strict)

      } else if strict {
        let _ = throw_reference_error("undefined variable")

      } else {
        let _ = property_set(obj, name, value, false)

      }
    BindingTarget::Env(target_env) =>
      env_set_with_strict_no_with(target_env, name, value, strict)
    BindingTarget::Unbound =>
      env_set_with_strict_no_with(env, name, value, strict)
  }
}

///|
fn env_get(env : Env, name : String) -> Value raise {
  let target = resolve_binding_target(env, name)
  get_binding_value_from_target(env, name, target, env.strict)
}

///|
fn find_this_env(env : Env) -> Env {
  let mut current : Env? = Some(env)
  while true {
    match current {
      Some(scope) =>
        if scope.bindings.contains("this") {
          return scope
        } else {
          current = scope.parent
        }
      None => return env
    }
  }
  env
}

///|
fn env_get_call_reference(env : Env, name : String) -> (Value, Value) raise {
  let target = resolve_binding_target(env, name)
  let value = get_binding_value_from_target(env, name, target, env.strict)
  let this_value = match target {
    BindingTarget::WithObject(obj) => obj
    _ => Undefined
  }
  (value, this_value)
}

///|
fn env_define_private_name(env : Env, name : String) -> String raise {
  match env.private_bindings.get(name) {
    Some(_) => {
      let _ = throw_syntax_error("duplicate private name")
      ""
    }
    None => {
      let key = private_prop_key(alloc_id())
      env.private_bindings.set(name, key)
      key
    }
  }
}

///|
fn env_private_key(env : Env, name : String) -> String? {
  match env.private_bindings.get(name) {
    Some(key) => Some(key)
    None =>
      match env.parent {
        Some(parent) => env_private_key(parent, name)
        None => None
      }
  }
}

///|
fn env_private_key_checked(env : Env, name : String) -> String raise {
  match env_private_key(env, name) {
    Some(key) => key
    None => {
      let _ = throw_syntax_error("invalid private reference")
      ""
    }
  }
}

///|
fn var_env_for(env : Env) -> Env {
  match env.var_env {
    Some(var_env) => if var_env.id == env.id { env } else { var_env }
    None =>
      match env.parent {
        Some(parent) => var_env_for(parent)
        None => env
      }
  }
}

///|
fn merge_arg_env_bindings(arg_env : Env, var_env : Env) -> Unit {
  for name, value in arg_env.bindings {
    if var_env.bindings.get(name) is None {
      env_bindings_set(var_env, name, value)
      if arg_env.deletable_bindings.get(name) is Some(_) {
        var_env.deletable_bindings.set(name, true)
      }
      if arg_env.readonly_bindings.get(name) is Some(_) {
        var_env.readonly_bindings.set(name, true)
      }
    }
  }
}

///|
fn env_declare(env : Env, name : String, value : Value) -> Unit {
  match env.with_object {
    Some(_) =>
      match env.parent {
        Some(parent) => env_declare(parent, name, value)
        None => env_bindings_set(env, name, value)
      }
    None =>
      match env.parent {
        Some(_) =>
          match env.bindings.get(name) {
            Some(_) => ()
            None => env_bindings_set(env, name, value)
          }
        None =>
          match global_object(env) {
            Some(obj) =>
              if !obj.props.contains(name) {
                props_set(obj.props, name, property_data_non_enum(value))
              }
            None => env_bindings_set(env, name, value)
          }
      }
  }
}

///|
fn property_data_global(value : Value, configurable : Bool) -> Property {
  Property::{
    value,
    writable: true,
    configurable,
    enumerable: true,
    getter: None,
    setter: None,
  }
}

///|
fn is_data_property(prop : Property) -> Bool {
  prop.getter is None && prop.setter is None
}

///|
fn global_has_lexical_declaration(env : Env, name : String) -> Bool {
  match env.bindings.get(name) {
    Some(_) => true
    None => false
  }
}

///|
fn global_can_declare_var(env : Env, name : String) -> Bool {
  match global_object(env) {
    Some(obj) =>
      match obj.props.get(name) {
        Some(_) => true
        None => obj.extensible
      }
    None => true
  }
}

///|
fn global_can_declare_function(env : Env, name : String) -> Bool {
  if global_has_lexical_declaration(env, name) {
    return false
  }
  match global_object(env) {
    Some(obj) =>
      match obj.props.get(name) {
        Some(prop) =>
          if prop.configurable {
            true
          } else {
            is_data_property(prop) && prop.writable && prop.enumerable
          }
        None => obj.extensible
      }
    None => true
  }
}

///|
fn global_has_restricted_property(env : Env, name : String) -> Bool {
  match global_object(env) {
    Some(obj) =>
      match obj.props.get(name) {
        Some(prop) => !prop.configurable
        None => false
      }
    None => false
  }
}

///|
fn global_create_var_binding(
  env : Env,
  name : String,
  configurable : Bool,
) -> Unit raise {
  match global_object(env) {
    Some(obj) =>
      match obj.props.get(name) {
        Some(_) => ()
        None =>
          if !obj.extensible {
            let _ = throw_type_error("cannot define property")

          } else {
            props_set(obj.props, name, property_data_global(Undefined, configurable))
          }
      }
    None => env_bindings_set(env, name, Undefined)
  }
}

///|
fn global_create_function_binding(
  env : Env,
  name : String,
  value : Value,
  configurable : Bool,
) -> Unit raise {
  match global_object(env) {
    Some(obj) =>
      match obj.props.get(name) {
        Some(prop) =>
          if prop.configurable {
            props_set(obj.props, name, property_data_global(value, configurable))
          } else if !is_data_property(prop) {
            let _ = throw_type_error("cannot redefine property")

          } else {
            props_set(obj.props, name, Property::{
              value,
              writable: prop.writable,
              configurable: prop.configurable,
              enumerable: prop.enumerable,
              getter: prop.getter,
              setter: prop.setter,
            })
          }
        None =>
          if !obj.extensible {
            let _ = throw_type_error("cannot define property")

          } else {
            props_set(obj.props, name, property_data_global(value, configurable))
          }
      }
    None => env_bindings_set(env, name, value)
  }
}

///|
fn env_declare_var(env : Env, name : String, configurable : Bool) -> Unit raise {
  match env.var_object {
    Some(Object(obj)) => {
      match obj.props.get(name) {
        Some(_) => ()
        None =>
          if !obj.extensible {
            let _ = throw_type_error("cannot define property")

          } else {
            props_set(obj.props, name, property_data_global(Undefined, configurable))
          }
      }
      return
    }
    Some(_) => return
    None => ()
  }
  match env.with_object {
    Some(_) =>
      match env.parent {
        Some(parent) => env_declare_var(parent, name, configurable)
        None =>
          if env.bindings.get(name) is None {
            env_bindings_set(env, name, Undefined)
            if configurable {
              env.deletable_bindings.set(name, true)
            }
          }
      }
    None =>
      match env.parent {
        Some(_) =>
          match env.bindings.get(name) {
            Some(_) => ()
            None => {
              env_bindings_set(env, name, Undefined)
              if configurable {
                env.deletable_bindings.set(name, true)
              }
            }
          }
        None => global_create_var_binding(env, name, configurable)
      }
  }
}

///|
fn env_set_local_or_var_object(
  env : Env,
  name : String,
  value : Value,
  configurable : Bool,
) -> Unit raise {
  match env.var_object {
    Some(Object(obj)) =>
      match obj.props.get(name) {
        Some(_) => {
          let _ = property_set(Value::Object(obj), name, value, env.strict)

        }
        None =>
          if !obj.extensible {
            let _ = throw_type_error("cannot define property")

          } else {
            props_set(obj.props, name, property_data_global(value, configurable))
          }
      }
    Some(_) => ()
    None => env_set_local(env, name, value)
  }
}

///|
fn env_declare_uninitialized(env : Env, name : String) -> Unit {
  env_bindings_set(env, name, Undefined)
  env.uninitialized_bindings.set(name, true)
}

///|
fn env_define_readonly(
  env : Env,
  name : String,
  value : Value,
  always_throw : Bool,
) -> Unit {
  env_bindings_set(env, name, value)
  env.uninitialized_bindings.remove(name)
  env.readonly_bindings.set(name, always_throw)
  update_module_export_binding(env, name, value)
  sync_module_env(env)
}

///|
fn env_initialize_binding(env : Env, name : String, value : Value) -> Unit {
  env.uninitialized_bindings.remove(name)
  env_bindings_set(env, name, value)
  update_module_export_binding(env, name, value)
  sync_module_env(env)
}

///|
fn env_set_local(env : Env, name : String, value : Value) -> Unit raise {
  match env.with_object {
    Some(_) =>
      match env.parent {
        Some(parent) => env_set_local(parent, name, value)
        None => {
          if env.uninitialized_bindings.contains(name) {
            let _ = throw_reference_error("undefined variable")
            return
          }
          env_bindings_set(env, name, value)
          update_module_export_binding(env, name, value)
        }
      }
    None =>
      match env.parent {
        Some(_) => {
          if env.uninitialized_bindings.contains(name) {
            let _ = throw_reference_error("undefined variable")
            return
          }
          env_bindings_set(env, name, value)
          update_module_export_binding(env, name, value)
        }
        None =>
          match global_object(env) {
            Some(obj) => {
              if env.uninitialized_bindings.contains(name) {
                let _ = throw_reference_error("undefined variable")
                return
              }
              let _ = set_property_in(
                obj.props,
                name,
                value,
                Value::Object(obj),
              )

            }
            None => env_bindings_set(env, name, value)
          }
      }
  }
}

///|
fn env_set(env : Env, name : String, value : Value) -> Unit raise {
  env_set_with_strict(env, name, value, env.strict)
}

///|
fn env_set_with_strict(
  env : Env,
  name : String,
  value : Value,
  strict : Bool,
) -> Unit raise {
  match env.with_object {
    Some(obj) =>
      if has_property_value(obj, name) && !is_unscopable(obj, name) {
        let _ = property_set(obj, name, value, strict)
        return
      }
    None => ()
  }
  match env.bindings.get(name) {
    Some(_) => {
      if env.uninitialized_bindings.contains(name) {
        let _ = throw_reference_error("undefined variable")
        return
      }
      match env.readonly_bindings.get(name) {
        Some(always_throw) =>
          if always_throw || strict {
            let _ = throw_type_error("invalid assignment")

          } else {
            ()
          }
        None => {
          env_bindings_set(env, name, value)
          update_module_export_binding(env, name, value)
        }
      }
    }
    None =>
      match env.parent {
        Some(parent) => env_set_with_strict(parent, name, value, strict)
        None =>
          match global_object(env) {
            Some(obj) =>
              if has_property_in_chain(obj.props, obj.proto, name) {
                let _ = property_set(Value::Object(obj), name, value, strict)

              } else if strict {
                let _ = throw_reference_error("undefined variable")

              } else {
                let _ = property_set(Value::Object(obj), name, value, false)

              }
            None => env_bindings_set(env, name, value)
          }
      }
  }
}

///|
fn env_set_with_strict_no_with(
  env : Env,
  name : String,
  value : Value,
  strict : Bool,
) -> Unit raise {
  match env.bindings.get(name) {
    Some(_) => {
      if env.uninitialized_bindings.contains(name) {
        let _ = throw_reference_error("undefined variable")
        return
      }
      match env.readonly_bindings.get(name) {
        Some(always_throw) =>
          if always_throw || strict {
            let _ = throw_type_error("invalid assignment")

          } else {
            ()
          }
        None => {
          env_bindings_set(env, name, value)
          update_module_export_binding(env, name, value)
        }
      }
    }
    None =>
      match env.parent {
        Some(parent) => env_set_with_strict_no_with(parent, name, value, strict)
        None =>
          match global_object(env) {
            Some(obj) =>
              if has_property_in_chain(obj.props, obj.proto, name) {
                let _ = property_set(Value::Object(obj), name, value, strict)

              } else if strict {
                let _ = throw_reference_error("undefined variable")

              } else {
                let _ = property_set(Value::Object(obj), name, value, false)

              }
            None => env_bindings_set(env, name, value)
          }
      }
  }
}

///|
fn global_object(env : Env) -> ObjectValue? {
  match env.parent {
    Some(parent) => global_object(parent)
    None =>
      match env.bindings.get("this") {
        Some(Object(obj)) => Some(obj)
        _ => None
      }
  }
}

///|
fn global_object_value(env : Env) -> Value {
  match global_object(env) {
    Some(obj) => Object(obj)
    None => Undefined
  }
}

///|
fn env_has(env : Env, name : String) -> Bool raise {
  match env.with_object {
    Some(obj) => if has_property_value(obj, name) { return true }
    None => ()
  }
  match env.bindings.get(name) {
    Some(_) => true
    None =>
      match env.parent {
        Some(parent) => env_has(parent, name)
        None =>
          match global_object(env) {
            Some(obj) => has_property_in_chain(obj.props, obj.proto, name)
            None => false
          }
      }
  }
}
