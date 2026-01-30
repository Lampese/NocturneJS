///|
fn eval_function_decl(env : Env, func : @engine.FunctionDef) -> Unit raise {
  if top_level_function_decl_contains(func) {
    return
  }
  let decl_value = Value::Function(to_function_value(env, func, false))
  let value = match func.name {
    Some(name) =>
      match env.bindings.get(name) {
        Some(_) => env_get_binding_value(env, name)
        None =>
          match env.var_object {
            Some(Object(obj)) =>
              match props_get(obj.props, name) {
                Some(prop) =>
                  if is_data_property(prop) {
                    prop.value
                  } else {
                    property_get(Value::Object(obj), name)
                  }
                None => decl_value
              }
            Some(_) => decl_value
            None =>
              if env.parent is None {
                match global_object(env) {
                  Some(obj) =>
                    match props_get(obj.props, name) {
                      Some(prop) =>
                        if is_data_property(prop) {
                          prop.value
                        } else {
                          property_get(Value::Object(obj), name)
                        }
                      None => decl_value
                    }
                  None => decl_value
                }
              } else {
                decl_value
              }
          }
      }
    None => decl_value
  }
  match func.name {
    Some(name) => {
      env_set_local_or_var_object(env, name, value, true)
      if !env.strict && !func.is_async && !func.is_generator {
        let var_env = var_env_for(env)
        if var_env.id != env.id && annex_b_var_func_decl_contains(func) {
          env_set(var_env, name, decl_value)
        }
      }
    }
    None => ()
  }
}
