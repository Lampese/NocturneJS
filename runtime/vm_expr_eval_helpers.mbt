///|
fn build_template_object(
  env : Env,
  parts : @engine.TemplateParts,
) -> Value raise {
  let registry = template_registry_for_env(env)
  match registry.get(parts.site_id) {
    Some(value) => value
    None => {
      let cooked : Array[Value?] = []
      let raw_parts : Array[Value?] = []
      for part in parts.cooked {
        match part {
          Some(text) => cooked.push(Some(String(text)))
          None => cooked.push(Some(Undefined))
        }
      }
      for part in parts.raw {
        raw_parts.push(Some(String(part)))
      }
      let cooked_value = new_array_value(cooked)
      let raw_value = new_array_value(raw_parts)
      match cooked_value {
        Array(arr) => props_set(arr.props, "raw", property_data_non_enum(raw_value))
        _ => ()
      }
      let _ = object_freeze(raw_value)
      let _ = object_freeze(cooked_value)
      template_registry_set(registry, parts.site_id, cooked_value)
      cooked_value
    }
  }
}

///|
fn eval_expr(env : Env, expr : @engine.Expr) -> Value raise {
  bytecode_eval_expr(env, expr)
}
