///|
fn eval_template_literal(
  env : Env,
  parts : @engine.TemplateParts,
  exprs : Array[@engine.Expr],
) -> Value raise {
  let mut result = ""
  let cooked = parts.cooked
  if !cooked.is_empty() {
    match cooked[0] {
      Some(text) => result = text
      None => {
        let _ = throw_syntax_error(
          "invalid escape sequence in template literal",
        )

      }
    }
  }
  let mut i = 0
  while i < exprs.length() {
    let value = eval_expr(env, exprs[i])
    result = result + to_string_strict(value)
    if i + 1 < cooked.length() {
      match cooked[i + 1] {
        Some(text) => result = result + text
        None => {
          let _ = throw_syntax_error(
            "invalid escape sequence in template literal",
          )

        }
      }
    }
    i = i + 1
  }
  String(result)
}

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
fn eval_tagged_template(
  env : Env,
  callee : @engine.Expr,
  parts : @engine.TemplateParts,
  exprs : Array[@engine.Expr],
) -> Value raise {
  let mut this_value : Value? = None
  let callee_value = match callee {
    @engine.Expr::Member(obj_expr, key, _) => {
      let target = eval_expr(env, obj_expr)
      this_value = Some(target)
      match key {
        @engine.MemberKey::Private(name) => {
          let key_name = env_private_key_checked(env, name)
          private_property_get(target, key_name, name)
        }
        _ => {
          let name = member_key_name(env, key)
          let target_obj = to_object(target)
          property_get_with_receiver(target_obj, name, target)
        }
      }
    }
    _ => eval_expr(env, callee)
  }
  let template_obj = build_template_object(env, parts)
  let argv : Array[Value] = [template_obj]
  for expr in exprs {
    argv.push(eval_expr(env, expr))
  }
  match this_value {
    Some(target) => call_value_with_this(callee_value, argv, target)
    None => call_value(callee_value, argv)
  }
}

///|
fn eval_typeof(env : Env, expr : @engine.Expr) -> Value raise {
  match expr {
    @engine.Expr::Ident(name, _) =>
      if env_has(env, name) {
        String(typeof_value(env_get(env, name)))
      } else {
        String("undefined")
      }
    @engine.Expr::Paren(inner) => eval_typeof(env, inner)
    _ => {
      let value = eval_expr(env, expr)
      String(typeof_value(value))
    }
  }
}

///|
fn eval_expr(env : Env, expr : @engine.Expr) -> Value raise {
  bytecode_eval_expr(env, expr)
}
