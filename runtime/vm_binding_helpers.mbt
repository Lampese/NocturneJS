///|
#warnings("-unused_error_type")
fn initialize_binding_assign(
  env : Env,
  name : String,
  value : Value,
) -> Unit raise {
  env_initialize_binding(env, name, value)
}

///|
fn assign_binding_value(
  env : Env,
  binding : @engine.VarBinding,
  value : Value,
  assign : (String, Value) -> Unit raise,
) -> Unit raise {
  match binding {
    @engine.VarBinding::Name(name) => assign(name, value)
    @engine.VarBinding::ArrayPattern(pattern) =>
      destructure_array_pattern(env, pattern, value, assign)
    @engine.VarBinding::ObjectPattern(pattern) =>
      destructure_object_pattern(env, pattern, value, assign)
    @engine.VarBinding::Target(expr) => assign_for_in_expr(env, expr, value)
  }
}

///|
fn prepare_identifier_assignment_target(
  env : Env,
  name : String,
  strict : Bool,
) -> ((Value) -> Unit raise) raise {
  match env.with_object {
    Some(obj) =>
      if has_property_value(obj, name) && !is_unscopable(obj, name) {
        return fn(value) raise {
          if has_property_value(obj, name) {
            let _ = property_set(obj, name, value, strict)

          } else if strict {
            let _ = throw_reference_error("undefined variable")

          } else {
            let _ = property_set(obj, name, value, false)

          }
        }
      }
    None => ()
  }
  match env.bindings.get(name) {
    Some(_) =>
      fn(value) raise { env_set_with_strict_no_with(env, name, value, strict) }
    None =>
      match env.parent {
        Some(parent) =>
          prepare_identifier_assignment_target(parent, name, strict)
        None =>
          match global_object(env) {
            Some(obj) =>
              if has_property_in_chain(obj.props, obj.proto, name) {
                fn(value) raise {
                  if has_property_in_chain(obj.props, obj.proto, name) {
                    let _ = property_set(
                      Value::Object(obj),
                      name,
                      value,
                      strict,
                    )

                  } else if strict {
                    let _ = throw_reference_error("undefined variable")

                  } else {
                    let _ = property_set(Value::Object(obj), name, value, false)

                  }
                }
              } else {
                fn(value) raise {
                  if strict {
                    let _ = throw_reference_error("undefined variable")

                  } else {
                    let _ = property_set(Value::Object(obj), name, value, false)

                  }
                }
              }
            None => fn(value) { env.bindings.set(name, value) }
          }
      }
  }
}

///|
fn prepare_member_assignment_target_with_roots(
  env : Env,
  obj_expr : @engine.Expr,
  key : @engine.MemberKey,
) -> (Array[Value], (Value) -> Unit raise) raise {
  let roots : Array[Value] = []
  match obj_expr {
    @engine.Expr::Super =>
      match key {
        @engine.MemberKey::Private(_) => {
          let _ = throw_syntax_error(
            "private class field forbidden after super",
          )
          (roots, fn(_) { () })
        }
        @engine.MemberKey::Computed(expr) => {
          let receiver = env_get(env, "this")
          let base = resolve_super_base()
          let key_value = eval_expr(env, expr)
          roots.push(receiver)
          roots.push(base)
          roots.push(key_value)
          (
            roots,
            fn(value) raise {
              let name = property_key_name(key_value)
              super_set_property(env, base, receiver, name, value)
            },
          )
        }
        _ => {
          let receiver = env_get(env, "this")
          let base = resolve_super_base()
          let name = member_key_name(env, key)
          roots.push(receiver)
          roots.push(base)
          (
            roots,
            fn(value) raise {
              super_set_property(env, base, receiver, name, value)
            },
          )
        }
      }
    _ =>
      match key {
        @engine.MemberKey::Private(name) => {
          let target = eval_expr(env, obj_expr)
          let key_name = env_private_key_checked(env, name)
          roots.push(target)
          (
            roots,
            fn(value) raise {
              private_property_set(target, key_name, name, value)
            },
          )
        }
        @engine.MemberKey::Computed(expr) => {
          let target = eval_expr(env, obj_expr)
          let key_value = eval_expr(env, expr)
          roots.push(target)
          roots.push(key_value)
          (
            roots,
            fn(value) raise {
              let name = property_key_name(key_value)
              let _ = set_property_for_value(target, name, value, env.strict)

            },
          )
        }
        _ => {
          let target = eval_expr(env, obj_expr)
          let name = member_key_name(env, key)
          roots.push(target)
          (
            roots,
            fn(value) raise {
              let _ = set_property_for_value(target, name, value, env.strict)

            },
          )
        }
      }
  }
}

///|
fn prepare_assignment_target_with_roots(
  env : Env,
  expr : @engine.Expr,
) -> (Array[Value], (Value) -> Unit raise) raise {
  match expr {
    @engine.Expr::Ident(name, _) =>
      ([], prepare_identifier_assignment_target(env, name, env.strict))
    @engine.Expr::Member(obj_expr, key, _) =>
      prepare_member_assignment_target_with_roots(env, obj_expr, key)
    @engine.Expr::Paren(inner) =>
      prepare_assignment_target_with_roots(env, inner)
    _ => {
      let _ = throw_type_error("invalid assignment target")
      ([], fn(_) { () })
    }
  }
}
