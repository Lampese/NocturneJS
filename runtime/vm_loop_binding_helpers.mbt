///|
fn initialize_var_binding(
  env : Env,
  binding : @engine.VarBinding,
  value : Value,
) -> Unit raise {
  let assign = fn(name : String, v : Value) raise {
    initialize_binding_assign(env, name, v)
  }
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
fn assign_var_binding(
  env : Env,
  binding : @engine.VarBinding,
  value : Value,
  use_env_set : Bool,
) -> Unit raise {
  let assign = fn(name : String, v : Value) raise {
    if use_env_set {
      env_set(env, name, v)
    } else {
      env_set_local(env, name, v)
    }
  }
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
fn assign_for_in_expr(
  env : Env,
  expr : @engine.Expr,
  value : Value,
) -> Unit raise {
  match expr {
    @engine.Expr::Ident(name, _) => env_set(env, name, value)
    @engine.Expr::Member(obj_expr, key, _) =>
      match obj_expr {
        @engine.Expr::Super =>
          match key {
            @engine.MemberKey::Private(_) => {
              let _ = throw_syntax_error(
                "private class field forbidden after super",
              )
              ()
            }
            @engine.MemberKey::Computed(expr) => {
              let receiver = env_get(env, "this")
              let base = resolve_super_base()
              let key_value = eval_expr(env, expr)
              let name = property_key_name(key_value)
              super_set_property(env, base, receiver, name, value)
            }
            _ => {
              let receiver = env_get(env, "this")
              let base = resolve_super_base()
              let name = member_key_name(env, key)
              super_set_property(env, base, receiver, name, value)
            }
          }
        _ =>
          match key {
            @engine.MemberKey::Private(name) => {
              let target = eval_expr(env, obj_expr)
              let key_name = env_private_key_checked(env, name)
              private_property_set(target, key_name, name, value)
            }
            _ => {
              let target = eval_expr(env, obj_expr)
              let name = member_key_name(env, key)
              let _ = set_property_for_value(target, name, value, env.strict)

            }
          }
      }
    @engine.Expr::Paren(inner) => assign_for_in_expr(env, inner, value)
    @engine.Expr::AssignArray(pattern, _) =>
      destructure_array_pattern(env, pattern, value, fn(name, v) raise {
        env_set(env, name, v)
      })
    @engine.Expr::AssignObject(pattern, _) =>
      destructure_object_pattern(env, pattern, value, fn(name, v) raise {
        env_set(env, name, v)
      })
    @engine.Expr::Call(_, _, _) => {
      let _ = eval_expr(env, expr)
      let _ = throw_reference_error("invalid assignment target")

    }
    _ => {
      let _ = throw_type_error("invalid for-in target")

    }
  }
}

///|
fn gen_assign_for_in_expr(
  env : Env,
  expr : @engine.Expr,
  value : Value,
  k : () -> GenStep raise,
) -> GenStep raise {
  gen_with_root_frame(fn() raise {
    let _ = gen_root_push(value)
    match expr {
      @engine.Expr::Ident(name, _) => {
        env_set(env, name, value)
        k()
      }
      @engine.Expr::Member(obj_expr, key, _) =>
        match obj_expr {
          @engine.Expr::Super =>
            match key {
              @engine.MemberKey::Private(_) => {
                let _ = throw_syntax_error(
                  "private class field forbidden after super",
                )
                k()
              }
              @engine.MemberKey::Computed(expr) =>
                gen_eval_expr_cont(env, expr, fn(key_value) raise {
                  let receiver = env_get(env, "this")
                  let base = resolve_super_base()
                  let name = property_key_name(key_value)
                  super_set_property(env, base, receiver, name, value)
                  k()
                })
              _ => {
                let receiver = env_get(env, "this")
                let base = resolve_super_base()
                let name = member_key_name(env, key)
                super_set_property(env, base, receiver, name, value)
                k()
              }
            }
          _ =>
            match key {
              @engine.MemberKey::Private(name) =>
                gen_eval_expr_cont(env, obj_expr, fn(target) raise {
                  let key_name = env_private_key_checked(env, name)
                  private_property_set(target, key_name, name, value)
                  k()
                })
              _ =>
                gen_eval_expr_cont(env, obj_expr, fn(target) raise {
                  let _ = gen_root_push(target)
                  gen_eval_member_key(env, key, fn(name) raise {
                    let _ = set_property_for_value(
                      target,
                      name,
                      value,
                      env.strict,
                    )
                    k()
                  })
                })
            }
        }
      @engine.Expr::Paren(inner) => gen_assign_for_in_expr(env, inner, value, k)
      @engine.Expr::AssignArray(pattern, _) =>
        gen_destructure_array_pattern(
          env,
          pattern,
          value,
          fn(name, v) raise { env_set(env, name, v) },
          k,
        )
      @engine.Expr::AssignObject(pattern, _) => {
        let obj = to_object(value)
        gen_destructure_object_pattern(
          env,
          pattern,
          obj,
          [],
          0,
          fn(name, v) raise { env_set(env, name, v) },
          k,
        )
      }
      @engine.Expr::Call(_, _, _) =>
        gen_eval_expr_cont(env, expr, fn(_) raise {
          let _ = throw_reference_error("invalid assignment target")
          Done(Throw(Undefined))
        })
      _ => {
        let _ = throw_type_error("invalid for-in target")
        Done(Throw(Undefined))
      }
    }
  })
}
