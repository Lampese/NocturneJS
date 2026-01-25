///|
fn eval_var_decls(
  env : Env,
  kind : @engine.VarKind,
  decls : Array[(@engine.VarBinding, @engine.Expr?)],
) -> Unit raise {
  let assign = fn(name : String, value : Value) raise {
    match kind {
      @engine.VarKind::ConstDecl => env_define_readonly(env, name, value, true)
      @engine.VarKind::Var => env_set(env, name, value)
      _ => env_initialize_binding(env, name, value)
    }
  }
  for decl in decls {
    let (binding, init) = decl
    match binding {
      @engine.VarBinding::Name(name) =>
        match init {
          Some(expr) =>
            if kind is @engine.VarKind::Var {
              let target = resolve_binding_target(env, name)
              let value = eval_named_expr(env, expr, name, None)
              match target {
                BindingTarget::WithObject(obj) => {
                  let _ = property_set(obj, name, value, env.strict)

                }
                BindingTarget::VarObject(obj) => {
                  let _ = property_set(obj, name, value, env.strict)

                }
                BindingTarget::Env(target_env) =>
                  env_set_local(target_env, name, value)
                BindingTarget::Unbound =>
                  env_set_with_strict_no_with(
                    var_env_for(env),
                    name,
                    value,
                    env.strict,
                  )
              }
            } else {
              let value = eval_named_expr(env, expr, name, None)
              assign(name, value)
            }
          None =>
            if kind is @engine.VarKind::ConstDecl {
              let _ = throw_syntax_error(
                "missing initializer in const declaration",
              )

            } else if !(kind is @engine.VarKind::Var) {
              env_initialize_binding(env, name, Undefined)
            }
        }
      @engine.VarBinding::ArrayPattern(pattern) =>
        match init {
          Some(expr) => {
            let value = eval_expr(env, expr)
            destructure_array_pattern(env, pattern, value, fn(name, v) raise {
              assign(name, v)
            })
          }
          None =>
            if kind is @engine.VarKind::ConstDecl {
              let _ = throw_syntax_error(
                "missing initializer in const declaration",
              )

            } else if !(kind is @engine.VarKind::Var) {
              declare_binding_names(env, binding)
            }
        }
      @engine.VarBinding::ObjectPattern(pattern) =>
        match init {
          Some(expr) => {
            let value = eval_expr(env, expr)
            destructure_object_pattern(env, pattern, value, fn(name, v) raise {
              assign(name, v)
            })
          }
          None =>
            if kind is @engine.VarKind::ConstDecl {
              let _ = throw_syntax_error(
                "missing initializer in const declaration",
              )

            } else if !(kind is @engine.VarKind::Var) {
              declare_binding_names(env, binding)
            }
        }
      @engine.VarBinding::Target(_) => {
        let _ = throw_syntax_error("invalid binding")

      }
    }
  }
}

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

///|
fn eval_var_decls_no_name(
  env : Env,
  kind : @engine.VarKind,
  decls : Array[(@engine.VarBinding, @engine.Expr?)],
) -> Unit raise {
  let assign = fn(name : String, value : Value) raise {
    match kind {
      @engine.VarKind::ConstDecl => env_define_readonly(env, name, value, true)
      @engine.VarKind::Var => env_set(env, name, value)
      _ => env_initialize_binding(env, name, value)
    }
  }
  for decl in decls {
    let (binding, init) = decl
    match binding {
      @engine.VarBinding::Name(name) =>
        match init {
          Some(expr) =>
            if kind is @engine.VarKind::Var {
              let target = resolve_binding_target(env, name)
              let value = eval_expr(env, expr)
              match target {
                BindingTarget::WithObject(obj) => {
                  let _ = property_set(obj, name, value, env.strict)

                }
                BindingTarget::VarObject(obj) => {
                  let _ = property_set(obj, name, value, env.strict)

                }
                BindingTarget::Env(target_env) =>
                  env_set_local(target_env, name, value)
                BindingTarget::Unbound =>
                  env_set_with_strict_no_with(
                    var_env_for(env),
                    name,
                    value,
                    env.strict,
                  )
              }
            } else {
              let value = eval_named_expr(env, expr, name, None)
              assign(name, value)
            }
          None =>
            if kind is @engine.VarKind::ConstDecl {
              let _ = throw_syntax_error(
                "missing initializer in const declaration",
              )

            } else if !(kind is @engine.VarKind::Var) {
              env_initialize_binding(env, name, Undefined)
            }
        }
      @engine.VarBinding::ArrayPattern(pattern) =>
        match init {
          Some(expr) => {
            let value = eval_expr(env, expr)
            destructure_array_pattern(env, pattern, value, fn(name, v) raise {
              assign(name, v)
            })
          }
          None =>
            if kind is @engine.VarKind::ConstDecl {
              let _ = throw_syntax_error(
                "missing initializer in const declaration",
              )

            } else if !(kind is @engine.VarKind::Var) {
              declare_binding_names(env, binding)
            }
        }
      @engine.VarBinding::ObjectPattern(pattern) =>
        match init {
          Some(expr) => {
            let value = eval_expr(env, expr)
            destructure_object_pattern(env, pattern, value, fn(name, v) raise {
              assign(name, v)
            })
          }
          None =>
            if kind is @engine.VarKind::ConstDecl {
              let _ = throw_syntax_error(
                "missing initializer in const declaration",
              )

            } else if !(kind is @engine.VarKind::Var) {
              declare_binding_names(env, binding)
            }
        }
      @engine.VarBinding::Target(_) => {
        let _ = throw_syntax_error("invalid binding")

      }
    }
  }
}

///|
fn eval_for_init(env : Env, init : @engine.ForInit) -> Unit raise {
  match init {
    @engine.ForInit::None => ()
    @engine.ForInit::Var(kind, decls) => eval_var_decls(env, kind, decls)
    @engine.ForInit::Expr(expr) => {
      let _ = eval_expr(env, expr)

    }
  }
}
