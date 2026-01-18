///|
fn hoist_decls(
  env : Env,
  body : Array[@engine.Stmt],
  configurable : Bool,
  eval_skip_outer_var : Bool,
) -> Unit raise {
  for stmt in body {
    hoist_functions(env, stmt, false, configurable)
  }
  for stmt in body {
    hoist_vars(env, stmt, configurable, eval_skip_outer_var)
  }
}

///|
fn hoist_functions(
  env : Env,
  stmt : @engine.Stmt,
  in_block : Bool,
  configurable : Bool,
) -> Unit raise {
  match stmt {
    FunctionDecl(func) => {
      if env.strict && in_block {
        return
      }
      if in_block && (func.is_async || func.is_generator) {
        return
      }
      match func.name {
        Some(name) => {
          ensure_strict_binding_name(env, name)
          let target_env = var_env_for(env)
          if in_block {
            if annex_b_var_func_decl_contains(func) {
              env_declare_var(target_env, name, configurable)
            }
          } else {
            let value = Value::Function(to_function_value(env, func, false))
            let is_new = target_env.bindings.get(name) is None
            match target_env.parent {
              None =>
                global_create_function_binding(
                  target_env, name, value, configurable,
                )
              Some(_) =>
                if target_env.var_object is Some(_) {
                  env_set_local_or_var_object(
                    target_env, name, value, configurable,
                  )
                } else {
                  if configurable && is_new {
                    target_env.deletable_bindings.set(name, true)
                  }
                  env_set_local(target_env, name, value)
                }
            }
          }
        }
        None => ()
      }
    }
    Block(stmts) =>
      if current_module_env_id() is Some(_) && block_is_export_wrapper(stmts) {
        for inner in stmts {
          hoist_functions(env, inner, in_block, configurable)
        }
      } else {
        for inner in stmts {
          hoist_functions(env, inner, true, configurable)
        }
      }
    Label(_, body) => hoist_functions(env, body, in_block, configurable)
    With(_, body) => hoist_functions(env, body, true, configurable)
    If(_, conseq, alt) => {
      hoist_functions(env, conseq, true, configurable)
      match alt {
        Some(stmt) => hoist_functions(env, stmt, true, configurable)
        None => ()
      }
    }
    While(_, body) => hoist_functions(env, body, true, configurable)
    For(_, _, _, body) => hoist_functions(env, body, true, configurable)
    ForIn(_, _, body) => hoist_functions(env, body, true, configurable)
    ForOf(_, _, body) => hoist_functions(env, body, true, configurable)
    ForAwaitOf(_, _, body) => hoist_functions(env, body, true, configurable)
    DoWhile(body, _) => hoist_functions(env, body, true, configurable)
    Switch(_, cases) =>
      for clause in cases {
        let body = match clause {
          @engine.SwitchCase::Case(_, stmts) => stmts
          @engine.SwitchCase::Default(stmts) => stmts
        }
        for stmt in body {
          hoist_functions(env, stmt, true, configurable)
        }
      }
    Try(try_body, catch_clause, finally_body) => {
      hoist_functions(env, try_body, true, configurable)
      match catch_clause {
        Some(clause) => hoist_functions(env, clause.body, true, configurable)
        None => ()
      }
      match finally_body {
        Some(stmt) => hoist_functions(env, stmt, true, configurable)
        None => ()
      }
    }
    _ => ()
  }
}

///|
fn hoist_vars(
  env : Env,
  stmt : @engine.Stmt,
  configurable : Bool,
  eval_skip_outer_var : Bool,
) -> Unit raise {
  match stmt {
    VarDecl(kind, decls) =>
      if kind is @engine.VarKind::Var {
        let target_env = var_env_for(env)
        for decl in decls {
          let (binding, _) = decl
          if eval_skip_outer_var {
            declare_var_binding_names_skip_outer_var(
              env, target_env, binding, configurable,
            )
          } else {
            declare_var_binding_names(target_env, binding, configurable)
          }
        }
      }
    Block(stmts) =>
      for inner in stmts {
        hoist_vars(env, inner, configurable, eval_skip_outer_var)
      }
    Label(_, body) => hoist_vars(env, body, configurable, eval_skip_outer_var)
    With(_, body) => hoist_vars(env, body, configurable, eval_skip_outer_var)
    If(_, conseq, alt) => {
      hoist_vars(env, conseq, configurable, eval_skip_outer_var)
      match alt {
        Some(stmt) => hoist_vars(env, stmt, configurable, eval_skip_outer_var)
        None => ()
      }
    }
    While(_, body) => hoist_vars(env, body, configurable, eval_skip_outer_var)
    For(init, _, _, body) => {
      match init {
        @engine.ForInit::Var(kind, decls) =>
          if kind is @engine.VarKind::Var {
            let target_env = var_env_for(env)
            for decl in decls {
              let (binding, _) = decl
              if eval_skip_outer_var {
                declare_var_binding_names_skip_outer_var(
                  env, target_env, binding, configurable,
                )
              } else {
                declare_var_binding_names(target_env, binding, configurable)
              }
            }
          }
        _ => ()
      }
      hoist_vars(env, body, configurable, eval_skip_outer_var)
    }
    ForIn(init, _, body) => {
      match init {
        @engine.ForInit::Var(kind, decls) =>
          if kind is @engine.VarKind::Var {
            let target_env = var_env_for(env)
            for decl in decls {
              let (binding, _) = decl
              if eval_skip_outer_var {
                declare_var_binding_names_skip_outer_var(
                  env, target_env, binding, configurable,
                )
              } else {
                declare_var_binding_names(target_env, binding, configurable)
              }
            }
          }
        _ => ()
      }
      hoist_vars(env, body, configurable, eval_skip_outer_var)
    }
    ForOf(init, _, body) => {
      match init {
        @engine.ForInit::Var(kind, decls) =>
          if kind is @engine.VarKind::Var {
            let target_env = var_env_for(env)
            for decl in decls {
              let (binding, _) = decl
              if eval_skip_outer_var {
                declare_var_binding_names_skip_outer_var(
                  env, target_env, binding, configurable,
                )
              } else {
                declare_var_binding_names(target_env, binding, configurable)
              }
            }
          }
        _ => ()
      }
      hoist_vars(env, body, configurable, eval_skip_outer_var)
    }
    ForAwaitOf(init, _, body) => {
      match init {
        @engine.ForInit::Var(kind, decls) =>
          if kind is @engine.VarKind::Var {
            let target_env = var_env_for(env)
            for decl in decls {
              let (binding, _) = decl
              if eval_skip_outer_var {
                declare_var_binding_names_skip_outer_var(
                  env, target_env, binding, configurable,
                )
              } else {
                declare_var_binding_names(target_env, binding, configurable)
              }
            }
          }
        _ => ()
      }
      hoist_vars(env, body, configurable, eval_skip_outer_var)
    }
    DoWhile(body, _) => hoist_vars(env, body, configurable, eval_skip_outer_var)
    Switch(_, cases) =>
      for clause in cases {
        let body = match clause {
          @engine.SwitchCase::Case(_, stmts) => stmts
          @engine.SwitchCase::Default(stmts) => stmts
        }
        for stmt in body {
          hoist_vars(env, stmt, configurable, eval_skip_outer_var)
        }
      }
    Try(try_body, catch_clause, finally_body) => {
      hoist_vars(env, try_body, configurable, eval_skip_outer_var)
      match catch_clause {
        Some(clause) =>
          hoist_vars(env, clause.body, configurable, eval_skip_outer_var)
        None => ()
      }
      match finally_body {
        Some(stmt) => hoist_vars(env, stmt, configurable, eval_skip_outer_var)
        None => ()
      }
    }
    Throw(_) => ()
    FunctionDecl(_) => ()
    ClassDecl(_) => ()
    Return(_) => ()
    ExprStmt(_) => ()
    Break(_) => ()
    Continue(_) => ()
    Empty => ()
  }
}
