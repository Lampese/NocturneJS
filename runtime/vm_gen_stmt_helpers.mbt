///|
fn gen_eval_stmt_seq(
  env : Env,
  stmts : Array[@engine.Stmt],
  index : Int,
) -> GenStep raise {
  if index >= stmts.length() {
    return Done(Normal(Undefined))
  }
  gen_bind(gen_eval_stmt(env, stmts[index]), fn(control) {
    match control {
      Normal(value) => {
        gc_maybe_collect_with_root(value)
        gen_eval_stmt_seq(env, stmts, index + 1)
      }
      _ => Done(control)
    }
  })
}

///|
fn gen_eval_block(env : Env, stmts : Array[@engine.Stmt]) -> GenStep raise {
  gen_bytecode_eval_block(env, stmts)
}

///|
fn gen_eval_stmt_in_block_if_needed(
  env : Env,
  stmt : @engine.Stmt,
) -> GenStep raise {
  match stmt {
    @engine.Stmt::Block(_) => gen_eval_stmt(env, stmt)
    _ =>
      if stmt_needs_lex_env(stmt, env.strict) {
        let block_env = Env::new(Some(env))
        let stmts : Array[@engine.Stmt] = [stmt]
        hoist_block_functions(block_env, stmts)
        with_env_step(block_env, fn() raise {
          eval_predeclare_lexical_decls(block_env, stmts)
          gen_eval_stmt(block_env, stmt)
        })
      } else {
        gen_eval_stmt(env, stmt)
      }
  }
}

///|
fn gen_eval_stmt(env : Env, stmt : @engine.Stmt) -> GenStep raise {
  gen_bytecode_eval_block(env, [stmt])
}

///|
fn gen_eval_var_decls(
  env : Env,
  kind : @engine.VarKind,
  decls : Array[(@engine.VarBinding, @engine.Expr?)],
  index : Int,
) -> GenStep raise {
  if index >= decls.length() {
    return Done(Normal(Undefined))
  }
  let assign = fn(name : String, value : Value) raise {
    match kind {
      @engine.VarKind::ConstDecl => env_define_readonly(env, name, value, true)
      @engine.VarKind::Var => env_set(env, name, value)
      _ => env_initialize_binding(env, name, value)
    }
  }
  let (binding, init) = decls[index]
  match binding {
    @engine.VarBinding::Name(name) =>
      match init {
        Some(expr) =>
          gen_eval_expr_cont(env, expr, fn(value) raise {
            assign(name, value)
            gen_eval_var_decls(env, kind, decls, index + 1)
          })
        None =>
          if kind is @engine.VarKind::ConstDecl {
            let _ = throw_syntax_error(
              "missing initializer in const declaration",
            )
            Done(Throw(Undefined))
          } else if kind is @engine.VarKind::Var {
            gen_eval_var_decls(env, kind, decls, index + 1)
          } else {
            env_declare(env, name, Undefined)
            gen_eval_var_decls(env, kind, decls, index + 1)
          }
      }
    @engine.VarBinding::ArrayPattern(pattern) =>
      match init {
        Some(expr) =>
          gen_eval_expr_cont(env, expr, fn(value) raise {
            gen_destructure_array_pattern(
              env,
              pattern,
              value,
              fn(name, v) raise { assign(name, v) },
              fn() raise { gen_eval_var_decls(env, kind, decls, index + 1) },
            )
          })
        None =>
          if kind is @engine.VarKind::ConstDecl {
            let _ = throw_syntax_error(
              "missing initializer in const declaration",
            )
            Done(Throw(Undefined))
          } else if kind is @engine.VarKind::Var {
            gen_eval_var_decls(env, kind, decls, index + 1)
          } else {
            declare_binding_names(env, binding)
            gen_eval_var_decls(env, kind, decls, index + 1)
          }
      }
    @engine.VarBinding::ObjectPattern(pattern) =>
      match init {
        Some(expr) =>
          gen_eval_expr_cont(env, expr, fn(value) raise {
            let obj = to_object(value)
            gen_destructure_object_pattern(
              env,
              pattern,
              obj,
              [],
              0,
              fn(name, v) raise { assign(name, v) },
              fn() raise { gen_eval_var_decls(env, kind, decls, index + 1) },
            )
          })
        None =>
          if kind is @engine.VarKind::ConstDecl {
            let _ = throw_syntax_error(
              "missing initializer in const declaration",
            )
            Done(Throw(Undefined))
          } else if kind is @engine.VarKind::Var {
            gen_eval_var_decls(env, kind, decls, index + 1)
          } else {
            declare_binding_names(env, binding)
            gen_eval_var_decls(env, kind, decls, index + 1)
          }
      }
    @engine.VarBinding::Target(_) => {
      let _ = throw_syntax_error("invalid binding")
      Done(Throw(Undefined))
    }
  }
}

///|
fn gen_eval_try(
  env : Env,
  try_body : @engine.Stmt,
  catch_clause : @engine.CatchClause?,
  finally_body : @engine.Stmt?,
) -> GenStep raise {
  gen_bind(gen_eval_stmt(env, try_body), fn(control) raise {
    let handled = match control {
      Throw(err_value) =>
        match catch_clause {
          Some(clause) => gen_eval_catch(env, clause, err_value)
          None => Done(Throw(err_value))
        }
      _ => Done(control)
    }
    gen_bind(handled, fn(pending) raise {
      match finally_body {
        Some(final_body) =>
          gen_apply_finally(pending, gen_eval_stmt(env, final_body))
        None => Done(pending)
      }
    })
  })
}

///|
fn gen_eval_catch(
  env : Env,
  clause : @engine.CatchClause,
  err_value : Value,
) -> GenStep raise {
  let catch_env = Env::new(Some(env))
  catch_env.is_catch_env = true
  match clause.name {
    Some(name) => env_set_local(catch_env, name, err_value)
    None => ()
  }
  gen_eval_stmt(catch_env, clause.body)
}

///|
fn gen_apply_finally(
  pending : GenControl,
  final_step : GenStep,
) -> GenStep raise {
  gen_bind(final_step, fn(final_control) {
    match final_control {
      Normal(_) => Done(pending)
      Return(value) => Done(Return(value))
      Throw(value) => Done(Throw(value))
      Break(label, value_opt) => Done(Break(label, value_opt))
      Continue(label, value_opt) => Done(Continue(label, value_opt))
    }
  })
}
