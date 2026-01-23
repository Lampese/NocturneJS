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
  if current_module_env_id() is Some(_) && block_is_export_wrapper(stmts) {
    return gen_eval_stmt_seq(env, stmts, 0)
  }
  if block_needs_lex_env(stmts, env.strict) {
    let block_env = Env::new(Some(env))
    hoist_block_functions(block_env, stmts)
    with_env_step(block_env, fn() raise {
      eval_predeclare_lexical_decls(block_env, stmts)
      gen_eval_stmt_seq(block_env, stmts, 0)
    })
  } else {
    gen_eval_stmt_seq(env, stmts, 0)
  }
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
  if !stmt_contains_yield(stmt) && !current_function_is_generator() {
    return try {
      let value = eval_stmt(env, stmt)
      Done(Normal(value))
    } catch {
      ReturnSignal(value) => Done(Return(value))
      BreakSignal((label, value_opt)) => Done(Break(label, value_opt))
      ContinueSignal((label, value_opt)) => Done(Continue(label, value_opt))
      ThrowSignal(value) => Done(Throw(value))
      err => raise err
    }
  }
  match stmt {
    Empty => Done(Normal(Undefined))
    ExprStmt(expr) =>
      gen_eval_expr_cont(env, expr, fn(value) { Done(Normal(value)) })
    VarDecl(kind, decls) => gen_eval_var_decls(env, kind, decls, 0)
    Block(stmts) => gen_eval_block(env, stmts)
    FunctionDecl(func) => {
      eval_function_decl(env, func)
      Done(Normal(Undefined))
    }
    ClassDecl(class_def) =>
      gen_bind(gen_eval_class(env, class_def), fn(control) {
        match control {
          Normal(value) => {
            match class_def.name {
              Some(name) => env_initialize_binding(env, name, value)
              None => ()
            }
            Done(Normal(Undefined))
          }
          Return(value) => Done(Return(value))
          Throw(value) => Done(Throw(value))
          Break(label, value_opt) => Done(Break(label, value_opt))
          Continue(label, value_opt) => Done(Continue(label, value_opt))
        }
      })
    If(cond_expr, conseq, alt) =>
      gen_eval_expr_cont(env, cond_expr, fn(cond) {
        if is_truthy(cond) {
          gen_eval_stmt_in_block_if_needed(env, conseq)
        } else {
          match alt {
            Some(stmt) => gen_eval_stmt_in_block_if_needed(env, stmt)
            None => Done(Normal(Undefined))
          }
        }
      })
    With(expr, body) =>
      if env.strict {
        let _ = throw_syntax_error("with not allowed in strict mode")
        Done(Throw(Undefined))
      } else {
        gen_eval_expr_cont(env, expr, fn(value) raise {
          let obj = to_object(value)
          let with_env = Env::{
            id: alloc_id(),
            parent: Some(env),
            bindings: @hashmap.new(),
            readonly_bindings: @hashmap.new(),
            deletable_bindings: @hashmap.new(),
            private_bindings: @hashmap.new(),
            strict: env.strict,
            with_object: Some(obj),
            var_object: None,
            new_target: env.new_target,
            allow_new_target: env.allow_new_target,
            allow_super: env.allow_super,
            allow_super_call: env.allow_super_call,
            this_uninitialized: false,
            is_param_env: false,
            is_catch_env: false,
            param_binding_names: @hashmap.new(),
            uninitialized_bindings: @hashmap.new(),
            var_env: None,
          }
          register_env(with_env)
          with_env_step(with_env, fn() raise { gen_eval_stmt(with_env, body) })
        })
      }
    While(cond_expr, body) => gen_eval_while(env, None, cond_expr, body)
    For(init, test_expr, update_expr, body) =>
      gen_eval_for(env, None, init, test_expr, update_expr, body)
    ForIn(init, expr, body) => gen_eval_for_in(env, None, init, expr, body)
    ForOf(init, expr, body) => gen_eval_for_of(env, None, init, expr, body)
    ForAwaitOf(init, expr, body) =>
      gen_eval_for_await_of(env, None, init, expr, body)
    DoWhile(body, cond_expr) => gen_eval_do_while(env, None, body, cond_expr)
    Throw(expr) =>
      gen_eval_expr_cont(env, expr, fn(value) { Done(Throw(value)) })
    Try(try_body, catch_clause, finally_body) =>
      gen_eval_try(env, try_body, catch_clause, finally_body)
    Break(label) => Done(Break(label, None))
    Continue(label) => Done(Continue(label, None))
    Return(expr) => {
      let is_async_gen = current_function_is_async_generator()
      match expr {
        Some(inner) =>
          gen_eval_expr_cont(env, inner, fn(value) {
            if is_async_gen {
              gen_async_return(value)
            } else {
              Done(Return(value))
            }
          })
        None =>
          if is_async_gen {
            Done(Return(Undefined))
          } else {
            Done(Return(Undefined))
          }
      }
    }
    Label(name, body) =>
      match body {
        While(cond_expr, inner) =>
          gen_eval_while(env, Some(name), cond_expr, inner)
        For(init, test_expr, update_expr, inner) =>
          gen_eval_for(env, Some(name), init, test_expr, update_expr, inner)
        ForIn(init, expr, inner) =>
          gen_eval_for_in(env, Some(name), init, expr, inner)
        ForOf(init, expr, inner) =>
          gen_eval_for_of(env, Some(name), init, expr, inner)
        ForAwaitOf(init, expr, inner) =>
          gen_eval_for_await_of(env, Some(name), init, expr, inner)
        DoWhile(inner, cond_expr) =>
          gen_eval_do_while(env, Some(name), inner, cond_expr)
        _ =>
          gen_bind(gen_eval_stmt(env, body), fn(control) {
            match control {
              Break(label, value_opt) =>
                match label {
                  Some(label_name) =>
                    if label_name == name {
                      Done(Normal(update_empty(value_opt, Undefined)))
                    } else {
                      Done(Break(label, value_opt))
                    }
                  None => Done(Break(label, value_opt))
                }
              Continue(label, value_opt) => Done(Continue(label, value_opt))
              _ => Done(control)
            }
          })
      }
    _ => {
      let _ = throw_type_error("unsupported generator statement")
      Done(Throw(Undefined))
    }
  }
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
