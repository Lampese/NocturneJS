///|
fn eval_stmt(env : Env, stmt : @engine.Stmt) -> Value raise {
  if current_module_env_id() is Some(_) && stmt_is_import_decl(stmt) {
    return Undefined
  }
  match stmt {
    Empty => Undefined
    ExprStmt(expr) => eval_expr(env, expr)
    VarDecl(kind, decls) => {
      eval_var_decls(env, kind, decls)
      Undefined
    }
    Block(stmts) => eval_block(env, stmts)
    FunctionDecl(func) => {
      if top_level_function_decl_contains(func) {
        return Undefined
      }
      let decl_value = Value::Function(to_function_value(env, func, false))
      let value = match func.name {
        Some(name) =>
          match env.bindings.get(name) {
            Some(_) => env_get_binding_value(env, name)
            None =>
              match env.var_object {
                Some(Object(obj)) =>
                  match obj.props.get(name) {
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
                        match obj.props.get(name) {
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
      Undefined
    }
    ClassDecl(class_def) => {
      let value = eval_class(env, class_def)
      match class_def.name {
        Some(name) => env_initialize_binding(env, name, value)
        None => ()
      }
      Undefined
    }
    If(cond_expr, conseq, alt) => {
      let cond = eval_expr(env, cond_expr)
      if is_truthy(cond) {
        eval_stmt_in_block_if_needed(env, conseq)
      } else {
        match alt {
          Some(stmt) => eval_stmt_in_block_if_needed(env, stmt)
          None => Undefined
        }
      }
    }
    With(expr, body) => {
      if env.strict {
        let _ = throw_syntax_error("with not allowed in strict mode")

      }
      let obj = to_object(eval_expr(env, expr))
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
      with_env_value(with_env, fn() raise { eval_stmt(with_env, body) })
    }
    While(cond_expr, body) => eval_while_with_label(env, None, cond_expr, body)
    For(init, test_expr, update_expr, body) =>
      eval_for_with_label(env, None, init, test_expr, update_expr, body)
    ForIn(init, expr, body) =>
      eval_for_in_with_label(env, None, init, expr, body)
    ForOf(init, expr, body) =>
      eval_for_of_with_label(env, None, init, expr, body)
    ForAwaitOf(init, expr, body) =>
      eval_for_await_of_with_label(env, None, init, expr, body)
    DoWhile(body, cond_expr) =>
      eval_do_while_with_label(env, None, body, cond_expr)
    Switch(discriminant, cases) =>
      eval_switch_with_label(env, None, discriminant, cases)
    Throw(expr) => {
      let value = eval_expr(env, expr)
      raise ThrowSignal(value)
    }
    Try(try_body, catch_clause, finally_body) => {
      let (pending, try_last) = match try_body {
        @engine.Stmt::Block(stmts) =>
          eval_block_control_with_last_stmts(env, stmts)
        _ => (eval_block_control(env, try_body), Undefined)
      }
      let handled = match pending {
        Throw(err_value) =>
          match catch_clause {
            Some(clause) => {
              let catch_env = Env::new(Some(env))
              catch_env.is_catch_env = true
              match clause.name {
                Some(name) => env_set_local(catch_env, name, err_value)
                None => ()
              }
              let catch_updates = stmt_updates_completion(clause.body)
              let caught = match clause.body {
                @engine.Stmt::Block(stmts) =>
                  match clause.name {
                    Some(name) =>
                      if stmts.length() > 0 {
                        match stmts[0] {
                          @engine.Stmt::VarDecl(kind, decls) =>
                            if !(kind is @engine.VarKind::Let) {
                              eval_block_control(catch_env, clause.body)
                            } else if decls.length() == 1 {
                              let (_, init_expr_opt) = decls[0]
                              match init_expr_opt {
                                Some(@engine.Expr::Ident(init_name, _)) =>
                                  if init_name == name {
                                    eval_var_decls(catch_env, kind, decls)
                                    let rest : Array[@engine.Stmt] = []
                                    let mut idx = 1
                                    while idx < stmts.length() {
                                      rest.push(stmts[idx])
                                      idx = idx + 1
                                    }
                                    eval_block_control_stmts(catch_env, rest)
                                  } else {
                                    eval_block_control(catch_env, clause.body)
                                  }
                                _ => eval_block_control(catch_env, clause.body)
                              }
                            } else {
                              eval_block_control(catch_env, clause.body)
                            }
                          _ => eval_block_control(catch_env, clause.body)
                        }
                      } else {
                        eval_block_control(catch_env, clause.body)
                      }
                    None => eval_block_control(catch_env, clause.body)
                  }
                _ => eval_block_control(catch_env, clause.body)
              }
              match caught {
                Normal(value) =>
                  if catch_updates {
                    Normal(value)
                  } else {
                    Normal(try_last)
                  }
                Break(label, value_opt) =>
                  match value_opt {
                    Some(_) => caught
                    None => Break(label, Some(try_last))
                  }
                Continue(label, value_opt) =>
                  match value_opt {
                    Some(_) => caught
                    None => Continue(label, Some(try_last))
                  }
                _ => caught
              }
            }
            None => pending
          }
        _ => pending
      }
      let final_control = match finally_body {
        Some(final_body) =>
          apply_finally_control(handled, eval_block_control(env, final_body))
        None => handled
      }
      match final_control {
        Normal(value) => value
        Throw(value) => raise ThrowSignal(value)
        Return(value) => raise ReturnSignal(value)
        Break(label, value) => raise BreakSignal((label, value))
        Continue(label, value) => raise ContinueSignal((label, value))
      }
    }
    Break(label) => raise BreakSignal((label, None))
    Continue(label) => raise ContinueSignal((label, None))
    Label(name, body) =>
      match body {
        While(cond_expr, inner) =>
          eval_while_with_label(env, Some(name), cond_expr, inner)
        For(init, test_expr, update_expr, inner) =>
          eval_for_with_label(
            env,
            Some(name),
            init,
            test_expr,
            update_expr,
            inner,
          )
        ForIn(init, expr, inner) =>
          eval_for_in_with_label(env, Some(name), init, expr, inner)
        ForOf(init, expr, inner) =>
          eval_for_of_with_label(env, Some(name), init, expr, inner)
        ForAwaitOf(init, expr, inner) =>
          eval_for_await_of_with_label(env, Some(name), init, expr, inner)
        DoWhile(inner, cond_expr) =>
          eval_do_while_with_label(env, Some(name), inner, cond_expr)
        Switch(discriminant, cases) =>
          eval_switch_with_label(env, Some(name), discriminant, cases)
        _ =>
          eval_stmt(env, body) catch {
            BreakSignal((signal_label, value_opt)) =>
              match signal_label {
                Some(label) =>
                  if label == name {
                    update_empty(value_opt, Undefined)
                  } else {
                    raise BreakSignal((signal_label, value_opt))
                  }
                None => raise BreakSignal((signal_label, value_opt))
              }
            ContinueSignal((signal_label, value_opt)) =>
              raise ContinueSignal((signal_label, value_opt))
            err => raise err
          }
      }
    Return(expr) => {
      let value = match expr {
        Some(inner) => eval_expr(env, inner)
        None => Undefined
      }
      raise ReturnSignal(value)
    }
  }
}

///|
fn expr_is_export_call(expr : @engine.Expr) -> Bool {
  match expr {
    @engine.Expr::Call(callee, _, _) =>
      match callee {
        @engine.Expr::Ident(name, _) => name == "__export"
        _ => false
      }
    @engine.Expr::Sequence(exprs) => {
      for inner in exprs {
        if expr_is_export_call(inner) {
          return true
        }
      }
      false
    }
    @engine.Expr::Paren(inner) => expr_is_export_call(inner)
    _ => false
  }
}

///|
fn block_is_export_wrapper(body : Array[@engine.Stmt]) -> Bool {
  if body.length() == 1 {
    return match body[0] {
      @engine.Stmt::ExprStmt(expr) => expr_is_export_call(expr)
      _ => false
    }
  }
  if body.length() != 2 {
    return false
  }
  let first_ok = match body[0] {
    @engine.Stmt::FunctionDecl(_)
    | @engine.Stmt::ClassDecl(_)
    | @engine.Stmt::VarDecl(_, _) => true
    _ => false
  }
  if !first_ok {
    return false
  }
  match body[1] {
    @engine.Stmt::ExprStmt(expr) => expr_is_export_call(expr)
    _ => false
  }
}

///|
fn eval_block(env : Env, body : Array[@engine.Stmt]) -> Value raise {
  if current_module_env_id() is Some(_) && block_is_export_wrapper(body) {
    return eval_block_raw(env, body)
  }
  if block_needs_lex_env(body, env.strict) {
    let block_env = Env::new(Some(env))
    hoist_block_functions(block_env, body)
    eval_predeclare_lexical_decls(block_env, body)
    with_env_value(block_env, fn() raise { eval_block_raw(block_env, body) })
  } else {
    eval_block_raw(env, body)
  }
}

///|
fn eval_block_raw(env : Env, body : Array[@engine.Stmt]) -> Value raise {
  let mut last = Undefined
  for stmt in body {
    try {
      let value = eval_stmt(env, stmt)
      if stmt_updates_completion(stmt) {
        last = value
      }
    } catch {
      BreakSignal((label, value_opt)) => {
        let updated = update_empty(value_opt, last)
        raise BreakSignal((label, Some(updated)))
      }
      ContinueSignal((label, value_opt)) => {
        let updated = update_empty(value_opt, last)
        raise ContinueSignal((label, Some(updated)))
      }
      err => raise err
    }
  }
  last
}

///|
fn eval_stmt_in_block_if_needed(env : Env, stmt : @engine.Stmt) -> Value raise {
  match stmt {
    @engine.Stmt::Block(_) => eval_stmt(env, stmt)
    _ =>
      if stmt_needs_lex_env(stmt, env.strict) {
        let block_env = Env::new(Some(env))
        let stmts : Array[@engine.Stmt] = [stmt]
        hoist_block_functions(block_env, stmts)
        eval_predeclare_lexical_decls(block_env, stmts)
        with_env_value(block_env, fn() raise { eval_stmt(block_env, stmt) })
      } else {
        eval_stmt(env, stmt)
      }
  }
}

///|
fn block_needs_lex_env(body : Array[@engine.Stmt], is_strict : Bool) -> Bool {
  for stmt in body {
    if stmt_needs_lex_env(stmt, is_strict) {
      return true
    }
  }
  false
}

///|
fn stmt_needs_lex_env(stmt : @engine.Stmt, is_strict : Bool) -> Bool {
  match stmt {
    @engine.Stmt::VarDecl(kind, _) => !(kind is @engine.VarKind::Var)
    @engine.Stmt::ClassDecl(_) => true
    @engine.Stmt::FunctionDecl(_) => true
    @engine.Stmt::Label(_, inner) => stmt_needs_lex_env(inner, is_strict)
    _ => false
  }
}

///|
fn hoist_block_functions(
  block_env : Env,
  body : Array[@engine.Stmt],
) -> Unit raise {
  for stmt in body {
    hoist_block_functions_in_stmt(block_env, stmt)
  }
}

///|
fn hoist_block_functions_in_stmt(
  block_env : Env,
  stmt : @engine.Stmt,
) -> Unit raise {
  match stmt {
    @engine.Stmt::FunctionDecl(func) =>
      match func.name {
        Some(name) => {
          let value = Value::Function(to_function_value(block_env, func, false))
          env_set_local(block_env, name, value)
        }
        None => ()
      }
    @engine.Stmt::Label(_, inner) =>
      hoist_block_functions_in_stmt(block_env, inner)
    _ => ()
  }
}
