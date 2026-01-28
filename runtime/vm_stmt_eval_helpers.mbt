///|
fn eval_stmt(
  env : Env,
  stmt : @engine.Stmt,
) -> Value raise {
  if current_module_env_id() is Some(_) && stmt_is_import_decl(stmt) {
    return Undefined
  }
  bytecode_eval_block(env, [stmt])
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
  bytecode_eval_block(env, body)
}

///|
fn eval_block_raw(env : Env, body : Array[@engine.Stmt]) -> Value raise {
  bytecode_eval_block(env, body)
}

///|
fn eval_stmt_in_block_if_needed(
  env : Env,
  stmt : @engine.Stmt,
) -> Value raise {
  eval_stmt(env, stmt)
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
