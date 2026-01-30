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
