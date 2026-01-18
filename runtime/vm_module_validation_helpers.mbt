///|
fn validate_context_in_script(
  script : @engine.Script,
  allow_new_target : Bool,
  allow_super : Bool,
  allow_super_call : Bool,
) -> Unit raise {
  if script.is_module {
    validate_module_decl_positions(script.body)
    validate_module_export_bindings(script)
    validate_module_returns(script.body)
  }
  validate_break_continue_in_script(script)
  for stmt in script.body {
    validate_context_in_stmt(
      stmt, allow_new_target, allow_super, allow_super_call,
    )
  }
}

///|
fn validate_module_decl_positions(body : Array[@engine.Stmt]) -> Unit raise {
  for stmt in body {
    validate_module_decl_positions_in_stmt(stmt, true)
  }
}

///|
fn validate_module_returns(body : Array[@engine.Stmt]) -> Unit raise {
  for stmt in body {
    if module_stmt_contains_return(stmt) {
      let _ = throw_syntax_error("illegal return")

    }
  }
}

///|
fn module_stmt_contains_return(stmt : @engine.Stmt) -> Bool {
  match stmt {
    @engine.Stmt::Return(_) => true
    @engine.Stmt::Block(stmts) => module_stmts_contain_return(stmts)
    @engine.Stmt::Label(_, body) => module_stmt_contains_return(body)
    @engine.Stmt::If(_, conseq, alt) =>
      if module_stmt_contains_return(conseq) {
        true
      } else {
        match alt {
          Some(inner) => module_stmt_contains_return(inner)
          None => false
        }
      }
    @engine.Stmt::While(_, body) => module_stmt_contains_return(body)
    @engine.Stmt::For(_, _, _, body) => module_stmt_contains_return(body)
    @engine.Stmt::ForIn(_, _, body) => module_stmt_contains_return(body)
    @engine.Stmt::ForOf(_, _, body) => module_stmt_contains_return(body)
    @engine.Stmt::ForAwaitOf(_, _, body) => module_stmt_contains_return(body)
    @engine.Stmt::DoWhile(body, _) => module_stmt_contains_return(body)
    @engine.Stmt::Switch(_, cases) => module_cases_contain_return(cases)
    @engine.Stmt::Try(try_body, catch_clause, finally_body) => {
      if module_stmt_contains_return(try_body) {
        return true
      }
      match catch_clause {
        Some(clause) =>
          if module_stmt_contains_return(clause.body) {
            return true
          }
        None => ()
      }
      match finally_body {
        Some(inner) => module_stmt_contains_return(inner)
        None => false
      }
    }
    @engine.Stmt::FunctionDecl(_) | @engine.Stmt::ClassDecl(_) => false
    _ => false
  }
}

///|
fn module_stmts_contain_return(stmts : Array[@engine.Stmt]) -> Bool {
  for stmt in stmts {
    if module_stmt_contains_return(stmt) {
      return true
    }
  }
  false
}

///|
fn module_cases_contain_return(cases : Array[@engine.SwitchCase]) -> Bool {
  for clause in cases {
    let body = match clause {
      @engine.SwitchCase::Case(_, stmts) => stmts
      @engine.SwitchCase::Default(stmts) => stmts
    }
    if module_stmts_contain_return(body) {
      return true
    }
  }
  false
}

///|
fn validate_module_export_bindings(script : @engine.Script) -> Unit raise {
  let info = module_export_info_from_script(script)
  let declared = module_declared_names_from_script(script)
  for _, binding in info.local_exports {
    if binding.is_empty() {
      continue
    }
    if !declared.contains(binding) {
      let _ = throw_syntax_error(
        "exported variable '\{binding}' does not exist",
      )

    }
  }
}

///|
fn module_declared_names_from_script(
  script : @engine.Script,
) -> @hashmap.HashMap[String, Bool] {
  let names = @hashmap.new()
  for stmt in script.body {
    module_declared_names_in_stmt(stmt, names)
  }
  names
}

///|
fn module_declared_names_in_stmt(
  stmt : @engine.Stmt,
  names : @hashmap.HashMap[String, Bool],
) -> Unit {
  match stmt {
    @engine.Stmt::VarDecl(_, decls) =>
      for decl in decls {
        let (binding, _) = decl
        collect_binding_names(binding, names)
      }
    @engine.Stmt::FunctionDecl(func) =>
      match func.name {
        Some(name) => names.set(name, true)
        None => ()
      }
    @engine.Stmt::ClassDecl(class_def) =>
      match class_def.name {
        Some(name) => names.set(name, true)
        None => ()
      }
    @engine.Stmt::Block(body) =>
      if block_is_export_wrapper(body) {
        for inner in body {
          module_declared_names_in_stmt(inner, names)
        }
      }
    _ => ()
  }
}

///|
fn stmt_is_export_decl(stmt : @engine.Stmt) -> Bool {
  match stmt {
    @engine.Stmt::ExprStmt(expr) =>
      expr_is_export_call(expr) || expr_is_export_star(expr)
    @engine.Stmt::Block(body) => block_is_export_wrapper(body)
    _ => false
  }
}

///|
fn validate_module_decl_positions_in_stmt(
  stmt : @engine.Stmt,
  top_level : Bool,
) -> Unit raise {
  if top_level {
    if stmt_is_import_decl(stmt) || stmt_is_export_decl(stmt) {
      return
    }
  } else if stmt_is_import_decl(stmt) || stmt_is_export_decl(stmt) {
    let _ = throw_syntax_error("invalid declaration position")

  }
  match stmt {
    @engine.Stmt::Block(stmts) =>
      for inner in stmts {
        validate_module_decl_positions_in_stmt(inner, false)
      }
    @engine.Stmt::Label(_, body) =>
      validate_module_decl_positions_in_stmt(body, false)
    @engine.Stmt::If(_, conseq, alt) => {
      validate_module_decl_positions_in_stmt(conseq, false)
      match alt {
        Some(inner) => validate_module_decl_positions_in_stmt(inner, false)
        None => ()
      }
    }
    @engine.Stmt::While(_, body) =>
      validate_module_decl_positions_in_stmt(body, false)
    @engine.Stmt::For(_, _, _, body) =>
      validate_module_decl_positions_in_stmt(body, false)
    @engine.Stmt::ForIn(_, _, body) =>
      validate_module_decl_positions_in_stmt(body, false)
    @engine.Stmt::ForOf(_, _, body) =>
      validate_module_decl_positions_in_stmt(body, false)
    @engine.Stmt::ForAwaitOf(_, _, body) =>
      validate_module_decl_positions_in_stmt(body, false)
    @engine.Stmt::DoWhile(body, _) =>
      validate_module_decl_positions_in_stmt(body, false)
    @engine.Stmt::Switch(_, cases) =>
      for clause in cases {
        let body = match clause {
          @engine.SwitchCase::Case(_, stmts) => stmts
          @engine.SwitchCase::Default(stmts) => stmts
        }
        for inner in body {
          validate_module_decl_positions_in_stmt(inner, false)
        }
      }
    @engine.Stmt::Try(try_body, catch_clause, finally_body) => {
      validate_module_decl_positions_in_stmt(try_body, false)
      match catch_clause {
        Some(clause) =>
          validate_module_decl_positions_in_stmt(clause.body, false)
        None => ()
      }
      match finally_body {
        Some(inner) => validate_module_decl_positions_in_stmt(inner, false)
        None => ()
      }
    }
    _ => ()
  }
}
