///|
fn expr_is_import_decl(expr : @engine.Expr) -> Bool {
  match expr {
    @engine.Expr::Call(callee, _, _) =>
      match callee {
        @engine.Expr::Ident(name, _) =>
          name == "__import" || name == "__import_binding"
        _ => false
      }
    _ => false
  }
}

///|
fn expr_is_export_star(expr : @engine.Expr) -> Bool {
  match expr {
    @engine.Expr::Call(callee, _, _) =>
      match callee {
        @engine.Expr::Ident(name, _) => name == "__export_star"
        _ => false
      }
    _ => false
  }
}

///|
fn export_call_is_import(args : Array[@engine.Expr]) -> Bool {
  if args.length() < 2 {
    return false
  }
  match args[1] {
    @engine.Expr::Call(callee, _, _) =>
      match callee {
        @engine.Expr::Ident(name, _) =>
          name == "__import" || name == "__import_binding"
        _ => false
      }
    _ => false
  }
}

///|
fn preload_module_import_expr(env : Env, expr : @engine.Expr) -> Unit raise {
  match expr {
    @engine.Expr::Sequence(exprs) =>
      for inner in exprs {
        preload_module_import_expr(env, inner)
      }
    @engine.Expr::Paren(inner) => preload_module_import_expr(env, inner)
    @engine.Expr::Call(callee, args, _) =>
      match callee {
        @engine.Expr::Ident(name, _) =>
          if name == "__import" || name == "__import_binding" {
            let _ = eval_expr(env, expr)

          } else if name == "__export_star" {
            let _ = eval_expr(env, expr)

          } else if name == "__export" && export_call_is_import(args) {
            let _ = eval_expr(env, expr)

          }
        _ => ()
      }
    _ => ()
  }
}

///|
fn stmt_is_import_decl(stmt : @engine.Stmt) -> Bool {
  match stmt {
    @engine.Stmt::ExprStmt(expr) => expr_is_import_decl(expr)
    @engine.Stmt::VarDecl(kind, decls) =>
      match kind {
        @engine.VarKind::ConstDecl => {
          if decls.is_empty() {
            return false
          }
          for entry in decls {
            let (_, init) = entry
            match init {
              Some(expr) => if !expr_is_import_decl(expr) { return false }
              None => return false
            }
          }
          true
        }
        _ => false
      }
    _ => false
  }
}

///|
fn preload_module_imports(env : Env, script : @engine.Script) -> Unit raise {
  for stmt in script.body {
    match stmt {
      @engine.Stmt::ExprStmt(expr) => preload_module_import_expr(env, expr)
      @engine.Stmt::VarDecl(_, decls) =>
        if stmt_is_import_decl(stmt) {
          for entry in decls {
            let (_, init) = entry
            match init {
              Some(expr) => {
                let value = eval_expr(env, expr)
                let (binding, _) = entry
                match binding {
                  @engine.VarBinding::Name(name) =>
                    env_define_readonly(env, name, value, true)
                  _ => ()
                }
              }
              None => ()
            }
          }
        }
      _ => ()
    }
  }
}

///|
fn predeclare_module_exports(
  env_id : Int,
  exports : ObjectValue,
  script : @engine.Script,
) -> Unit raise {
  for stmt in script.body {
    predeclare_module_exports_in_stmt(env_id, exports, stmt)
  }
}

///|
fn predeclare_module_exports_in_stmt(
  env_id : Int,
  exports : ObjectValue,
  stmt : @engine.Stmt,
) -> Unit raise {
  match stmt {
    @engine.Stmt::ExprStmt(expr) =>
      predeclare_module_exports_in_expr(env_id, exports, expr)
    @engine.Stmt::Block(stmts) =>
      for inner in stmts {
        predeclare_module_exports_in_stmt(env_id, exports, inner)
      }
    @engine.Stmt::Label(_, body) =>
      predeclare_module_exports_in_stmt(env_id, exports, body)
    @engine.Stmt::If(_, conseq, alt) => {
      predeclare_module_exports_in_stmt(env_id, exports, conseq)
      match alt {
        Some(inner) => predeclare_module_exports_in_stmt(env_id, exports, inner)
        None => ()
      }
    }
    @engine.Stmt::Switch(_, cases) =>
      for clause in cases {
        let body = match clause {
          @engine.SwitchCase::Case(_, stmts) => stmts
          @engine.SwitchCase::Default(stmts) => stmts
        }
        for inner in body {
          predeclare_module_exports_in_stmt(env_id, exports, inner)
        }
      }
    @engine.Stmt::Try(try_body, catch_clause, finally_body) => {
      predeclare_module_exports_in_stmt(env_id, exports, try_body)
      match catch_clause {
        Some(clause) =>
          predeclare_module_exports_in_stmt(env_id, exports, clause.body)
        None => ()
      }
      match finally_body {
        Some(inner) => predeclare_module_exports_in_stmt(env_id, exports, inner)
        None => ()
      }
    }
    _ => ()
  }
}

///|
fn predeclare_module_exports_in_expr(
  env_id : Int,
  exports : ObjectValue,
  expr : @engine.Expr,
) -> Unit raise {
  match expr {
    @engine.Expr::Sequence(exprs) =>
      for inner in exprs {
        predeclare_module_exports_in_expr(env_id, exports, inner)
      }
    @engine.Expr::Paren(inner) =>
      predeclare_module_exports_in_expr(env_id, exports, inner)
    @engine.Expr::Call(callee, args, _) =>
      match callee {
        @engine.Expr::Ident(name, _) =>
          if name == "__export" {
            predeclare_module_export_call(env_id, exports, args)
          }
        _ => ()
      }
    _ => ()
  }
}

///|
fn predeclare_module_export_call(
  env_id : Int,
  exports : ObjectValue,
  args : Array[@engine.Expr],
) -> Unit raise {
  if args.length() < 2 {
    return
  }
  let export_name = match args[0] {
    @engine.Expr::String(name, _, _) => name
    _ => return
  }
  if !props_contains(exports.props, export_name) {
    props_set(exports.props, export_name, Property::{
      value: Undefined,
      writable: false,
      configurable: false,
      enumerable: true,
      getter: None,
      setter: None,
    })
    match current_source_path() {
      Some(path) => module_namespace_add_export(path, exports, export_name)
      None => ()
    }
  }
  if args.length() > 2 {
    match args[2] {
      @engine.Expr::String(binding, _, _) =>
        module_env_export_names_add(env_id, binding, export_name)
      _ => ()
    }
  } else {
    module_env_export_uninitialized_add(env_id, export_name)
  }
}

///|
fn module_imported_bindings_from_script(
  script : @engine.Script,
) -> Map[String, (String, String)] {
  let imported : Map[String, (String, String)] = Map::new()
  for stmt in script.body {
    match stmt {
      @engine.Stmt::VarDecl(_, decls) =>
        if stmt_is_import_decl(stmt) {
          for entry in decls {
            let (binding, init) = entry
            match (binding, init) {
              (@engine.VarBinding::Name(local_name), Some(expr)) =>
                match module_export_info_import_call(expr) {
                  Some((specifier, import_name)) =>
                    imported.set(local_name, (specifier, import_name))
                  None => ()
                }
              _ => ()
            }
          }
        }
      _ => ()
    }
  }
  imported
}

///|
fn module_export_info_from_script(
  script : @engine.Script,
) -> ModuleExportInfo raise {
  let info = ModuleExportInfo::{
    local_exports: Map::new(),
    indirect_exports: Map::new(),
    star_exports: [],
  }
  let imported_bindings = module_imported_bindings_from_script(script)
  for stmt in script.body {
    module_export_info_in_stmt(info, imported_bindings, stmt)
  }
  info
}

///|
fn module_export_info_in_stmt(
  info : ModuleExportInfo,
  imported_bindings : Map[String, (String, String)],
  stmt : @engine.Stmt,
) -> Unit raise {
  match stmt {
    @engine.Stmt::ExprStmt(expr) =>
      module_export_info_in_expr(info, imported_bindings, expr)
    @engine.Stmt::Block(stmts) =>
      for inner in stmts {
        module_export_info_in_stmt(info, imported_bindings, inner)
      }
    @engine.Stmt::Label(_, body) =>
      module_export_info_in_stmt(info, imported_bindings, body)
    @engine.Stmt::If(_, conseq, alt) => {
      module_export_info_in_stmt(info, imported_bindings, conseq)
      match alt {
        Some(inner) =>
          module_export_info_in_stmt(info, imported_bindings, inner)
        None => ()
      }
    }
    @engine.Stmt::Switch(_, cases) =>
      for clause in cases {
        let body = match clause {
          @engine.SwitchCase::Case(_, stmts) => stmts
          @engine.SwitchCase::Default(stmts) => stmts
        }
        for inner in body {
          module_export_info_in_stmt(info, imported_bindings, inner)
        }
      }
    @engine.Stmt::Try(try_body, catch_clause, finally_body) => {
      module_export_info_in_stmt(info, imported_bindings, try_body)
      match catch_clause {
        Some(clause) =>
          module_export_info_in_stmt(info, imported_bindings, clause.body)
        None => ()
      }
      match finally_body {
        Some(inner) =>
          module_export_info_in_stmt(info, imported_bindings, inner)
        None => ()
      }
    }
    _ => ()
  }
}

///|
fn module_export_info_in_expr(
  info : ModuleExportInfo,
  imported_bindings : Map[String, (String, String)],
  expr : @engine.Expr,
) -> Unit raise {
  match expr {
    @engine.Expr::Sequence(exprs) =>
      for inner in exprs {
        module_export_info_in_expr(info, imported_bindings, inner)
      }
    @engine.Expr::Paren(inner) =>
      module_export_info_in_expr(info, imported_bindings, inner)
    @engine.Expr::Call(callee, args, _) =>
      match callee {
        @engine.Expr::Ident(name, _) =>
          if name == "__export" {
            module_export_info_record_export(info, imported_bindings, args)
          } else if name == "__export_star" {
            module_export_info_record_star(info, args)
          }
        _ => ()
      }
    _ => ()
  }
}

///|
fn module_export_info_record_export(
  info : ModuleExportInfo,
  imported_bindings : Map[String, (String, String)],
  args : Array[@engine.Expr],
) -> Unit raise {
  if args.length() < 2 {
    return
  }
  let export_name = match args[0] {
    @engine.Expr::String(name, _, _) => name
    _ => return
  }
  if info.local_exports.contains(export_name) ||
    info.indirect_exports.contains(export_name) {
    let _ = throw_syntax_error("duplicate exported name '\{export_name}'")

  }
  if args.length() > 2 {
    match args[2] {
      @engine.Expr::String(binding, _, _) => {
        let _ = imported_bindings.get(binding)
        info.local_exports.set(export_name, binding)
        return
      }
      _ => ()
    }
  }
  match module_export_info_import_call(args[1]) {
    Some((specifier, import_name)) => {
      info.indirect_exports.set(export_name, (specifier, import_name))
      return
    }
    None => ()
  }
  info.local_exports.set(export_name, "")
}

///|
fn module_export_info_record_star(
  info : ModuleExportInfo,
  args : Array[@engine.Expr],
) -> Unit {
  if args.is_empty() {
    return
  }
  let specifier = match args[0] {
    @engine.Expr::String(text, _, _) => text
    _ => return
  }
  if !info.star_exports.contains(specifier) {
    info.star_exports.push(specifier)
  }
}

///|
fn module_export_info_import_call(expr : @engine.Expr) -> (String, String)? {
  match expr {
    @engine.Expr::Call(callee, args, _) =>
      match callee {
        @engine.Expr::Ident(name, _) =>
          if name == "__import_binding" {
            if args.length() < 2 {
              return None
            }
            match (args[0], args[1]) {
              (
                @engine.Expr::String(specifier, _, _),
                @engine.Expr::String(import_name, _, _),
              ) => Some((specifier, import_name))
              _ => None
            }
          } else if name == "__import" {
            if args.is_empty() {
              return None
            }
            match args[0] {
              @engine.Expr::String(specifier, _, _) => Some((specifier, "*"))
              _ => None
            }
          } else {
            None
          }
        _ => None
      }
    _ => None
  }
}
