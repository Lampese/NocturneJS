///|
fn check_eval_var_conflict_name(env : Env, name : String) -> Unit raise {
  match env.param_binding_names.get(name) {
    Some(_) => {
      let _ = throw_syntax_error("invalid identifier")

    }
    None => ()
  }
}

///|
fn check_eval_var_conflicts_in_binding(
  env : Env,
  binding : @engine.VarBinding,
) -> Unit raise {
  match binding {
    @engine.VarBinding::Name(name) => check_eval_var_conflict_name(env, name)
    @engine.VarBinding::ArrayPattern(pattern) =>
      for elem in pattern {
        match elem {
          @engine.ArrayPatternElem::Bind(inner, _) =>
            check_eval_var_conflicts_in_binding(env, inner)
          @engine.ArrayPatternElem::Rest(inner) =>
            check_eval_var_conflicts_in_binding(env, inner)
          @engine.ArrayPatternElem::Hole => ()
        }
      }
    @engine.VarBinding::ObjectPattern(pattern) =>
      for elem in pattern {
        match elem {
          @engine.ObjectPatternElem::Bind(_, inner, _) =>
            check_eval_var_conflicts_in_binding(env, inner)
          @engine.ObjectPatternElem::Rest(inner) =>
            check_eval_var_conflicts_in_binding(env, inner)
        }
      }
    @engine.VarBinding::Target(_) => ()
  }
}

///|
fn check_eval_var_conflicts_in_stmt(
  env : Env,
  stmt : @engine.Stmt,
) -> Unit raise {
  match stmt {
    VarDecl(kind, decls) =>
      if kind is @engine.VarKind::Var {
        for decl in decls {
          let (binding, _) = decl
          check_eval_var_conflicts_in_binding(env, binding)
        }
      }
    FunctionDecl(func) =>
      match func.name {
        Some(name) => check_eval_var_conflict_name(env, name)
        None => ()
      }
    Block(stmts) =>
      for inner in stmts {
        check_eval_var_conflicts_in_stmt(env, inner)
      }
    Label(_, body) => check_eval_var_conflicts_in_stmt(env, body)
    With(_, body) => check_eval_var_conflicts_in_stmt(env, body)
    If(_, conseq, alt) => {
      check_eval_var_conflicts_in_stmt(env, conseq)
      match alt {
        Some(inner) => check_eval_var_conflicts_in_stmt(env, inner)
        None => ()
      }
    }
    While(_, body) => check_eval_var_conflicts_in_stmt(env, body)
    For(init, _, _, body) => {
      match init {
        @engine.ForInit::Var(kind, decls) =>
          if kind is @engine.VarKind::Var {
            for decl in decls {
              let (binding, _) = decl
              check_eval_var_conflicts_in_binding(env, binding)
            }
          }
        _ => ()
      }
      check_eval_var_conflicts_in_stmt(env, body)
    }
    ForIn(init, _, body) => {
      match init {
        @engine.ForInit::Var(kind, decls) =>
          if kind is @engine.VarKind::Var {
            for decl in decls {
              let (binding, _) = decl
              check_eval_var_conflicts_in_binding(env, binding)
            }
          }
        _ => ()
      }
      check_eval_var_conflicts_in_stmt(env, body)
    }
    ForOf(init, _, body) => {
      match init {
        @engine.ForInit::Var(kind, decls) =>
          if kind is @engine.VarKind::Var {
            for decl in decls {
              let (binding, _) = decl
              check_eval_var_conflicts_in_binding(env, binding)
            }
          }
        _ => ()
      }
      check_eval_var_conflicts_in_stmt(env, body)
    }
    ForAwaitOf(init, _, body) => {
      match init {
        @engine.ForInit::Var(kind, decls) =>
          if kind is @engine.VarKind::Var {
            for decl in decls {
              let (binding, _) = decl
              check_eval_var_conflicts_in_binding(env, binding)
            }
          }
        _ => ()
      }
      check_eval_var_conflicts_in_stmt(env, body)
    }
    DoWhile(body, _) => check_eval_var_conflicts_in_stmt(env, body)
    Switch(_, cases) =>
      for clause in cases {
        let body = match clause {
          @engine.SwitchCase::Case(_, stmts) => stmts
          @engine.SwitchCase::Default(stmts) => stmts
        }
        for inner in body {
          check_eval_var_conflicts_in_stmt(env, inner)
        }
      }
    Try(try_body, catch_clause, finally_body) => {
      check_eval_var_conflicts_in_stmt(env, try_body)
      match catch_clause {
        Some(clause) => check_eval_var_conflicts_in_stmt(env, clause.body)
        None => ()
      }
      match finally_body {
        Some(inner) => check_eval_var_conflicts_in_stmt(env, inner)
        None => ()
      }
    }
    _ => ()
  }
}

///|
fn check_eval_var_conflicts(
  env : Env,
  body : Array[@engine.Stmt],
) -> Unit raise {
  if !env.is_param_env || env.param_binding_names.is_empty() {
    return
  }
  for stmt in body {
    check_eval_var_conflicts_in_stmt(env, stmt)
  }
}

///|
fn eval_body_has_lexical_decls(body : Array[@engine.Stmt]) -> Bool {
  for stmt in body {
    match stmt {
      VarDecl(kind, _) => if !(kind is @engine.VarKind::Var) { return true }
      ClassDecl(_) => return true
      Block(stmts) =>
        if current_module_env_id() is Some(_) && block_is_export_wrapper(stmts) {
          for inner in stmts {
            match inner {
              VarDecl(kind, _) =>
                if !(kind is @engine.VarKind::Var) {
                  return true
                }
              ClassDecl(_) => return true
              _ => ()
            }
          }
        }
      _ => ()
    }
  }
  false
}

///|
fn collect_top_level_function_decls(
  body : Array[@engine.Stmt],
) -> @hashmap.HashMap[Int, Bool] {
  let out : @hashmap.HashMap[Int, Bool] = @hashmap.new()
  for stmt in body {
    match stmt {
      FunctionDecl(func) => out.set(func.offset, true)
      Block(stmts) =>
        if current_module_env_id() is Some(_) && block_is_export_wrapper(stmts) {
          for inner in stmts {
            match inner {
              FunctionDecl(func) => out.set(func.offset, true)
              _ => ()
            }
          }
        }
      _ => ()
    }
  }
  out
}

///|
fn eval_predeclare_lexical_decls(
  env : Env,
  body : Array[@engine.Stmt],
) -> Unit raise {
  for stmt in body {
    if current_module_env_id() is Some(_) && stmt_is_import_decl(stmt) {
      continue
    }
    match stmt {
      Block(stmts) =>
        if current_module_env_id() is Some(_) && block_is_export_wrapper(stmts) {
          eval_predeclare_lexical_decls(env, stmts)
        }
      VarDecl(kind, decls) =>
        if !(kind is @engine.VarKind::Var) {
          for decl in decls {
            let (binding, _) = decl
            if env.parent is None {
              let names : Array[String] = []
              eval_collect_var_names_in_binding(binding, names)
              for name in names {
                if global_has_restricted_property(env, name) {
                  let _ = throw_syntax_error("invalid identifier")

                }
              }
            }
            declare_binding_uninitialized(env, binding)
          }
        }
      ClassDecl(class_def) =>
        match class_def.name {
          Some(name) =>
            if env.parent is None && global_has_restricted_property(env, name) {
              let _ = throw_syntax_error("invalid identifier")

            } else {
              env_declare_uninitialized(env, name)
            }
          None => ()
        }
      _ => ()
    }
  }
}

///|
fn eval_collect_var_names_in_binding(
  binding : @engine.VarBinding,
  names : Array[String],
) -> Unit {
  match binding {
    @engine.VarBinding::Name(name) => names.push(name)
    @engine.VarBinding::ArrayPattern(pattern) =>
      for elem in pattern {
        match elem {
          @engine.ArrayPatternElem::Bind(inner, _) =>
            eval_collect_var_names_in_binding(inner, names)
          @engine.ArrayPatternElem::Rest(inner) =>
            eval_collect_var_names_in_binding(inner, names)
          @engine.ArrayPatternElem::Hole => ()
        }
      }
    @engine.VarBinding::ObjectPattern(pattern) =>
      for elem in pattern {
        match elem {
          @engine.ObjectPatternElem::Bind(_, inner, _) =>
            eval_collect_var_names_in_binding(inner, names)
          @engine.ObjectPatternElem::Rest(inner) =>
            eval_collect_var_names_in_binding(inner, names)
        }
      }
    @engine.VarBinding::Target(_) => ()
  }
}

///|
fn eval_collect_var_names_in_stmt(
  stmt : @engine.Stmt,
  names : Array[String],
) -> Unit {
  match stmt {
    VarDecl(kind, decls) =>
      if kind is @engine.VarKind::Var {
        for decl in decls {
          let (binding, _) = decl
          eval_collect_var_names_in_binding(binding, names)
        }
      }
    Block(stmts) =>
      for inner in stmts {
        eval_collect_var_names_in_stmt(inner, names)
      }
    Label(_, body) => eval_collect_var_names_in_stmt(body, names)
    With(_, body) => eval_collect_var_names_in_stmt(body, names)
    If(_, conseq, alt) => {
      eval_collect_var_names_in_stmt(conseq, names)
      match alt {
        Some(inner) => eval_collect_var_names_in_stmt(inner, names)
        None => ()
      }
    }
    While(_, body) => eval_collect_var_names_in_stmt(body, names)
    For(init, _, _, body) => {
      match init {
        @engine.ForInit::Var(kind, decls) =>
          if kind is @engine.VarKind::Var {
            for decl in decls {
              let (binding, _) = decl
              eval_collect_var_names_in_binding(binding, names)
            }
          }
        _ => ()
      }
      eval_collect_var_names_in_stmt(body, names)
    }
    ForIn(init, _, body) => {
      match init {
        @engine.ForInit::Var(kind, decls) =>
          if kind is @engine.VarKind::Var {
            for decl in decls {
              let (binding, _) = decl
              eval_collect_var_names_in_binding(binding, names)
            }
          }
        _ => ()
      }
      eval_collect_var_names_in_stmt(body, names)
    }
    ForOf(init, _, body) => {
      match init {
        @engine.ForInit::Var(kind, decls) =>
          if kind is @engine.VarKind::Var {
            for decl in decls {
              let (binding, _) = decl
              eval_collect_var_names_in_binding(binding, names)
            }
          }
        _ => ()
      }
      eval_collect_var_names_in_stmt(body, names)
    }
    ForAwaitOf(init, _, body) => {
      match init {
        @engine.ForInit::Var(kind, decls) =>
          if kind is @engine.VarKind::Var {
            for decl in decls {
              let (binding, _) = decl
              eval_collect_var_names_in_binding(binding, names)
            }
          }
        _ => ()
      }
      eval_collect_var_names_in_stmt(body, names)
    }
    DoWhile(body, _) => eval_collect_var_names_in_stmt(body, names)
    Switch(_, cases) =>
      for clause in cases {
        let body = match clause {
          @engine.SwitchCase::Case(_, stmts) => stmts
          @engine.SwitchCase::Default(stmts) => stmts
        }
        for inner in body {
          eval_collect_var_names_in_stmt(inner, names)
        }
      }
    Try(try_body, catch_clause, finally_body) => {
      eval_collect_var_names_in_stmt(try_body, names)
      match catch_clause {
        Some(clause) => eval_collect_var_names_in_stmt(clause.body, names)
        None => ()
      }
      match finally_body {
        Some(inner) => eval_collect_var_names_in_stmt(inner, names)
        None => ()
      }
    }
    _ => ()
  }
}

///|
fn eval_collect_var_names(body : Array[@engine.Stmt]) -> Array[String] {
  let names : Array[String] = []
  for stmt in body {
    eval_collect_var_names_in_stmt(stmt, names)
  }
  names
}

///|
fn eval_collect_function_decls_in_stmt(
  stmt : @engine.Stmt,
  in_block : Bool,
  is_strict : Bool,
  decls : Array[@engine.FunctionDef],
) -> Unit {
  match stmt {
    FunctionDecl(func) =>
      if !(is_strict && in_block) {
        match func.name {
          Some(_) =>
            if !in_block || annex_b_var_func_decl_contains(func) {
              decls.push(func)
            }
          None => ()
        }
      }
    Block(stmts) =>
      for inner in stmts {
        eval_collect_function_decls_in_stmt(inner, true, is_strict, decls)
      }
    Label(_, body) =>
      eval_collect_function_decls_in_stmt(body, in_block, is_strict, decls)
    With(_, body) =>
      eval_collect_function_decls_in_stmt(body, true, is_strict, decls)
    If(_, conseq, alt) => {
      eval_collect_function_decls_in_stmt(conseq, true, is_strict, decls)
      match alt {
        Some(inner) =>
          eval_collect_function_decls_in_stmt(inner, true, is_strict, decls)
        None => ()
      }
    }
    While(_, body) =>
      eval_collect_function_decls_in_stmt(body, true, is_strict, decls)
    For(_, _, _, body) =>
      eval_collect_function_decls_in_stmt(body, true, is_strict, decls)
    ForIn(_, _, body) =>
      eval_collect_function_decls_in_stmt(body, true, is_strict, decls)
    ForOf(_, _, body) =>
      eval_collect_function_decls_in_stmt(body, true, is_strict, decls)
    ForAwaitOf(_, _, body) =>
      eval_collect_function_decls_in_stmt(body, true, is_strict, decls)
    DoWhile(body, _) =>
      eval_collect_function_decls_in_stmt(body, true, is_strict, decls)
    Switch(_, cases) =>
      for clause in cases {
        let body = match clause {
          @engine.SwitchCase::Case(_, stmts) => stmts
          @engine.SwitchCase::Default(stmts) => stmts
        }
        for inner in body {
          eval_collect_function_decls_in_stmt(inner, true, is_strict, decls)
        }
      }
    Try(try_body, catch_clause, finally_body) => {
      eval_collect_function_decls_in_stmt(try_body, true, is_strict, decls)
      match catch_clause {
        Some(clause) =>
          eval_collect_function_decls_in_stmt(
            clause.body,
            true,
            is_strict,
            decls,
          )
        None => ()
      }
      match finally_body {
        Some(inner) =>
          eval_collect_function_decls_in_stmt(inner, true, is_strict, decls)
        None => ()
      }
    }
    _ => ()
  }
}

///|
fn eval_collect_function_decls(
  body : Array[@engine.Stmt],
  is_strict : Bool,
) -> Array[@engine.FunctionDef] {
  let decls : Array[@engine.FunctionDef] = []
  for stmt in body {
    eval_collect_function_decls_in_stmt(stmt, false, is_strict, decls)
  }
  decls
}

///|
fn eval_check_global_var_lex_conflicts(
  env : Env,
  body : Array[@engine.Stmt],
  is_strict : Bool,
) -> Unit raise {
  if !(env.parent is None) {
    return
  }
  let names = eval_collect_var_names(body)
  for name in names {
    if global_has_lexical_declaration(env, name) {
      let _ = throw_syntax_error("invalid identifier")

    }
  }
  let funcs = eval_collect_function_decls(body, is_strict)
  for func in funcs {
    match func.name {
      Some(name) =>
        if global_has_lexical_declaration(env, name) {
          let _ = throw_syntax_error("invalid identifier")

        }
      None => ()
    }
  }
}

///|
fn eval_check_var_lex_conflicts(
  env : Env,
  var_env : Env,
  body : Array[@engine.Stmt],
  is_strict : Bool,
) -> Unit raise {
  if env.id == var_env.id {
    return
  }
  let names = eval_collect_var_names(body)
  let funcs = eval_collect_function_decls(body, is_strict)
  for func in funcs {
    match func.name {
      Some(name) => names.push(name)
      None => ()
    }
  }
  let mut current : Env? = Some(env)
  while true {
    match current {
      Some(scope) =>
        if scope.id == var_env.id {
          break
        } else {
          if scope.with_object is None && !scope.is_catch_env {
            for name in names {
              if scope.bindings.contains(name) {
                let _ = throw_syntax_error("invalid identifier")

              }
            }
          }
          current = scope.parent
        }
      None => break
    }
  }
}

///|
fn eval_check_global_function_decls(
  env : Env,
  body : Array[@engine.Stmt],
  is_strict : Bool,
) -> Unit raise {
  if !(env.parent is None) {
    return
  }
  let decls = eval_collect_function_decls(body, is_strict)
  let seen = Map::new()
  let mut i = decls.length()
  while i > 0 {
    i = i - 1
    match decls[i].name {
      Some(name) =>
        if !seen.contains(name) {
          seen.set(name, true)
          if !global_can_declare_function(env, name) {
            let _ = throw_type_error("cannot declare global function")

          }
        }
      None => ()
    }
  }
}

///|
fn check_global_script_decls(
  env : Env,
  body : Array[@engine.Stmt],
  is_strict : Bool,
) -> Unit raise {
  if !(env.parent is None) {
    return
  }
  let lex_names = collect_global_lex_names(body)
  for name, _ in lex_names {
    if global_has_restricted_property(env, name) ||
      global_has_lexical_declaration(env, name) {
      let _ = throw_syntax_error("invalid identifier")

    }
  }
  let var_names = eval_collect_var_names(body)
  let funcs = eval_collect_function_decls(body, is_strict)
  let var_like_names : Array[String] = []
  for name in var_names {
    var_like_names.push(name)
  }
  for func in funcs {
    match func.name {
      Some(name) => var_like_names.push(name)
      None => ()
    }
  }
  for name in var_like_names {
    if lex_names.contains(name) || global_has_lexical_declaration(env, name) {
      let _ = throw_syntax_error("invalid identifier")

    }
  }
  for name in var_names {
    if !global_can_declare_var(env, name) {
      let _ = throw_type_error("cannot declare global var")

    }
  }
  eval_check_global_function_decls(env, body, is_strict)
}
