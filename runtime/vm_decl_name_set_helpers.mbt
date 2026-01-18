///|
fn clone_name_set(
  source : @hashmap.HashMap[String, Bool],
) -> @hashmap.HashMap[String, Bool] {
  let next : @hashmap.HashMap[String, Bool] = @hashmap.new()
  for name, _ in source {
    next.set(name, true)
  }
  next
}

///|
fn merge_name_sets(
  base : @hashmap.HashMap[String, Bool],
  extra : @hashmap.HashMap[String, Bool],
) -> @hashmap.HashMap[String, Bool] {
  let merged = clone_name_set(base)
  for name, _ in extra {
    merged.set(name, true)
  }
  merged
}

///|
fn lex_name_contains(
  outer : @hashmap.HashMap[String, Bool],
  local_set : @hashmap.HashMap[String, Bool],
  name : String,
) -> Bool {
  if local_set.contains(name) {
    true
  } else {
    outer.contains(name)
  }
}

///|
fn collect_global_lex_names(
  body : Array[@engine.Stmt],
) -> @hashmap.HashMap[String, Bool] {
  let names : @hashmap.HashMap[String, Bool] = @hashmap.new()
  for stmt in body {
    collect_global_lex_names_in_stmt(stmt, names)
  }
  names
}

///|
fn collect_global_lex_names_in_stmt(
  stmt : @engine.Stmt,
  out : @hashmap.HashMap[String, Bool],
) -> Unit {
  match stmt {
    @engine.Stmt::VarDecl(kind, decls) =>
      if !(kind is @engine.VarKind::Var) {
        for decl in decls {
          let (binding, _) = decl
          collect_binding_names(binding, out)
        }
      }
    @engine.Stmt::ClassDecl(class_def) =>
      match class_def.name {
        Some(name) => out.set(name, true)
        None => ()
      }
    _ => ()
  }
}

///|
fn collect_annex_b_var_func_names_in_stmt(
  stmt : @engine.Stmt,
  out : @hashmap.HashMap[Int, Bool],
  outer_lex : @hashmap.HashMap[String, Bool],
  local_lex : @hashmap.HashMap[String, Bool],
  blocked : @hashmap.HashMap[String, Bool],
  in_block : Bool,
  is_strict : Bool,
  has_arguments_binding : Bool,
) -> Unit {
  match stmt {
    @engine.Stmt::FunctionDecl(func) =>
      match func.name {
        Some(name) => {
          if !in_block {
            out.set(func.offset, true)
          } else if !is_strict && !func.is_async && !func.is_generator {
            if !blocked.contains(name) &&
              !lex_name_contains(outer_lex, local_lex, name) &&
              !(has_arguments_binding && name == "arguments") {
              out.set(func.offset, true)
            }
          }
          if in_block {
            local_lex.set(name, true)
          }
        }
        None => ()
      }
    @engine.Stmt::VarDecl(kind, decls) =>
      if !(kind is @engine.VarKind::Var) {
        for decl in decls {
          let (binding, _) = decl
          collect_binding_names(binding, local_lex)
        }
      }
    @engine.Stmt::ClassDecl(class_def) =>
      match class_def.name {
        Some(name) => local_lex.set(name, true)
        None => ()
      }
    @engine.Stmt::Block(stmts) => {
      let block_outer = merge_name_sets(outer_lex, local_lex)
      let block_local : @hashmap.HashMap[String, Bool] = @hashmap.new()
      for inner in stmts {
        collect_annex_b_var_func_names_in_stmt(
          inner, out, block_outer, block_local, blocked, true, is_strict, has_arguments_binding,
        )
      }
    }
    @engine.Stmt::Label(_, inner) =>
      collect_annex_b_var_func_names_in_stmt(
        inner, out, outer_lex, local_lex, blocked, in_block, is_strict, has_arguments_binding,
      )
    @engine.Stmt::With(_, body) => {
      let with_outer = merge_name_sets(outer_lex, local_lex)
      let with_local : @hashmap.HashMap[String, Bool] = @hashmap.new()
      collect_annex_b_var_func_names_in_stmt(
        body, out, with_outer, with_local, blocked, true, is_strict, has_arguments_binding,
      )
    }
    @engine.Stmt::If(_, conseq, alt) => {
      let if_outer = merge_name_sets(outer_lex, local_lex)
      let if_local : @hashmap.HashMap[String, Bool] = @hashmap.new()
      collect_annex_b_var_func_names_in_stmt(
        conseq, out, if_outer, if_local, blocked, true, is_strict, has_arguments_binding,
      )
      match alt {
        Some(inner) =>
          collect_annex_b_var_func_names_in_stmt(
            inner, out, if_outer, if_local, blocked, true, is_strict, has_arguments_binding,
          )
        None => ()
      }
    }
    @engine.Stmt::While(_, body) =>
      collect_annex_b_var_func_names_in_stmt(
        body, out, outer_lex, local_lex, blocked, true, is_strict, has_arguments_binding,
      )
    @engine.Stmt::For(init, _, _, body) => {
      let for_outer = merge_name_sets(outer_lex, local_lex)
      let for_local : @hashmap.HashMap[String, Bool] = @hashmap.new()
      match init {
        @engine.ForInit::Var(kind, decls) =>
          if !(kind is @engine.VarKind::Var) {
            for decl in decls {
              let (binding, _) = decl
              collect_binding_names(binding, for_local)
            }
          }
        _ => ()
      }
      collect_annex_b_var_func_names_in_stmt(
        body, out, for_outer, for_local, blocked, true, is_strict, has_arguments_binding,
      )
    }
    @engine.Stmt::ForIn(init, _, body)
    | @engine.Stmt::ForOf(init, _, body)
    | @engine.Stmt::ForAwaitOf(init, _, body) => {
      let for_outer = merge_name_sets(outer_lex, local_lex)
      let for_local : @hashmap.HashMap[String, Bool] = @hashmap.new()
      match init {
        @engine.ForInit::Var(kind, decls) =>
          if !(kind is @engine.VarKind::Var) {
            for decl in decls {
              let (binding, _) = decl
              collect_binding_names(binding, for_local)
            }
          }
        _ => ()
      }
      collect_annex_b_var_func_names_in_stmt(
        body, out, for_outer, for_local, blocked, true, is_strict, has_arguments_binding,
      )
    }
    @engine.Stmt::DoWhile(body, _) =>
      collect_annex_b_var_func_names_in_stmt(
        body, out, outer_lex, local_lex, blocked, true, is_strict, has_arguments_binding,
      )
    @engine.Stmt::Switch(_, cases) => {
      let switch_outer = merge_name_sets(outer_lex, local_lex)
      let switch_local : @hashmap.HashMap[String, Bool] = @hashmap.new()
      for clause in cases {
        let body = match clause {
          @engine.SwitchCase::Case(_, stmts) => stmts
          @engine.SwitchCase::Default(stmts) => stmts
        }
        for inner in body {
          collect_annex_b_var_func_names_in_stmt(
            inner, out, switch_outer, switch_local, blocked, true, is_strict, has_arguments_binding,
          )
        }
      }
    }
    @engine.Stmt::Try(try_body, catch_clause, finally_body) => {
      collect_annex_b_var_func_names_in_stmt(
        try_body, out, outer_lex, local_lex, blocked, true, is_strict, has_arguments_binding,
      )
      match catch_clause {
        Some(clause) =>
          collect_annex_b_var_func_names_in_stmt(
            clause.body,
            out,
            outer_lex,
            local_lex,
            blocked,
            true,
            is_strict,
            has_arguments_binding,
          )
        None => ()
      }
      match finally_body {
        Some(inner) =>
          collect_annex_b_var_func_names_in_stmt(
            inner, out, outer_lex, local_lex, blocked, true, is_strict, has_arguments_binding,
          )
        None => ()
      }
    }
    _ => ()
  }
}

///|
fn compute_annex_b_var_func_decls(
  body : Array[@engine.Stmt],
  is_strict : Bool,
  param_names : @hashmap.HashMap[String, Bool],
  has_arguments_binding : Bool,
) -> @hashmap.HashMap[Int, Bool] {
  if is_strict {
    return @hashmap.new()
  }
  let out : @hashmap.HashMap[Int, Bool] = @hashmap.new()
  let blocked : @hashmap.HashMap[String, Bool] = clone_name_set(param_names)
  let outer_lex : @hashmap.HashMap[String, Bool] = @hashmap.new()
  let local_lex : @hashmap.HashMap[String, Bool] = @hashmap.new()
  for stmt in body {
    collect_annex_b_var_func_names_in_stmt(
      stmt, out, outer_lex, local_lex, blocked, false, is_strict, has_arguments_binding,
    )
  }
  out
}
