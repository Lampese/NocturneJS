///|
fn label_targets_iteration(stmt : @engine.Stmt) -> Bool {
  match stmt {
    @engine.Stmt::While(_, _)
    | @engine.Stmt::For(_, _, _, _)
    | @engine.Stmt::ForIn(_, _, _)
    | @engine.Stmt::ForOf(_, _, _)
    | @engine.Stmt::ForAwaitOf(_, _, _)
    | @engine.Stmt::DoWhile(_, _) => true
    @engine.Stmt::Label(_, inner) => label_targets_iteration(inner)
    _ => false
  }
}

///|
fn find_label(labels : Array[(String, Bool)], name : String) -> Bool? {
  if labels.is_empty() {
    return None
  }
  let mut i = labels.length() - 1
  while i >= 0 {
    let (label_name, is_iteration) = labels[i]
    if label_name == name {
      return Some(is_iteration)
    }
    i = i - 1
  }
  None
}

///|
fn validate_break_continue_in_script(script : @engine.Script) -> Unit raise {
  let labels : Array[(String, Bool)] = []
  for stmt in script.body {
    validate_break_continue_in_stmt(stmt, 0, 0, labels)
  }
}

///|
fn validate_break_continue_in_stmt(
  stmt : @engine.Stmt,
  loop_depth : Int,
  switch_depth : Int,
  labels : Array[(String, Bool)],
) -> Unit raise {
  match stmt {
    @engine.Stmt::Break(label_opt) =>
      match label_opt {
        None =>
          if loop_depth == 0 && switch_depth == 0 {
            let _ = throw_syntax_error("break must be inside loop or switch")

          }
        Some(name) =>
          match find_label(labels, name) {
            Some(_) => ()
            None => {
              let _ = throw_syntax_error("break/continue label not found")

            }
          }
      }
    @engine.Stmt::Continue(label_opt) =>
      match label_opt {
        None =>
          if loop_depth == 0 {
            let _ = throw_syntax_error("continue must be inside loop")

          }
        Some(name) =>
          match find_label(labels, name) {
            Some(true) => ()
            _ => {
              let _ = throw_syntax_error("break/continue label not found")

            }
          }
      }
    @engine.Stmt::Block(stmts) =>
      for inner in stmts {
        validate_break_continue_in_stmt(inner, loop_depth, switch_depth, labels)
      }
    @engine.Stmt::Label(name, body) => {
      match find_label(labels, name) {
        Some(_) => {
          let _ = throw_syntax_error("duplicate label")

        }
        None => ()
      }
      let is_iteration = label_targets_iteration(body)
      labels.push((name, is_iteration))
      validate_break_continue_in_stmt(body, loop_depth, switch_depth, labels)
      let _ = labels.pop()

    }
    @engine.Stmt::With(_, body) =>
      validate_break_continue_in_stmt(body, loop_depth, switch_depth, labels)
    @engine.Stmt::If(_, conseq, alt) => {
      validate_break_continue_in_stmt(conseq, loop_depth, switch_depth, labels)
      match alt {
        Some(inner) =>
          validate_break_continue_in_stmt(
            inner, loop_depth, switch_depth, labels,
          )
        None => ()
      }
    }
    @engine.Stmt::While(_, body) =>
      validate_break_continue_in_stmt(
        body,
        loop_depth + 1,
        switch_depth,
        labels,
      )
    @engine.Stmt::For(_, _, _, body) =>
      validate_break_continue_in_stmt(
        body,
        loop_depth + 1,
        switch_depth,
        labels,
      )
    @engine.Stmt::ForIn(_, _, body) =>
      validate_break_continue_in_stmt(
        body,
        loop_depth + 1,
        switch_depth,
        labels,
      )
    @engine.Stmt::ForOf(_, _, body) =>
      validate_break_continue_in_stmt(
        body,
        loop_depth + 1,
        switch_depth,
        labels,
      )
    @engine.Stmt::ForAwaitOf(_, _, body) =>
      validate_break_continue_in_stmt(
        body,
        loop_depth + 1,
        switch_depth,
        labels,
      )
    @engine.Stmt::DoWhile(body, _) =>
      validate_break_continue_in_stmt(
        body,
        loop_depth + 1,
        switch_depth,
        labels,
      )
    @engine.Stmt::Switch(_, cases) =>
      for clause in cases {
        let body = match clause {
          @engine.SwitchCase::Case(_, stmts) => stmts
          @engine.SwitchCase::Default(stmts) => stmts
        }
        for inner in body {
          validate_break_continue_in_stmt(
            inner,
            loop_depth,
            switch_depth + 1,
            labels,
          )
        }
      }
    @engine.Stmt::Try(try_body, catch_clause, finally_body) => {
      validate_break_continue_in_stmt(
        try_body, loop_depth, switch_depth, labels,
      )
      match catch_clause {
        Some(clause) =>
          validate_break_continue_in_stmt(
            clause.body,
            loop_depth,
            switch_depth,
            labels,
          )
        None => ()
      }
      match finally_body {
        Some(inner) =>
          validate_break_continue_in_stmt(
            inner, loop_depth, switch_depth, labels,
          )
        None => ()
      }
    }
    @engine.Stmt::FunctionDecl(func) => {
      let inner_labels : Array[(String, Bool)] = []
      for inner in func.body {
        validate_break_continue_in_stmt(inner, 0, 0, inner_labels)
      }
    }
    @engine.Stmt::ClassDecl(class_def) =>
      for member_ in class_def.body {
        match member_ {
          @engine.ClassMember::Method(_, func, _)
          | @engine.ClassMember::Getter(_, func, _)
          | @engine.ClassMember::Setter(_, func, _) => {
            let inner_labels : Array[(String, Bool)] = []
            for inner in func.body {
              validate_break_continue_in_stmt(inner, 0, 0, inner_labels)
            }
          }
          @engine.ClassMember::StaticBlock(stmts) => {
            let inner_labels : Array[(String, Bool)] = []
            for inner in stmts {
              validate_break_continue_in_stmt(inner, 0, 0, inner_labels)
            }
          }
          _ => ()
        }
      }
    _ => ()
  }
}
