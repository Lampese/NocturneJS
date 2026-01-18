///|
fn validate_context_in_expr_opt(
  expr_opt : @engine.Expr?,
  allow_new_target : Bool,
  allow_super : Bool,
  allow_super_call : Bool,
) -> Unit raise {
  match expr_opt {
    Some(expr) =>
      validate_context_in_expr(
        expr, allow_new_target, allow_super, allow_super_call,
      )
    None => ()
  }
}

///|
fn validate_context_in_expr_list(
  exprs : Array[@engine.Expr],
  allow_new_target : Bool,
  allow_super : Bool,
  allow_super_call : Bool,
) -> Unit raise {
  for expr in exprs {
    validate_context_in_expr(
      expr, allow_new_target, allow_super, allow_super_call,
    )
  }
}

///|
fn validate_context_in_member_key(
  key : @engine.MemberKey,
  allow_new_target : Bool,
  allow_super : Bool,
  allow_super_call : Bool,
) -> Unit raise {
  match key {
    @engine.MemberKey::Computed(expr) =>
      validate_context_in_expr(
        expr, allow_new_target, allow_super, allow_super_call,
      )
    _ => ()
  }
}

///|
fn validate_context_in_obj_key(
  key : @engine.ObjKey,
  allow_new_target : Bool,
  allow_super : Bool,
  allow_super_call : Bool,
) -> Unit raise {
  match key {
    @engine.ObjKey::Computed(expr) =>
      validate_context_in_expr(
        expr, allow_new_target, allow_super, allow_super_call,
      )
    _ => ()
  }
}

///|
fn validate_context_in_var_binding(
  binding : @engine.VarBinding,
  allow_new_target : Bool,
  allow_super : Bool,
  allow_super_call : Bool,
) -> Unit raise {
  match binding {
    @engine.VarBinding::Name(_) => ()
    @engine.VarBinding::ArrayPattern(pattern) =>
      validate_context_in_array_pattern(
        pattern, allow_new_target, allow_super, allow_super_call,
      )
    @engine.VarBinding::ObjectPattern(pattern) =>
      validate_context_in_object_pattern(
        pattern, allow_new_target, allow_super, allow_super_call,
      )
    @engine.VarBinding::Target(expr) =>
      validate_context_in_expr(
        expr, allow_new_target, allow_super, allow_super_call,
      )
  }
}

///|
fn validate_context_in_array_pattern(
  pattern : Array[@engine.ArrayPatternElem],
  allow_new_target : Bool,
  allow_super : Bool,
  allow_super_call : Bool,
) -> Unit raise {
  for elem in pattern {
    match elem {
      @engine.ArrayPatternElem::Bind(inner, init) => {
        validate_context_in_var_binding(
          inner, allow_new_target, allow_super, allow_super_call,
        )
        validate_context_in_expr_opt(
          init, allow_new_target, allow_super, allow_super_call,
        )
      }
      @engine.ArrayPatternElem::Rest(inner) =>
        validate_context_in_var_binding(
          inner, allow_new_target, allow_super, allow_super_call,
        )
      @engine.ArrayPatternElem::Hole => ()
    }
  }
}

///|
fn validate_context_in_object_pattern(
  pattern : Array[@engine.ObjectPatternElem],
  allow_new_target : Bool,
  allow_super : Bool,
  allow_super_call : Bool,
) -> Unit raise {
  for elem in pattern {
    match elem {
      @engine.ObjectPatternElem::Bind(key, inner, init) => {
        validate_context_in_obj_key(
          key, allow_new_target, allow_super, allow_super_call,
        )
        validate_context_in_var_binding(
          inner, allow_new_target, allow_super, allow_super_call,
        )
        validate_context_in_expr_opt(
          init, allow_new_target, allow_super, allow_super_call,
        )
      }
      @engine.ObjectPatternElem::Rest(inner) =>
        validate_context_in_var_binding(
          inner, allow_new_target, allow_super, allow_super_call,
        )
    }
  }
}

///|
fn validate_context_in_function(
  func : @engine.FunctionDef,
  parent_allow_new_target : Bool,
  parent_allow_super : Bool,
  parent_allow_super_call : Bool,
  allow_super : Bool,
  allow_super_call : Bool,
) -> Unit raise {
  let (next_allow_new_target, next_allow_super, next_allow_super_call) = if func.is_arrow {
    (parent_allow_new_target, parent_allow_super, parent_allow_super_call)
  } else {
    (true, allow_super, allow_super_call)
  }
  for init in func.param_inits {
    validate_context_in_expr_opt(
      init, next_allow_new_target, next_allow_super, next_allow_super_call,
    )
  }
  for param in func.params {
    validate_context_in_var_binding(
      param.binding,
      next_allow_new_target,
      next_allow_super,
      next_allow_super_call,
    )
  }
  for stmt in func.body {
    validate_context_in_stmt(
      stmt, next_allow_new_target, next_allow_super, next_allow_super_call,
    )
  }
}

///|
fn obj_key_is_constructor(key : @engine.ObjKey) -> Bool {
  match key {
    @engine.ObjKey::Ident(name) => name == "constructor"
    @engine.ObjKey::String(name) => name == "constructor"
    _ => false
  }
}

///|
fn validate_context_in_class(
  class_def : @engine.ClassDef,
  allow_new_target : Bool,
  allow_super : Bool,
  allow_super_call : Bool,
) -> Unit raise {
  match class_def.super_class {
    Some(expr) =>
      validate_context_in_expr(
        expr, allow_new_target, allow_super, allow_super_call,
      )
    None => ()
  }
  let is_derived = class_def.super_class is Some(_)
  for member_ in class_def.body {
    match member_ {
      @engine.ClassMember::Method(key, func, is_static) => {
        validate_context_in_obj_key(
          key, allow_new_target, allow_super, allow_super_call,
        )
        let is_ctor = !is_static && obj_key_is_constructor(key)
        let ctor_allow_super_call = is_ctor && is_derived
        validate_context_in_function(
          func, allow_new_target, allow_super, allow_super_call, true, ctor_allow_super_call,
        )
      }
      @engine.ClassMember::Getter(key, func, _) => {
        validate_context_in_obj_key(
          key, allow_new_target, allow_super, allow_super_call,
        )
        validate_context_in_function(
          func, allow_new_target, allow_super, allow_super_call, true, false,
        )
      }
      @engine.ClassMember::Setter(key, func, _) => {
        validate_context_in_obj_key(
          key, allow_new_target, allow_super, allow_super_call,
        )
        validate_context_in_function(
          func, allow_new_target, allow_super, allow_super_call, true, false,
        )
      }
      @engine.ClassMember::Field(key, init, _) => {
        validate_context_in_obj_key(
          key, allow_new_target, allow_super, allow_super_call,
        )
        match init {
          Some(expr) => validate_context_in_expr(expr, true, true, false)
          None => ()
        }
      }
      @engine.ClassMember::StaticBlock(stmts) =>
        for stmt in stmts {
          validate_context_in_stmt(stmt, true, true, false)
        }
    }
  }
}

///|
fn validate_context_in_expr(
  expr : @engine.Expr,
  allow_new_target : Bool,
  allow_super : Bool,
  allow_super_call : Bool,
) -> Unit raise {
  match expr {
    @engine.Expr::TemplateLiteral(_, exprs) =>
      validate_context_in_expr_list(
        exprs, allow_new_target, allow_super, allow_super_call,
      )
    @engine.Expr::TaggedTemplate(callee, _, exprs) => {
      validate_context_in_expr(
        callee, allow_new_target, allow_super, allow_super_call,
      )
      validate_context_in_expr_list(
        exprs, allow_new_target, allow_super, allow_super_call,
      )
    }
    @engine.Expr::Await(inner, _) =>
      validate_context_in_expr(
        inner, allow_new_target, allow_super, allow_super_call,
      )
    @engine.Expr::Unary(_, inner, _) =>
      validate_context_in_expr(
        inner, allow_new_target, allow_super, allow_super_call,
      )
    @engine.Expr::Update(_, inner, _, _) =>
      validate_context_in_expr(
        inner, allow_new_target, allow_super, allow_super_call,
      )
    @engine.Expr::Yield(expr_opt, _) =>
      validate_context_in_expr_opt(
        expr_opt, allow_new_target, allow_super, allow_super_call,
      )
    @engine.Expr::NewTarget(_) =>
      if !allow_new_target {
        let _ = throw_syntax_error("new.target only allowed within functions")

      }
    @engine.Expr::Super => {
      let _ = throw_syntax_error("invalid use of 'super'")

    }
    @engine.Expr::New(callee, args) => {
      validate_context_in_expr(
        callee, allow_new_target, allow_super, allow_super_call,
      )
      validate_context_in_expr_list(
        args, allow_new_target, allow_super, allow_super_call,
      )
    }
    @engine.Expr::Binary(_, left, right, _) => {
      validate_context_in_expr(
        left, allow_new_target, allow_super, allow_super_call,
      )
      validate_context_in_expr(
        right, allow_new_target, allow_super, allow_super_call,
      )
    }
    @engine.Expr::PrivateIn(_, rhs, _) =>
      validate_context_in_expr(
        rhs, allow_new_target, allow_super, allow_super_call,
      )
    @engine.Expr::Conditional(cond_expr, conseq, alt) => {
      validate_context_in_expr(
        cond_expr, allow_new_target, allow_super, allow_super_call,
      )
      validate_context_in_expr(
        conseq, allow_new_target, allow_super, allow_super_call,
      )
      validate_context_in_expr(
        alt, allow_new_target, allow_super, allow_super_call,
      )
    }
    @engine.Expr::Sequence(exprs) =>
      validate_context_in_expr_list(
        exprs, allow_new_target, allow_super, allow_super_call,
      )
    @engine.Expr::Paren(inner) =>
      validate_context_in_expr(
        inner, allow_new_target, allow_super, allow_super_call,
      )
    @engine.Expr::Assign(_, rhs, _, _) =>
      validate_context_in_expr(
        rhs, allow_new_target, allow_super, allow_super_call,
      )
    @engine.Expr::AssignInvalid(lhs, rhs, _) => {
      validate_context_in_expr(
        lhs, allow_new_target, allow_super, allow_super_call,
      )
      validate_context_in_expr(
        rhs, allow_new_target, allow_super, allow_super_call,
      )
    }
    @engine.Expr::AssignArray(pattern, rhs) => {
      validate_context_in_array_pattern(
        pattern, allow_new_target, allow_super, allow_super_call,
      )
      validate_context_in_expr(
        rhs, allow_new_target, allow_super, allow_super_call,
      )
    }
    @engine.Expr::AssignObject(pattern, rhs) => {
      validate_context_in_object_pattern(
        pattern, allow_new_target, allow_super, allow_super_call,
      )
      validate_context_in_expr(
        rhs, allow_new_target, allow_super, allow_super_call,
      )
    }
    @engine.Expr::AssignOp(_, lhs, rhs, _) => {
      validate_context_in_expr(
        lhs, allow_new_target, allow_super, allow_super_call,
      )
      validate_context_in_expr(
        rhs, allow_new_target, allow_super, allow_super_call,
      )
    }
    @engine.Expr::AssignMember(obj, key, rhs, _) =>
      match obj {
        @engine.Expr::Super => {
          if !allow_super {
            let _ = throw_syntax_error("'super' is only valid in a method")

          }
          validate_context_in_member_key(
            key, allow_new_target, allow_super, allow_super_call,
          )
          validate_context_in_expr(
            rhs, allow_new_target, allow_super, allow_super_call,
          )
        }
        _ => {
          validate_context_in_expr(
            obj, allow_new_target, allow_super, allow_super_call,
          )
          validate_context_in_member_key(
            key, allow_new_target, allow_super, allow_super_call,
          )
          validate_context_in_expr(
            rhs, allow_new_target, allow_super, allow_super_call,
          )
        }
      }
    @engine.Expr::Call(callee, args, _) => {
      match callee {
        @engine.Expr::Super =>
          if !allow_super_call {
            let _ = throw_syntax_error(
              "super() is only valid in a derived class constructor",
            )

          }
        @engine.Expr::Member(@engine.Expr::Super, key, _) => {
          if !allow_super {
            let _ = throw_syntax_error("'super' is only valid in a method")

          }
          validate_context_in_member_key(
            key, allow_new_target, allow_super, allow_super_call,
          )
        }
        _ =>
          validate_context_in_expr(
            callee, allow_new_target, allow_super, allow_super_call,
          )
      }
      validate_context_in_expr_list(
        args, allow_new_target, allow_super, allow_super_call,
      )
    }
    @engine.Expr::Member(obj, key, _) =>
      match obj {
        @engine.Expr::Super => {
          if !allow_super {
            let _ = throw_syntax_error("'super' is only valid in a method")

          }
          validate_context_in_member_key(
            key, allow_new_target, allow_super, allow_super_call,
          )
        }
        _ => {
          validate_context_in_expr(
            obj, allow_new_target, allow_super, allow_super_call,
          )
          validate_context_in_member_key(
            key, allow_new_target, allow_super, allow_super_call,
          )
        }
      }
    @engine.Expr::OptionalChain(base, segments) => {
      validate_context_in_expr(
        base, allow_new_target, allow_super, allow_super_call,
      )
      for segment in segments {
        match segment {
          @engine.ChainSegment::Member(key, _, _) =>
            validate_context_in_member_key(
              key, allow_new_target, allow_super, allow_super_call,
            )
          @engine.ChainSegment::Call(args, _, _) =>
            validate_context_in_expr_list(
              args, allow_new_target, allow_super, allow_super_call,
            )
        }
      }
    }
    @engine.Expr::FunctionExpr(func) => {
      let inner_labels : Array[(String, Bool)] = []
      for inner in func.body {
        validate_break_continue_in_stmt(inner, 0, 0, inner_labels)
      }
      validate_context_in_function(
        func, allow_new_target, allow_super, allow_super_call, false, false,
      )
    }
    @engine.Expr::ClassExpr(class_def) => {
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
      validate_context_in_class(
        class_def, allow_new_target, allow_super, allow_super_call,
      )
    }
    @engine.Expr::ObjectLiteral(props) =>
      for prop in props {
        match prop {
          @engine.ObjectProp::KeyValue(key, value) => {
            validate_context_in_obj_key(
              key, allow_new_target, allow_super, allow_super_call,
            )
            validate_context_in_expr(
              value, allow_new_target, allow_super, allow_super_call,
            )
          }
          @engine.ObjectProp::Method(key, func) => {
            validate_context_in_obj_key(
              key, allow_new_target, allow_super, allow_super_call,
            )
            validate_context_in_function(
              func, allow_new_target, allow_super, allow_super_call, true, false,
            )
          }
          @engine.ObjectProp::Getter(key, func) => {
            validate_context_in_obj_key(
              key, allow_new_target, allow_super, allow_super_call,
            )
            validate_context_in_function(
              func, allow_new_target, allow_super, allow_super_call, true, false,
            )
          }
          @engine.ObjectProp::Setter(key, func) => {
            validate_context_in_obj_key(
              key, allow_new_target, allow_super, allow_super_call,
            )
            validate_context_in_function(
              func, allow_new_target, allow_super, allow_super_call, true, false,
            )
          }
          @engine.ObjectProp::Shorthand(_) => ()
          @engine.ObjectProp::Spread(inner) =>
            validate_context_in_expr(
              inner, allow_new_target, allow_super, allow_super_call,
            )
        }
      }
    @engine.Expr::ArrayLiteral(elems) =>
      for elem in elems {
        match elem {
          Some(inner) =>
            validate_context_in_expr(
              inner, allow_new_target, allow_super, allow_super_call,
            )
          None => ()
        }
      }
    @engine.Expr::Spread(inner) =>
      validate_context_in_expr(
        inner, allow_new_target, allow_super, allow_super_call,
      )
    _ => ()
  }
}

///|
fn validate_context_in_stmt(
  stmt : @engine.Stmt,
  allow_new_target : Bool,
  allow_super : Bool,
  allow_super_call : Bool,
) -> Unit raise {
  match stmt {
    @engine.Stmt::Empty => ()
    @engine.Stmt::ExprStmt(expr) =>
      validate_context_in_expr(
        expr, allow_new_target, allow_super, allow_super_call,
      )
    @engine.Stmt::VarDecl(_, decls) =>
      for decl in decls {
        let (binding, init) = decl
        validate_context_in_var_binding(
          binding, allow_new_target, allow_super, allow_super_call,
        )
        validate_context_in_expr_opt(
          init, allow_new_target, allow_super, allow_super_call,
        )
      }
    @engine.Stmt::Block(stmts) =>
      for inner in stmts {
        validate_context_in_stmt(
          inner, allow_new_target, allow_super, allow_super_call,
        )
      }
    @engine.Stmt::Label(_, body) =>
      validate_context_in_stmt(
        body, allow_new_target, allow_super, allow_super_call,
      )
    @engine.Stmt::FunctionDecl(func) =>
      validate_context_in_function(
        func, allow_new_target, allow_super, allow_super_call, false, false,
      )
    @engine.Stmt::ClassDecl(class_def) =>
      validate_context_in_class(
        class_def, allow_new_target, allow_super, allow_super_call,
      )
    @engine.Stmt::Return(expr_opt) =>
      validate_context_in_expr_opt(
        expr_opt, allow_new_target, allow_super, allow_super_call,
      )
    @engine.Stmt::If(cond_expr, conseq, alt) => {
      validate_context_in_expr(
        cond_expr, allow_new_target, allow_super, allow_super_call,
      )
      validate_context_in_stmt(
        conseq, allow_new_target, allow_super, allow_super_call,
      )
      match alt {
        Some(inner) =>
          validate_context_in_stmt(
            inner, allow_new_target, allow_super, allow_super_call,
          )
        None => ()
      }
    }
    @engine.Stmt::With(expr, body) => {
      validate_context_in_expr(
        expr, allow_new_target, allow_super, allow_super_call,
      )
      validate_context_in_stmt(
        body, allow_new_target, allow_super, allow_super_call,
      )
    }
    @engine.Stmt::While(cond_expr, body) => {
      validate_context_in_expr(
        cond_expr, allow_new_target, allow_super, allow_super_call,
      )
      validate_context_in_stmt(
        body, allow_new_target, allow_super, allow_super_call,
      )
    }
    @engine.Stmt::For(init, test_expr, update_expr, body) => {
      match init {
        @engine.ForInit::Var(_, decls) =>
          for decl in decls {
            let (binding, init_expr) = decl
            validate_context_in_var_binding(
              binding, allow_new_target, allow_super, allow_super_call,
            )
            validate_context_in_expr_opt(
              init_expr, allow_new_target, allow_super, allow_super_call,
            )
          }
        @engine.ForInit::Expr(expr) =>
          validate_context_in_expr(
            expr, allow_new_target, allow_super, allow_super_call,
          )
        @engine.ForInit::None => ()
      }
      validate_context_in_expr_opt(
        test_expr, allow_new_target, allow_super, allow_super_call,
      )
      validate_context_in_expr_opt(
        update_expr, allow_new_target, allow_super, allow_super_call,
      )
      validate_for_head_lexical_conflicts(init, body)
      validate_context_in_stmt(
        body, allow_new_target, allow_super, allow_super_call,
      )
    }
    @engine.Stmt::ForIn(init, expr, body) => {
      match init {
        @engine.ForInit::Var(_, decls) =>
          for decl in decls {
            let (binding, init_expr) = decl
            validate_context_in_var_binding(
              binding, allow_new_target, allow_super, allow_super_call,
            )
            validate_context_in_expr_opt(
              init_expr, allow_new_target, allow_super, allow_super_call,
            )
          }
        @engine.ForInit::Expr(expr) =>
          validate_context_in_expr(
            expr, allow_new_target, allow_super, allow_super_call,
          )
        @engine.ForInit::None => ()
      }
      validate_context_in_expr(
        expr, allow_new_target, allow_super, allow_super_call,
      )
      validate_for_head_lexical_conflicts(init, body)
      validate_context_in_stmt(
        body, allow_new_target, allow_super, allow_super_call,
      )
    }
    @engine.Stmt::ForOf(init, expr, body) => {
      match init {
        @engine.ForInit::Var(_, decls) =>
          for decl in decls {
            let (binding, init_expr) = decl
            validate_context_in_var_binding(
              binding, allow_new_target, allow_super, allow_super_call,
            )
            validate_context_in_expr_opt(
              init_expr, allow_new_target, allow_super, allow_super_call,
            )
          }
        @engine.ForInit::Expr(expr) =>
          validate_context_in_expr(
            expr, allow_new_target, allow_super, allow_super_call,
          )
        @engine.ForInit::None => ()
      }
      validate_context_in_expr(
        expr, allow_new_target, allow_super, allow_super_call,
      )
      validate_for_head_lexical_conflicts(init, body)
      validate_context_in_stmt(
        body, allow_new_target, allow_super, allow_super_call,
      )
    }
    @engine.Stmt::ForAwaitOf(init, expr, body) => {
      match init {
        @engine.ForInit::Var(_, decls) =>
          for decl in decls {
            let (binding, init_expr) = decl
            validate_context_in_var_binding(
              binding, allow_new_target, allow_super, allow_super_call,
            )
            validate_context_in_expr_opt(
              init_expr, allow_new_target, allow_super, allow_super_call,
            )
          }
        @engine.ForInit::Expr(expr) =>
          validate_context_in_expr(
            expr, allow_new_target, allow_super, allow_super_call,
          )
        @engine.ForInit::None => ()
      }
      validate_context_in_expr(
        expr, allow_new_target, allow_super, allow_super_call,
      )
      validate_for_head_lexical_conflicts(init, body)
      validate_context_in_stmt(
        body, allow_new_target, allow_super, allow_super_call,
      )
    }
    @engine.Stmt::DoWhile(body, cond_expr) => {
      validate_context_in_stmt(
        body, allow_new_target, allow_super, allow_super_call,
      )
      validate_context_in_expr(
        cond_expr, allow_new_target, allow_super, allow_super_call,
      )
    }
    @engine.Stmt::Switch(discriminant, cases) => {
      validate_context_in_expr(
        discriminant, allow_new_target, allow_super, allow_super_call,
      )
      for clause in cases {
        let body = match clause {
          @engine.SwitchCase::Case(expr, stmts) => {
            validate_context_in_expr(
              expr, allow_new_target, allow_super, allow_super_call,
            )
            stmts
          }
          @engine.SwitchCase::Default(stmts) => stmts
        }
        for inner in body {
          validate_context_in_stmt(
            inner, allow_new_target, allow_super, allow_super_call,
          )
        }
      }
    }
    @engine.Stmt::Throw(expr) =>
      validate_context_in_expr(
        expr, allow_new_target, allow_super, allow_super_call,
      )
    @engine.Stmt::Try(try_body, catch_clause, finally_body) => {
      validate_context_in_stmt(
        try_body, allow_new_target, allow_super, allow_super_call,
      )
      match catch_clause {
        Some(clause) =>
          validate_context_in_stmt(
            clause.body,
            allow_new_target,
            allow_super,
            allow_super_call,
          )
        None => ()
      }
      match finally_body {
        Some(inner) =>
          validate_context_in_stmt(
            inner, allow_new_target, allow_super, allow_super_call,
          )
        None => ()
      }
    }
    _ => ()
  }
}

///|
fn validate_for_head_lexical_conflicts(
  init : @engine.ForInit,
  body : @engine.Stmt,
) -> Unit raise {
  match init {
    @engine.ForInit::Var(kind, decls) =>
      if !(kind is @engine.VarKind::Var) {
        let bound_names : Array[String] = []
        for decl in decls {
          let (binding, _) = decl
          eval_collect_var_names_in_binding(binding, bound_names)
        }
        if !bound_names.is_empty() {
          let var_names : Array[String] = []
          eval_collect_var_names_in_stmt(body, var_names)
          let seen : Map[String, Bool] = Map::new()
          for name in var_names {
            seen.set(name, true)
          }
          for name in bound_names {
            if seen.contains(name) {
              let _ = throw_syntax_error("invalid identifier")

            }
          }
        }
      }
    _ => ()
  }
}
