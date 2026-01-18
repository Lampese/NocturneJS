///|
fn expr_contains_yield(expr : @engine.Expr) -> Bool {
  match expr {
    Yield(_, _) => true
    Await(_, _) => true
    Unary(_, inner, _) => expr_contains_yield(inner)
    Update(_, inner, _, _) => expr_contains_yield(inner)
    New(callee, args) =>
      expr_contains_yield(callee) || expr_list_contains_yield(args)
    Binary(_, left, right, _) =>
      expr_contains_yield(left) || expr_contains_yield(right)
    PrivateIn(_, right, _) => expr_contains_yield(right)
    Conditional(test_expr, conseq, alt) =>
      expr_contains_yield(test_expr) ||
      expr_contains_yield(conseq) ||
      expr_contains_yield(alt)
    Sequence(exprs) => expr_list_contains_yield(exprs)
    Assign(_, rhs, _, _) => expr_contains_yield(rhs)
    AssignInvalid(lhs, rhs, _) =>
      expr_contains_yield(lhs) || expr_contains_yield(rhs)
    AssignArray(pattern, rhs) =>
      array_pattern_contains_yield(pattern) || expr_contains_yield(rhs)
    AssignObject(pattern, rhs) =>
      object_pattern_contains_yield(pattern) || expr_contains_yield(rhs)
    AssignOp(_, lhs, rhs, _) =>
      expr_contains_yield(lhs) || expr_contains_yield(rhs)
    AssignMember(obj, key, rhs, _) =>
      expr_contains_yield(obj) ||
      member_key_contains_yield(key) ||
      expr_contains_yield(rhs)
    Call(callee, args, _) =>
      expr_contains_yield(callee) || expr_list_contains_yield(args)
    Member(obj, key, _) =>
      expr_contains_yield(obj) || member_key_contains_yield(key)
    OptionalChain(base, segments) =>
      expr_contains_yield(base) || chain_segments_contain_yield(segments)
    TemplateLiteral(_, exprs) => expr_list_contains_yield(exprs)
    TaggedTemplate(callee, _, exprs) =>
      expr_contains_yield(callee) || expr_list_contains_yield(exprs)
    FunctionExpr(_) => false
    ClassExpr(class_def) => class_def_contains_yield(class_def)
    ObjectLiteral(props) => object_props_contain_yield(props)
    ArrayLiteral(elems) => expr_option_list_contains_yield(elems)
    Spread(expr) => expr_contains_yield(expr)
    Paren(expr) => expr_contains_yield(expr)
    _ => false
  }
}

///|
fn class_def_contains_yield(class_def : @engine.ClassDef) -> Bool {
  match class_def.name {
    Some(name) => if name == "yield" { return true }
    None => ()
  }
  match class_def.super_class {
    Some(expr) => if expr_contains_yield(expr) { return true }
    None => ()
  }
  for member_ in class_def.body {
    match member_ {
      @engine.ClassMember::Method(key, _, _)
      | @engine.ClassMember::Getter(key, _, _)
      | @engine.ClassMember::Setter(key, _, _) =>
        if obj_key_contains_yield(key) {
          return true
        }
      @engine.ClassMember::Field(key, init, _) => {
        if obj_key_contains_yield(key) {
          return true
        }
        match init {
          Some(value) => if expr_contains_yield(value) { return true }
          None => ()
        }
      }
      @engine.ClassMember::StaticBlock(stmts) =>
        if stmts_contain_yield(stmts) {
          return true
        }
    }
  }
  false
}

///|
fn chain_segments_contain_yield(segments : Array[@engine.ChainSegment]) -> Bool {
  for segment in segments {
    match segment {
      @engine.ChainSegment::Member(key, _, _) =>
        if member_key_contains_yield(key) {
          return true
        }
      @engine.ChainSegment::Call(args, _, _) =>
        if expr_list_contains_yield(args) {
          return true
        }
    }
  }
  false
}

///|
fn array_pattern_contains_yield(
  pattern : Array[@engine.ArrayPatternElem],
) -> Bool {
  for elem in pattern {
    match elem {
      @engine.ArrayPatternElem::Bind(binding, default) => {
        if var_binding_contains_yield(binding) {
          return true
        }
        match default {
          Some(expr) => if expr_contains_yield(expr) { return true }
          None => ()
        }
      }
      @engine.ArrayPatternElem::Rest(binding) =>
        if var_binding_contains_yield(binding) {
          return true
        }
      @engine.ArrayPatternElem::Hole => ()
    }
  }
  false
}

///|
fn object_pattern_contains_yield(
  pattern : Array[@engine.ObjectPatternElem],
) -> Bool {
  for elem in pattern {
    match elem {
      @engine.ObjectPatternElem::Bind(key, binding, default) => {
        if obj_key_contains_yield(key) {
          return true
        }
        if var_binding_contains_yield(binding) {
          return true
        }
        match default {
          Some(expr) => if expr_contains_yield(expr) { return true }
          None => ()
        }
      }
      @engine.ObjectPatternElem::Rest(binding) =>
        if var_binding_contains_yield(binding) {
          return true
        }
    }
  }
  false
}

///|
fn var_binding_contains_yield(binding : @engine.VarBinding) -> Bool {
  match binding {
    @engine.VarBinding::Name(_) => false
    @engine.VarBinding::ArrayPattern(pattern) =>
      array_pattern_contains_yield(pattern)
    @engine.VarBinding::ObjectPattern(pattern) =>
      object_pattern_contains_yield(pattern)
    @engine.VarBinding::Target(expr) => expr_contains_yield(expr)
  }
}

///|
fn expr_list_contains_yield(exprs : Array[@engine.Expr]) -> Bool {
  for expr in exprs {
    if expr_contains_yield(expr) {
      return true
    }
  }
  false
}

///|
fn expr_option_list_contains_yield(exprs : Array[@engine.Expr?]) -> Bool {
  for expr in exprs {
    match expr {
      Some(value) => if expr_contains_yield(value) { return true }
      None => ()
    }
  }
  false
}

///|
fn member_key_contains_yield(key : @engine.MemberKey) -> Bool {
  match key {
    @engine.MemberKey::Ident(_) => false
    @engine.MemberKey::Private(_) => false
    @engine.MemberKey::Computed(expr) => expr_contains_yield(expr)
  }
}

///|
fn obj_key_contains_yield(key : @engine.ObjKey) -> Bool {
  match key {
    @engine.ObjKey::Computed(expr) => expr_contains_yield(expr)
    _ => false
  }
}

///|
fn object_props_contain_yield(props : Array[@engine.ObjectProp]) -> Bool {
  for prop in props {
    match prop {
      @engine.ObjectProp::KeyValue(key, expr) =>
        if obj_key_contains_yield(key) || expr_contains_yield(expr) {
          return true
        }
      @engine.ObjectProp::Method(key, _) =>
        if obj_key_contains_yield(key) {
          return true
        }
      @engine.ObjectProp::Shorthand(_) => ()
      @engine.ObjectProp::Getter(key, _) =>
        if obj_key_contains_yield(key) {
          return true
        }
      @engine.ObjectProp::Setter(key, _) =>
        if obj_key_contains_yield(key) {
          return true
        }
      @engine.ObjectProp::Spread(expr) =>
        if expr_contains_yield(expr) {
          return true
        }
    }
  }
  false
}

///|
fn stmt_contains_yield(stmt : @engine.Stmt) -> Bool {
  match stmt {
    ExprStmt(expr) => expr_contains_yield(expr)
    VarDecl(_, decls) => {
      for decl in decls {
        let (binding, init) = decl
        if var_binding_contains_yield(binding) {
          return true
        }
        match init {
          Some(expr) => if expr_contains_yield(expr) { return true }
          None => ()
        }
      }
      false
    }
    Block(stmts) => stmts_contain_yield(stmts)
    Label(_, body) => stmt_contains_yield(body)
    FunctionDecl(_) => false
    ClassDecl(class_def) => class_def_contains_yield(class_def)
    If(cond_expr, conseq, alt) =>
      if expr_contains_yield(cond_expr) || stmt_contains_yield(conseq) {
        true
      } else {
        match alt {
          Some(stmt) => stmt_contains_yield(stmt)
          None => false
        }
      }
    With(expr, body) => expr_contains_yield(expr) || stmt_contains_yield(body)
    While(cond_expr, body) =>
      expr_contains_yield(cond_expr) || stmt_contains_yield(body)
    For(init, test_expr, update_expr, body) => {
      let init_has_yield = for_init_contains_yield(init)
      let test_has_yield = match test_expr {
        Some(expr) => expr_contains_yield(expr)
        None => false
      }
      let update_has_yield = match update_expr {
        Some(expr) => expr_contains_yield(expr)
        None => false
      }
      init_has_yield ||
      test_has_yield ||
      update_has_yield ||
      stmt_contains_yield(body)
    }
    ForIn(init, expr, body) =>
      for_init_contains_yield(init) ||
      expr_contains_yield(expr) ||
      stmt_contains_yield(body)
    ForOf(init, expr, body) =>
      for_init_contains_yield(init) ||
      expr_contains_yield(expr) ||
      stmt_contains_yield(body)
    ForAwaitOf(_, _, _) => true
    DoWhile(body, cond_expr) =>
      stmt_contains_yield(body) || expr_contains_yield(cond_expr)
    Switch(discriminant, cases) => {
      if expr_contains_yield(discriminant) {
        return true
      }
      for clause in cases {
        match clause {
          @engine.SwitchCase::Case(expr, body) =>
            if expr_contains_yield(expr) || stmts_contain_yield(body) {
              return true
            }
          @engine.SwitchCase::Default(body) =>
            if stmts_contain_yield(body) {
              return true
            }
        }
      }
      false
    }
    Throw(expr) => expr_contains_yield(expr)
    Try(try_body, catch_clause, finally_body) =>
      if stmt_contains_yield(try_body) {
        true
      } else {
        let catch_has_yield = match catch_clause {
          Some(clause) => stmt_contains_yield(clause.body)
          None => false
        }
        if catch_has_yield {
          true
        } else {
          match finally_body {
            Some(body) => stmt_contains_yield(body)
            None => false
          }
        }
      }
    Return(expr) =>
      match expr {
        Some(inner) => expr_contains_yield(inner)
        None => false
      }
    _ => false
  }
}

///|
fn for_init_contains_yield(init : @engine.ForInit) -> Bool {
  match init {
    @engine.ForInit::None => false
    @engine.ForInit::Var(_, decls) => {
      for decl in decls {
        let (binding, init) = decl
        if var_binding_contains_yield(binding) {
          return true
        }
        match init {
          Some(expr) => if expr_contains_yield(expr) { return true }
          None => ()
        }
      }
      false
    }
    @engine.ForInit::Expr(expr) => expr_contains_yield(expr)
  }
}

///|
fn stmts_contain_yield(stmts : Array[@engine.Stmt]) -> Bool {
  for stmt in stmts {
    if stmt_contains_yield(stmt) {
      return true
    }
  }
  false
}

///|
fn expr_is_direct_eval(expr : @engine.Expr) -> Bool {
  match expr {
    @engine.Expr::Ident(name, _) => name == "eval"
    @engine.Expr::Paren(inner) => expr_is_direct_eval(inner)
    _ => false
  }
}

///|
fn expr_contains_function_like(expr : @engine.Expr) -> Bool {
  match expr {
    @engine.Expr::FunctionExpr(_) => true
    @engine.Expr::ClassExpr(_) => true
    @engine.Expr::Unary(_, inner, _) => expr_contains_function_like(inner)
    @engine.Expr::Update(_, inner, _, _) => expr_contains_function_like(inner)
    @engine.Expr::New(callee, args) =>
      expr_contains_function_like(callee) ||
      expr_list_contains_function_like(args)
    @engine.Expr::Binary(_, left, right, _) =>
      expr_contains_function_like(left) || expr_contains_function_like(right)
    @engine.Expr::PrivateIn(_, right, _) => expr_contains_function_like(right)
    @engine.Expr::Conditional(test_expr, conseq, alt) =>
      expr_contains_function_like(test_expr) ||
      expr_contains_function_like(conseq) ||
      expr_contains_function_like(alt)
    @engine.Expr::Sequence(exprs) => expr_list_contains_function_like(exprs)
    @engine.Expr::Assign(_, rhs, _, _) => expr_contains_function_like(rhs)
    @engine.Expr::AssignInvalid(lhs, rhs, _) =>
      expr_contains_function_like(lhs) || expr_contains_function_like(rhs)
    @engine.Expr::AssignArray(pattern, rhs) =>
      array_pattern_contains_function_like(pattern) ||
      expr_contains_function_like(rhs)
    @engine.Expr::AssignObject(pattern, rhs) =>
      object_pattern_contains_function_like(pattern) ||
      expr_contains_function_like(rhs)
    @engine.Expr::AssignOp(_, lhs, rhs, _) =>
      expr_contains_function_like(lhs) || expr_contains_function_like(rhs)
    @engine.Expr::AssignMember(obj, key, rhs, _) =>
      expr_contains_function_like(obj) ||
      member_key_contains_function_like(key) ||
      expr_contains_function_like(rhs)
    @engine.Expr::Call(callee, args, _) =>
      if expr_is_direct_eval(callee) {
        true
      } else {
        expr_contains_function_like(callee) ||
        expr_list_contains_function_like(args)
      }
    @engine.Expr::Member(obj, key, _) =>
      expr_contains_function_like(obj) || member_key_contains_function_like(key)
    @engine.Expr::OptionalChain(base, segments) =>
      expr_contains_function_like(base) ||
      chain_segments_contain_function_like(segments)
    @engine.Expr::TemplateLiteral(_, exprs) =>
      expr_list_contains_function_like(exprs)
    @engine.Expr::TaggedTemplate(callee, _, exprs) =>
      expr_contains_function_like(callee) ||
      expr_list_contains_function_like(exprs)
    @engine.Expr::ObjectLiteral(props) =>
      object_props_contain_function_like(props)
    @engine.Expr::ArrayLiteral(elems) =>
      expr_option_list_contains_function_like(elems)
    @engine.Expr::Spread(expr) => expr_contains_function_like(expr)
    @engine.Expr::Paren(expr) => expr_contains_function_like(expr)
    _ => false
  }
}

///|
fn expr_list_contains_function_like(exprs : Array[@engine.Expr]) -> Bool {
  for expr in exprs {
    if expr_contains_function_like(expr) {
      return true
    }
  }
  false
}

///|
fn expr_option_list_contains_function_like(
  exprs : Array[@engine.Expr?],
) -> Bool {
  for expr in exprs {
    match expr {
      Some(value) => if expr_contains_function_like(value) { return true }
      None => ()
    }
  }
  false
}

///|
fn member_key_contains_function_like(key : @engine.MemberKey) -> Bool {
  match key {
    @engine.MemberKey::Ident(_) => false
    @engine.MemberKey::Private(_) => false
    @engine.MemberKey::Computed(expr) => expr_contains_function_like(expr)
  }
}

///|
fn obj_key_contains_function_like(key : @engine.ObjKey) -> Bool {
  match key {
    @engine.ObjKey::Computed(expr) => expr_contains_function_like(expr)
    _ => false
  }
}

///|
fn chain_segments_contain_function_like(
  segments : Array[@engine.ChainSegment],
) -> Bool {
  for segment in segments {
    match segment {
      @engine.ChainSegment::Member(key, _, _) =>
        if member_key_contains_function_like(key) {
          return true
        }
      @engine.ChainSegment::Call(args, _, _) =>
        if expr_list_contains_function_like(args) {
          return true
        }
    }
  }
  false
}

///|
fn object_props_contain_function_like(
  props : Array[@engine.ObjectProp],
) -> Bool {
  for prop in props {
    match prop {
      @engine.ObjectProp::KeyValue(key, expr) =>
        if obj_key_contains_function_like(key) ||
          expr_contains_function_like(expr) {
          return true
        }
      @engine.ObjectProp::Method(_, _) => return true
      @engine.ObjectProp::Getter(_, _) => return true
      @engine.ObjectProp::Setter(_, _) => return true
      @engine.ObjectProp::Shorthand(_) => ()
      @engine.ObjectProp::Spread(expr) =>
        if expr_contains_function_like(expr) {
          return true
        }
    }
  }
  false
}

///|
fn array_pattern_contains_function_like(
  pattern : Array[@engine.ArrayPatternElem],
) -> Bool {
  for elem in pattern {
    match elem {
      @engine.ArrayPatternElem::Bind(binding, default) => {
        if var_binding_contains_function_like(binding) {
          return true
        }
        match default {
          Some(expr) => if expr_contains_function_like(expr) { return true }
          None => ()
        }
      }
      @engine.ArrayPatternElem::Rest(binding) =>
        if var_binding_contains_function_like(binding) {
          return true
        }
      @engine.ArrayPatternElem::Hole => ()
    }
  }
  false
}

///|
fn object_pattern_contains_function_like(
  pattern : Array[@engine.ObjectPatternElem],
) -> Bool {
  for elem in pattern {
    match elem {
      @engine.ObjectPatternElem::Bind(key, binding, default) => {
        if obj_key_contains_function_like(key) {
          return true
        }
        if var_binding_contains_function_like(binding) {
          return true
        }
        match default {
          Some(expr) => if expr_contains_function_like(expr) { return true }
          None => ()
        }
      }
      @engine.ObjectPatternElem::Rest(binding) =>
        if var_binding_contains_function_like(binding) {
          return true
        }
    }
  }
  false
}

///|
fn var_binding_contains_function_like(binding : @engine.VarBinding) -> Bool {
  match binding {
    @engine.VarBinding::Name(_) => false
    @engine.VarBinding::ArrayPattern(pattern) =>
      array_pattern_contains_function_like(pattern)
    @engine.VarBinding::ObjectPattern(pattern) =>
      object_pattern_contains_function_like(pattern)
    @engine.VarBinding::Target(expr) => expr_contains_function_like(expr)
  }
}

///|
fn stmt_contains_function_like(stmt : @engine.Stmt) -> Bool {
  match stmt {
    @engine.Stmt::ExprStmt(expr) => expr_contains_function_like(expr)
    @engine.Stmt::VarDecl(_, decls) => {
      for decl in decls {
        let (binding, init) = decl
        if var_binding_contains_function_like(binding) {
          return true
        }
        match init {
          Some(expr) => if expr_contains_function_like(expr) { return true }
          None => ()
        }
      }
      false
    }
    @engine.Stmt::Block(stmts) => stmts_contain_function_like(stmts)
    @engine.Stmt::Label(_, body) => stmt_contains_function_like(body)
    @engine.Stmt::FunctionDecl(_) => true
    @engine.Stmt::ClassDecl(_) => true
    @engine.Stmt::If(cond_expr, conseq, alt) =>
      if expr_contains_function_like(cond_expr) ||
        stmt_contains_function_like(conseq) {
        true
      } else {
        match alt {
          Some(inner) => stmt_contains_function_like(inner)
          None => false
        }
      }
    @engine.Stmt::With(expr, body) =>
      expr_contains_function_like(expr) || stmt_contains_function_like(body)
    @engine.Stmt::While(cond_expr, body) =>
      expr_contains_function_like(cond_expr) ||
      stmt_contains_function_like(body)
    @engine.Stmt::For(init, test_expr, update_expr, body) => {
      let init_has = for_init_contains_function_like(init)
      let test_has = match test_expr {
        Some(expr) => expr_contains_function_like(expr)
        None => false
      }
      let update_has = match update_expr {
        Some(expr) => expr_contains_function_like(expr)
        None => false
      }
      init_has || test_has || update_has || stmt_contains_function_like(body)
    }
    @engine.Stmt::ForIn(init, expr, body) =>
      for_init_contains_function_like(init) ||
      expr_contains_function_like(expr) ||
      stmt_contains_function_like(body)
    @engine.Stmt::ForOf(init, expr, body) =>
      for_init_contains_function_like(init) ||
      expr_contains_function_like(expr) ||
      stmt_contains_function_like(body)
    @engine.Stmt::ForAwaitOf(init, expr, body) =>
      for_init_contains_function_like(init) ||
      expr_contains_function_like(expr) ||
      stmt_contains_function_like(body)
    @engine.Stmt::DoWhile(body, cond_expr) =>
      stmt_contains_function_like(body) ||
      expr_contains_function_like(cond_expr)
    @engine.Stmt::Switch(discriminant, cases) => {
      if expr_contains_function_like(discriminant) {
        return true
      }
      for clause in cases {
        match clause {
          @engine.SwitchCase::Case(expr, body) =>
            if expr_contains_function_like(expr) ||
              stmts_contain_function_like(body) {
              return true
            }
          @engine.SwitchCase::Default(body) =>
            if stmts_contain_function_like(body) {
              return true
            }
        }
      }
      false
    }
    @engine.Stmt::Throw(expr) => expr_contains_function_like(expr)
    @engine.Stmt::Try(try_body, catch_clause, finally_body) =>
      if stmt_contains_function_like(try_body) {
        true
      } else {
        let catch_has = match catch_clause {
          Some(clause) => stmt_contains_function_like(clause.body)
          None => false
        }
        if catch_has {
          true
        } else {
          match finally_body {
            Some(body) => stmt_contains_function_like(body)
            None => false
          }
        }
      }
    @engine.Stmt::Return(expr) =>
      match expr {
        Some(inner) => expr_contains_function_like(inner)
        None => false
      }
    _ => false
  }
}

///|
fn stmts_contain_function_like(stmts : Array[@engine.Stmt]) -> Bool {
  for stmt in stmts {
    if stmt_contains_function_like(stmt) {
      return true
    }
  }
  false
}

///|
fn for_init_contains_function_like(init : @engine.ForInit) -> Bool {
  match init {
    @engine.ForInit::None => false
    @engine.ForInit::Var(_, decls) => {
      for decl in decls {
        let (binding, init) = decl
        if var_binding_contains_function_like(binding) {
          return true
        }
        match init {
          Some(expr) => if expr_contains_function_like(expr) { return true }
          None => ()
        }
      }
      false
    }
    @engine.ForInit::Expr(expr) => expr_contains_function_like(expr)
  }
}

///|
fn for_loop_needs_per_iteration_env(
  init : @engine.ForInit,
  test_expr : @engine.Expr?,
  update_expr : @engine.Expr?,
  body : @engine.Stmt,
) -> Bool {
  if stmt_contains_function_like(body) {
    return true
  }
  if for_init_contains_function_like(init) {
    return true
  }
  match test_expr {
    Some(expr) => if expr_contains_function_like(expr) { return true }
    None => ()
  }
  match update_expr {
    Some(expr) => if expr_contains_function_like(expr) { return true }
    None => ()
  }
  false
}

///|
fn require_generator(this_value : Value?) -> ObjectValue raise {
  match this_value {
    Some(Object(obj)) =>
      match obj.generator_data {
        Some(_) =>
          if obj.async_generator_data is None {
            obj
          } else {
            let _ = throw_type_error("not a generator")
            obj
          }
        None => {
          let _ = throw_type_error("not a generator")
          obj
        }
      }
    _ => {
      let _ = throw_type_error("not a generator")
      new_object_struct(None)
    }
  }
}

///|
