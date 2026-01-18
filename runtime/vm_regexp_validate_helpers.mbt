///|
fn build_stack_from_positions(
  message : String,
  source : String,
  positions : Array[Int],
) -> (String, Int, Int) {
  let lines : Array[String] = []
  let mut top_line = 1
  let mut top_column = 1
  let mut i = positions.length()
  while i > 0 {
    let pos = positions[i - 1]
    let (line, column) = offset_to_line_column(source, pos)
    if lines.is_empty() {
      top_line = line
      top_column = column
      lines.push("\{message}:\{line}:\{column}")
    } else {
      lines.push("at:\{line}:\{column}")
    }
    i = i - 1
  }
  let funcs = current_function_stack()
  let mut j = funcs.length()
  while j > 0 {
    j = j - 1
    let func = funcs[j]
    let name = match func.name {
      Some(value) => if value.is_empty() { "<anonymous>" } else { value }
      None => "<anonymous>"
    }
    lines.push("at " + name)
  }
  (lines.join("\n"), top_line, top_column)
}

///|
fn validate_regexp_in_script(script : @engine.Script) -> Unit raise {
  for stmt in script.body {
    validate_regexp_in_stmt(stmt)
  }
}

///|
fn validate_regexp_in_stmt(stmt : @engine.Stmt) -> Unit raise {
  match stmt {
    Empty => ()
    ExprStmt(expr) => validate_regexp_in_expr(expr)
    VarDecl(_, decls) =>
      for entry in decls {
        let (binding, init) = entry
        validate_regexp_in_binding(binding)
        match init {
          Some(value) => validate_regexp_in_expr(value)
          None => ()
        }
      }
    Block(body) =>
      for entry in body {
        validate_regexp_in_stmt(entry)
      }
    Label(_, body) => validate_regexp_in_stmt(body)
    FunctionDecl(def) => validate_regexp_in_function(def)
    ClassDecl(def) => validate_regexp_in_class(def)
    Return(value) =>
      match value {
        Some(expr) => validate_regexp_in_expr(expr)
        None => ()
      }
    If(cond, then_branch, else_branch) => {
      validate_regexp_in_expr(cond)
      validate_regexp_in_stmt(then_branch)
      match else_branch {
        Some(value) => validate_regexp_in_stmt(value)
        None => ()
      }
    }
    With(expr, body) => {
      validate_regexp_in_expr(expr)
      validate_regexp_in_stmt(body)
    }
    While(cond, body) => {
      validate_regexp_in_expr(cond)
      validate_regexp_in_stmt(body)
    }
    For(init, test_expr, update_expr, body) => {
      validate_regexp_in_for_init(init)
      match test_expr {
        Some(value) => validate_regexp_in_expr(value)
        None => ()
      }
      match update_expr {
        Some(value) => validate_regexp_in_expr(value)
        None => ()
      }
      validate_regexp_in_stmt(body)
    }
    ForIn(init, right, body) => {
      validate_regexp_in_for_init(init)
      validate_regexp_in_expr(right)
      validate_regexp_in_stmt(body)
    }
    ForOf(init, right, body) => {
      validate_regexp_in_for_init(init)
      validate_regexp_in_expr(right)
      validate_regexp_in_stmt(body)
    }
    ForAwaitOf(init, right, body) => {
      validate_regexp_in_for_init(init)
      validate_regexp_in_expr(right)
      validate_regexp_in_stmt(body)
    }
    DoWhile(body, cond) => {
      validate_regexp_in_stmt(body)
      validate_regexp_in_expr(cond)
    }
    Switch(discriminant, cases) => {
      validate_regexp_in_expr(discriminant)
      for entry in cases {
        validate_regexp_in_switch_case(entry)
      }
    }
    Throw(expr) => validate_regexp_in_expr(expr)
    Try(body, catch_clause, finally_clause) => {
      validate_regexp_in_stmt(body)
      match catch_clause {
        Some(value) => validate_regexp_in_stmt(value.body)
        None => ()
      }
      match finally_clause {
        Some(value) => validate_regexp_in_stmt(value)
        None => ()
      }
    }
    Break(_) => ()
    Continue(_) => ()
  }
}

///|
fn validate_regexp_in_for_init(init : @engine.ForInit) -> Unit raise {
  match init {
    None => ()
    Var(_, decls) =>
      for entry in decls {
        let (binding, init_expr) = entry
        validate_regexp_in_binding(binding)
        match init_expr {
          Some(value) => validate_regexp_in_expr(value)
          None => ()
        }
      }
    Expr(expr) => validate_regexp_in_expr(expr)
  }
}

///|
fn validate_regexp_in_switch_case(
  case_entry : @engine.SwitchCase,
) -> Unit raise {
  match case_entry {
    Case(test_expr, body) => {
      validate_regexp_in_expr(test_expr)
      for stmt in body {
        validate_regexp_in_stmt(stmt)
      }
    }
    Default(body) =>
      for stmt in body {
        validate_regexp_in_stmt(stmt)
      }
  }
}

///|
fn validate_regexp_in_function(def : @engine.FunctionDef) -> Unit raise {
  for param in def.params {
    validate_regexp_in_binding(param.binding)
  }
  for init in def.param_inits {
    match init {
      Some(value) => validate_regexp_in_expr(value)
      None => ()
    }
  }
  for stmt in def.body {
    validate_regexp_in_stmt(stmt)
  }
}

///|
fn validate_regexp_in_class(def : @engine.ClassDef) -> Unit raise {
  match def.super_class {
    Some(value) => validate_regexp_in_expr(value)
    None => ()
  }
  for member_ in def.body {
    validate_regexp_in_class_member(member_)
  }
}

///|
fn validate_regexp_in_class_member(member_ : @engine.ClassMember) -> Unit raise {
  match member_ {
    Method(key, def, _) => {
      validate_regexp_in_obj_key(key)
      validate_regexp_in_function(def)
    }
    Getter(key, def, _) => {
      validate_regexp_in_obj_key(key)
      validate_regexp_in_function(def)
    }
    Setter(key, def, _) => {
      validate_regexp_in_obj_key(key)
      validate_regexp_in_function(def)
    }
    Field(key, init, _) => {
      validate_regexp_in_obj_key(key)
      match init {
        Some(value) => validate_regexp_in_expr(value)
        None => ()
      }
    }
    StaticBlock(stmts) =>
      for stmt in stmts {
        validate_regexp_in_stmt(stmt)
      }
  }
}

///|
fn validate_regexp_in_binding(binding : @engine.VarBinding) -> Unit raise {
  match binding {
    Name(_) => ()
    ArrayPattern(items) =>
      for entry in items {
        validate_regexp_in_array_pattern_elem(entry)
      }
    ObjectPattern(items) =>
      for entry in items {
        validate_regexp_in_object_pattern_elem(entry)
      }
    Target(expr) => validate_regexp_in_expr(expr)
  }
}

///|
fn validate_regexp_in_array_pattern_elem(
  elem : @engine.ArrayPatternElem,
) -> Unit raise {
  match elem {
    Bind(binding, init) => {
      validate_regexp_in_binding(binding)
      match init {
        Some(value) => validate_regexp_in_expr(value)
        None => ()
      }
    }
    Rest(binding) => validate_regexp_in_binding(binding)
    Hole => ()
  }
}

///|
fn validate_regexp_in_object_pattern_elem(
  elem : @engine.ObjectPatternElem,
) -> Unit raise {
  match elem {
    Bind(key, binding, init) => {
      validate_regexp_in_obj_key(key)
      validate_regexp_in_binding(binding)
      match init {
        Some(value) => validate_regexp_in_expr(value)
        None => ()
      }
    }
    Rest(binding) => validate_regexp_in_binding(binding)
  }
}

///|
fn validate_regexp_in_obj_key(key : @engine.ObjKey) -> Unit raise {
  match key {
    Ident(_) => ()
    String(_) => ()
    Number(_) => ()
    Private(_) => ()
    Computed(expr) => validate_regexp_in_expr(expr)
  }
}

///|
fn validate_regexp_in_member_key(key : @engine.MemberKey) -> Unit raise {
  match key {
    Ident(_) => ()
    Private(_) => ()
    Computed(expr) => validate_regexp_in_expr(expr)
  }
}

///|
fn validate_regexp_in_chain_segment(
  segment : @engine.ChainSegment,
) -> Unit raise {
  match segment {
    Member(key, _, _) => validate_regexp_in_member_key(key)
    Call(args, _, _) =>
      for arg in args {
        validate_regexp_in_expr(arg)
      }
  }
}

///|
fn validate_regexp_in_object_prop(prop : @engine.ObjectProp) -> Unit raise {
  match prop {
    KeyValue(key, value) => {
      validate_regexp_in_obj_key(key)
      validate_regexp_in_expr(value)
    }
    Method(key, def) => {
      validate_regexp_in_obj_key(key)
      validate_regexp_in_function(def)
    }
    Shorthand(_) => ()
    Getter(key, def) => {
      validate_regexp_in_obj_key(key)
      validate_regexp_in_function(def)
    }
    Setter(key, def) => {
      validate_regexp_in_obj_key(key)
      validate_regexp_in_function(def)
    }
    Spread(expr) => validate_regexp_in_expr(expr)
  }
}

///|
fn validate_regexp_in_expr(expr : @engine.Expr) -> Unit raise {
  match expr {
    Number(_, _, _) => ()
    Bool(_) => ()
    Null => ()
    String(_, _, _) => ()
    TemplateLiteral(_, exprs) =>
      for entry in exprs {
        validate_regexp_in_expr(entry)
      }
    TaggedTemplate(tag, _, exprs) => {
      validate_regexp_in_expr(tag)
      for entry in exprs {
        validate_regexp_in_expr(entry)
      }
    }
    Regexp(pattern, flags, _) => validate_regexp_literal(pattern, flags)
    BigInt(_) => ()
    Ident(_, _) => ()
    This => ()
    Super => ()
    Yield(value, _) =>
      match value {
        Some(expr) => validate_regexp_in_expr(expr)
        None => ()
      }
    Await(value, _) => validate_regexp_in_expr(value)
    Unary(_, value, _) => validate_regexp_in_expr(value)
    Update(_, value, _, _) => validate_regexp_in_expr(value)
    NewTarget(_) => ()
    ImportMeta(_) => ()
    New(callee, args) => {
      validate_regexp_in_expr(callee)
      for arg in args {
        validate_regexp_in_expr(arg)
      }
    }
    Binary(_, left, right, _) => {
      validate_regexp_in_expr(left)
      validate_regexp_in_expr(right)
    }
    PrivateIn(_, rhs, _) => validate_regexp_in_expr(rhs)
    Conditional(test_expr, consequent, alternate) => {
      validate_regexp_in_expr(test_expr)
      validate_regexp_in_expr(consequent)
      validate_regexp_in_expr(alternate)
    }
    Sequence(items) =>
      for entry in items {
        validate_regexp_in_expr(entry)
      }
    Paren(inner) => validate_regexp_in_expr(inner)
    Assign(_, value, _, _) => validate_regexp_in_expr(value)
    AssignInvalid(lhs, rhs, _) => {
      validate_regexp_in_expr(lhs)
      validate_regexp_in_expr(rhs)
    }
    AssignArray(items, value) => {
      for entry in items {
        validate_regexp_in_array_pattern_elem(entry)
      }
      validate_regexp_in_expr(value)
    }
    AssignObject(items, value) => {
      for entry in items {
        validate_regexp_in_object_pattern_elem(entry)
      }
      validate_regexp_in_expr(value)
    }
    AssignOp(_, left, right, _) => {
      validate_regexp_in_expr(left)
      validate_regexp_in_expr(right)
    }
    AssignMember(obj, key, value, _) => {
      validate_regexp_in_expr(obj)
      validate_regexp_in_member_key(key)
      validate_regexp_in_expr(value)
    }
    Call(callee, args, _) => {
      validate_regexp_in_expr(callee)
      for arg in args {
        validate_regexp_in_expr(arg)
      }
    }
    Member(obj, key, _) => {
      validate_regexp_in_expr(obj)
      validate_regexp_in_member_key(key)
    }
    OptionalChain(base, segments) => {
      validate_regexp_in_expr(base)
      for entry in segments {
        validate_regexp_in_chain_segment(entry)
      }
    }
    FunctionExpr(def) => validate_regexp_in_function(def)
    ClassExpr(def) => validate_regexp_in_class(def)
    ObjectLiteral(props) =>
      for entry in props {
        validate_regexp_in_object_prop(entry)
      }
    ArrayLiteral(items) =>
      for entry in items {
        match entry {
          Some(value) => validate_regexp_in_expr(value)
          None => ()
        }
      }
    Spread(value) => validate_regexp_in_expr(value)
  }
}

///|
fn validate_regexp_literal(pattern : String, flags : String) -> Unit raise {
  let _ = regexp_data_from_pattern(pattern, flags)

}
