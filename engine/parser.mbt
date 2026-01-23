///|
pub(all) suberror ParseError {
  At(offset~ : Int, message~ : String)
} derive(Show, Eq, ToJson)

///|
fn[T] parse_fail(
  tokens : Array[Token],
  index : Int,
  message : String,
) -> T raise {
  let len = tokens.length()
  let idx = if index < len { index } else { len - 1 }
  let tok = tokens[idx]
  raise ParseError::At(offset=tok.offset, message~)
}

///|
fn[T] parse_fail_offset(offset : Int, message : String) -> T raise {
  raise ParseError::At(offset~, message~)
}

///|
let yield_expr_allowed_ref : Ref[Bool] = Ref::new(false)

///|
let yield_ident_allowed_ref : Ref[Bool] = Ref::new(true)

///|
let await_expr_allowed_ref : Ref[Bool] = Ref::new(false)

///|
let await_ident_allowed_ref : Ref[Bool] = Ref::new(true)

///|
let await_param_override_ref : Ref[Bool] = Ref::new(false)

///|
let return_allowed_ref : Ref[Bool] = Ref::new(false)

///|
let module_context_ref : Ref[Bool] = Ref::new(false)

///|
let in_expr_allowed_ref : Ref[Bool] = Ref::new(true)

///|
let strict_context_ref : Ref[Bool] = Ref::new(false)

///|
let template_site_id_ref : Ref[Int] = Ref::new(1)

///|
let source_chars_ref : Ref[Array[Char]] = Ref::new([])

///|
fn next_template_site_id() -> Int {
  let mut value = 0
  let _ = template_site_id_ref.update(fn(current) {
    value = current
    current + 1
  })
  value
}

///|
fn source_chars() -> Array[Char] {
  let mut value : Array[Char] = []
  let _ = source_chars_ref.update(fn(current) {
    value = current
    current
  })
  value
}

///|
fn set_source_chars(chars : Array[Char]) -> Unit {
  let _ = source_chars_ref.update(fn(_) { chars })

}

///|
fn yield_expr_allowed() -> Bool {
  let mut value = false
  yield_expr_allowed_ref.update(fn(current) {
    value = current
    current
  })
  value
}

///|
fn yield_ident_allowed() -> Bool {
  let mut value = true
  yield_ident_allowed_ref.update(fn(current) {
    value = current
    current
  })
  value
}

///|
fn await_expr_allowed() -> Bool {
  await_expr_allowed_raw()
}

///|
fn await_expr_allowed_raw() -> Bool {
  let mut value = false
  await_expr_allowed_ref.update(fn(current) {
    value = current
    current
  })
  value
}

///|
fn await_ident_allowed() -> Bool {
  let allow = await_ident_allowed_raw()
  if module_context_raw() {
    false
  } else {
    allow
  }
}

///|
fn await_ident_allowed_raw() -> Bool {
  let mut value = true
  await_ident_allowed_ref.update(fn(current) {
    value = current
    current
  })
  value
}

///|
fn await_param_override() -> Bool {
  let mut value = false
  await_param_override_ref.update(fn(current) {
    value = current
    current
  })
  value
}

///|
fn return_allowed() -> Bool {
  let mut value = false
  return_allowed_ref.update(fn(current) {
    value = current
    current
  })
  value
}

///|
fn module_context_raw() -> Bool {
  let mut value = false
  module_context_ref.update(fn(current) {
    value = current
    current
  })
  value
}

///|
fn in_expr_allowed() -> Bool {
  let mut value = true
  in_expr_allowed_ref.update(fn(current) {
    value = current
    current
  })
  value
}

///|
fn strict_context() -> Bool {
  let mut value = false
  strict_context_ref.update(fn(current) {
    value = current
    current
  })
  value
}

///|
fn set_yield_expr_allowed(value : Bool) -> Unit {
  let _ = yield_expr_allowed_ref.update(fn(_) { value })

}

///|
fn set_yield_ident_allowed(value : Bool) -> Unit {
  let _ = yield_ident_allowed_ref.update(fn(_) { value })

}

///|
fn set_await_expr_allowed(value : Bool) -> Unit {
  let _ = await_expr_allowed_ref.update(fn(_) { value })

}

///|
fn set_await_ident_allowed(value : Bool) -> Unit {
  let _ = await_ident_allowed_ref.update(fn(_) { value })

}

///|
fn set_await_param_override(value : Bool) -> Unit {
  let _ = await_param_override_ref.update(fn(_) { value })

}

///|
fn set_return_allowed(value : Bool) -> Unit {
  let _ = return_allowed_ref.update(fn(_) { value })

}

///|
fn set_module_context(value : Bool) -> Unit {
  let _ = module_context_ref.update(fn(_) { value })

}

///|
fn set_in_expr_allowed(value : Bool) -> Unit {
  let _ = in_expr_allowed_ref.update(fn(_) { value })

}

///|
fn set_strict_context(value : Bool) -> Unit {
  let _ = strict_context_ref.update(fn(_) { value })

}

///|
fn[T] with_yield_context(
  allow_expr : Bool,
  allow_ident : Bool,
  f : () -> T raise,
) -> T raise {
  let prev_expr = yield_expr_allowed()
  let prev_ident = yield_ident_allowed()
  set_yield_expr_allowed(allow_expr)
  set_yield_ident_allowed(allow_ident)
  try {
    let result = f()
    set_yield_expr_allowed(prev_expr)
    set_yield_ident_allowed(prev_ident)
    result
  } catch {
    err => {
      set_yield_expr_allowed(prev_expr)
      set_yield_ident_allowed(prev_ident)
      raise err
    }
  }
}

///|
fn[T] with_await_context(
  allow_expr : Bool,
  allow_ident : Bool,
  f : () -> T raise,
) -> T raise {
  let prev_expr = await_expr_allowed_raw()
  let prev_ident = await_ident_allowed_raw()
  set_await_expr_allowed(allow_expr)
  set_await_ident_allowed(allow_ident)
  try {
    let result = f()
    set_await_expr_allowed(prev_expr)
    set_await_ident_allowed(prev_ident)
    result
  } catch {
    err => {
      set_await_expr_allowed(prev_expr)
      set_await_ident_allowed(prev_ident)
      raise err
    }
  }
}

///|
fn[T] with_await_param_override(allow : Bool, f : () -> T raise) -> T raise {
  let prev = await_param_override()
  set_await_param_override(allow)
  try {
    let result = f()
    set_await_param_override(prev)
    result
  } catch {
    err => {
      set_await_param_override(prev)
      raise err
    }
  }
}

///|
fn[T] with_return_context(allow_return : Bool, f : () -> T raise) -> T raise {
  let prev_return = return_allowed()
  set_return_allowed(allow_return)
  try {
    let result = f()
    set_return_allowed(prev_return)
    result
  } catch {
    err => {
      set_return_allowed(prev_return)
      raise err
    }
  }
}

///|
fn[T] with_module_context(is_module : Bool, f : () -> T raise) -> T raise {
  let prev_module = module_context_raw()
  let prev_expr = await_expr_allowed_raw()
  let prev_ident = await_ident_allowed_raw()
  set_module_context(is_module)
  set_await_expr_allowed(is_module)
  set_await_ident_allowed(true)
  try {
    let result = f()
    set_module_context(prev_module)
    set_await_expr_allowed(prev_expr)
    set_await_ident_allowed(prev_ident)
    result
  } catch {
    err => {
      set_module_context(prev_module)
      set_await_expr_allowed(prev_expr)
      set_await_ident_allowed(prev_ident)
      raise err
    }
  }
}

///|
fn[T] with_strict_context(is_strict : Bool, f : () -> T raise) -> T raise {
  let prev_strict = strict_context()
  set_strict_context(is_strict)
  try {
    let result = f()
    set_strict_context(prev_strict)
    result
  } catch {
    err => {
      set_strict_context(prev_strict)
      raise err
    }
  }
}

///|
fn[T] with_source_context(chars : Array[Char], f : () -> T raise) -> T raise {
  let prev_chars = source_chars()
  set_source_chars(chars)
  try {
    let result = f()
    set_source_chars(prev_chars)
    result
  } catch {
    err => {
      set_source_chars(prev_chars)
      raise err
    }
  }
}

///|
fn[T] with_in_context(allow_in : Bool, f : () -> T raise) -> T raise {
  let prev_in = in_expr_allowed()
  set_in_expr_allowed(allow_in)
  try {
    let result = f()
    set_in_expr_allowed(prev_in)
    result
  } catch {
    err => {
      set_in_expr_allowed(prev_in)
      raise err
    }
  }
}

///|
fn token_end_offset(tokens : Array[Token], index : Int) -> Int {
  let tok = peek(tokens, index)
  tok.offset + tok.lexeme.length()
}

///|
pub fn parse(source : String) -> Script raise {
  parse_with_mode(source, false, false)
}

///|
pub fn parse_with_mode(
  source : String,
  force_module : Bool,
  force_strict : Bool,
) -> Script raise {
  let tokens = tokenize_with_options(source, !force_module)
  let (script, _) = parse_script_with_mode(
    tokens, 0, source, force_module, force_strict,
  )
  script
}

///|
pub fn parse_with_mode_private(
  source : String,
  force_module : Bool,
  force_strict : Bool,
  private_names : Array[String],
) -> Script raise {
  let tokens = tokenize_with_options(source, !force_module)
  let names = private_name_map_from_list(private_names)
  let (script, _) = parse_script_with_mode_private(
    tokens,
    0,
    source,
    force_module,
    force_strict,
    Some(names),
  )
  script
}

///|
pub fn script_contains_arguments(script : Script) -> Bool {
  stmts_contain_arguments(script.body)
}

///|
pub fn contains_module_syntax(source : String) -> Bool {
  let tokens = tokenize_with_options(source, true)
  has_module_syntax(tokens)
}

///|
fn parse_script_with_mode(
  tokens : Array[Token],
  index : Int,
  source : String,
  force_module : Bool,
  force_strict : Bool,
) -> (Script, Int) raise {
  parse_script_with_mode_private(
    tokens,
    index,
    source,
    force_module,
    force_strict,
    None,
  )
}

///|
fn private_name_map_from_list(
  private_names : Array[String],
) -> Map[String, (Bool, Int)] {
  let names : Map[String, (Bool, Int)] = Map::new()
  for name in private_names {
    names.set(name, (false, 0))
  }
  names
}

///|
fn parse_script_with_mode_private(
  tokens : Array[Token],
  index : Int,
  source : String,
  force_module : Bool,
  force_strict : Bool,
  private_names : Map[String, (Bool, Int)]?,
) -> (Script, Int) raise {
  let is_module = force_module
  let chars = source.iter().to_array()
  with_source_context(chars, fn() raise {
    with_module_context(is_module, fn() raise {
      let parse_body = fn(strict_for_parse : Bool) raise {
        with_strict_context(strict_for_parse, fn() raise {
          let body : Array[Stmt] = []
          let mut i = index
          while !is_eof(tokens, i) {
            let (stmt, next_i) = parse_statement(tokens, i, true, is_module)
            body.push(stmt)
            i = next_i
          }
          (body, i)
        })
      }
      let mut strict_for_parse = force_strict || is_module
      let (stmts0, i0) = parse_body(strict_for_parse)
      let mut stmts = stmts0
      let mut i = i0
      let mut use_strict = has_use_strict(stmts)
      if use_strict && !strict_for_parse {
        strict_for_parse = true
        let (strict_stmts, strict_i) = parse_body(true)
        stmts = strict_stmts
        i = strict_i
        use_strict = has_use_strict(stmts)
      }
      if !is_module {
        match find_import_meta_in_stmts(stmts) {
          Some(offset) =>
            parse_fail_offset(offset, "import.meta only valid in module code")
          None => ()
        }
      }
      validate_private_names_in_stmts(stmts, private_names)
      let strict_here = use_strict || is_module || force_strict
      if strict_here && stmts_contain_with(stmts) {
        parse_fail(tokens, index, "with not allowed in strict mode")
      }
      validate_strict_assignments_in_stmts(stmts, strict_here)
      with_strict_context(strict_here, fn() raise {
        validate_block_scope_early_errors_in_stmts(stmts, is_module)
      })
      (
        Script::{
          body: stmts,
          is_strict: use_strict || force_strict,
          is_module,
          source,
        },
        i,
      )
    })
  })
}

///|
fn find_import_meta_in_stmts(stmts : Array[Stmt]) -> Int? {
  for stmt in stmts {
    match find_import_meta_in_stmt(stmt) {
      Some(offset) => return Some(offset)
      None => ()
    }
  }
  None
}

///|
let private_kind_field = 1

///|
let private_kind_method = 2

///|
let private_kind_getter = 4

///|
let private_kind_setter = 8

///|
fn validate_private_names_in_stmts(
  stmts : Array[Stmt],
  names : Map[String, (Bool, Int)]?,
) -> Unit raise {
  for stmt in stmts {
    validate_private_names_in_stmt(stmt, names)
  }
}

///|
fn validate_private_names_in_stmt(
  stmt : Stmt,
  names : Map[String, (Bool, Int)]?,
) -> Unit raise {
  match stmt {
    Empty => ()
    ExprStmt(expr) => validate_private_names_in_expr(expr, names)
    VarDecl(_, decls) =>
      for decl in decls {
        let (binding, init) = decl
        validate_private_names_in_binding(binding, names)
        validate_private_names_in_expr_opt(init, names)
      }
    Block(stmts) => validate_private_names_in_stmts(stmts, names)
    Label(_, inner) => validate_private_names_in_stmt(inner, names)
    FunctionDecl(func) => validate_private_names_in_function(func, names)
    ClassDecl(class_def) => validate_private_names_in_class(class_def, names)
    Return(expr_opt) => validate_private_names_in_expr_opt(expr_opt, names)
    If(cond_expr, conseq, alt) => {
      validate_private_names_in_expr(cond_expr, names)
      validate_private_names_in_stmt(conseq, names)
      match alt {
        Some(stmt) => validate_private_names_in_stmt(stmt, names)
        None => ()
      }
    }
    With(expr, body) => {
      validate_private_names_in_expr(expr, names)
      validate_private_names_in_stmt(body, names)
    }
    While(expr, body) => {
      validate_private_names_in_expr(expr, names)
      validate_private_names_in_stmt(body, names)
    }
    For(init, test_expr, update, body) => {
      validate_private_names_in_for_init(init, names)
      validate_private_names_in_expr_opt(test_expr, names)
      validate_private_names_in_expr_opt(update, names)
      validate_private_names_in_stmt(body, names)
    }
    ForIn(init, expr, body) => {
      validate_private_names_in_for_init(init, names)
      validate_private_names_in_expr(expr, names)
      validate_private_names_in_stmt(body, names)
    }
    ForOf(init, expr, body) => {
      validate_private_names_in_for_init(init, names)
      validate_private_names_in_expr(expr, names)
      validate_private_names_in_stmt(body, names)
    }
    ForAwaitOf(init, expr, body) => {
      validate_private_names_in_for_init(init, names)
      validate_private_names_in_expr(expr, names)
      validate_private_names_in_stmt(body, names)
    }
    DoWhile(body, expr) => {
      validate_private_names_in_stmt(body, names)
      validate_private_names_in_expr(expr, names)
    }
    Switch(expr, cases) => {
      validate_private_names_in_expr(expr, names)
      for case in cases {
        match case {
          SwitchCase::Case(cond, body) => {
            validate_private_names_in_expr(cond, names)
            validate_private_names_in_stmts(body, names)
          }
          SwitchCase::Default(body) =>
            validate_private_names_in_stmts(body, names)
        }
      }
    }
    Throw(expr) => validate_private_names_in_expr(expr, names)
    Try(body, catch_clause, finally_clause) => {
      validate_private_names_in_stmt(body, names)
      match catch_clause {
        Some(clause) => validate_private_names_in_stmt(clause.body, names)
        None => ()
      }
      match finally_clause {
        Some(body_stmt) => validate_private_names_in_stmt(body_stmt, names)
        None => ()
      }
    }
    Break(_) => ()
    Continue(_) => ()
  }
}

///|
fn validate_private_names_in_for_init(
  init : ForInit,
  names : Map[String, (Bool, Int)]?,
) -> Unit raise {
  match init {
    ForInit::None => ()
    ForInit::Expr(expr) => validate_private_names_in_expr(expr, names)
    ForInit::Var(_, decls) =>
      for decl in decls {
        let (binding, init_expr) = decl
        validate_private_names_in_binding(binding, names)
        validate_private_names_in_expr_opt(init_expr, names)
      }
  }
}

///|
fn validate_private_names_in_binding(
  binding : VarBinding,
  names : Map[String, (Bool, Int)]?,
) -> Unit raise {
  match binding {
    VarBinding::Name(_) => ()
    VarBinding::ArrayPattern(pattern) =>
      validate_private_names_in_array_pattern(pattern, names)
    VarBinding::ObjectPattern(pattern) =>
      validate_private_names_in_object_pattern(pattern, names)
    VarBinding::Target(expr) => validate_private_names_in_expr(expr, names)
  }
}

///|
fn validate_private_names_in_array_pattern(
  pattern : Array[ArrayPatternElem],
  names : Map[String, (Bool, Int)]?,
) -> Unit raise {
  for elem in pattern {
    match elem {
      ArrayPatternElem::Bind(binding, init) => {
        validate_private_names_in_binding(binding, names)
        validate_private_names_in_expr_opt(init, names)
      }
      ArrayPatternElem::Rest(binding) =>
        validate_private_names_in_binding(binding, names)
      ArrayPatternElem::Hole => ()
    }
  }
}

///|
fn validate_private_names_in_object_pattern(
  pattern : Array[ObjectPatternElem],
  names : Map[String, (Bool, Int)]?,
) -> Unit raise {
  for elem in pattern {
    match elem {
      ObjectPatternElem::Bind(key, binding, init) => {
        validate_private_names_in_obj_key(key, names)
        validate_private_names_in_binding(binding, names)
        validate_private_names_in_expr_opt(init, names)
      }
      ObjectPatternElem::Rest(binding) =>
        validate_private_names_in_binding(binding, names)
    }
  }
}

///|
fn validate_private_names_in_expr_opt(
  expr_opt : Expr?,
  names : Map[String, (Bool, Int)]?,
) -> Unit raise {
  match expr_opt {
    Some(expr) => validate_private_names_in_expr(expr, names)
    None => ()
  }
}

///|
fn validate_private_names_in_expr_list(
  exprs : Array[Expr],
  names : Map[String, (Bool, Int)]?,
) -> Unit raise {
  for expr in exprs {
    validate_private_names_in_expr(expr, names)
  }
}

///|
fn validate_private_names_in_expr(
  expr : Expr,
  names : Map[String, (Bool, Int)]?,
) -> Unit raise {
  match expr {
    TemplateLiteral(_, exprs) =>
      validate_private_names_in_expr_list(exprs, names)
    TaggedTemplate(callee, _, exprs) => {
      validate_private_names_in_expr(callee, names)
      validate_private_names_in_expr_list(exprs, names)
    }
    Await(inner, _) => validate_private_names_in_expr(inner, names)
    Unary(_, inner, _) => validate_private_names_in_expr(inner, names)
    Update(_, inner, _, _) => validate_private_names_in_expr(inner, names)
    Yield(expr_opt, _) => validate_private_names_in_expr_opt(expr_opt, names)
    NewTarget(_) => ()
    New(callee, args) => {
      validate_private_names_in_expr(callee, names)
      validate_private_names_in_expr_list(args, names)
    }
    Binary(_, left, right, _) => {
      validate_private_names_in_expr(left, names)
      validate_private_names_in_expr(right, names)
    }
    PrivateIn(name, rhs, offset) => {
      validate_private_name_ref(name, offset, names)
      validate_private_names_in_expr(rhs, names)
    }
    Conditional(cond_expr, conseq, alt) => {
      validate_private_names_in_expr(cond_expr, names)
      validate_private_names_in_expr(conseq, names)
      validate_private_names_in_expr(alt, names)
    }
    Sequence(exprs) => validate_private_names_in_expr_list(exprs, names)
    Paren(inner) => validate_private_names_in_expr(inner, names)
    Assign(_, rhs, _, _) => validate_private_names_in_expr(rhs, names)
    AssignInvalid(lhs, rhs, _) => {
      validate_private_names_in_expr(lhs, names)
      validate_private_names_in_expr(rhs, names)
    }
    AssignArray(pattern, rhs) => {
      validate_private_names_in_array_pattern(pattern, names)
      validate_private_names_in_expr(rhs, names)
    }
    AssignObject(pattern, rhs) => {
      validate_private_names_in_object_pattern(pattern, names)
      validate_private_names_in_expr(rhs, names)
    }
    AssignOp(_, lhs, rhs, _) => {
      validate_private_names_in_expr(lhs, names)
      validate_private_names_in_expr(rhs, names)
    }
    AssignMember(obj, key, rhs, offset) => {
      validate_private_names_in_expr(obj, names)
      validate_private_names_in_member_key(key, offset, names)
      validate_private_names_in_expr(rhs, names)
    }
    Call(callee, args, _) => {
      validate_private_names_in_expr(callee, names)
      validate_private_names_in_expr_list(args, names)
    }
    Member(obj, key, offset) => {
      validate_private_names_in_expr(obj, names)
      validate_private_names_in_member_key(key, offset, names)
    }
    OptionalChain(base, segments) => {
      validate_private_names_in_expr(base, names)
      validate_private_names_in_chain_segments(segments, names)
    }
    FunctionExpr(func) => validate_private_names_in_function(func, names)
    ClassExpr(class_def) => validate_private_names_in_class(class_def, names)
    ObjectLiteral(props) => validate_private_names_in_object_props(props, names)
    ArrayLiteral(elements) =>
      for elem in elements {
        match elem {
          Some(inner) => validate_private_names_in_expr(inner, names)
          None => ()
        }
      }
    Spread(inner) => validate_private_names_in_expr(inner, names)
    Number(_, _, _)
    | Bool(_)
    | Null
    | Expr::String(_, _, _)
    | Regexp(_, _, _)
    | BigInt(_)
    | Ident(_, _)
    | This
    | Super
    | ImportMeta(_) => ()
  }
}

///|
fn validate_private_names_in_member_key(
  key : MemberKey,
  offset : Int,
  names : Map[String, (Bool, Int)]?,
) -> Unit raise {
  match key {
    MemberKey::Ident(_) => ()
    MemberKey::Private(name) => validate_private_name_ref(name, offset, names)
    MemberKey::Computed(expr) => validate_private_names_in_expr(expr, names)
  }
}

///|
fn validate_private_names_in_chain_segments(
  segments : Array[ChainSegment],
  names : Map[String, (Bool, Int)]?,
) -> Unit raise {
  for segment in segments {
    match segment {
      ChainSegment::Member(key, _, offset) =>
        validate_private_names_in_member_key(key, offset, names)
      ChainSegment::Call(args, _, _) =>
        validate_private_names_in_expr_list(args, names)
    }
  }
}

///|
fn validate_private_names_in_object_props(
  props : Array[ObjectProp],
  names : Map[String, (Bool, Int)]?,
) -> Unit raise {
  for prop in props {
    match prop {
      ObjectProp::KeyValue(key, value) => {
        validate_private_names_in_obj_key(key, names)
        validate_private_names_in_expr(value, names)
      }
      ObjectProp::Method(key, func) => {
        validate_private_names_in_obj_key(key, names)
        validate_private_names_in_function(func, names)
      }
      ObjectProp::Getter(key, func) => {
        validate_private_names_in_obj_key(key, names)
        validate_private_names_in_function(func, names)
      }
      ObjectProp::Setter(key, func) => {
        validate_private_names_in_obj_key(key, names)
        validate_private_names_in_function(func, names)
      }
      ObjectProp::Spread(expr) => validate_private_names_in_expr(expr, names)
      ObjectProp::Shorthand(_) => ()
    }
  }
}

///|
fn validate_private_names_in_obj_key(
  key : ObjKey,
  names : Map[String, (Bool, Int)]?,
) -> Unit raise {
  match key {
    ObjKey::Computed(expr) => validate_private_names_in_expr(expr, names)
    _ => ()
  }
}

///|
fn validate_private_names_in_function(
  func : FunctionDef,
  names : Map[String, (Bool, Int)]?,
) -> Unit raise {
  for expr in func.param_inits {
    validate_private_names_in_expr_opt(expr, names)
  }
  validate_private_names_in_stmts(func.body, names)
}

///|
fn merge_private_names(
  outer_names : Map[String, (Bool, Int)]?,
  current_names : Map[String, (Bool, Int)],
) -> Map[String, (Bool, Int)] {
  let merged : Map[String, (Bool, Int)] = Map::new()
  match outer_names {
    Some(names) =>
      for name, value in names {
        merged.set(name, value)
      }
    None => ()
  }
  for name, value in current_names {
    merged.set(name, value)
  }
  merged
}

///|
fn validate_private_names_in_class(
  class_def : ClassDef,
  outer_names : Map[String, (Bool, Int)]?,
) -> Unit raise {
  let private_names = collect_private_names(class_def)
  let available_names = merge_private_names(outer_names, private_names)
  match class_def.super_class {
    Some(expr) => validate_private_names_in_expr(expr, outer_names)
    None => ()
  }
  for class_member in class_def.body {
    match class_member {
      ClassMember::Method(key, func, _) => {
        validate_private_names_in_obj_key(key, Some(available_names))
        validate_private_names_in_function(func, Some(available_names))
      }
      ClassMember::Getter(key, func, _) => {
        validate_private_names_in_obj_key(key, Some(available_names))
        validate_private_names_in_function(func, Some(available_names))
      }
      ClassMember::Setter(key, func, _) => {
        validate_private_names_in_obj_key(key, Some(available_names))
        validate_private_names_in_function(func, Some(available_names))
      }
      ClassMember::Field(key, init, _) => {
        validate_private_names_in_obj_key(key, Some(available_names))
        validate_private_names_in_expr_opt(init, Some(available_names))
      }
      ClassMember::StaticBlock(stmts) =>
        validate_private_names_in_stmts(stmts, Some(available_names))
    }
  }
}

///|
fn collect_private_names(
  class_def : ClassDef,
) -> Map[String, (Bool, Int)] raise {
  let names : Map[String, (Bool, Int)] = Map::new()
  for class_member in class_def.body {
    match class_member {
      ClassMember::Method(ObjKey::Private(name), _, is_static) =>
        register_private_name(
          names,
          name,
          is_static,
          private_kind_method,
          class_def.offset,
        )
      ClassMember::Getter(ObjKey::Private(name), _, is_static) =>
        register_private_name(
          names,
          name,
          is_static,
          private_kind_getter,
          class_def.offset,
        )
      ClassMember::Setter(ObjKey::Private(name), _, is_static) =>
        register_private_name(
          names,
          name,
          is_static,
          private_kind_setter,
          class_def.offset,
        )
      ClassMember::Field(ObjKey::Private(name), _, is_static) =>
        register_private_name(
          names,
          name,
          is_static,
          private_kind_field,
          class_def.offset,
        )
      _ => ()
    }
  }
  names
}

///|
fn register_private_name(
  names : Map[String, (Bool, Int)],
  name : String,
  is_static : Bool,
  kind : Int,
  offset : Int,
) -> Unit raise {
  let has_field = Int::land(kind, private_kind_field) != 0
  let has_method = Int::land(kind, private_kind_method) != 0
  let has_getter = Int::land(kind, private_kind_getter) != 0
  let has_setter = Int::land(kind, private_kind_setter) != 0
  match names.get(name) {
    Some((existing_static, flags)) => {
      if existing_static != is_static {
        parse_fail_offset(offset, "private class field is already defined")
      }
      let conflict = if has_field || has_method {
        flags != 0
      } else if has_getter {
        Int::land(
          flags,
          private_kind_field | private_kind_method | private_kind_getter,
        ) !=
        0
      } else if has_setter {
        Int::land(
          flags,
          private_kind_field | private_kind_method | private_kind_setter,
        ) !=
        0
      } else {
        false
      }
      if conflict {
        parse_fail_offset(offset, "private class field is already defined")
      }
      names.set(name, (existing_static, Int::lor(flags, kind)))
    }
    None => names.set(name, (is_static, kind))
  }
}

///|
fn validate_private_name_ref(
  name : String,
  offset : Int,
  names : Map[String, (Bool, Int)]?,
) -> Unit raise {
  match names {
    Some(map) =>
      match map.get(name) {
        Some(_) => ()
        None => parse_fail_offset(offset, "invalid private reference")
      }
    None => parse_fail_offset(offset, "invalid private reference")
  }
}

///|
fn validate_strict_binding_names(
  binding : VarBinding,
  is_strict : Bool,
  offset : Int,
) -> Unit raise {
  if !is_strict {
    return
  }
  let names : Array[String] = []
  collect_binding_names(binding, names)
  for name in names {
    if is_strict_reserved_name(name) {
      parse_fail_offset(offset, "invalid binding name in strict code")
    }
  }
}

///|
fn validate_strict_assignments_in_expr_opt(
  expr_opt : Expr?,
  is_strict : Bool,
) -> Unit raise {
  match expr_opt {
    Some(expr) => validate_strict_assignments_in_expr(expr, is_strict)
    None => ()
  }
}

///|
fn validate_strict_assignments_in_expr_list(
  exprs : Array[Expr],
  is_strict : Bool,
) -> Unit raise {
  for expr in exprs {
    validate_strict_assignments_in_expr(expr, is_strict)
  }
}

///|
fn validate_strict_assignments_in_obj_key(
  key : ObjKey,
  is_strict : Bool,
) -> Unit raise {
  match key {
    ObjKey::Computed(expr) =>
      validate_strict_assignments_in_expr(expr, is_strict)
    _ => ()
  }
}

///|
fn validate_strict_assignments_in_member_key(
  key : MemberKey,
  is_strict : Bool,
) -> Unit raise {
  match key {
    MemberKey::Computed(expr) =>
      validate_strict_assignments_in_expr(expr, is_strict)
    _ => ()
  }
}

///|
fn validate_strict_assignments_in_binding_pattern(
  binding : VarBinding,
  is_strict : Bool,
) -> Unit raise {
  match binding {
    VarBinding::Name(_) => ()
    VarBinding::ArrayPattern(pattern) =>
      validate_strict_assignments_in_array_pattern(pattern, is_strict)
    VarBinding::ObjectPattern(pattern) =>
      validate_strict_assignments_in_object_pattern(pattern, is_strict)
    VarBinding::Target(expr) =>
      validate_strict_assignments_in_expr(expr, is_strict)
  }
}

///|
fn validate_strict_assignments_in_array_pattern(
  pattern : Array[ArrayPatternElem],
  is_strict : Bool,
) -> Unit raise {
  for elem in pattern {
    match elem {
      ArrayPatternElem::Bind(binding, init) => {
        validate_strict_binding_names(binding, is_strict, 0)
        validate_strict_assignments_in_expr_opt(init, is_strict)
        validate_strict_assignments_in_binding_pattern(binding, is_strict)
      }
      ArrayPatternElem::Rest(binding) => {
        validate_strict_binding_names(binding, is_strict, 0)
        validate_strict_assignments_in_binding_pattern(binding, is_strict)
      }
      ArrayPatternElem::Hole => ()
    }
  }
}

///|
fn validate_strict_assignments_in_object_pattern(
  pattern : Array[ObjectPatternElem],
  is_strict : Bool,
) -> Unit raise {
  for elem in pattern {
    match elem {
      ObjectPatternElem::Bind(key, binding, init) => {
        validate_strict_assignments_in_obj_key(key, is_strict)
        validate_strict_binding_names(binding, is_strict, 0)
        validate_strict_assignments_in_expr_opt(init, is_strict)
        validate_strict_assignments_in_binding_pattern(binding, is_strict)
      }
      ObjectPatternElem::Rest(binding) => {
        validate_strict_binding_names(binding, is_strict, 0)
        validate_strict_assignments_in_binding_pattern(binding, is_strict)
      }
    }
  }
}

///|
fn validate_strict_assignments_in_function(
  func : FunctionDef,
  is_strict : Bool,
  is_method : Bool,
) -> Unit raise {
  let strict_here = is_strict || func.is_strict
  if strict_here && !is_method {
    match func.name {
      Some(func_name) =>
        if is_strict_reserved_name(func_name) {
          parse_fail_offset(func.offset, "invalid function name in strict code")
        }
      None => ()
    }
  }
  if strict_here {
    let names : Array[String] = []
    for param in func.params {
      collect_binding_names(param.binding, names)
    }
    for param_name in names {
      if is_strict_reserved_name(param_name) {
        parse_fail_offset(func.offset, "invalid argument name in strict code")
      }
    }
    let simple = is_simple_parameter_list(func.params, func.param_inits)
    if strict_here || !simple || func.is_arrow || is_method {
      if params_have_duplicates(names) {
        parse_fail_offset(
          func.offset,
          "duplicate argument names not allowed in this context",
        )
      }
    }
  }
  for init in func.param_inits {
    validate_strict_assignments_in_expr_opt(init, strict_here)
  }
  validate_strict_assignments_in_stmts(func.body, strict_here)
}

///|
fn validate_strict_assignments_in_class(
  class_def : ClassDef,
  is_strict : Bool,
) -> Unit raise {
  let class_strict = is_strict || true
  match class_def.super_class {
    Some(expr) => validate_strict_assignments_in_expr(expr, class_strict)
    None => ()
  }
  for class_member in class_def.body {
    match class_member {
      ClassMember::Method(key, func, _) => {
        validate_strict_assignments_in_obj_key(key, class_strict)
        validate_strict_assignments_in_function(func, class_strict, true)
      }
      ClassMember::Getter(key, func, _) => {
        validate_strict_assignments_in_obj_key(key, class_strict)
        validate_strict_assignments_in_function(func, class_strict, true)
      }
      ClassMember::Setter(key, func, _) => {
        validate_strict_assignments_in_obj_key(key, class_strict)
        validate_strict_assignments_in_function(func, class_strict, true)
      }
      ClassMember::Field(key, init, _) => {
        validate_strict_assignments_in_obj_key(key, class_strict)
        validate_strict_assignments_in_expr_opt(init, class_strict)
      }
      ClassMember::StaticBlock(stmts) =>
        validate_strict_assignments_in_stmts(stmts, class_strict)
    }
  }
}

///|
fn is_delete_identifier_reference(expr : Expr) -> Bool {
  match expr {
    Ident(_, _) => true
    Paren(inner) => is_delete_identifier_reference(inner)
    _ => false
  }
}

///|
fn unwrap_paren_expr(expr : Expr) -> Expr {
  match expr {
    Paren(inner) => unwrap_paren_expr(inner)
    _ => expr
  }
}

///|
fn validate_strict_assignments_in_expr(
  expr : Expr,
  is_strict : Bool,
) -> Unit raise {
  match expr {
    String(_, _, has_legacy_escape) =>
      if is_strict && has_legacy_escape {
        parse_fail_offset(0, "invalid escape sequence in strict code")
      }
    TemplateLiteral(_, exprs) =>
      validate_strict_assignments_in_expr_list(exprs, is_strict)
    TaggedTemplate(callee, _, exprs) => {
      validate_strict_assignments_in_expr(callee, is_strict)
      validate_strict_assignments_in_expr_list(exprs, is_strict)
    }
    Await(inner, _) => validate_strict_assignments_in_expr(inner, is_strict)
    Unary(op, inner, offset) => {
      if is_strict {
        if op == UnaryOp::Delete && is_delete_identifier_reference(inner) {
          parse_fail_offset(offset, "invalid delete in strict code")
        }
      }
      validate_strict_assignments_in_expr(inner, is_strict)
    }
    Update(_, inner, _, _) => {
      if is_strict {
        match unwrap_paren_expr(inner) {
          Ident(name, ident_offset) =>
            if is_strict_reserved_name(name) {
              parse_fail_offset(
                ident_offset, "invalid assignment in strict code",
              )
            }
          _ => ()
        }
      }
      validate_strict_assignments_in_expr(inner, is_strict)
    }
    Yield(expr_opt, _) =>
      validate_strict_assignments_in_expr_opt(expr_opt, is_strict)
    NewTarget(_) => ()
    New(callee, args) => {
      validate_strict_assignments_in_expr(callee, is_strict)
      validate_strict_assignments_in_expr_list(args, is_strict)
    }
    Binary(_, left, right, _) => {
      validate_strict_assignments_in_expr(left, is_strict)
      validate_strict_assignments_in_expr(right, is_strict)
    }
    PrivateIn(_, rhs, _) => validate_strict_assignments_in_expr(rhs, is_strict)
    Conditional(cond_expr, conseq, alt) => {
      validate_strict_assignments_in_expr(cond_expr, is_strict)
      validate_strict_assignments_in_expr(conseq, is_strict)
      validate_strict_assignments_in_expr(alt, is_strict)
    }
    Sequence(exprs) =>
      validate_strict_assignments_in_expr_list(exprs, is_strict)
    Paren(inner) => validate_strict_assignments_in_expr(inner, is_strict)
    Assign(name, rhs, offset, _) => {
      if is_strict && is_strict_reserved_name(name) {
        parse_fail_offset(offset, "invalid assignment in strict code")
      }
      validate_strict_assignments_in_expr(rhs, is_strict)
    }
    AssignInvalid(lhs, rhs, _) => {
      validate_strict_assignments_in_expr(lhs, is_strict)
      validate_strict_assignments_in_expr(rhs, is_strict)
    }
    AssignArray(pattern, rhs) => {
      validate_strict_binding_names(
        VarBinding::ArrayPattern(pattern),
        is_strict,
        0,
      )
      validate_strict_assignments_in_array_pattern(pattern, is_strict)
      validate_strict_assignments_in_expr(rhs, is_strict)
    }
    AssignObject(pattern, rhs) => {
      validate_strict_binding_names(
        VarBinding::ObjectPattern(pattern),
        is_strict,
        0,
      )
      validate_strict_assignments_in_object_pattern(pattern, is_strict)
      validate_strict_assignments_in_expr(rhs, is_strict)
    }
    AssignOp(_, lhs, rhs, _) => {
      if is_strict {
        match unwrap_paren_expr(lhs) {
          Ident(name, ident_offset) =>
            if is_strict_reserved_name(name) {
              parse_fail_offset(
                ident_offset, "invalid assignment in strict code",
              )
            }
          _ => ()
        }
      }
      validate_strict_assignments_in_expr(lhs, is_strict)
      validate_strict_assignments_in_expr(rhs, is_strict)
    }
    AssignMember(obj, key, rhs, _) => {
      validate_strict_assignments_in_expr(obj, is_strict)
      validate_strict_assignments_in_member_key(key, is_strict)
      validate_strict_assignments_in_expr(rhs, is_strict)
    }
    Call(callee, args, _) => {
      validate_strict_assignments_in_expr(callee, is_strict)
      validate_strict_assignments_in_expr_list(args, is_strict)
    }
    Member(obj, key, _) => {
      validate_strict_assignments_in_expr(obj, is_strict)
      validate_strict_assignments_in_member_key(key, is_strict)
    }
    OptionalChain(base, segments) => {
      validate_strict_assignments_in_expr(base, is_strict)
      for segment in segments {
        match segment {
          ChainSegment::Member(key, _, _) =>
            validate_strict_assignments_in_member_key(key, is_strict)
          ChainSegment::Call(args, _, _) =>
            validate_strict_assignments_in_expr_list(args, is_strict)
        }
      }
    }
    FunctionExpr(func) =>
      validate_strict_assignments_in_function(func, is_strict, false)
    ClassExpr(class_def) =>
      validate_strict_assignments_in_class(class_def, is_strict)
    ObjectLiteral(props) =>
      for prop in props {
        match prop {
          ObjectProp::KeyValue(key, value) => {
            validate_strict_assignments_in_obj_key(key, is_strict)
            validate_strict_assignments_in_expr(value, is_strict)
          }
          ObjectProp::Method(key, func) => {
            validate_strict_assignments_in_obj_key(key, is_strict)
            validate_strict_assignments_in_function(func, is_strict, true)
          }
          ObjectProp::Getter(key, func) => {
            validate_strict_assignments_in_obj_key(key, is_strict)
            validate_strict_assignments_in_function(func, is_strict, true)
          }
          ObjectProp::Setter(key, func) => {
            validate_strict_assignments_in_obj_key(key, is_strict)
            validate_strict_assignments_in_function(func, is_strict, true)
          }
          ObjectProp::Spread(inner) =>
            validate_strict_assignments_in_expr(inner, is_strict)
          ObjectProp::Shorthand(name) =>
            if is_strict && is_strict_future_keyword(name) {
              parse_fail_offset(0, "invalid identifier in strict code")
            }
        }
      }
    ArrayLiteral(elements) =>
      for elem in elements {
        match elem {
          Some(inner) => validate_strict_assignments_in_expr(inner, is_strict)
          None => ()
        }
      }
    Spread(inner) => validate_strict_assignments_in_expr(inner, is_strict)
    Ident(name, offset) =>
      if is_strict && is_strict_future_keyword(name) {
        parse_fail_offset(offset, "invalid identifier in strict code")
      }
    Number(_, legacy_octal, offset) =>
      if is_strict && legacy_octal {
        parse_fail_offset(offset, "invalid number in strict code")
      }
    Bool(_)
    | Null
    | Regexp(_, _, _)
    | BigInt(_)
    | This
    | Super
    | ImportMeta(_) => ()
  }
}

///|
fn validate_strict_assignments_in_for_init(
  init : ForInit,
  is_strict : Bool,
) -> Unit raise {
  match init {
    ForInit::None => ()
    ForInit::Expr(expr) => validate_strict_assignments_in_expr(expr, is_strict)
    ForInit::Var(_, decls) =>
      for decl in decls {
        let (binding, init_expr) = decl
        validate_strict_binding_names(binding, is_strict, 0)
        validate_strict_assignments_in_expr_opt(init_expr, is_strict)
      }
  }
}

///|
fn binding_is_destructuring(binding : VarBinding) -> Bool {
  match binding {
    VarBinding::Name(_) => false
    VarBinding::Target(_) => false
    _ => true
  }
}

///|
fn validate_for_in_of_initializer(
  init : ForInit,
  is_strict : Bool,
  is_for_of : Bool,
) -> Unit raise {
  match init {
    ForInit::Var(kind, decls) => {
      let mut has_initializer = false
      let mut has_destructuring = false
      for decl in decls {
        let (binding, init_expr) = decl
        if init_expr is Some(_) {
          has_initializer = true
        }
        if binding_is_destructuring(binding) {
          has_destructuring = true
        }
      }
      if has_initializer {
        if is_for_of {
          parse_fail_offset(
            0, "a declaration in the head of a for-of loop can't have an initializer",
          )
        } else if kind != VarKind::Var || is_strict || has_destructuring {
          parse_fail_offset(
            0, "a declaration in the head of a for-in loop can't have an initializer",
          )
        }
      }
    }
    _ => ()
  }
}

///|
fn validate_for_in_of_binding_count(init : ForInit) -> Unit raise {
  match init {
    ForInit::Var(_, decls) =>
      if decls.length() != 1 {
        parse_fail_offset(0, "for-in/of declaration must have a single binding")
      }
    _ => ()
  }
}

///|
fn stmt_is_function_decl(stmt : Stmt) -> Bool {
  match stmt {
    FunctionDecl(_) => true
    Label(_, inner) => stmt_is_function_decl(inner)
    _ => false
  }
}

///|
fn validate_strict_assignments_in_stmt(
  stmt : Stmt,
  is_strict : Bool,
) -> Unit raise {
  match stmt {
    Empty => ()
    ExprStmt(expr) => validate_strict_assignments_in_expr(expr, is_strict)
    VarDecl(_, decls) =>
      for decl in decls {
        let (binding, init) = decl
        validate_strict_binding_names(binding, is_strict, 0)
        validate_strict_assignments_in_expr_opt(init, is_strict)
      }
    Block(stmts) => validate_strict_assignments_in_stmts(stmts, is_strict)
    Label(name, inner) => {
      if is_strict && is_strict_future_keyword(name) {
        parse_fail_offset(0, "invalid label in strict code")
      }
      validate_strict_assignments_in_stmt(inner, is_strict)
    }
    FunctionDecl(func) =>
      validate_strict_assignments_in_function(func, is_strict, false)
    ClassDecl(class_def) =>
      validate_strict_assignments_in_class(class_def, is_strict)
    Return(expr_opt) =>
      validate_strict_assignments_in_expr_opt(expr_opt, is_strict)
    If(cond_expr, conseq, alt) => {
      if is_strict && stmt_is_function_decl(conseq) {
        parse_fail_offset(
          0, "function declaration not allowed in statement position",
        )
      }
      validate_strict_assignments_in_expr(cond_expr, is_strict)
      validate_strict_assignments_in_stmt(conseq, is_strict)
      match alt {
        Some(stmt) => {
          if is_strict && stmt_is_function_decl(stmt) {
            parse_fail_offset(
              0, "function declaration not allowed in statement position",
            )
          }
          validate_strict_assignments_in_stmt(stmt, is_strict)
        }
        None => ()
      }
    }
    With(expr, body) => {
      validate_strict_assignments_in_expr(expr, is_strict)
      validate_strict_assignments_in_stmt(body, is_strict)
    }
    While(expr, body) => {
      if stmt_is_function_decl(body) {
        parse_fail_offset(
          0, "function declaration not allowed in statement position",
        )
      }
      validate_strict_assignments_in_expr(expr, is_strict)
      validate_strict_assignments_in_stmt(body, is_strict)
    }
    For(init, test_expr, update_expr, body) => {
      if stmt_is_function_decl(body) {
        parse_fail_offset(
          0, "function declaration not allowed in statement position",
        )
      }
      validate_strict_assignments_in_for_init(init, is_strict)
      validate_strict_assignments_in_expr_opt(test_expr, is_strict)
      validate_strict_assignments_in_expr_opt(update_expr, is_strict)
      validate_strict_assignments_in_stmt(body, is_strict)
    }
    ForIn(init, expr, body) => {
      if stmt_is_function_decl(body) {
        parse_fail_offset(
          0, "function declaration not allowed in statement position",
        )
      }
      validate_for_in_of_binding_count(init)
      validate_for_in_of_initializer(init, is_strict, false)
      validate_strict_assignments_in_for_init(init, is_strict)
      validate_strict_assignments_in_expr(expr, is_strict)
      validate_strict_assignments_in_stmt(body, is_strict)
    }
    ForOf(init, expr, body) => {
      if stmt_is_function_decl(body) {
        parse_fail_offset(
          0, "function declaration not allowed in statement position",
        )
      }
      validate_for_in_of_binding_count(init)
      validate_for_in_of_initializer(init, is_strict, true)
      validate_strict_assignments_in_for_init(init, is_strict)
      validate_strict_assignments_in_expr(expr, is_strict)
      validate_strict_assignments_in_stmt(body, is_strict)
    }
    ForAwaitOf(init, expr, body) => {
      if stmt_is_function_decl(body) {
        parse_fail_offset(
          0, "function declaration not allowed in statement position",
        )
      }
      validate_for_in_of_binding_count(init)
      validate_for_in_of_initializer(init, is_strict, true)
      validate_strict_assignments_in_for_init(init, is_strict)
      validate_strict_assignments_in_expr(expr, is_strict)
      validate_strict_assignments_in_stmt(body, is_strict)
    }
    DoWhile(body, expr) => {
      if stmt_is_function_decl(body) {
        parse_fail_offset(
          0, "function declaration not allowed in statement position",
        )
      }
      validate_strict_assignments_in_stmt(body, is_strict)
      validate_strict_assignments_in_expr(expr, is_strict)
    }
    Switch(expr, cases) => {
      validate_strict_assignments_in_expr(expr, is_strict)
      for case_item in cases {
        match case_item {
          SwitchCase::Case(case_expr, case_stmts) => {
            validate_strict_assignments_in_expr(case_expr, is_strict)
            validate_strict_assignments_in_stmts(case_stmts, is_strict)
          }
          SwitchCase::Default(case_stmts) =>
            validate_strict_assignments_in_stmts(case_stmts, is_strict)
        }
      }
    }
    Throw(expr) => validate_strict_assignments_in_expr(expr, is_strict)
    Try(body, catch_clause, finally_clause) => {
      validate_strict_assignments_in_stmt(body, is_strict)
      match catch_clause {
        Some(clause) => {
          match clause.name {
            Some(name) =>
              if is_strict && is_strict_reserved_name(name) {
                parse_fail_offset(0, "invalid binding name in strict code")
              }
            None => ()
          }
          validate_strict_assignments_in_stmt(clause.body, is_strict)
        }
        None => ()
      }
      match finally_clause {
        Some(stmt) => validate_strict_assignments_in_stmt(stmt, is_strict)
        None => ()
      }
    }
    Break(_) | Continue(_) => ()
  }
}

///|
fn validate_strict_assignments_in_stmts(
  stmts : Array[Stmt],
  is_strict : Bool,
) -> Unit raise {
  for stmt in stmts {
    validate_strict_assignments_in_stmt(stmt, is_strict)
  }
}

///|
fn validate_block_scope_early_errors_in_stmts(
  stmts : Array[Stmt],
  in_block : Bool,
) -> Unit raise {
  check_block_redeclarations(stmts, in_block)
  for stmt in stmts {
    validate_block_scope_early_errors_in_stmt(stmt)
  }
}

///|
fn check_for_head_lex_var_conflicts(init : ForInit, body : Stmt) -> Unit raise {
  match init {
    ForInit::Var(kind, decls) =>
      if kind != VarKind::Var {
        let bound_names : Array[String] = []
        for decl in decls {
          let (binding, _) = decl
          collect_binding_names(binding, bound_names)
        }
        if !bound_names.is_empty() {
          let var_names : Array[String] = []
          collect_block_var_names(body, var_names, false)
          let bound_seen : Map[String, Bool] = Map::new()
          for name in bound_names {
            bound_seen.set(name, true)
          }
          for name in var_names {
            if bound_seen.contains(name) {
              parse_fail_offset(0, "lexical declaration conflicts with var")
            }
          }
        }
      }
    _ => ()
  }
}

///|
fn validate_block_scope_early_errors_in_stmt(stmt : Stmt) -> Unit raise {
  match stmt {
    Empty => ()
    ExprStmt(expr) => validate_block_scope_early_errors_in_expr(expr)
    VarDecl(_, decls) =>
      for decl in decls {
        let (_, init) = decl
        validate_block_scope_early_errors_in_expr_opt(init)
      }
    Block(stmts) => validate_block_scope_early_errors_in_stmts(stmts, true)
    Label(_, inner) => validate_block_scope_early_errors_in_stmt(inner)
    FunctionDecl(func) => validate_block_scope_early_errors_in_function(func)
    ClassDecl(class_def) =>
      validate_block_scope_early_errors_in_class(class_def)
    Return(expr_opt) => validate_block_scope_early_errors_in_expr_opt(expr_opt)
    If(cond_expr, conseq, alt) => {
      validate_block_scope_early_errors_in_expr(cond_expr)
      validate_block_scope_early_errors_in_stmt(conseq)
      match alt {
        Some(stmt) => validate_block_scope_early_errors_in_stmt(stmt)
        None => ()
      }
    }
    With(expr, body) => {
      validate_block_scope_early_errors_in_expr(expr)
      validate_block_scope_early_errors_in_stmt(body)
    }
    While(expr, body) => {
      validate_block_scope_early_errors_in_expr(expr)
      validate_block_scope_early_errors_in_stmt(body)
    }
    For(init, test_expr, update_expr, body) => {
      validate_block_scope_early_errors_in_for_init(init)
      validate_block_scope_early_errors_in_expr_opt(test_expr)
      validate_block_scope_early_errors_in_expr_opt(update_expr)
      validate_block_scope_early_errors_in_stmt(body)
    }
    ForIn(init, expr, body) => {
      validate_block_scope_early_errors_in_for_init(init)
      validate_block_scope_early_errors_in_expr(expr)
      validate_block_scope_early_errors_in_stmt(body)
      check_for_head_lex_var_conflicts(init, body)
    }
    ForOf(init, expr, body) => {
      validate_block_scope_early_errors_in_for_init(init)
      validate_block_scope_early_errors_in_expr(expr)
      validate_block_scope_early_errors_in_stmt(body)
      check_for_head_lex_var_conflicts(init, body)
    }
    ForAwaitOf(init, expr, body) => {
      validate_block_scope_early_errors_in_for_init(init)
      validate_block_scope_early_errors_in_expr(expr)
      validate_block_scope_early_errors_in_stmt(body)
      check_for_head_lex_var_conflicts(init, body)
    }
    DoWhile(body, expr) => {
      validate_block_scope_early_errors_in_stmt(body)
      validate_block_scope_early_errors_in_expr(expr)
    }
    Switch(expr, cases) => {
      validate_block_scope_early_errors_in_expr(expr)
      let all_stmts : Array[Stmt] = []
      for clause in cases {
        let body = match clause {
          SwitchCase::Case(_, case_stmts) => case_stmts
          SwitchCase::Default(case_stmts) => case_stmts
        }
        for stmt in body {
          all_stmts.push(stmt)
        }
      }
      check_block_redeclarations(all_stmts, true)
      for clause in cases {
        let body = match clause {
          SwitchCase::Case(_, case_stmts) => case_stmts
          SwitchCase::Default(case_stmts) => case_stmts
        }
        for stmt in body {
          validate_block_scope_early_errors_in_stmt(stmt)
        }
      }
    }
    Throw(expr) => validate_block_scope_early_errors_in_expr(expr)
    Try(body, catch_clause, finally_clause) => {
      validate_block_scope_early_errors_in_stmt(body)
      match catch_clause {
        Some(clause) => validate_block_scope_early_errors_in_stmt(clause.body)
        None => ()
      }
      match finally_clause {
        Some(stmt) => validate_block_scope_early_errors_in_stmt(stmt)
        None => ()
      }
    }
    Break(_) | Continue(_) => ()
  }
}

///|
fn validate_block_scope_early_errors_in_for_init(init : ForInit) -> Unit raise {
  match init {
    ForInit::None => ()
    ForInit::Expr(expr) => validate_block_scope_early_errors_in_expr(expr)
    ForInit::Var(_, decls) =>
      for decl in decls {
        let (_, init_expr) = decl
        validate_block_scope_early_errors_in_expr_opt(init_expr)
      }
  }
}

///|
fn validate_block_scope_early_errors_in_function(
  func : FunctionDef,
) -> Unit raise {
  for init in func.param_inits {
    validate_block_scope_early_errors_in_expr_opt(init)
  }
  with_strict_context(func.is_strict, fn() raise {
    validate_block_scope_early_errors_in_stmts(func.body, false)
  })
}

///|
fn validate_block_scope_early_errors_in_class(
  class_def : ClassDef,
) -> Unit raise {
  for class_member in class_def.body {
    match class_member {
      ClassMember::Method(key, func, _) => {
        validate_block_scope_early_errors_in_obj_key(key)
        validate_block_scope_early_errors_in_function(func)
      }
      ClassMember::Getter(key, func, _) => {
        validate_block_scope_early_errors_in_obj_key(key)
        validate_block_scope_early_errors_in_function(func)
      }
      ClassMember::Setter(key, func, _) => {
        validate_block_scope_early_errors_in_obj_key(key)
        validate_block_scope_early_errors_in_function(func)
      }
      ClassMember::Field(key, init, _) => {
        validate_block_scope_early_errors_in_obj_key(key)
        validate_block_scope_early_errors_in_expr_opt(init)
      }
      ClassMember::StaticBlock(stmts) =>
        with_strict_context(true, fn() raise {
          validate_block_scope_early_errors_in_stmts(stmts, true)
        })
    }
  }
}

///|
fn validate_block_scope_early_errors_in_expr_opt(
  expr_opt : Expr?,
) -> Unit raise {
  match expr_opt {
    Some(expr) => validate_block_scope_early_errors_in_expr(expr)
    None => ()
  }
}

///|
fn validate_block_scope_early_errors_in_expr_list(
  exprs : Array[Expr],
) -> Unit raise {
  for expr in exprs {
    validate_block_scope_early_errors_in_expr(expr)
  }
}

///|
fn validate_block_scope_early_errors_in_expr(expr : Expr) -> Unit raise {
  match expr {
    TemplateLiteral(_, exprs) =>
      validate_block_scope_early_errors_in_expr_list(exprs)
    TaggedTemplate(callee, _, exprs) => {
      validate_block_scope_early_errors_in_expr(callee)
      validate_block_scope_early_errors_in_expr_list(exprs)
    }
    Await(inner, _) => validate_block_scope_early_errors_in_expr(inner)
    Unary(_, inner, _) => validate_block_scope_early_errors_in_expr(inner)
    Update(_, inner, _, _) => validate_block_scope_early_errors_in_expr(inner)
    Yield(expr_opt, _) =>
      validate_block_scope_early_errors_in_expr_opt(expr_opt)
    NewTarget(_) => ()
    New(callee, args) => {
      validate_block_scope_early_errors_in_expr(callee)
      validate_block_scope_early_errors_in_expr_list(args)
    }
    Binary(_, left, right, _) => {
      validate_block_scope_early_errors_in_expr(left)
      validate_block_scope_early_errors_in_expr(right)
    }
    PrivateIn(_, rhs, _) => validate_block_scope_early_errors_in_expr(rhs)
    Conditional(cond_expr, conseq, alt) => {
      validate_block_scope_early_errors_in_expr(cond_expr)
      validate_block_scope_early_errors_in_expr(conseq)
      validate_block_scope_early_errors_in_expr(alt)
    }
    Sequence(exprs) => validate_block_scope_early_errors_in_expr_list(exprs)
    Paren(inner) => validate_block_scope_early_errors_in_expr(inner)
    Assign(_, rhs, _, _) => validate_block_scope_early_errors_in_expr(rhs)
    AssignInvalid(lhs, rhs, _) => {
      validate_block_scope_early_errors_in_expr(lhs)
      validate_block_scope_early_errors_in_expr(rhs)
    }
    AssignArray(pattern, rhs) => {
      validate_block_scope_early_errors_in_array_pattern(pattern)
      validate_block_scope_early_errors_in_expr(rhs)
    }
    AssignObject(pattern, rhs) => {
      validate_block_scope_early_errors_in_object_pattern(pattern)
      validate_block_scope_early_errors_in_expr(rhs)
    }
    AssignOp(_, lhs, rhs, _) => {
      validate_block_scope_early_errors_in_expr(lhs)
      validate_block_scope_early_errors_in_expr(rhs)
    }
    AssignMember(obj, key, rhs, _) => {
      validate_block_scope_early_errors_in_expr(obj)
      validate_block_scope_early_errors_in_member_key(key)
      validate_block_scope_early_errors_in_expr(rhs)
    }
    Call(callee, args, _) => {
      validate_block_scope_early_errors_in_expr(callee)
      validate_block_scope_early_errors_in_expr_list(args)
    }
    Member(obj, key, _) => {
      validate_block_scope_early_errors_in_expr(obj)
      validate_block_scope_early_errors_in_member_key(key)
    }
    OptionalChain(base, segments) => {
      validate_block_scope_early_errors_in_expr(base)
      validate_block_scope_early_errors_in_chain_segments(segments)
    }
    FunctionExpr(func) => validate_block_scope_early_errors_in_function(func)
    ClassExpr(class_def) =>
      validate_block_scope_early_errors_in_class(class_def)
    ObjectLiteral(props) =>
      validate_block_scope_early_errors_in_object_props(props)
    ArrayLiteral(elements) =>
      for elem in elements {
        match elem {
          Some(inner) => validate_block_scope_early_errors_in_expr(inner)
          None => ()
        }
      }
    Spread(inner) => validate_block_scope_early_errors_in_expr(inner)
    Number(_, _, _)
    | Bool(_)
    | Null
    | String(_, _, _)
    | Regexp(_, _, _)
    | BigInt(_)
    | Ident(_, _)
    | This
    | Super
    | ImportMeta(_) => ()
  }
}

///|
fn validate_block_scope_early_errors_in_chain_segments(
  segments : Array[ChainSegment],
) -> Unit raise {
  for segment in segments {
    match segment {
      ChainSegment::Member(key, _, _) =>
        validate_block_scope_early_errors_in_member_key(key)
      ChainSegment::Call(args, _, _) =>
        validate_block_scope_early_errors_in_expr_list(args)
    }
  }
}

///|
fn validate_block_scope_early_errors_in_member_key(
  key : MemberKey,
) -> Unit raise {
  match key {
    MemberKey::Computed(expr) => validate_block_scope_early_errors_in_expr(expr)
    _ => ()
  }
}

///|
fn validate_block_scope_early_errors_in_object_props(
  props : Array[ObjectProp],
) -> Unit raise {
  for prop in props {
    match prop {
      ObjectProp::KeyValue(key, value) => {
        validate_block_scope_early_errors_in_obj_key(key)
        validate_block_scope_early_errors_in_expr(value)
      }
      ObjectProp::Method(key, func) => {
        validate_block_scope_early_errors_in_obj_key(key)
        validate_block_scope_early_errors_in_function(func)
      }
      ObjectProp::Getter(key, func) => {
        validate_block_scope_early_errors_in_obj_key(key)
        validate_block_scope_early_errors_in_function(func)
      }
      ObjectProp::Setter(key, func) => {
        validate_block_scope_early_errors_in_obj_key(key)
        validate_block_scope_early_errors_in_function(func)
      }
      ObjectProp::Spread(expr) =>
        validate_block_scope_early_errors_in_expr(expr)
      ObjectProp::Shorthand(_) => ()
    }
  }
}

///|
fn validate_block_scope_early_errors_in_obj_key(key : ObjKey) -> Unit raise {
  match key {
    ObjKey::Computed(expr) => validate_block_scope_early_errors_in_expr(expr)
    _ => ()
  }
}

///|
fn validate_block_scope_early_errors_in_array_pattern(
  pattern : Array[ArrayPatternElem],
) -> Unit raise {
  for elem in pattern {
    match elem {
      ArrayPatternElem::Bind(_, init) =>
        validate_block_scope_early_errors_in_expr_opt(init)
      ArrayPatternElem::Rest(_) => ()
      ArrayPatternElem::Hole => ()
    }
  }
}

///|
fn validate_block_scope_early_errors_in_object_pattern(
  pattern : Array[ObjectPatternElem],
) -> Unit raise {
  for elem in pattern {
    match elem {
      ObjectPatternElem::Bind(_, _, init) =>
        validate_block_scope_early_errors_in_expr_opt(init)
      ObjectPatternElem::Rest(_) => ()
    }
  }
}

///|
fn check_block_redeclarations(
  stmts : Array[Stmt],
  in_block : Bool,
) -> Unit raise {
  let is_strict = strict_context()
  let lex_decl_kinds : Map[String, LexDeclKind] = Map::new()
  for stmt in stmts {
    let lex_decls : Array[(String, LexDeclKind)] = []
    collect_block_lex_decl_kinds(stmt, lex_decls, in_block)
    for entry in lex_decls {
      let (name, kind) = entry
      match lex_decl_kinds.get(name) {
        Some(prev_kind) =>
          if !(kind == LexDeclKind::PlainFunc &&
            prev_kind == LexDeclKind::PlainFunc &&
            !is_strict) {
            parse_fail_offset(0, "duplicate lexical declaration")
          }
        None => lex_decl_kinds.set(name, kind)
      }
    }
  }
  let var_seen : Map[String, Bool] = Map::new()
  for stmt in stmts {
    let var_names : Array[String] = []
    collect_block_var_names(stmt, var_names, false)
    for name in var_names {
      var_seen.set(name, true)
    }
  }
  if !in_block {
    let scope_lex : Map[String, Bool] = Map::new()
    for name, _ in lex_decl_kinds {
      scope_lex.set(name, true)
    }
    let func_var_seen : Map[String, Bool] = Map::new()
    for stmt in stmts {
      collect_annex_b_var_func_names_in_stmt(
        stmt, func_var_seen, scope_lex, in_block, is_strict,
      )
    }
    for name, _ in func_var_seen {
      var_seen.set(name, true)
    }
  }
  for name, _ in lex_decl_kinds {
    if var_seen.contains(name) {
      parse_fail_offset(0, "lexical declaration conflicts with var")
    }
  }
}

///|
fn expr_is_export_call(expr : Expr) -> Bool {
  match expr {
    Call(callee, _, _) =>
      match callee {
        Ident(name, _) => name == "__export"
        _ => false
      }
    Sequence(exprs) => {
      for inner in exprs {
        if expr_is_export_call(inner) {
          return true
        }
      }
      false
    }
    Paren(inner) => expr_is_export_call(inner)
    _ => false
  }
}

///|
fn block_is_export_wrapper(body : Array[Stmt]) -> Bool {
  if body.length() == 1 {
    return match body[0] {
      ExprStmt(expr) => expr_is_export_call(expr)
      _ => false
    }
  }
  if body.length() != 2 {
    return false
  }
  let first_ok = match body[0] {
    FunctionDecl(_) | ClassDecl(_) | VarDecl(_, _) => true
    _ => false
  }
  if !first_ok {
    return false
  }
  match body[1] {
    ExprStmt(expr) => expr_is_export_call(expr)
    _ => false
  }
}

///|
enum LexDeclKind {
  NonFunc
  PlainFunc
  NonPlainFunc
} derive(Show, Eq)

///|
fn collect_binding_names_with_kind(
  binding : VarBinding,
  kind : LexDeclKind,
  out : Array[(String, LexDeclKind)],
) -> Unit {
  let names : Array[String] = []
  collect_binding_names(binding, names)
  for name in names {
    out.push((name, kind))
  }
}

///|
fn collect_block_lex_decl_kinds(
  stmt : Stmt,
  out : Array[(String, LexDeclKind)],
  include_functions : Bool,
) -> Unit {
  match stmt {
    VarDecl(kind, decls) =>
      if kind != VarKind::Var {
        for decl in decls {
          let (binding, _) = decl
          collect_binding_names_with_kind(binding, LexDeclKind::NonFunc, out)
        }
      }
    FunctionDecl(func) =>
      if include_functions {
        match func.name {
          Some(name) => {
            let kind = if func.is_async || func.is_generator {
              LexDeclKind::NonPlainFunc
            } else {
              LexDeclKind::PlainFunc
            }
            out.push((name, kind))
          }
          None => ()
        }
      }
    ClassDecl(class_def) =>
      match class_def.name {
        Some(name) => out.push((name, LexDeclKind::NonFunc))
        None => ()
      }
    Block(stmts) =>
      if block_is_export_wrapper(stmts) {
        for inner in stmts {
          collect_block_lex_decl_kinds(inner, out, include_functions)
        }
      }
    Label(_, inner) =>
      collect_block_lex_decl_kinds(inner, out, include_functions)
    _ => ()
  }
}

///|
fn collect_block_lex_names(
  stmt : Stmt,
  out : Array[String],
  include_functions : Bool,
) -> Unit {
  match stmt {
    VarDecl(kind, decls) =>
      if kind != VarKind::Var {
        for decl in decls {
          let (binding, _) = decl
          collect_binding_names(binding, out)
        }
      }
    FunctionDecl(func) =>
      if include_functions {
        match func.name {
          Some(name) => out.push(name)
          None => ()
        }
      }
    ClassDecl(class_def) =>
      match class_def.name {
        Some(name) => out.push(name)
        None => ()
      }
    Block(stmts) =>
      if block_is_export_wrapper(stmts) {
        for inner in stmts {
          collect_block_lex_names(inner, out, include_functions)
        }
      }
    Label(_, inner) => collect_block_lex_names(inner, out, include_functions)
    _ => ()
  }
}

///|
fn collect_block_var_names(
  stmt : Stmt,
  out : Array[String],
  include_functions : Bool,
) -> Unit {
  match stmt {
    VarDecl(kind, decls) =>
      if kind is VarKind::Var {
        for decl in decls {
          let (binding, _) = decl
          collect_binding_names(binding, out)
        }
      }
    FunctionDecl(func) =>
      if include_functions {
        match func.name {
          Some(name) => out.push(name)
          None => ()
        }
      }
    Block(stmts) =>
      for inner in stmts {
        collect_block_var_names(inner, out, false)
      }
    Label(_, inner) => collect_block_var_names(inner, out, include_functions)
    If(_, conseq, alt) => {
      collect_block_var_names(conseq, out, include_functions)
      match alt {
        Some(stmt) => collect_block_var_names(stmt, out, include_functions)
        None => ()
      }
    }
    With(_, body) => collect_block_var_names(body, out, include_functions)
    While(_, body) => collect_block_var_names(body, out, include_functions)
    For(init, _, _, body) => {
      match init {
        ForInit::Var(kind, decls) =>
          if kind is VarKind::Var {
            for decl in decls {
              let (binding, _) = decl
              collect_binding_names(binding, out)
            }
          }
        _ => ()
      }
      collect_block_var_names(body, out, include_functions)
    }
    ForIn(init, _, body) => {
      match init {
        ForInit::Var(kind, decls) =>
          if kind is VarKind::Var {
            for decl in decls {
              let (binding, _) = decl
              collect_binding_names(binding, out)
            }
          }
        _ => ()
      }
      collect_block_var_names(body, out, include_functions)
    }
    ForOf(init, _, body) => {
      match init {
        ForInit::Var(kind, decls) =>
          if kind is VarKind::Var {
            for decl in decls {
              let (binding, _) = decl
              collect_binding_names(binding, out)
            }
          }
        _ => ()
      }
      collect_block_var_names(body, out, include_functions)
    }
    ForAwaitOf(init, _, body) => {
      match init {
        ForInit::Var(kind, decls) =>
          if kind is VarKind::Var {
            for decl in decls {
              let (binding, _) = decl
              collect_binding_names(binding, out)
            }
          }
        _ => ()
      }
      collect_block_var_names(body, out, include_functions)
    }
    DoWhile(body, _) => collect_block_var_names(body, out, include_functions)
    Switch(_, cases) =>
      for clause in cases {
        let body = match clause {
          SwitchCase::Case(_, case_stmts) => case_stmts
          SwitchCase::Default(case_stmts) => case_stmts
        }
        for inner in body {
          collect_block_var_names(inner, out, include_functions)
        }
      }
    Try(body, catch_clause, finally_clause) => {
      collect_block_var_names(body, out, include_functions)
      match catch_clause {
        Some(clause) =>
          collect_block_var_names(clause.body, out, include_functions)
        None => ()
      }
      match finally_clause {
        Some(stmt) => collect_block_var_names(stmt, out, include_functions)
        None => ()
      }
    }
    ClassDecl(_)
    | Return(_)
    | Throw(_)
    | ExprStmt(_)
    | Break(_)
    | Continue(_)
    | Empty => ()
  }
}

///|
fn collect_annex_b_var_func_names_in_stmt(
  stmt : Stmt,
  out : Map[String, Bool],
  scope_lex : Map[String, Bool],
  in_block : Bool,
  is_strict : Bool,
) -> Unit {
  match stmt {
    FunctionDecl(func) =>
      match func.name {
        Some(name) =>
          if !in_block {
            out.set(name, true)
          } else if !is_strict && !func.is_async && !func.is_generator {
            if !scope_lex.contains(name) {
              out.set(name, true)
            }
          }
        None => ()
      }
    Block(stmts) =>
      for inner in stmts {
        collect_annex_b_var_func_names_in_stmt(
          inner, out, scope_lex, true, is_strict,
        )
      }
    Label(_, inner) =>
      collect_annex_b_var_func_names_in_stmt(
        inner, out, scope_lex, true, is_strict,
      )
    With(_, body) =>
      collect_annex_b_var_func_names_in_stmt(
        body, out, scope_lex, true, is_strict,
      )
    If(_, conseq, alt) => {
      collect_annex_b_var_func_names_in_stmt(
        conseq, out, scope_lex, true, is_strict,
      )
      match alt {
        Some(inner) =>
          collect_annex_b_var_func_names_in_stmt(
            inner, out, scope_lex, true, is_strict,
          )
        None => ()
      }
    }
    While(_, body) =>
      collect_annex_b_var_func_names_in_stmt(
        body, out, scope_lex, true, is_strict,
      )
    For(_, _, _, body) =>
      collect_annex_b_var_func_names_in_stmt(
        body, out, scope_lex, true, is_strict,
      )
    ForIn(_, _, body) =>
      collect_annex_b_var_func_names_in_stmt(
        body, out, scope_lex, true, is_strict,
      )
    ForOf(_, _, body) =>
      collect_annex_b_var_func_names_in_stmt(
        body, out, scope_lex, true, is_strict,
      )
    ForAwaitOf(_, _, body) =>
      collect_annex_b_var_func_names_in_stmt(
        body, out, scope_lex, true, is_strict,
      )
    DoWhile(body, _) =>
      collect_annex_b_var_func_names_in_stmt(
        body, out, scope_lex, true, is_strict,
      )
    Switch(_, cases) =>
      for clause in cases {
        let body = match clause {
          SwitchCase::Case(_, stmts) => stmts
          SwitchCase::Default(stmts) => stmts
        }
        for inner in body {
          collect_annex_b_var_func_names_in_stmt(
            inner, out, scope_lex, true, is_strict,
          )
        }
      }
    Try(try_body, catch_clause, finally_body) => {
      collect_annex_b_var_func_names_in_stmt(
        try_body, out, scope_lex, true, is_strict,
      )
      match catch_clause {
        Some(clause) =>
          collect_annex_b_var_func_names_in_stmt(
            clause.body,
            out,
            scope_lex,
            true,
            is_strict,
          )
        None => ()
      }
      match finally_body {
        Some(inner) =>
          collect_annex_b_var_func_names_in_stmt(
            inner, out, scope_lex, true, is_strict,
          )
        None => ()
      }
    }
    _ => ()
  }
}

///|
fn find_import_meta_in_stmt(stmt : Stmt) -> Int? {
  match stmt {
    Empty => None
    ExprStmt(expr) => find_import_meta_in_expr(expr)
    VarDecl(_, decls) => {
      for decl in decls {
        let (binding, init) = decl
        match find_import_meta_in_binding(binding) {
          Some(offset) => return Some(offset)
          None => ()
        }
        match find_import_meta_in_expr_opt(init) {
          Some(offset) => return Some(offset)
          None => ()
        }
      }
      None
    }
    Block(stmts) => find_import_meta_in_stmts(stmts)
    Label(_, inner) => find_import_meta_in_stmt(inner)
    FunctionDecl(func) => find_import_meta_in_function(func)
    ClassDecl(class_def) => find_import_meta_in_class(class_def)
    Return(expr_opt) => find_import_meta_in_expr_opt(expr_opt)
    If(cond_expr, conseq, alt) => {
      match find_import_meta_in_expr(cond_expr) {
        Some(offset) => return Some(offset)
        None => ()
      }
      match find_import_meta_in_stmt(conseq) {
        Some(offset) => return Some(offset)
        None => ()
      }
      match alt {
        Some(stmt) => find_import_meta_in_stmt(stmt)
        None => None
      }
    }
    With(expr, body) =>
      match find_import_meta_in_expr(expr) {
        Some(offset) => Some(offset)
        None => find_import_meta_in_stmt(body)
      }
    While(expr, body) =>
      match find_import_meta_in_expr(expr) {
        Some(offset) => Some(offset)
        None => find_import_meta_in_stmt(body)
      }
    For(init, test_expr, update, body) => {
      match find_import_meta_in_for_init(init) {
        Some(offset) => return Some(offset)
        None => ()
      }
      match find_import_meta_in_expr_opt(test_expr) {
        Some(offset) => return Some(offset)
        None => ()
      }
      match find_import_meta_in_expr_opt(update) {
        Some(offset) => return Some(offset)
        None => ()
      }
      find_import_meta_in_stmt(body)
    }
    ForIn(init, expr, body) => {
      match find_import_meta_in_for_init(init) {
        Some(offset) => return Some(offset)
        None => ()
      }
      match find_import_meta_in_expr(expr) {
        Some(offset) => return Some(offset)
        None => ()
      }
      find_import_meta_in_stmt(body)
    }
    ForOf(init, expr, body) => {
      match find_import_meta_in_for_init(init) {
        Some(offset) => return Some(offset)
        None => ()
      }
      match find_import_meta_in_expr(expr) {
        Some(offset) => return Some(offset)
        None => ()
      }
      find_import_meta_in_stmt(body)
    }
    ForAwaitOf(init, expr, body) => {
      match find_import_meta_in_for_init(init) {
        Some(offset) => return Some(offset)
        None => ()
      }
      match find_import_meta_in_expr(expr) {
        Some(offset) => return Some(offset)
        None => ()
      }
      find_import_meta_in_stmt(body)
    }
    DoWhile(body, expr) =>
      match find_import_meta_in_stmt(body) {
        Some(offset) => Some(offset)
        None => find_import_meta_in_expr(expr)
      }
    Switch(expr, cases) => {
      match find_import_meta_in_expr(expr) {
        Some(offset) => return Some(offset)
        None => ()
      }
      for clause in cases {
        match clause {
          SwitchCase::Case(case_expr, body) => {
            match find_import_meta_in_expr(case_expr) {
              Some(offset) => return Some(offset)
              None => ()
            }
            match find_import_meta_in_stmts(body) {
              Some(offset) => return Some(offset)
              None => ()
            }
          }
          SwitchCase::Default(body) =>
            match find_import_meta_in_stmts(body) {
              Some(offset) => return Some(offset)
              None => ()
            }
        }
      }
      None
    }
    Throw(expr) => find_import_meta_in_expr(expr)
    Try(try_body, catch_clause, finally_body) => {
      match find_import_meta_in_stmt(try_body) {
        Some(offset) => return Some(offset)
        None => ()
      }
      match catch_clause {
        Some(clause) =>
          match find_import_meta_in_stmt(clause.body) {
            Some(offset) => return Some(offset)
            None => ()
          }
        None => ()
      }
      match finally_body {
        Some(stmt) => find_import_meta_in_stmt(stmt)
        None => None
      }
    }
    Break(_) => None
    Continue(_) => None
  }
}

///|
fn find_import_meta_in_for_init(init : ForInit) -> Int? {
  match init {
    ForInit::None => None
    ForInit::Expr(expr) => find_import_meta_in_expr(expr)
    ForInit::Var(_, decls) => {
      for decl in decls {
        let (binding, init_expr) = decl
        match find_import_meta_in_binding(binding) {
          Some(offset) => return Some(offset)
          None => ()
        }
        match find_import_meta_in_expr_opt(init_expr) {
          Some(offset) => return Some(offset)
          None => ()
        }
      }
      None
    }
  }
}

///|
fn find_import_meta_in_binding(binding : VarBinding) -> Int? {
  match binding {
    VarBinding::Name(_) => None
    VarBinding::ArrayPattern(pattern) =>
      find_import_meta_in_array_pattern(pattern)
    VarBinding::ObjectPattern(pattern) =>
      find_import_meta_in_object_pattern(pattern)
    VarBinding::Target(expr) => find_import_meta_in_expr(expr)
  }
}

///|
fn find_import_meta_in_array_pattern(pattern : Array[ArrayPatternElem]) -> Int? {
  for elem in pattern {
    match elem {
      ArrayPatternElem::Bind(binding, init) => {
        match find_import_meta_in_binding(binding) {
          Some(offset) => return Some(offset)
          None => ()
        }
        match find_import_meta_in_expr_opt(init) {
          Some(offset) => return Some(offset)
          None => ()
        }
      }
      ArrayPatternElem::Rest(binding) =>
        match find_import_meta_in_binding(binding) {
          Some(offset) => return Some(offset)
          None => ()
        }
      ArrayPatternElem::Hole => ()
    }
  }
  None
}

///|
fn find_import_meta_in_object_pattern(
  pattern : Array[ObjectPatternElem],
) -> Int? {
  for elem in pattern {
    match elem {
      ObjectPatternElem::Bind(_, binding, init) => {
        match find_import_meta_in_binding(binding) {
          Some(offset) => return Some(offset)
          None => ()
        }
        match find_import_meta_in_expr_opt(init) {
          Some(offset) => return Some(offset)
          None => ()
        }
      }
      ObjectPatternElem::Rest(binding) =>
        match find_import_meta_in_binding(binding) {
          Some(offset) => return Some(offset)
          None => ()
        }
    }
  }
  None
}

///|
fn find_import_meta_in_expr_opt(expr_opt : Expr?) -> Int? {
  match expr_opt {
    Some(expr) => find_import_meta_in_expr(expr)
    None => None
  }
}

///|
fn for_init_contains_private_in(init : ForInit) -> Bool {
  match init {
    ForInit::None => false
    ForInit::Expr(expr) => expr_contains_private_in(expr)
    ForInit::Var(_, decls) => {
      for decl in decls {
        let (binding, init_expr) = decl
        if binding_contains_private_in(binding) {
          return true
        }
        if expr_opt_contains_private_in(init_expr) {
          return true
        }
      }
      false
    }
  }
}

///|
fn binding_contains_private_in(binding : VarBinding) -> Bool {
  match binding {
    VarBinding::Name(_) => false
    VarBinding::ArrayPattern(pattern) =>
      array_pattern_contains_private_in(pattern)
    VarBinding::ObjectPattern(pattern) =>
      object_pattern_contains_private_in(pattern)
    VarBinding::Target(expr) => expr_contains_private_in(expr)
  }
}

///|
fn array_pattern_contains_private_in(pattern : Array[ArrayPatternElem]) -> Bool {
  for elem in pattern {
    match elem {
      ArrayPatternElem::Bind(binding, init) => {
        if binding_contains_private_in(binding) {
          return true
        }
        if expr_opt_contains_private_in(init) {
          return true
        }
      }
      ArrayPatternElem::Rest(binding) =>
        if binding_contains_private_in(binding) {
          return true
        }
      ArrayPatternElem::Hole => ()
    }
  }
  false
}

///|
fn object_pattern_contains_private_in(
  pattern : Array[ObjectPatternElem],
) -> Bool {
  for elem in pattern {
    match elem {
      ObjectPatternElem::Bind(_, binding, init) => {
        if binding_contains_private_in(binding) {
          return true
        }
        if expr_opt_contains_private_in(init) {
          return true
        }
      }
      ObjectPatternElem::Rest(binding) =>
        if binding_contains_private_in(binding) {
          return true
        }
    }
  }
  false
}

///|
fn expr_opt_contains_private_in(expr_opt : Expr?) -> Bool {
  match expr_opt {
    Some(expr) => expr_contains_private_in(expr)
    None => false
  }
}

///|
fn expr_contains_private_in(expr : Expr) -> Bool {
  match expr {
    PrivateIn(_, _, _) => true
    TemplateLiteral(_, exprs) => expr_list_contains_private_in(exprs)
    TaggedTemplate(callee, _, exprs) =>
      expr_contains_private_in(callee) || expr_list_contains_private_in(exprs)
    Await(inner, _) => expr_contains_private_in(inner)
    Unary(_, inner, _) => expr_contains_private_in(inner)
    Update(_, inner, _, _) => expr_contains_private_in(inner)
    Yield(expr_opt, _) => expr_opt_contains_private_in(expr_opt)
    NewTarget(_) => false
    New(callee, args) =>
      expr_contains_private_in(callee) || expr_list_contains_private_in(args)
    Binary(_, left, right, _) =>
      expr_contains_private_in(left) || expr_contains_private_in(right)
    Conditional(cond_expr, conseq, alt) =>
      expr_contains_private_in(cond_expr) ||
      expr_contains_private_in(conseq) ||
      expr_contains_private_in(alt)
    Sequence(exprs) => expr_list_contains_private_in(exprs)
    Paren(inner) => expr_contains_private_in(inner)
    Assign(_, rhs, _, _) => expr_contains_private_in(rhs)
    AssignInvalid(lhs, rhs, _) =>
      expr_contains_private_in(lhs) || expr_contains_private_in(rhs)
    AssignArray(pattern, rhs) =>
      array_pattern_contains_private_in(pattern) ||
      expr_contains_private_in(rhs)
    AssignObject(pattern, rhs) =>
      object_pattern_contains_private_in(pattern) ||
      expr_contains_private_in(rhs)
    AssignOp(_, lhs, rhs, _) =>
      expr_contains_private_in(lhs) || expr_contains_private_in(rhs)
    AssignMember(obj, key, rhs, _) =>
      expr_contains_private_in(obj) ||
      member_key_contains_private_in(key) ||
      expr_contains_private_in(rhs)
    Call(callee, args, _) =>
      expr_contains_private_in(callee) || expr_list_contains_private_in(args)
    Member(obj, key, _) =>
      expr_contains_private_in(obj) || member_key_contains_private_in(key)
    OptionalChain(base, segments) =>
      expr_contains_private_in(base) ||
      chain_segments_contains_private_in(segments)
    FunctionExpr(_) => false
    ClassExpr(class_def) => class_def_contains_await(class_def)
    ObjectLiteral(props) => object_props_contains_private_in(props)
    ArrayLiteral(elements) => {
      for elem in elements {
        match elem {
          Some(inner) => if expr_contains_private_in(inner) { return true }
          None => ()
        }
      }
      false
    }
    Spread(inner) => expr_contains_private_in(inner)
    Number(_, _, _)
    | Bool(_)
    | Null
    | String(_, _, _)
    | Regexp(_, _, _)
    | BigInt(_)
    | Ident(_, _)
    | This
    | Super
    | ImportMeta(_) => false
  }
}

///|
fn expr_list_contains_private_in(exprs : Array[Expr]) -> Bool {
  for expr in exprs {
    if expr_contains_private_in(expr) {
      return true
    }
  }
  false
}

///|
fn member_key_contains_private_in(key : MemberKey) -> Bool {
  match key {
    MemberKey::Ident(_) => false
    MemberKey::Private(_) => false
    MemberKey::Computed(expr) => expr_contains_private_in(expr)
  }
}

///|
fn obj_key_contains_private_in(key : ObjKey) -> Bool {
  match key {
    ObjKey::Ident(_) => false
    ObjKey::Private(_) => false
    ObjKey::String(_) => false
    ObjKey::Number(_) => false
    ObjKey::Computed(expr) => expr_contains_private_in(expr)
  }
}

///|
fn chain_segments_contains_private_in(segments : Array[ChainSegment]) -> Bool {
  for segment in segments {
    match segment {
      ChainSegment::Member(key, _, _) =>
        if member_key_contains_private_in(key) {
          return true
        }
      ChainSegment::Call(args, _, _) =>
        if expr_list_contains_private_in(args) {
          return true
        }
    }
  }
  false
}

///|
fn object_props_contains_private_in(props : Array[ObjectProp]) -> Bool {
  for prop in props {
    match prop {
      ObjectProp::KeyValue(key, value) =>
        if obj_key_contains_private_in(key) || expr_contains_private_in(value) {
          return true
        }
      ObjectProp::Method(key, _) =>
        if obj_key_contains_private_in(key) {
          return true
        }
      ObjectProp::Getter(key, _) =>
        if obj_key_contains_private_in(key) {
          return true
        }
      ObjectProp::Setter(key, _) =>
        if obj_key_contains_private_in(key) {
          return true
        }
      ObjectProp::Spread(expr) =>
        if expr_contains_private_in(expr) {
          return true
        }
      ObjectProp::Shorthand(_) => ()
    }
  }
  false
}

///|
fn find_import_meta_in_expr(expr : Expr) -> Int? {
  match expr {
    ImportMeta(offset) => Some(offset)
    TemplateLiteral(_, exprs) => find_import_meta_in_expr_list(exprs)
    TaggedTemplate(callee, _, exprs) =>
      match find_import_meta_in_expr(callee) {
        Some(offset) => Some(offset)
        None => find_import_meta_in_expr_list(exprs)
      }
    Await(inner, _) => find_import_meta_in_expr(inner)
    Unary(_, inner, _) => find_import_meta_in_expr(inner)
    Update(_, inner, _, _) => find_import_meta_in_expr(inner)
    Yield(expr_opt, _) => find_import_meta_in_expr_opt(expr_opt)
    NewTarget(_) => None
    New(callee, args) =>
      match find_import_meta_in_expr(callee) {
        Some(offset) => Some(offset)
        None => find_import_meta_in_expr_list(args)
      }
    Binary(_, left, right, _) =>
      match find_import_meta_in_expr(left) {
        Some(offset) => Some(offset)
        None => find_import_meta_in_expr(right)
      }
    PrivateIn(_, rhs, _) => find_import_meta_in_expr(rhs)
    Conditional(cond_expr, conseq, alt) =>
      match find_import_meta_in_expr(cond_expr) {
        Some(offset) => Some(offset)
        None =>
          match find_import_meta_in_expr(conseq) {
            Some(offset) => Some(offset)
            None => find_import_meta_in_expr(alt)
          }
      }
    Sequence(exprs) => find_import_meta_in_expr_list(exprs)
    Paren(inner) => find_import_meta_in_expr(inner)
    Assign(_, rhs, _, _) => find_import_meta_in_expr(rhs)
    AssignInvalid(lhs, rhs, _) =>
      match find_import_meta_in_expr(lhs) {
        Some(offset) => Some(offset)
        None => find_import_meta_in_expr(rhs)
      }
    AssignArray(pattern, rhs) =>
      match find_import_meta_in_array_pattern(pattern) {
        Some(offset) => Some(offset)
        None => find_import_meta_in_expr(rhs)
      }
    AssignObject(pattern, rhs) =>
      match find_import_meta_in_object_pattern(pattern) {
        Some(offset) => Some(offset)
        None => find_import_meta_in_expr(rhs)
      }
    AssignOp(_, lhs, rhs, _) =>
      match find_import_meta_in_expr(lhs) {
        Some(offset) => Some(offset)
        None => find_import_meta_in_expr(rhs)
      }
    AssignMember(obj, key, rhs, _) =>
      match find_import_meta_in_expr(obj) {
        Some(offset) => Some(offset)
        None =>
          match find_import_meta_in_member_key(key) {
            Some(offset) => Some(offset)
            None => find_import_meta_in_expr(rhs)
          }
      }
    Call(callee, args, _) =>
      match find_import_meta_in_expr(callee) {
        Some(offset) => Some(offset)
        None => find_import_meta_in_expr_list(args)
      }
    Member(obj, key, _) =>
      match find_import_meta_in_expr(obj) {
        Some(offset) => Some(offset)
        None => find_import_meta_in_member_key(key)
      }
    OptionalChain(base, segments) =>
      match find_import_meta_in_expr(base) {
        Some(offset) => Some(offset)
        None => find_import_meta_in_chain_segments(segments)
      }
    FunctionExpr(func) => find_import_meta_in_function(func)
    ClassExpr(class_def) => find_import_meta_in_class(class_def)
    ObjectLiteral(props) => find_import_meta_in_object_props(props)
    ArrayLiteral(elements) => {
      for elem in elements {
        match elem {
          Some(inner) =>
            match find_import_meta_in_expr(inner) {
              Some(offset) => return Some(offset)
              None => ()
            }
          None => ()
        }
      }
      None
    }
    Spread(inner) => find_import_meta_in_expr(inner)
    Number(_, _, _)
    | Bool(_)
    | Null
    | String(_, _, _)
    | Regexp(_, _, _)
    | BigInt(_)
    | Ident(_, _)
    | This
    | Super => None
  }
}

///|
fn find_import_meta_in_expr_list(exprs : Array[Expr]) -> Int? {
  for expr in exprs {
    match find_import_meta_in_expr(expr) {
      Some(offset) => return Some(offset)
      None => ()
    }
  }
  None
}

///|
fn find_import_meta_in_member_key(key : MemberKey) -> Int? {
  match key {
    MemberKey::Ident(_) => None
    MemberKey::Private(_) => None
    MemberKey::Computed(expr) => find_import_meta_in_expr(expr)
  }
}

///|
fn find_import_meta_in_chain_segments(segments : Array[ChainSegment]) -> Int? {
  for segment in segments {
    match segment {
      ChainSegment::Member(key, _, _) =>
        match find_import_meta_in_member_key(key) {
          Some(offset) => return Some(offset)
          None => ()
        }
      ChainSegment::Call(args, _, _) =>
        match find_import_meta_in_expr_list(args) {
          Some(offset) => return Some(offset)
          None => ()
        }
    }
  }
  None
}

///|
fn find_import_meta_in_object_props(props : Array[ObjectProp]) -> Int? {
  for prop in props {
    match prop {
      ObjectProp::KeyValue(_, value) =>
        match find_import_meta_in_expr(value) {
          Some(offset) => return Some(offset)
          None => ()
        }
      ObjectProp::Method(_, func) =>
        match find_import_meta_in_function(func) {
          Some(offset) => return Some(offset)
          None => ()
        }
      ObjectProp::Getter(_, func) =>
        match find_import_meta_in_function(func) {
          Some(offset) => return Some(offset)
          None => ()
        }
      ObjectProp::Setter(_, func) =>
        match find_import_meta_in_function(func) {
          Some(offset) => return Some(offset)
          None => ()
        }
      ObjectProp::Spread(expr) =>
        match find_import_meta_in_expr(expr) {
          Some(offset) => return Some(offset)
          None => ()
        }
      ObjectProp::Shorthand(_) => ()
    }
  }
  None
}

///|
fn find_import_meta_in_function(func : FunctionDef) -> Int? {
  for init in func.param_inits {
    match find_import_meta_in_expr_opt(init) {
      Some(offset) => return Some(offset)
      None => ()
    }
  }
  find_import_meta_in_stmts(func.body)
}

///|
fn find_import_meta_in_class(class_def : ClassDef) -> Int? {
  match class_def.super_class {
    Some(expr) =>
      match find_import_meta_in_expr(expr) {
        Some(offset) => return Some(offset)
        None => ()
      }
    None => ()
  }
  for class_member in class_def.body {
    match class_member {
      ClassMember::Method(_, func, _) =>
        match find_import_meta_in_function(func) {
          Some(offset) => return Some(offset)
          None => ()
        }
      ClassMember::Getter(_, func, _) =>
        match find_import_meta_in_function(func) {
          Some(offset) => return Some(offset)
          None => ()
        }
      ClassMember::Setter(_, func, _) =>
        match find_import_meta_in_function(func) {
          Some(offset) => return Some(offset)
          None => ()
        }
      ClassMember::Field(_, init, _) =>
        match find_import_meta_in_expr_opt(init) {
          Some(offset) => return Some(offset)
          None => ()
        }
      ClassMember::StaticBlock(stmts) =>
        match find_import_meta_in_stmts(stmts) {
          Some(offset) => return Some(offset)
          None => ()
        }
    }
  }
  None
}

///|
fn has_use_strict(stmts : Array[Stmt]) -> Bool {
  for stmt in stmts {
    match stmt {
      ExprStmt(String(value, has_escape, _)) =>
        if value == "use strict" && !has_escape {
          return true
        }
      ExprStmt(_) => break
      _ => break
    }
  }
  false
}

///|
fn string_literal_has_legacy_escape(raw : String) -> Bool {
  let chars = raw.iter().to_array()
  let length = chars.length()
  if length < 2 {
    return false
  }
  let end = length - 1
  let mut i = 1
  while i < end {
    let ch = chars[i]
    if ch != '\\' {
      i = i + 1
      continue
    }
    if i + 1 >= end {
      return false
    }
    let next = chars[i + 1]
    if next == '0' {
      if i + 2 < end && chars[i + 2].is_ascii_digit() {
        return true
      }
      i = i + 2
      continue
    }
    if next >= '1' && next <= '7' {
      return true
    }
    if next == '8' || next == '9' {
      return true
    }
    if next == 'x' {
      if i + 3 >= end {
        return false
      }
      i = i + 4
      continue
    }
    if next == 'u' {
      if i + 2 < end && chars[i + 2] == '{' {
        let mut j = i + 3
        while j < end && chars[j] != '}' {
          j = j + 1
        }
        if j < end {
          i = j + 1
        } else {
          return false
        }
      } else {
        if i + 5 >= end {
          return false
        }
        i = i + 6
      }
      continue
    }
    if next == '\r' {
      if i + 2 < end && chars[i + 2] == '\n' {
        i = i + 3
      } else {
        i = i + 2
      }
      continue
    }
    if next == '\n' || next == '\u2028' || next == '\u2029' {
      i = i + 2
      continue
    }
    i = i + 2
  }
  false
}

///|
fn is_unescaped_ident(
  tokens : Array[Token],
  index : Int,
  name : String,
) -> Bool {
  if !match_kind(tokens, index, Ident) {
    return false
  }
  let tok = peek(tokens, index)
  tok.lexeme == name && tok.raw_lexeme is None
}

///|
fn has_module_syntax(tokens : Array[Token]) -> Bool {
  let len = tokens.length()
  if len == 0 {
    return false
  }
  let tok = tokens[0]
  match tok.kind {
    KeywordExport => true
    KeywordImport =>
      if len > 1 {
        match tokens[1].kind {
          Punct('(') | Punct('.') => false
          _ => true
        }
      } else {
        true
      }
    _ => false
  }
}

///|
fn is_let_decl(tokens : Array[Token], index : Int, allow_decl : Bool) -> Bool {
  if !match_kind(tokens, index, KeywordLet) {
    return false
  }
  if match_punct(tokens, index + 1, '[') {
    return true
  }
  let next_tok = peek(tokens, index + 1)
  let is_binding_start = match next_tok.kind {
    Punct('{')
    | Ident
    | KeywordLet
    | KeywordYield
    | KeywordAwait
    | KeywordOf
    | KeywordStatic
    | KeywordImplements
    | KeywordInterface
    | KeywordPackage
    | KeywordPrivate
    | KeywordProtected
    | KeywordPublic => true
    _ => false
  }
  if !is_binding_start {
    return false
  }
  if allow_decl || !next_tok.line_break_before {
    return true
  }
  false
}

///|
fn parse_statement(
  tokens : Array[Token],
  index : Int,
  allow_decl : Bool,
  allow_module_decl : Bool,
) -> (Stmt, Int) raise {
  let tok = peek(tokens, index)
  if !allow_decl {
    match tok.kind {
      KeywordLet =>
        if is_let_decl(tokens, index, false) {
          parse_fail(tokens, index, "lexical declarations not allowed here")
        }
      KeywordConst | KeywordClass =>
        parse_fail(tokens, index, "lexical declarations not allowed here")
      _ => ()
    }
  }
  if !allow_decl && match_kind(tokens, index, KeywordFunction) {
    if strict_context() {
      parse_fail(
        tokens, index, "function declarations can't appear in single-statement context",
      )
    }
  }
  if !allow_module_decl {
    match tok.kind {
      KeywordExport => parse_fail(tokens, index, "export not allowed here")
      KeywordImport =>
        if !match_punct(tokens, index + 1, '(') &&
          !match_punct(tokens, index + 1, '.') {
          parse_fail(tokens, index, "import not allowed here")
        }
      _ => ()
    }
  }
  match tok.kind {
    KeywordVar => {
      let (stmt, next_i) = parse_var_decl(tokens, index)
      if next_i > 0 && match_punct(tokens, next_i - 1, ';') {
        return (stmt, next_i)
      }
      ensure_asi(tokens, next_i)
      (stmt, next_i)
    }
    KeywordLet =>
      if is_let_decl(tokens, index, allow_decl) {
        let (stmt, next_i) = parse_var_decl(tokens, index)
        if next_i > 0 && match_punct(tokens, next_i - 1, ';') {
          return (stmt, next_i)
        }
        ensure_asi(tokens, next_i)
        (stmt, next_i)
      } else if match_punct(tokens, index + 1, ':') {
        if strict_context() {
          parse_fail(tokens, index, "unexpected token")
        }
        let name = tok.lexeme
        let (stmt, next_i) = parse_statement(tokens, index + 2, false, false)
        if strict_context() && stmt_is_function_decl(stmt) {
          parse_fail(tokens, index, "labelled function declaration not allowed")
        }
        (Label(name, stmt), next_i)
      } else {
        let (expr, next_i) = parse_expression(tokens, index)
        let mut i = next_i
        if match_punct(tokens, i, ';') {
          i = i + 1
        } else {
          ensure_asi(tokens, i)
        }
        (ExprStmt(expr), i)
      }
    KeywordConst => {
      let (stmt, next_i) = parse_var_decl(tokens, index)
      if next_i > 0 && match_punct(tokens, next_i - 1, ';') {
        return (stmt, next_i)
      }
      ensure_asi(tokens, next_i)
      (stmt, next_i)
    }
    KeywordFunction => {
      if !allow_decl && match_punct(tokens, index + 1, '*') {
        parse_fail(tokens, index, "generator declaration not allowed here")
      }
      let (func, next_i) = parse_function(tokens, index, true, false, false)
      (FunctionDecl(func), next_i)
    }
    KeywordClass => {
      let (class_def, next_i) = parse_class(tokens, index, true)
      (ClassDecl(class_def), next_i)
    }
    KeywordIf => parse_if(tokens, index)
    KeywordWhile => parse_while(tokens, index)
    KeywordFor => parse_for(tokens, index)
    KeywordDo => parse_do_while(tokens, index)
    KeywordSwitch => parse_switch(tokens, index)
    KeywordThrow => parse_throw(tokens, index)
    KeywordTry => parse_try(tokens, index)
    KeywordWith => parse_with(tokens, index)
    KeywordBreak => parse_break(tokens, index)
    KeywordContinue => parse_continue(tokens, index)
    KeywordReturn => parse_return(tokens, index)
    KeywordStatic
    | KeywordImplements
    | KeywordInterface
    | KeywordPackage
    | KeywordPrivate
    | KeywordProtected
    | KeywordPublic =>
      if match_punct(tokens, index + 1, ':') {
        if strict_context() {
          parse_fail(tokens, index, "unexpected token")
        }
        let name = tok.lexeme
        let (stmt, next_i) = parse_statement(tokens, index + 2, false, false)
        if strict_context() && stmt_is_function_decl(stmt) {
          parse_fail(tokens, index, "labelled function declaration not allowed")
        }
        (Label(name, stmt), next_i)
      } else {
        let (expr, next_i) = parse_expression(tokens, index)
        let mut i = next_i
        if match_punct(tokens, i, ';') {
          i = i + 1
        } else {
          ensure_asi(tokens, i)
        }
        (ExprStmt(expr), i)
      }
    KeywordAwait =>
      if await_expr_allowed() {
        let (expr, next_i) = parse_expression(tokens, index)
        let mut i = next_i
        if match_punct(tokens, i, ';') {
          i = i + 1
        } else {
          ensure_asi(tokens, i)
        }
        (ExprStmt(expr), i)
      } else if await_ident_allowed() {
        if match_punct(tokens, index + 1, ':') {
          let name = tok.lexeme
          let (stmt, next_i) = parse_statement(tokens, index + 2, false, false)
          if strict_context() && stmt_is_function_decl(stmt) {
            parse_fail(
              tokens, index, "labelled function declaration not allowed",
            )
          }
          (Label(name, stmt), next_i)
        } else {
          let (expr, next_i) = parse_expression(tokens, index)
          let mut i = next_i
          if match_punct(tokens, i, ';') {
            i = i + 1
          } else {
            ensure_asi(tokens, i)
          }
          (ExprStmt(expr), i)
        }
      } else {
        parse_fail(tokens, index, "unexpected token")
      }
    KeywordYield =>
      if yield_expr_allowed() {
        let (expr, next_i) = parse_expression(tokens, index)
        let mut i = next_i
        if match_punct(tokens, i, ';') {
          i = i + 1
        } else {
          ensure_asi(tokens, i)
        }
        (ExprStmt(expr), i)
      } else if yield_ident_allowed() {
        if match_punct(tokens, index + 1, ':') {
          let name = tok.lexeme
          let (stmt, next_i) = parse_statement(tokens, index + 2, false, false)
          if strict_context() && stmt_is_function_decl(stmt) {
            parse_fail(
              tokens, index, "labelled function declaration not allowed",
            )
          }
          (Label(name, stmt), next_i)
        } else {
          let (expr, next_i) = parse_expression(tokens, index)
          let mut i = next_i
          if match_punct(tokens, i, ';') {
            i = i + 1
          } else {
            ensure_asi(tokens, i)
          }
          (ExprStmt(expr), i)
        }
      } else {
        parse_fail(tokens, index, "unexpected token")
      }
    KeywordDebugger => {
      let mut i = index + 1
      if match_punct(tokens, i, ';') {
        i = i + 1
      } else {
        ensure_asi(tokens, i)
      }
      (Empty, i)
    }
    KeywordImport =>
      if match_punct(tokens, index + 1, '(') ||
        match_punct(tokens, index + 1, '.') {
        let (expr, next_i) = parse_expression(tokens, index)
        let mut i = next_i
        if match_punct(tokens, i, ';') {
          i = i + 1
        } else {
          ensure_asi(tokens, i)
        }
        (ExprStmt(expr), i)
      } else {
        parse_import(tokens, index)
      }
    KeywordExport => parse_export(tokens, index)
    Punct('{') => parse_block(tokens, index)
    Punct(';') => (Empty, index + 1)
    Ident =>
      if tok.lexeme == "await" && !await_ident_allowed() {
        parse_fail(tokens, index, "unexpected token")
      } else if tok.lexeme == "yield" && !yield_ident_allowed() {
        parse_fail(tokens, index, "unexpected token")
      } else if is_unescaped_ident(tokens, index, "async") &&
        match_kind(tokens, index + 1, KeywordFunction) &&
        !peek(tokens, index + 1).line_break_before {
        if !allow_decl {
          parse_fail(
            tokens, index, "async function declaration not allowed here",
          )
        }
        let (func, next_i) = parse_function(
          tokens,
          index + 1,
          true,
          true,
          false,
        )
        (FunctionDecl(with_function_offset(func, tok.offset)), next_i)
      } else if match_punct(tokens, index + 1, ':') {
        if escaped_keyword_disallowed(tok) {
          parse_fail(tokens, index, "unexpected token")
        }
        if strict_context() && is_strict_reserved_name(tok.lexeme) {
          parse_fail(tokens, index, "unexpected token")
        }
        let name = tok.lexeme
        let (stmt, next_i) = parse_statement(tokens, index + 2, false, false)
        if strict_context() && stmt_is_function_decl(stmt) {
          parse_fail(tokens, index, "labelled function declaration not allowed")
        }
        (Label(name, stmt), next_i)
      } else {
        let (expr, next_i) = parse_expression(tokens, index)
        let mut i = next_i
        if match_punct(tokens, i, ';') {
          i = i + 1
        } else {
          ensure_asi(tokens, i)
        }
        (ExprStmt(expr), i)
      }
    _ => {
      let (expr, next_i) = parse_expression(tokens, index)
      let mut i = next_i
      if match_punct(tokens, i, ';') {
        i = i + 1
      } else {
        ensure_asi(tokens, i)
      }
      (ExprStmt(expr), i)
    }
  }
}

///|

///|
fn import_call_expr(
  specifier : String,
  attributes : Expr?,
  offset : Int,
) -> Expr {
  let callee = Expr::Ident("__import", offset)
  let args : Array[Expr] = [Expr::String(specifier, false, false)]
  match attributes {
    Some(value) => args.push(value)
    None => ()
  }
  Expr::Call(callee, args, offset)
}

///|
fn import_binding_call_expr(
  specifier : String,
  name : String,
  attributes : Expr?,
  offset : Int,
) -> Expr {
  let callee = Expr::Ident("__import_binding", offset)
  let args : Array[Expr] = [
    Expr::String(specifier, false, false),
    Expr::String(name, false, false),
  ]
  match attributes {
    Some(value) => args.push(value)
    None => ()
  }
  Expr::Call(callee, args, offset)
}

///|
fn export_call_expr(
  name : String,
  value : Expr,
  binding_name : String?,
  offset : Int,
) -> Expr {
  let callee = Expr::Ident("__export", offset)
  let args = match binding_name {
    Some(binding) =>
      [
        Expr::String(name, false, false),
        value,
        Expr::String(binding, false, false),
      ]
    None => [Expr::String(name, false, false), value]
  }
  Expr::Call(callee, args, offset)
}

///|
fn export_star_call_expr(
  specifier : String,
  attributes : Expr?,
  offset : Int,
) -> Expr {
  let callee = Expr::Ident("__export_star", offset)
  let args : Array[Expr] = [Expr::String(specifier, false, false)]
  match attributes {
    Some(value) => args.push(value)
    None => ()
  }
  Expr::Call(callee, args, offset)
}

///|
fn parse_with_clause(tokens : Array[Token], index : Int) -> (Expr?, Int) raise {
  if !match_kind(tokens, index, KeywordWith) &&
    !is_unescaped_ident(tokens, index, "with") {
    return (None, index)
  }
  let mut i = index + 1
  if !match_punct(tokens, i, '{') {
    parse_fail(tokens, i, "expected '{'")
  }
  i = i + 1
  let props : Array[ObjectProp] = []
  let seen : Map[String, Bool] = Map::new()
  while !match_punct(tokens, i, '}') {
    let key_index = i
    let key_tok = peek(tokens, i)
    let key = if match_kind(tokens, i, String) {
      key_tok.lexeme
    } else if is_identifier_name(key_tok.kind) {
      key_tok.lexeme
    } else {
      parse_fail(tokens, i, "expected identifier")
    }
    i = i + 1
    if !match_punct(tokens, i, ':') {
      parse_fail(tokens, i, "expected ':'")
    }
    i = i + 1
    if !match_kind(tokens, i, String) {
      parse_fail(tokens, i, "expected string")
    }
    let value_tok = peek(tokens, i)
    let has_escape = match value_tok.raw_lexeme {
      Some(_) => true
      None => false
    }
    let has_legacy_escape = match value_tok.raw_lexeme {
      Some(raw) => string_literal_has_legacy_escape(raw)
      None => false
    }
    let value_expr = Expr::String(
      value_tok.lexeme,
      has_escape,
      has_legacy_escape,
    )
    if seen.contains(key) {
      parse_fail(tokens, key_index, "duplicate with key")
    }
    seen.set(key, true)
    props.push(ObjectProp::KeyValue(ObjKey::String(key), value_expr))
    i = i + 1
    if match_punct(tokens, i, ',') {
      i = i + 1
      continue
    }
    if match_punct(tokens, i, '}') {
      break
    }
    parse_fail(tokens, i, "expected '}'")
  }
  if !match_punct(tokens, i, '}') {
    parse_fail(tokens, i, "expected '}'")
  }
  i = i + 1
  if props.length() == 0 {
    return (None, i)
  }
  (Some(Expr::ObjectLiteral(props)), i)
}

///|
fn parse_import(tokens : Array[Token], index : Int) -> (Stmt, Int) raise {
  let import_offset = peek(tokens, index).offset
  let mut i = index + 1
  if match_kind(tokens, i, String) {
    let specifier = peek(tokens, i).lexeme
    i = i + 1
    let (attributes, next_i) = parse_with_clause(tokens, i)
    i = next_i
    if match_punct(tokens, i, ';') {
      i = i + 1
    } else {
      ensure_asi(tokens, i)
    }
    let expr = import_call_expr(specifier, attributes, import_offset)
    return (ExprStmt(expr), i)
  }
  if match_punct(tokens, i, '*') {
    if !is_unescaped_ident(tokens, i + 1, "as") {
      parse_fail(tokens, i + 1, "expected 'as'")
    }
    let name_tok = peek(tokens, i + 2)
    if !is_identifier_name(name_tok.kind) {
      parse_fail(tokens, i + 2, "expected identifier")
    }
    let local_name = name_tok.lexeme
    i = i + 3
    if !is_unescaped_ident(tokens, i, "from") {
      parse_fail(tokens, i, "expected 'from'")
    }
    i = i + 1
    if !match_kind(tokens, i, String) {
      parse_fail(tokens, i, "expected string")
    }
    let specifier = peek(tokens, i).lexeme
    i = i + 1
    let (attributes, next_i) = parse_with_clause(tokens, i)
    i = next_i
    if match_punct(tokens, i, ';') {
      i = i + 1
    } else {
      ensure_asi(tokens, i)
    }
    let import_expr = import_call_expr(specifier, attributes, import_offset)
    let decls : Array[(VarBinding, Expr?)] = [
      (VarBinding::Name(local_name), Some(import_expr)),
    ]
    return (VarDecl(VarKind::ConstDecl, decls), i)
  }
  if is_identifier_name(peek(tokens, i).kind) {
    let default_name = peek(tokens, i).lexeme
    i = i + 1
    let mut namespace_name : String? = None
    let named_bindings : Array[(String, String)] = []
    if match_punct(tokens, i, ',') {
      i = i + 1
      if match_punct(tokens, i, '*') {
        if !is_unescaped_ident(tokens, i + 1, "as") {
          parse_fail(tokens, i + 1, "expected 'as'")
        }
        let name_tok = peek(tokens, i + 2)
        if !is_identifier_name(name_tok.kind) {
          parse_fail(tokens, i + 2, "expected identifier")
        }
        namespace_name = Some(name_tok.lexeme)
        i = i + 3
      } else if match_punct(tokens, i, '{') {
        i = i + 1
        while !match_punct(tokens, i, '}') {
          let name_tok = peek(tokens, i)
          let import_name = match name_tok.kind {
            String => name_tok.lexeme
            _ if is_identifier_name(name_tok.kind) => name_tok.lexeme
            _ => parse_fail(tokens, i, "expected identifier")
          }
          let mut local_name = import_name
          i = i + 1
          if is_unescaped_ident(tokens, i, "as") {
            let local_tok = peek(tokens, i + 1)
            if !is_identifier_name(local_tok.kind) {
              parse_fail(tokens, i + 1, "expected identifier")
            }
            local_name = local_tok.lexeme
            i = i + 2
          }
          named_bindings.push((import_name, local_name))
          if match_punct(tokens, i, ',') {
            i = i + 1
            continue
          }
          if match_punct(tokens, i, '}') {
            break
          }
          parse_fail(tokens, i, "expected '}'")
        }
        if !match_punct(tokens, i, '}') {
          parse_fail(tokens, i, "expected '}'")
        }
        i = i + 1
      } else {
        parse_fail(tokens, i, "expected named import")
      }
    }
    if !is_unescaped_ident(tokens, i, "from") {
      parse_fail(tokens, i, "expected 'from'")
    }
    i = i + 1
    if !match_kind(tokens, i, String) {
      parse_fail(tokens, i, "expected string")
    }
    let specifier = peek(tokens, i).lexeme
    i = i + 1
    let (attributes, next_i) = parse_with_clause(tokens, i)
    i = next_i
    if match_punct(tokens, i, ';') {
      i = i + 1
    } else {
      ensure_asi(tokens, i)
    }
    let decls : Array[(VarBinding, Expr?)] = []
    let default_expr = import_binding_call_expr(
      specifier, "default", attributes, import_offset,
    )
    decls.push((VarBinding::Name(default_name), Some(default_expr)))
    match namespace_name {
      Some(name) => {
        let ns_expr = import_call_expr(specifier, attributes, import_offset)
        decls.push((VarBinding::Name(name), Some(ns_expr)))
      }
      None => ()
    }
    for binding in named_bindings {
      let (import_name, local_name) = binding
      let binding_expr = import_binding_call_expr(
        specifier, import_name, attributes, import_offset,
      )
      decls.push((VarBinding::Name(local_name), Some(binding_expr)))
    }
    return (VarDecl(VarKind::ConstDecl, decls), i)
  }
  if match_punct(tokens, i, '{') {
    i = i + 1
    let bindings : Array[(String, String)] = []
    while !match_punct(tokens, i, '}') {
      let name_tok = peek(tokens, i)
      let import_name = match name_tok.kind {
        String => name_tok.lexeme
        _ if is_identifier_name(name_tok.kind) => name_tok.lexeme
        _ => parse_fail(tokens, i, "expected identifier")
      }
      let mut local_name = import_name
      i = i + 1
      if is_unescaped_ident(tokens, i, "as") {
        let local_tok = peek(tokens, i + 1)
        if !is_identifier_name(local_tok.kind) {
          parse_fail(tokens, i + 1, "expected identifier")
        }
        local_name = local_tok.lexeme
        i = i + 2
      }
      bindings.push((import_name, local_name))
      if match_punct(tokens, i, ',') {
        i = i + 1
        continue
      }
      if match_punct(tokens, i, '}') {
        break
      }
      parse_fail(tokens, i, "expected '}'")
    }
    if !match_punct(tokens, i, '}') {
      parse_fail(tokens, i, "expected '}'")
    }
    i = i + 1
    if !is_unescaped_ident(tokens, i, "from") {
      parse_fail(tokens, i, "expected 'from'")
    }
    i = i + 1
    if !match_kind(tokens, i, String) {
      parse_fail(tokens, i, "expected string")
    }
    let specifier = peek(tokens, i).lexeme
    i = i + 1
    let (attributes, next_i) = parse_with_clause(tokens, i)
    i = next_i
    if match_punct(tokens, i, ';') {
      i = i + 1
    } else {
      ensure_asi(tokens, i)
    }
    if bindings.is_empty() {
      let expr = import_call_expr(specifier, attributes, import_offset)
      return (ExprStmt(expr), i)
    }
    let decls : Array[(VarBinding, Expr?)] = []
    for binding in bindings {
      let (import_name, local_name) = binding
      let binding_expr = import_binding_call_expr(
        specifier, import_name, attributes, import_offset,
      )
      decls.push((VarBinding::Name(local_name), Some(binding_expr)))
    }
    return (VarDecl(VarKind::ConstDecl, decls), i)
  }
  parse_fail(tokens, i, "unsupported import")
}

///|
fn parse_export(tokens : Array[Token], index : Int) -> (Stmt, Int) raise {
  let export_offset = peek(tokens, index).offset
  let tok = peek(tokens, index + 1)
  match tok.kind {
    KeywordDefault => {
      let mut i = index + 2
      if is_unescaped_ident(tokens, i, "async") &&
        match_kind(tokens, i + 1, KeywordFunction) &&
        !peek(tokens, i + 1).line_break_before {
        let async_offset = peek(tokens, i).offset
        let (func, next_i) = parse_function(tokens, i + 1, false, true, false)
        let func = with_function_offset(func, async_offset)
        let stmts : Array[Stmt] = []
        let export_expr = match func.name {
          Some(name) => {
            stmts.push(FunctionDecl(func))
            export_call_expr(
              "default",
              Expr::Ident(name, export_offset),
              Some(name),
              export_offset,
            )
          }
          None => {
            let default_func = FunctionDef::{
              name: Some("default"),
              params: func.params,
              param_inits: func.param_inits,
              body: func.body,
              is_strict: func.is_strict,
              is_generator: func.is_generator,
              is_async: func.is_async,
              is_arrow: func.is_arrow,
              offset: func.offset,
              end_offset: func.end_offset,
            }
            stmts.push(FunctionDecl(default_func))
            export_call_expr(
              "default",
              Expr::Ident("default", export_offset),
              Some("default"),
              export_offset,
            )
          }
        }
        stmts.push(ExprStmt(export_expr))
        i = next_i
        if match_punct(tokens, i, ';') {
          i = i + 1
        }
        (Block(stmts), i)
      } else {
        let next_tok = peek(tokens, i)
        match next_tok.kind {
          KeywordFunction => {
            let (func, next_i) = parse_function(tokens, i, false, false, false)
            let stmts : Array[Stmt] = []
            let export_expr = match func.name {
              Some(name) => {
                stmts.push(FunctionDecl(func))
                export_call_expr(
                  "default",
                  Expr::Ident(name, export_offset),
                  Some(name),
                  export_offset,
                )
              }
              None => {
                let default_func = FunctionDef::{
                  name: Some("default"),
                  params: func.params,
                  param_inits: func.param_inits,
                  body: func.body,
                  is_strict: func.is_strict,
                  is_generator: func.is_generator,
                  is_async: func.is_async,
                  is_arrow: func.is_arrow,
                  offset: func.offset,
                  end_offset: func.end_offset,
                }
                stmts.push(FunctionDecl(default_func))
                export_call_expr(
                  "default",
                  Expr::Ident("default", export_offset),
                  Some("default"),
                  export_offset,
                )
              }
            }
            stmts.push(ExprStmt(export_expr))
            i = next_i
            if match_punct(tokens, i, ';') {
              i = i + 1
            }
            (Block(stmts), i)
          }
          KeywordClass => {
            let (class_def, next_i) = parse_class(tokens, i, false)
            let stmts : Array[Stmt] = []
            let export_expr = match class_def.name {
              Some(name) => {
                stmts.push(ClassDecl(class_def))
                export_call_expr(
                  "default",
                  Expr::Ident(name, export_offset),
                  Some(name),
                  export_offset,
                )
              }
              None => {
                let default_class = ClassDef::{
                  name: Some("default"),
                  super_class: class_def.super_class,
                  body: class_def.body,
                  offset: class_def.offset,
                  end_offset: class_def.end_offset,
                }
                stmts.push(ClassDecl(default_class))
                export_call_expr(
                  "default",
                  Expr::Ident("default", export_offset),
                  Some("default"),
                  export_offset,
                )
              }
            }
            stmts.push(ExprStmt(export_expr))
            i = next_i
            if match_punct(tokens, i, ';') {
              i = i + 1
            }
            (Block(stmts), i)
          }
          _ => {
            let (expr, next_i) = parse_assignment(tokens, i)
            i = next_i
            let export_expr = export_call_expr(
              "default",
              expr,
              None,
              export_offset,
            )
            if match_punct(tokens, i, ';') {
              i = i + 1
            } else {
              ensure_asi(tokens, i)
            }
            (ExprStmt(export_expr), i)
          }
        }
      }
    }
    KeywordFunction => {
      let (func, next_i) = parse_function(tokens, index + 1, true, false, false)
      let name = match func.name {
        Some(value) => value
        None => parse_fail(tokens, index + 1, "expected function name")
      }
      let export_expr = export_call_expr(
        name,
        Expr::Ident(name, export_offset),
        Some(name),
        export_offset,
      )
      let stmts : Array[Stmt] = [FunctionDecl(func), ExprStmt(export_expr)]
      let mut i = next_i
      if match_punct(tokens, i, ';') {
        i = i + 1
      }
      (Block(stmts), i)
    }
    KeywordClass => {
      let (class_def, next_i) = parse_class(tokens, index + 1, true)
      let name = match class_def.name {
        Some(value) => value
        None => parse_fail(tokens, index + 1, "expected class name")
      }
      let export_expr = export_call_expr(
        name,
        Expr::Ident(name, export_offset),
        Some(name),
        export_offset,
      )
      let stmts : Array[Stmt] = [ClassDecl(class_def), ExprStmt(export_expr)]
      let mut i = next_i
      if match_punct(tokens, i, ';') {
        i = i + 1
      }
      (Block(stmts), i)
    }
    Ident =>
      if is_unescaped_ident(tokens, index + 1, "async") &&
        match_kind(tokens, index + 2, KeywordFunction) &&
        !peek(tokens, index + 2).line_break_before {
        let async_offset = peek(tokens, index + 1).offset
        let (func, next_i) = parse_function(
          tokens,
          index + 2,
          true,
          true,
          false,
        )
        let func = with_function_offset(func, async_offset)
        let name = match func.name {
          Some(value) => value
          None => parse_fail(tokens, index + 2, "expected function name")
        }
        let export_expr = export_call_expr(
          name,
          Expr::Ident(name, export_offset),
          Some(name),
          export_offset,
        )
        let stmts : Array[Stmt] = [FunctionDecl(func), ExprStmt(export_expr)]
        let mut i = next_i
        if match_punct(tokens, i, ';') {
          i = i + 1
        }
        (Block(stmts), i)
      } else {
        parse_fail(tokens, index + 1, "unsupported export")
      }
    KeywordVar | KeywordLet | KeywordConst => {
      let (stmt, next_i) = parse_var_decl(tokens, index + 1)
      let i = next_i
      let mut result = stmt
      match stmt {
        VarDecl(kind, decls) => {
          let export_calls : Array[Expr] = []
          for decl in decls {
            let (binding, _) = decl
            let names : Array[String] = []
            collect_binding_names(binding, names)
            for name in names {
              let value_expr = Expr::Ident(name, export_offset)
              export_calls.push(
                export_call_expr(name, value_expr, Some(name), export_offset),
              )
            }
          }
          if !export_calls.is_empty() {
            let expr = if export_calls.length() == 1 {
              export_calls[0]
            } else {
              Expr::Sequence(export_calls)
            }
            result = Block([VarDecl(kind, decls), ExprStmt(expr)])
          }
        }
        _ => ()
      }
      if i > 0 && match_punct(tokens, i - 1, ';') {
        return (result, i)
      }
      ensure_asi(tokens, i)
      (result, i)
    }
    Punct('*') => {
      let mut i = index + 2
      if is_unescaped_ident(tokens, i, "as") {
        let name_tok = peek(tokens, i + 1)
        if !is_identifier_name(name_tok.kind) {
          parse_fail(tokens, i + 1, "expected identifier")
        }
        let export_name = name_tok.lexeme
        i = i + 2
        if !is_unescaped_ident(tokens, i, "from") {
          parse_fail(tokens, i, "expected 'from'")
        }
        i = i + 1
        if !match_kind(tokens, i, String) {
          parse_fail(tokens, i, "expected string")
        }
        let specifier = peek(tokens, i).lexeme
        i = i + 1
        let (attributes, next_i) = parse_with_clause(tokens, i)
        i = next_i
        if match_punct(tokens, i, ';') {
          i = i + 1
        } else {
          ensure_asi(tokens, i)
        }
        let value_expr = import_call_expr(specifier, attributes, export_offset)
        let export_expr = export_call_expr(
          export_name,
          value_expr,
          None,
          export_offset,
        )
        return (ExprStmt(export_expr), i)
      }
      if !is_unescaped_ident(tokens, i, "from") {
        parse_fail(tokens, i, "expected 'from'")
      }
      i = i + 1
      if !match_kind(tokens, i, String) {
        parse_fail(tokens, i, "expected string")
      }
      let specifier = peek(tokens, i).lexeme
      i = i + 1
      let (attributes, next_i) = parse_with_clause(tokens, i)
      i = next_i
      if match_punct(tokens, i, ';') {
        i = i + 1
      } else {
        ensure_asi(tokens, i)
      }
      let export_expr = export_star_call_expr(
        specifier, attributes, export_offset,
      )
      (ExprStmt(export_expr), i)
    }
    _ => {
      let mut i = index + 1
      if !match_punct(tokens, i, '{') {
        parse_fail(tokens, i, "unsupported export")
      }
      i = i + 1
      let exports : Array[(String, String)] = []
      while !match_punct(tokens, i, '}') {
        let name_tok = peek(tokens, i)
        if !is_identifier_name(name_tok.kind) {
          parse_fail(tokens, i, "expected identifier")
        }
        let local_name = name_tok.lexeme
        let mut export_name = local_name
        i = i + 1
        if is_unescaped_ident(tokens, i, "as") {
          let export_tok = peek(tokens, i + 1)
          export_name = match export_tok.kind {
            String => export_tok.lexeme
            _ if is_identifier_name(export_tok.kind) => export_tok.lexeme
            _ => parse_fail(tokens, i + 1, "expected identifier")
          }
          i = i + 2
        }
        exports.push((local_name, export_name))
        if match_punct(tokens, i, ',') {
          i = i + 1
          continue
        }
        if match_punct(tokens, i, '}') {
          break
        }
        parse_fail(tokens, i, "expected '}'")
      }
      if !match_punct(tokens, i, '}') {
        parse_fail(tokens, i, "expected '}'")
      }
      i = i + 1
      let mut from_specifier : String? = None
      let mut from_attributes : Expr? = None
      if is_unescaped_ident(tokens, i, "from") {
        i = i + 1
        if !match_kind(tokens, i, String) {
          parse_fail(tokens, i, "expected string")
        }
        from_specifier = Some(peek(tokens, i).lexeme)
        i = i + 1
        let (attributes, next_i) = parse_with_clause(tokens, i)
        from_attributes = attributes
        i = next_i
      }
      if match_punct(tokens, i, ';') {
        i = i + 1
      } else {
        ensure_asi(tokens, i)
      }
      let export_calls : Array[Expr] = []
      match from_specifier {
        Some(specifier) =>
          for binding in exports {
            let (local_name, export_name) = binding
            let value_expr = import_binding_call_expr(
              specifier, local_name, from_attributes, export_offset,
            )
            export_calls.push(
              export_call_expr(export_name, value_expr, None, export_offset),
            )
          }
        None =>
          for binding in exports {
            let (local_name, export_name) = binding
            let value_expr = Expr::Ident(local_name, export_offset)
            export_calls.push(
              export_call_expr(
                export_name,
                value_expr,
                Some(local_name),
                export_offset,
              ),
            )
          }
      }
      let expr = if export_calls.is_empty() {
        match from_specifier {
          Some(specifier) =>
            import_call_expr(specifier, from_attributes, export_offset)
          None => Null
        }
      } else if export_calls.length() == 1 {
        export_calls[0]
      } else {
        Expr::Sequence(export_calls)
      }
      (ExprStmt(expr), i)
    }
  }
}

///|
fn parse_expression(tokens : Array[Token], index : Int) -> (Expr, Int) raise {
  let (expr0, i0) = parse_assignment(tokens, index)
  if !match_punct(tokens, i0, ',') {
    return (expr0, i0)
  }
  let exprs : Array[Expr] = [expr0]
  let mut i = i0
  while match_punct(tokens, i, ',') {
    let (expr, next_i) = parse_assignment(tokens, i + 1)
    exprs.push(expr)
    i = next_i
  }
  (Sequence(exprs), i)
}

///|
fn parse_assignment(tokens : Array[Token], index : Int) -> (Expr, Int) raise {
  if match_kind(tokens, index, KeywordYield) {
    if yield_expr_allowed() {
      return parse_yield(tokens, index)
    }
  }
  if match_punct(tokens, index, '{') {
    match find_matching_punct(tokens, index, '{', '}') {
      Some(end_i) if match_punct(tokens, end_i + 1, '=') => {
        let (pattern, next_i) = parse_object_pattern(
          tokens, index, true, true, true, true,
        )
        if !match_punct(tokens, next_i, '=') {
          parse_fail(tokens, next_i, "expected '='")
        }
        let (rhs, after_i) = parse_assignment(tokens, next_i + 1)
        return (AssignObject(pattern, rhs), after_i)
      }
      _ => ()
    }
  }
  if match_punct(tokens, index, '[') {
    match find_matching_punct(tokens, index, '[', ']') {
      Some(end_i) if match_punct(tokens, end_i + 1, '=') => {
        let (pattern, next_i) = parse_array_pattern(
          tokens, index, true, true, true, true,
        )
        if !match_punct(tokens, next_i, '=') {
          parse_fail(tokens, next_i, "expected '='")
        }
        let (rhs, after_i) = parse_assignment(tokens, next_i + 1)
        return (AssignArray(pattern, rhs), after_i)
      }
      _ => ()
    }
  }
  match parse_arrow(tokens, index) {
    Some(result) => return result
    None => ()
  }
  let (lhs, i) = parse_conditional(tokens, index)
  let mut target = lhs
  let mut is_cover = false
  let mut keep_unwrap = true
  while keep_unwrap {
    match target {
      Paren(inner) => {
        is_cover = true
        target = inner
      }
      _ => keep_unwrap = false
    }
  }
  if match_punct(tokens, i, '=') {
    match target {
      Ident(name, ident_offset) => {
        let (rhs, next_i) = parse_assignment(tokens, i + 1)
        return (Assign(name, rhs, ident_offset, is_cover), next_i)
      }
      Member(obj, key, member_offset) => {
        let (rhs, next_i) = parse_assignment(tokens, i + 1)
        return (AssignMember(obj, key, rhs, member_offset), next_i)
      }
      ArrayLiteral(elements) =>
        if !is_cover {
          let offset = peek(tokens, index).offset
          let pattern = expr_to_array_pattern(elements, offset)
          let (rhs, next_i) = parse_assignment(tokens, i + 1)
          return (AssignArray(pattern, rhs), next_i)
        }
      ObjectLiteral(props) =>
        if !is_cover {
          let offset = peek(tokens, index).offset
          let pattern = expr_to_object_pattern(props, offset)
          let (rhs, next_i) = parse_assignment(tokens, i + 1)
          return (AssignObject(pattern, rhs), next_i)
        }
      Call(_, _, _) =>
        if !strict_context() && is_call_assignment_target(target) {
          let (rhs, next_i) = parse_assignment(tokens, i + 1)
          let offset = peek(tokens, index).offset
          return (AssignInvalid(lhs, rhs, offset), next_i)
        } else {
          parse_fail(tokens, i, "invalid assignment target")
        }
      _ => ()
    }
    match lhs {
      ArrayLiteral(elements) => {
        let offset = peek(tokens, index).offset
        let pattern = expr_to_array_pattern(elements, offset)
        let (rhs, next_i) = parse_assignment(tokens, i + 1)
        return (AssignArray(pattern, rhs), next_i)
      }
      ObjectLiteral(props) => {
        let offset = peek(tokens, index).offset
        let pattern = expr_to_object_pattern(props, offset)
        let (rhs, next_i) = parse_assignment(tokens, i + 1)
        return (AssignObject(pattern, rhs), next_i)
      }
      _ => parse_fail(tokens, i, "invalid assignment target")
    }
  }
  let tok = peek(tokens, i)
  let assign_op = match tok.kind {
    PlusAssign => Some(BinaryOp::Add)
    MinusAssign => Some(BinaryOp::Sub)
    MulAssign => Some(BinaryOp::Mul)
    DivAssign => Some(BinaryOp::Div)
    ModAssign => Some(BinaryOp::Mod)
    PowAssign => Some(BinaryOp::Pow)
    ShlAssign => Some(BinaryOp::Shl)
    SarAssign => Some(BinaryOp::Sar)
    ShrAssign => Some(BinaryOp::Shr)
    AndAssign => Some(BinaryOp::BitAnd)
    XorAssign => Some(BinaryOp::BitXor)
    OrAssign => Some(BinaryOp::BitOr)
    LandAssign => Some(BinaryOp::LogicalAnd)
    LorAssign => Some(BinaryOp::LogicalOr)
    DoubleQuestionMarkAssign => Some(BinaryOp::Coalesce)
    _ => None
  }
  match assign_op {
    Some(op) => {
      let is_logical_assign = match op {
        BinaryOp::LogicalAnd | BinaryOp::LogicalOr | BinaryOp::Coalesce => true
        _ => false
      }
      let mut target = lhs
      let mut keep_unwrap = true
      while keep_unwrap {
        match target {
          Paren(inner) => target = inner
          _ => keep_unwrap = false
        }
      }
      match target {
        Ident(_, _) | Member(_, _, _) => {
          let (rhs, next_i) = parse_assignment(tokens, i + 1)
          return (AssignOp(op, lhs, rhs, tok.offset), next_i)
        }
        _ =>
          if !is_logical_assign &&
            !strict_context() &&
            is_call_assignment_target(target) {
            let (rhs, next_i) = parse_assignment(tokens, i + 1)
            return (AssignOp(op, lhs, rhs, tok.offset), next_i)
          } else {
            parse_fail(tokens, i, "invalid assignment target")
          }
      }
    }
    None => ()
  }
  (lhs, i)
}

///|
fn expr_to_binding_with_default(
  expr : Expr,
  offset : Int,
) -> (VarBinding, Expr?) raise {
  match expr {
    Assign(name, rhs, _, _) => (VarBinding::Name(name), Some(rhs))
    AssignMember(obj, key, rhs, member_offset) =>
      (VarBinding::Target(Member(obj, key, member_offset)), Some(rhs))
    AssignArray(pattern, rhs) => (VarBinding::ArrayPattern(pattern), Some(rhs))
    AssignObject(pattern, rhs) =>
      (VarBinding::ObjectPattern(pattern), Some(rhs))
    Paren(inner) => expr_to_binding_with_default(inner, offset)
    _ => (expr_to_binding(expr, offset), None)
  }
}

///|
fn expr_to_binding(expr : Expr, offset : Int) -> VarBinding raise {
  match expr {
    Ident(name, _) => VarBinding::Name(name)
    Member(_, _, _) => VarBinding::Target(expr)
    ArrayLiteral(elements) =>
      VarBinding::ArrayPattern(expr_to_array_pattern(elements, offset))
    ObjectLiteral(props) =>
      VarBinding::ObjectPattern(expr_to_object_pattern(props, offset))
    Paren(inner) => expr_to_binding(inner, offset)
    _ => parse_fail_offset(offset, "invalid assignment target")
  }
}

///|
fn expr_to_array_pattern(
  elements : Array[Expr?],
  offset : Int,
) -> Array[ArrayPatternElem] raise {
  let bindings : Array[ArrayPatternElem] = []
  let mut saw_rest = false
  for elem in elements {
    if saw_rest {
      parse_fail_offset(offset, "rest element must be last")
    }
    match elem {
      None => bindings.push(ArrayPatternElem::Hole)
      Some(Spread(inner)) => {
        let binding = expr_to_binding(inner, offset)
        bindings.push(ArrayPatternElem::Rest(binding))
        saw_rest = true
      }
      Some(expr) => {
        let (binding, init) = expr_to_binding_with_default(expr, offset)
        bindings.push(ArrayPatternElem::Bind(binding, init))
      }
    }
  }
  bindings
}

///|
fn expr_to_object_pattern(
  props : Array[ObjectProp],
  offset : Int,
) -> Array[ObjectPatternElem] raise {
  let bindings : Array[ObjectPatternElem] = []
  let mut saw_rest = false
  for prop in props {
    if saw_rest {
      parse_fail_offset(offset, "rest element must be last")
    }
    match prop {
      ObjectProp::KeyValue(key, value) => {
        let (binding, init) = expr_to_binding_with_default(value, offset)
        bindings.push(ObjectPatternElem::Bind(key, binding, init))
      }
      ObjectProp::Shorthand(name) =>
        bindings.push(
          ObjectPatternElem::Bind(
            ObjKey::Ident(name),
            VarBinding::Name(name),
            None,
          ),
        )
      ObjectProp::Spread(expr) => {
        let binding = expr_to_binding(expr, offset)
        bindings.push(ObjectPatternElem::Rest(binding))
        saw_rest = true
      }
      _ => parse_fail_offset(offset, "invalid assignment target")
    }
  }
  bindings
}

///|
fn parse_yield(tokens : Array[Token], index : Int) -> (Expr, Int) raise {
  let mut i = index + 1
  let next_tok = peek(tokens, i)
  if next_tok.line_break_before {
    if match_punct(tokens, i, '*') {
      parse_fail(tokens, i, "unexpected token")
    }
    return (Yield(None, false), i)
  }
  let mut delegate = false
  if match_punct(tokens, i, '*') {
    delegate = true
    i = i + 1
  }
  let is_delim = match_punct(tokens, i, ';') ||
    match_punct(tokens, i, ')') ||
    match_punct(tokens, i, ']') ||
    match_punct(tokens, i, '}') ||
    match_punct(tokens, i, ',') ||
    match_punct(tokens, i, ':')
  if is_delim {
    if delegate {
      parse_fail(tokens, i, "unexpected token")
    }
    (Yield(None, delegate), i)
  } else {
    let (expr, next_i) = parse_assignment(tokens, i)
    (Yield(Some(expr), delegate), next_i)
  }
}

///|
fn parse_arrow(tokens : Array[Token], index : Int) -> (Expr, Int)? raise {
  let tok = peek(tokens, index)
  let allow_await_param = await_ident_allowed() || await_param_override()
  let is_param_ident = (
      tok.kind == Ident &&
      !(tok.lexeme == "await" && !allow_await_param) &&
      !(tok.lexeme == "yield" && !yield_ident_allowed())
    ) ||
    (tok.kind == KeywordYield && yield_ident_allowed()) ||
    (tok.kind == KeywordAwait && allow_await_param)
  if is_param_ident && match_kind(tokens, index + 1, Arrow) {
    if peek(tokens, index + 1).line_break_before {
      parse_fail(tokens, index + 1, "unexpected token")
    }
    let (param_name, _) = with_await_context(false, allow_await_param, fn() raise {
      parse_binding_identifier(tokens, index, false, true, true)
    })
    let params : Array[ParamBinding] = [
      ParamBinding::{ binding: VarBinding::Name(param_name), is_rest: false },
    ]
    let param_inits : Array[Expr?] = [None]
    let outer_strict = strict_context()
    let parse_body = fn() raise {
      with_await_context(false, true, fn() raise {
        with_yield_context(false, true, fn() raise {
          parse_arrow_body(tokens, index + 2)
        })
      })
    }
    let (body0, next_i0) = with_strict_context(outer_strict, fn() raise {
      parse_body()
    })
    let mut body = body0
    let mut next_i = next_i0
    let mut use_strict = has_use_strict(body)
    if use_strict && !outer_strict {
      let (strict_body, strict_next_i) = with_strict_context(true, fn() raise {
        parse_body()
      })
      body = strict_body
      next_i = strict_next_i
      use_strict = has_use_strict(body)
    }
    let is_strict = use_strict || outer_strict
    check_function_params(
      tokens,
      index,
      None,
      params,
      param_inits,
      is_strict,
      use_strict,
      true,
      false,
      false,
      false,
    )
    check_param_lexical_conflicts(tokens, index, params, body)
    let end_offset = if next_i > 0 {
      token_end_offset(tokens, next_i - 1)
    } else {
      tok.offset
    }
    let func = FunctionDef::{
      name: None,
      params,
      param_inits,
      body,
      is_strict,
      is_generator: false,
      is_async: false,
      is_arrow: true,
      offset: tok.offset,
      end_offset,
    }
    return Some((FunctionExpr(func), next_i))
  }
  if is_unescaped_ident(tokens, index, "async") &&
    !peek(tokens, index + 1).line_break_before &&
    (
      match_kind(tokens, index + 1, Ident) ||
      match_kind(tokens, index + 1, KeywordYield) ||
      match_kind(tokens, index + 1, KeywordAwait)
    ) &&
    match_kind(tokens, index + 2, Arrow) {
    if peek(tokens, index + 2).line_break_before {
      parse_fail(tokens, index + 2, "unexpected token")
    }
    let (param_name, _) = parse_binding_identifier(
      tokens,
      index + 1,
      false,
      true,
      true,
    )
    let params : Array[ParamBinding] = [
      ParamBinding::{ binding: VarBinding::Name(param_name), is_rest: false },
    ]
    let param_inits : Array[Expr?] = [None]
    let outer_strict = strict_context()
    let mut check_async = true
    if param_name == "await" && await_ident_allowed() {
      check_async = false
    }
    let parse_body = fn() raise {
      with_await_context(true, false, fn() raise {
        with_yield_context(false, true, fn() raise {
          parse_arrow_body(tokens, index + 3)
        })
      })
    }
    let (body0, next_i0) = with_strict_context(outer_strict, fn() raise {
      parse_body()
    })
    let mut body = body0
    let mut next_i = next_i0
    let mut use_strict = has_use_strict(body)
    if use_strict && !outer_strict {
      let (strict_body, strict_next_i) = with_strict_context(true, fn() raise {
        parse_body()
      })
      body = strict_body
      next_i = strict_next_i
      use_strict = has_use_strict(body)
    }
    let is_strict = use_strict || outer_strict
    check_function_params(
      tokens,
      index,
      None,
      params,
      param_inits,
      is_strict,
      use_strict,
      true,
      false,
      false,
      check_async,
    )
    check_param_lexical_conflicts(tokens, index, params, body)
    let end_offset = if next_i > 0 {
      token_end_offset(tokens, next_i - 1)
    } else {
      tok.offset
    }
    let func = FunctionDef::{
      name: None,
      params,
      param_inits,
      body,
      is_strict,
      is_generator: false,
      is_async: true,
      is_arrow: true,
      offset: tok.offset,
      end_offset,
    }
    return Some((FunctionExpr(func), next_i))
  }
  if is_unescaped_ident(tokens, index, "async") &&
    !peek(tokens, index + 1).line_break_before &&
    match_punct(tokens, index + 1, '(') &&
    is_arrow_param_list(tokens, index + 1) {
    let allow_yield_ident = yield_ident_allowed()
    let (params, param_inits, next_i) = with_await_context(false, false, fn() raise {
      with_yield_context(false, allow_yield_ident, fn() raise {
        parse_parameters(tokens, index + 1, true, false, false)
      })
    })
    if !match_kind(tokens, next_i, Arrow) {
      parse_fail(tokens, next_i, "expected '=>'")
    }
    if peek(tokens, next_i).line_break_before {
      parse_fail(tokens, next_i, "unexpected token")
    }
    let (body, after_body) = with_await_context(true, false, fn() raise {
      with_yield_context(false, true, fn() raise {
        parse_arrow_body(tokens, next_i + 1)
      })
    })
    let use_strict = has_use_strict(body)
    let is_strict = use_strict || strict_context()
    check_function_params(
      tokens,
      index,
      None,
      params,
      param_inits,
      is_strict,
      use_strict,
      true,
      false,
      false,
      true,
    )
    check_param_lexical_conflicts(tokens, index, params, body)
    let end_offset = if after_body > 0 {
      token_end_offset(tokens, after_body - 1)
    } else {
      tok.offset
    }
    let func = FunctionDef::{
      name: None,
      params,
      param_inits,
      body,
      is_strict,
      is_generator: false,
      is_async: true,
      is_arrow: true,
      offset: tok.offset,
      end_offset,
    }
    return Some((FunctionExpr(func), after_body))
  }
  if match_punct(tokens, index, '(') && is_arrow_param_list(tokens, index) {
    let allow_await_ident = await_ident_allowed() || await_param_override()
    let allow_yield_ident = yield_ident_allowed()
    let (params, param_inits, next_i) = with_await_context(
      false,
      allow_await_ident,
      fn() raise {
        with_yield_context(false, allow_yield_ident, fn() raise {
          parse_parameters(tokens, index, true, allow_await_ident, true)
        })
      },
    )
    if !match_kind(tokens, next_i, Arrow) {
      parse_fail(tokens, next_i, "expected '=>'")
    }
    if peek(tokens, next_i).line_break_before {
      parse_fail(tokens, next_i, "unexpected token")
    }
    let (body, after_body) = with_await_context(false, true, fn() raise {
      with_yield_context(false, true, fn() raise {
        parse_arrow_body(tokens, next_i + 1)
      })
    })
    let use_strict = has_use_strict(body)
    let is_strict = use_strict || strict_context()
    check_function_params(
      tokens,
      index,
      None,
      params,
      param_inits,
      is_strict,
      use_strict,
      true,
      false,
      false,
      false,
    )
    check_param_lexical_conflicts(tokens, index, params, body)
    let end_offset = if after_body > 0 {
      token_end_offset(tokens, after_body - 1)
    } else {
      tok.offset
    }
    let func = FunctionDef::{
      name: None,
      params,
      param_inits,
      body,
      is_strict,
      is_generator: false,
      is_async: false,
      is_arrow: true,
      offset: tok.offset,
      end_offset,
    }
    return Some((FunctionExpr(func), after_body))
  }
  None
}

///|
fn parse_arrow_body(
  tokens : Array[Token],
  index : Int,
) -> (Array[Stmt], Int) raise {
  if match_punct(tokens, index, '{') {
    let (stmt, next_i) = with_return_context(true, fn() raise {
      parse_block(tokens, index)
    })
    let body = match stmt {
      Block(stmts) => stmts
      _ => []
    }
    return (body, next_i)
  }
  let (expr, next_i) = with_in_context(true, fn() raise {
    parse_assignment(tokens, index)
  })
  let body = [Return(Some(expr))]
  (body, next_i)
}

///|
fn parse_conditional(tokens : Array[Token], index : Int) -> (Expr, Int) raise {
  let (test_expr, i) = parse_coalesce(tokens, index)
  if match_punct(tokens, i, '?') {
    let (conseq, next_i) = with_in_context(true, fn() raise {
      parse_assignment(tokens, i + 1)
    })
    if !match_punct(tokens, next_i, ':') {
      parse_fail(tokens, next_i, "expected ':'")
    }
    let (alt, end_i) = parse_assignment(tokens, next_i + 1)
    (Conditional(test_expr, conseq, alt), end_i)
  } else {
    (test_expr, i)
  }
}

///|
fn expr_is_mixed_logical(expr : Expr) -> Bool {
  match expr {
    Paren(_) => false
    Binary(BinaryOp::LogicalOr, _, _, _)
    | Binary(BinaryOp::LogicalAnd, _, _, _) => true
    _ => false
  }
}

///|
fn parse_coalesce(tokens : Array[Token], index : Int) -> (Expr, Int) raise {
  let (lhs0, i0) = parse_logical_or(tokens, index)
  let mut lhs = lhs0
  let mut i = i0
  while match_kind(tokens, i, DoubleQuestionMark) {
    let op_offset = peek(tokens, i).offset
    if expr_is_mixed_logical(lhs) {
      parse_fail(tokens, i, "unexpected token")
    }
    let (rhs, next_i) = parse_logical_or(tokens, i + 1)
    if expr_is_mixed_logical(rhs) {
      parse_fail(tokens, i, "unexpected token")
    }
    lhs = Binary(BinaryOp::Coalesce, lhs, rhs, op_offset)
    i = next_i
  }
  (lhs, i)
}

///|
fn parse_logical_or(tokens : Array[Token], index : Int) -> (Expr, Int) raise {
  let (lhs0, i0) = parse_logical_and(tokens, index)
  let mut lhs = lhs0
  let mut i = i0
  while match_kind(tokens, i, Lor) {
    let op_offset = peek(tokens, i).offset
    let (rhs, next_i) = parse_logical_and(tokens, i + 1)
    lhs = Binary(BinaryOp::LogicalOr, lhs, rhs, op_offset)
    i = next_i
  }
  (lhs, i)
}

///|
fn parse_logical_and(tokens : Array[Token], index : Int) -> (Expr, Int) raise {
  let (lhs0, i0) = parse_bitwise_or(tokens, index)
  let mut lhs = lhs0
  let mut i = i0
  while match_kind(tokens, i, Land) {
    let op_offset = peek(tokens, i).offset
    let (rhs, next_i) = parse_bitwise_or(tokens, i + 1)
    lhs = Binary(BinaryOp::LogicalAnd, lhs, rhs, op_offset)
    i = next_i
  }
  (lhs, i)
}

///|
fn parse_bitwise_or(tokens : Array[Token], index : Int) -> (Expr, Int) raise {
  let (lhs0, i0) = parse_bitwise_xor(tokens, index)
  let mut lhs = lhs0
  let mut i = i0
  while match_punct(tokens, i, '|') {
    let op_offset = peek(tokens, i).offset
    let (rhs, next_i) = parse_bitwise_xor(tokens, i + 1)
    lhs = Binary(BinaryOp::BitOr, lhs, rhs, op_offset)
    i = next_i
  }
  (lhs, i)
}

///|
fn parse_bitwise_xor(tokens : Array[Token], index : Int) -> (Expr, Int) raise {
  let (lhs0, i0) = parse_bitwise_and(tokens, index)
  let mut lhs = lhs0
  let mut i = i0
  while match_punct(tokens, i, '^') {
    let op_offset = peek(tokens, i).offset
    let (rhs, next_i) = parse_bitwise_and(tokens, i + 1)
    lhs = Binary(BinaryOp::BitXor, lhs, rhs, op_offset)
    i = next_i
  }
  (lhs, i)
}

///|
fn parse_bitwise_and(tokens : Array[Token], index : Int) -> (Expr, Int) raise {
  let (lhs0, i0) = parse_equality(tokens, index)
  let mut lhs = lhs0
  let mut i = i0
  while match_punct(tokens, i, '&') {
    let op_offset = peek(tokens, i).offset
    let (rhs, next_i) = parse_equality(tokens, i + 1)
    lhs = Binary(BinaryOp::BitAnd, lhs, rhs, op_offset)
    i = next_i
  }
  (lhs, i)
}

///|
fn parse_equality(tokens : Array[Token], index : Int) -> (Expr, Int) raise {
  let (lhs0, i0) = parse_relational(tokens, index)
  let mut lhs = lhs0
  let mut i = i0
  for {
    let tok = peek(tokens, i)
    match tok.kind {
      Eq => {
        let (rhs, next_i) = parse_relational(tokens, i + 1)
        lhs = Binary(BinaryOp::Eq, lhs, rhs, tok.offset)
        i = next_i
      }
      Neq => {
        let (rhs, next_i) = parse_relational(tokens, i + 1)
        lhs = Binary(BinaryOp::Neq, lhs, rhs, tok.offset)
        i = next_i
      }
      StrictEq => {
        let (rhs, next_i) = parse_relational(tokens, i + 1)
        lhs = Binary(BinaryOp::StrictEq, lhs, rhs, tok.offset)
        i = next_i
      }
      StrictNeq => {
        let (rhs, next_i) = parse_relational(tokens, i + 1)
        lhs = Binary(BinaryOp::StrictNeq, lhs, rhs, tok.offset)
        i = next_i
      }
      _ => break
    }
  }
  (lhs, i)
}

///|
fn parse_relational(tokens : Array[Token], index : Int) -> (Expr, Int) raise {
  let allow_in = in_expr_allowed()
  if allow_in &&
    match_kind(tokens, index, PrivateName) &&
    match_kind(tokens, index + 1, KeywordIn) {
    let tok = peek(tokens, index)
    let (rhs, next_i) = parse_shift(tokens, index + 2)
    return (PrivateIn(tok.lexeme, rhs, tok.offset), next_i)
  }
  let (lhs0, i0) = parse_shift(tokens, index)
  let mut lhs = lhs0
  let mut i = i0
  for {
    let tok = peek(tokens, i)
    match tok.kind {
      Lt => {
        let (rhs, next_i) = parse_shift(tokens, i + 1)
        lhs = Binary(BinaryOp::Lt, lhs, rhs, tok.offset)
        i = next_i
      }
      Lte => {
        let (rhs, next_i) = parse_shift(tokens, i + 1)
        lhs = Binary(BinaryOp::Lte, lhs, rhs, tok.offset)
        i = next_i
      }
      Gt => {
        let (rhs, next_i) = parse_shift(tokens, i + 1)
        lhs = Binary(BinaryOp::Gt, lhs, rhs, tok.offset)
        i = next_i
      }
      Gte => {
        let (rhs, next_i) = parse_shift(tokens, i + 1)
        lhs = Binary(BinaryOp::Gte, lhs, rhs, tok.offset)
        i = next_i
      }
      KeywordIn => {
        if !allow_in {
          break
        }
        let (rhs, next_i) = parse_shift(tokens, i + 1)
        lhs = Binary(BinaryOp::In, lhs, rhs, tok.offset)
        i = next_i
      }
      KeywordInstanceof => {
        let (rhs, next_i) = parse_shift(tokens, i + 1)
        lhs = Binary(BinaryOp::Instanceof, lhs, rhs, tok.offset)
        i = next_i
      }
      _ => break
    }
  }
  (lhs, i)
}

///|
fn parse_shift(tokens : Array[Token], index : Int) -> (Expr, Int) raise {
  let (lhs0, i0) = parse_additive(tokens, index)
  let mut lhs = lhs0
  let mut i = i0
  for {
    let tok = peek(tokens, i)
    match tok.kind {
      Shl => {
        let (rhs, next_i) = parse_additive(tokens, i + 1)
        lhs = Binary(BinaryOp::Shl, lhs, rhs, tok.offset)
        i = next_i
      }
      Sar => {
        let (rhs, next_i) = parse_additive(tokens, i + 1)
        lhs = Binary(BinaryOp::Sar, lhs, rhs, tok.offset)
        i = next_i
      }
      Shr => {
        let (rhs, next_i) = parse_additive(tokens, i + 1)
        lhs = Binary(BinaryOp::Shr, lhs, rhs, tok.offset)
        i = next_i
      }
      _ => break
    }
  }
  (lhs, i)
}

///|
fn parse_additive(tokens : Array[Token], index : Int) -> (Expr, Int) raise {
  let (lhs0, i0) = parse_multiplicative(tokens, index)
  let mut lhs = lhs0
  let mut i = i0
  for {
    let tok = peek(tokens, i)
    match tok.kind {
      Punct('+') => {
        let (rhs, next_i) = parse_multiplicative(tokens, i + 1)
        lhs = Binary(BinaryOp::Add, lhs, rhs, tok.offset)
        i = next_i
      }
      Punct('-') => {
        let (rhs, next_i) = parse_multiplicative(tokens, i + 1)
        lhs = Binary(BinaryOp::Sub, lhs, rhs, tok.offset)
        i = next_i
      }
      _ => break
    }
  }
  (lhs, i)
}

///|
fn parse_multiplicative(
  tokens : Array[Token],
  index : Int,
) -> (Expr, Int) raise {
  let (lhs0, i0) = parse_power(tokens, index)
  let mut lhs = lhs0
  let mut i = i0
  for {
    let tok = peek(tokens, i)
    match tok.kind {
      Punct('*') => {
        let (rhs, next_i) = parse_power(tokens, i + 1)
        lhs = Binary(BinaryOp::Mul, lhs, rhs, tok.offset)
        i = next_i
      }
      Punct('/') => {
        let (rhs, next_i) = parse_power(tokens, i + 1)
        lhs = Binary(BinaryOp::Div, lhs, rhs, tok.offset)
        i = next_i
      }
      Punct('%') => {
        let (rhs, next_i) = parse_power(tokens, i + 1)
        lhs = Binary(BinaryOp::Mod, lhs, rhs, tok.offset)
        i = next_i
      }
      _ => break
    }
  }
  (lhs, i)
}

///|
fn parse_power(tokens : Array[Token], index : Int) -> (Expr, Int) raise {
  let (lhs, i) = parse_unary(tokens, index)
  if match_kind(tokens, i, Pow) {
    if is_unary_expr(lhs) {
      parse_fail(
        tokens, i, "unparenthesized unary expression can't appear on the left-hand side of '**'",
      )
    }
    let (rhs, next_i) = parse_power(tokens, i + 1)
    let op_offset = peek(tokens, i).offset
    return (Binary(BinaryOp::Pow, lhs, rhs, op_offset), next_i)
  }
  (lhs, i)
}

///|
fn parse_unary(tokens : Array[Token], index : Int) -> (Expr, Int) raise {
  let tok = peek(tokens, index)
  match tok.kind {
    Inc => {
      let (expr, next_i) = parse_unary(tokens, index + 1)
      if !is_update_target(expr) &&
        !(!strict_context() && is_call_assignment_target(expr)) {
        parse_fail(tokens, index, "invalid update target")
      }
      (Update(UpdateOp::Inc, expr, UpdateKind::Prefix, tok.offset), next_i)
    }
    Dec => {
      let (expr, next_i) = parse_unary(tokens, index + 1)
      if !is_update_target(expr) &&
        !(!strict_context() && is_call_assignment_target(expr)) {
        parse_fail(tokens, index, "invalid update target")
      }
      (Update(UpdateOp::Dec, expr, UpdateKind::Prefix, tok.offset), next_i)
    }
    Punct('+') => {
      let (expr, next_i) = parse_unary(tokens, index + 1)
      (Unary(UnaryOp::Plus, expr, tok.offset), next_i)
    }
    Punct('-') => {
      let (expr, next_i) = parse_unary(tokens, index + 1)
      (Unary(UnaryOp::Minus, expr, tok.offset), next_i)
    }
    Punct('!') => {
      let (expr, next_i) = parse_unary(tokens, index + 1)
      (Unary(UnaryOp::Not, expr, tok.offset), next_i)
    }
    Punct('~') => {
      let (expr, next_i) = parse_unary(tokens, index + 1)
      (Unary(UnaryOp::BitNot, expr, tok.offset), next_i)
    }
    KeywordTypeof => {
      let (expr, next_i) = parse_unary(tokens, index + 1)
      (Unary(UnaryOp::Typeof, expr, tok.offset), next_i)
    }
    KeywordVoid => {
      let (expr, next_i) = parse_unary(tokens, index + 1)
      (Unary(UnaryOp::Void, expr, tok.offset), next_i)
    }
    KeywordDelete => {
      let (expr, next_i) = parse_unary(tokens, index + 1)
      if expr_has_private_member(expr) {
        parse_fail(tokens, index, "cannot delete a private class field")
      }
      (Unary(UnaryOp::Delete, expr, tok.offset), next_i)
    }
    KeywordAwait =>
      if await_expr_allowed() {
        let (expr, next_i) = parse_unary(tokens, index + 1)
        (Await(expr, tok.offset), next_i)
      } else {
        parse_postfix(tokens, index)
      }
    _ => parse_postfix(tokens, index)
  }
}

///|
fn parse_postfix(tokens : Array[Token], index : Int) -> (Expr, Int) raise {
  let (expr0, i0) = parse_primary(tokens, index)
  let mut expr = expr0
  let mut i = i0
  let mut chain_base : Expr? = None
  let segments : Array[ChainSegment] = []
  fn is_optional_chain_tag_target(expr : Expr) -> Bool {
    match expr {
      OptionalChain(_, _) => true
      Paren(inner) => is_optional_chain_tag_target(inner)
      _ => false
    }
  }

  for {
    if match_kind(tokens, i, Template) || match_kind(tokens, i, TemplateHead) {
      if is_optional_chain_tag_target(expr) {
        parse_fail(tokens, i, "invalid optional chain in tagged template")
      }
      let (parts, exprs, next_i) = parse_template_literal(tokens, i)
      expr = TaggedTemplate(expr, parts, exprs)
      i = next_i
      continue
    }
    if match_kind(tokens, i, QuestionMarkDot) {
      if chain_base is None {
        match expr {
          New(_, args) =>
            if args.is_empty() {
              parse_fail(
                tokens, i, "new keyword cannot be used with an optional chain",
              )
            }
          _ => ()
        }
      }
      let optional_offset = peek(tokens, i).offset
      let base = match chain_base {
        Some(value) => value
        None => {
          chain_base = Some(expr)
          expr
        }
      }
      if match_punct(tokens, i + 1, '(') {
        let (args, next_i) = parse_arguments(tokens, i + 1)
        segments.push(ChainSegment::Call(args, true, optional_offset))
        expr = OptionalChain(base, segments)
        i = next_i
        continue
      }
      if match_punct(tokens, i + 1, '[') {
        let (key_expr, next_i) = parse_expression(tokens, i + 2)
        if !match_punct(tokens, next_i, ']') {
          parse_fail(tokens, next_i, "expected ']'")
        }
        segments.push(
          ChainSegment::Member(
            MemberKey::Computed(key_expr),
            true,
            optional_offset,
          ),
        )
        expr = OptionalChain(base, segments)
        i = next_i + 1
        continue
      }
      let tok = peek(tokens, i + 1)
      if tok.kind == PrivateName {
        if base is Super {
          parse_fail(tokens, i + 1, "private class field forbidden after super")
        }
        segments.push(
          ChainSegment::Member(
            MemberKey::Private(tok.lexeme),
            true,
            optional_offset,
          ),
        )
        expr = OptionalChain(base, segments)
        i = i + 2
        continue
      }
      if !is_identifier_name(tok.kind) {
        parse_fail(tokens, i + 1, "expected property name")
      }
      segments.push(
        ChainSegment::Member(
          MemberKey::Ident(tok.lexeme),
          true,
          optional_offset,
        ),
      )
      expr = OptionalChain(base, segments)
      i = i + 2
      continue
    }
    if match_punct(tokens, i, '(') {
      let call_offset = peek(tokens, i).offset
      let (args, next_i) = parse_arguments(tokens, i)
      match chain_base {
        Some(base) => {
          segments.push(ChainSegment::Call(args, false, call_offset))
          expr = OptionalChain(base, segments)
          i = next_i
          continue
        }
        None => {
          expr = Call(expr, args, call_offset)
          i = next_i
          continue
        }
      }
    }
    if match_punct(tokens, i, '.') {
      let member_offset = peek(tokens, i).offset
      let tok = peek(tokens, i + 1)
      if tok.kind == PrivateName {
        if expr is Super {
          parse_fail(tokens, i + 1, "private class field forbidden after super")
        }
        match chain_base {
          Some(Super) =>
            parse_fail(
              tokens,
              i + 1,
              "private class field forbidden after super",
            )
          Some(base) => {
            segments.push(
              ChainSegment::Member(
                MemberKey::Private(tok.lexeme),
                false,
                member_offset,
              ),
            )
            expr = OptionalChain(base, segments)
            i = i + 2
            continue
          }
          None => {
            expr = Member(expr, MemberKey::Private(tok.lexeme), member_offset)
            i = i + 2
            continue
          }
        }
      }
      if !is_identifier_name(tok.kind) {
        parse_fail(tokens, i + 1, "expected property name")
      }
      match chain_base {
        Some(base) => {
          segments.push(
            ChainSegment::Member(
              MemberKey::Ident(tok.lexeme),
              false,
              member_offset,
            ),
          )
          expr = OptionalChain(base, segments)
          i = i + 2
          continue
        }
        None => {
          expr = Member(expr, MemberKey::Ident(tok.lexeme), member_offset)
          i = i + 2
          continue
        }
      }
    }
    if match_punct(tokens, i, '[') {
      let member_offset = peek(tokens, i).offset
      let (key_expr, next_i) = parse_expression(tokens, i + 1)
      if !match_punct(tokens, next_i, ']') {
        parse_fail(tokens, next_i, "expected ']'")
      }
      match chain_base {
        Some(base) => {
          segments.push(
            ChainSegment::Member(
              MemberKey::Computed(key_expr),
              false,
              member_offset,
            ),
          )
          expr = OptionalChain(base, segments)
          i = next_i + 1
          continue
        }
        None => {
          expr = Member(expr, MemberKey::Computed(key_expr), member_offset)
          i = next_i + 1
          continue
        }
      }
    }
    break
  }
  if match_kind(tokens, i, Inc) {
    let tok = peek(tokens, i)
    if tok.line_break_before {
      return (expr, i)
    }
    if !is_update_target(expr) &&
      !(!strict_context() && is_call_assignment_target(expr)) {
      parse_fail(tokens, i, "invalid update target")
    }
    return (Update(UpdateOp::Inc, expr, UpdateKind::Postfix, tok.offset), i + 1)
  }
  if match_kind(tokens, i, Dec) {
    let tok = peek(tokens, i)
    if tok.line_break_before {
      return (expr, i)
    }
    if !is_update_target(expr) &&
      !(!strict_context() && is_call_assignment_target(expr)) {
      parse_fail(tokens, i, "invalid update target")
    }
    return (Update(UpdateOp::Dec, expr, UpdateKind::Postfix, tok.offset), i + 1)
  }
  (expr, i)
}

///|
fn parse_template_literal(
  tokens : Array[Token],
  index : Int,
) -> (TemplateParts, Array[Expr], Int) raise {
  let tok = peek(tokens, index)
  let site_id = next_template_site_id()
  fn raw_from_token(tok : Token) -> String {
    match tok.raw_lexeme {
      Some(raw) => raw
      None => tok.lexeme
    }
  }

  match tok.kind {
    Template => {
      let raw = raw_from_token(tok)
      let parts : Array[String?] = [cook_template_raw(raw)]
      let raw_parts : Array[String] = [raw]
      let exprs : Array[Expr] = []
      (
        TemplateParts::{ cooked: parts, raw: raw_parts, site_id },
        exprs,
        index + 1,
      )
    }
    TemplateHead => {
      let raw = raw_from_token(tok)
      let parts : Array[String?] = [cook_template_raw(raw)]
      let raw_parts : Array[String] = [raw]
      let exprs : Array[Expr] = []
      let mut i = index + 1
      for {
        let (expr, next_i) = parse_expression(tokens, i)
        if !match_punct(tokens, next_i, '}') {
          parse_fail(tokens, next_i, "expected '}'")
        }
        exprs.push(expr)
        i = next_i + 1
        let next_tok = peek(tokens, i)
        match next_tok.kind {
          TemplateMiddle => {
            let raw = raw_from_token(next_tok)
            parts.push(cook_template_raw(raw))
            raw_parts.push(raw)
            i = i + 1
            continue
          }
          TemplateTail => {
            let raw = raw_from_token(next_tok)
            parts.push(cook_template_raw(raw))
            raw_parts.push(raw)
            return (
              TemplateParts::{ cooked: parts, raw: raw_parts, site_id },
              exprs,
              i + 1,
            )
          }
          _ => parse_fail(tokens, i, "expected template literal")
        }
      }
      (TemplateParts::{ cooked: parts, raw: raw_parts, site_id }, exprs, i)
    }
    _ => parse_fail(tokens, index, "expected template literal")
  }
}

///|
fn template_parts_has_invalid(parts : TemplateParts) -> Bool {
  for part in parts.cooked {
    match part {
      Some(_) => ()
      None => return true
    }
  }
  false
}

///|
fn is_identifier_name(kind : TokenKind) -> Bool {
  match kind {
    Ident
    | KeywordNull
    | KeywordFalse
    | KeywordTrue
    | KeywordIf
    | KeywordElse
    | KeywordReturn
    | KeywordVar
    | KeywordThis
    | KeywordDelete
    | KeywordVoid
    | KeywordTypeof
    | KeywordNew
    | KeywordIn
    | KeywordInstanceof
    | KeywordDo
    | KeywordWhile
    | KeywordFor
    | KeywordBreak
    | KeywordContinue
    | KeywordSwitch
    | KeywordCase
    | KeywordDefault
    | KeywordThrow
    | KeywordTry
    | KeywordCatch
    | KeywordFinally
    | KeywordFunction
    | KeywordDebugger
    | KeywordWith
    | KeywordClass
    | KeywordConst
    | KeywordEnum
    | KeywordExport
    | KeywordExtends
    | KeywordImport
    | KeywordSuper
    | KeywordImplements
    | KeywordInterface
    | KeywordLet
    | KeywordPackage
    | KeywordPrivate
    | KeywordProtected
    | KeywordPublic
    | KeywordStatic
    | KeywordYield
    | KeywordAwait
    | KeywordOf => true
    _ => false
  }
}

///|
fn keyword_kind_from_name(name : String) -> TokenKind? {
  match name {
    "null" => Some(KeywordNull)
    "false" => Some(KeywordFalse)
    "true" => Some(KeywordTrue)
    "if" => Some(KeywordIf)
    "else" => Some(KeywordElse)
    "return" => Some(KeywordReturn)
    "var" => Some(KeywordVar)
    "this" => Some(KeywordThis)
    "delete" => Some(KeywordDelete)
    "void" => Some(KeywordVoid)
    "typeof" => Some(KeywordTypeof)
    "new" => Some(KeywordNew)
    "in" => Some(KeywordIn)
    "instanceof" => Some(KeywordInstanceof)
    "do" => Some(KeywordDo)
    "while" => Some(KeywordWhile)
    "for" => Some(KeywordFor)
    "break" => Some(KeywordBreak)
    "continue" => Some(KeywordContinue)
    "switch" => Some(KeywordSwitch)
    "case" => Some(KeywordCase)
    "default" => Some(KeywordDefault)
    "throw" => Some(KeywordThrow)
    "try" => Some(KeywordTry)
    "catch" => Some(KeywordCatch)
    "finally" => Some(KeywordFinally)
    "function" => Some(KeywordFunction)
    "debugger" => Some(KeywordDebugger)
    "with" => Some(KeywordWith)
    "class" => Some(KeywordClass)
    "const" => Some(KeywordConst)
    "enum" => Some(KeywordEnum)
    "export" => Some(KeywordExport)
    "extends" => Some(KeywordExtends)
    "import" => Some(KeywordImport)
    "super" => Some(KeywordSuper)
    "implements" => Some(KeywordImplements)
    "interface" => Some(KeywordInterface)
    "let" => Some(KeywordLet)
    "package" => Some(KeywordPackage)
    "private" => Some(KeywordPrivate)
    "protected" => Some(KeywordProtected)
    "public" => Some(KeywordPublic)
    "static" => Some(KeywordStatic)
    "yield" => Some(KeywordYield)
    "await" => Some(KeywordAwait)
    "of" => Some(KeywordOf)
    _ => None
  }
}

///|
fn escaped_keyword_kind(tok : Token) -> TokenKind? {
  match tok.raw_lexeme {
    Some(_) => keyword_kind_from_name(tok.lexeme)
    None => None
  }
}

///|
fn escaped_keyword_disallowed(tok : Token) -> Bool {
  match escaped_keyword_kind(tok) {
    Some(kind) =>
      match kind {
        KeywordAwait => !await_ident_allowed()
        KeywordYield => !yield_ident_allowed()
        KeywordLet
        | KeywordStatic
        | KeywordOf
        | KeywordImplements
        | KeywordInterface
        | KeywordPackage
        | KeywordPrivate
        | KeywordProtected
        | KeywordPublic => false
        _ => true
      }
    None => false
  }
}

///|
fn binding_identifier_kind(tok : Token) -> TokenKind {
  match escaped_keyword_kind(tok) {
    Some(kind) => kind
    None => tok.kind
  }
}

///|
fn rescan_regexp_literal(tokens : Array[Token], index : Int) -> (String, Int)? {
  let chars = source_chars()
  if chars.is_empty() {
    return None
  }
  let tok = peek(tokens, index)
  scan_regexp_from(chars, tok.offset)
}

///|
fn parse_member(tokens : Array[Token], index : Int) -> (Expr, Int) raise {
  let (expr0, i0) = parse_primary(tokens, index)
  let mut expr = expr0
  let mut i = i0
  for {
    if match_punct(tokens, i, '.') {
      let member_offset = peek(tokens, i).offset
      let tok = peek(tokens, i + 1)
      if tok.kind == PrivateName {
        if expr is Super {
          parse_fail(tokens, i + 1, "private class field forbidden after super")
        }
        expr = Member(expr, MemberKey::Private(tok.lexeme), member_offset)
        i = i + 2
        continue
      }
      if !is_identifier_name(tok.kind) {
        parse_fail(tokens, i + 1, "expected property name")
      }
      expr = Member(expr, MemberKey::Ident(tok.lexeme), member_offset)
      i = i + 2
      continue
    }
    if match_punct(tokens, i, '[') {
      let member_offset = peek(tokens, i).offset
      let (key_expr, next_i) = parse_expression(tokens, i + 1)
      if !match_punct(tokens, next_i, ']') {
        parse_fail(tokens, next_i, "expected ']'")
      }
      expr = Member(expr, MemberKey::Computed(key_expr), member_offset)
      i = next_i + 1
      continue
    }
    break
  }
  (expr, i)
}

///|
fn parse_new(tokens : Array[Token], index : Int) -> (Expr, Int) raise {
  let (callee0, next_i) = parse_member(tokens, index + 1)
  let mut callee = callee0
  let mut i = next_i
  if match_kind(tokens, i, Template) || match_kind(tokens, i, TemplateHead) {
    let (parts, exprs, next_i) = parse_template_literal(tokens, i)
    callee = TaggedTemplate(callee, parts, exprs)
    i = next_i
  }
  let mut args : Array[Expr] = []
  if match_punct(tokens, i, '(') {
    let (parsed_args, after_args) = parse_arguments(tokens, i)
    args = parsed_args
    i = after_args
  }
  (New(callee, args), i)
}

///|
fn parse_primary(tokens : Array[Token], index : Int) -> (Expr, Int) raise {
  let tok = peek(tokens, index)
  match tok.kind {
    Number =>
      if tok.lexeme.has_suffix("n") {
        if bigint_has_leading_zero(tok.lexeme, tok.offset) {
          parse_fail_offset(tok.offset, "invalid bigint literal")
        }
        let len = tok.lexeme.length()
        let digits = tok.lexeme.unsafe_substring(start=0, end=len - 1)
        (BigInt(digits), index + 1)
      } else {
        let legacy_octal = is_legacy_decimal_literal(tok.lexeme, tok.offset)
        let value = parse_number_literal(tok.lexeme, tok.offset)
        (Number(value, legacy_octal, tok.offset), index + 1)
      }
    String => {
      let has_escape = match tok.raw_lexeme {
        Some(_) => true
        None => false
      }
      let has_legacy_escape = match tok.raw_lexeme {
        Some(raw) => string_literal_has_legacy_escape(raw)
        None => false
      }
      (String(tok.lexeme, has_escape, has_legacy_escape), index + 1)
    }
    Template | TemplateHead => {
      let (parts, exprs, next_i) = parse_template_literal(tokens, index)
      if template_parts_has_invalid(parts) {
        parse_fail(tokens, index, "invalid escape sequence in template literal")
      }
      (TemplateLiteral(parts, exprs), next_i)
    }
    Regexp => {
      let (pattern, flags) = split_regexp_lexeme(tok.lexeme)
      (Regexp(pattern, flags, tok.offset), index + 1)
    }
    Punct('/') =>
      match rescan_regexp_literal(tokens, index) {
        Some((lexeme, next_offset)) => {
          let (pattern, flags) = split_regexp_lexeme(lexeme)
          let next_i = advance_index_to_offset(tokens, index + 1, next_offset)
          (Regexp(pattern, flags, tok.offset), next_i)
        }
        None => parse_fail(tokens, index, "unexpected token")
      }
    KeywordTrue => (Bool(true), index + 1)
    KeywordFalse => (Bool(false), index + 1)
    KeywordNull => (Null, index + 1)
    KeywordThis => (This, index + 1)
    KeywordSuper => (Super, index + 1)
    KeywordNew => {
      if match_punct(tokens, index + 1, '.') {
        let name_tok = peek(tokens, index + 2)
        if name_tok.kind == Ident && name_tok.lexeme == "target" {
          if name_tok.raw_lexeme is None {
            return (NewTarget(tok.offset), index + 3)
          }
          parse_fail(tokens, index + 2, "unexpected token")
        }
      }
      if match_kind(tokens, index + 1, KeywordImport) &&
        match_punct(tokens, index + 2, '(') {
        parse_fail(tokens, index + 1, "invalid use of 'import()'")
      }
      parse_new(tokens, index)
    }
    KeywordImport => {
      if match_punct(tokens, index + 1, '.') {
        let name_tok = peek(tokens, index + 2)
        if name_tok.kind == Ident && name_tok.lexeme == "meta" {
          if name_tok.raw_lexeme is None {
            return (ImportMeta(tok.offset), index + 3)
          }
          parse_fail(tokens, index + 2, "unexpected token")
        }
      }
      if match_punct(tokens, index + 1, '(') {
        return parse_import_call(tokens, index)
      }
      parse_fail(tokens, index, "unsupported import")
    }
    KeywordClass => {
      let (class_def, next_i) = parse_class(tokens, index, false)
      (ClassExpr(class_def), next_i)
    }
    KeywordAwait =>
      if await_ident_allowed() {
        (Ident(tok.lexeme, tok.offset), index + 1)
      } else {
        parse_fail(tokens, index, "unexpected token")
      }
    KeywordYield =>
      if !yield_expr_allowed() && yield_ident_allowed() {
        (Ident(tok.lexeme, tok.offset), index + 1)
      } else {
        parse_fail(tokens, index, "unexpected token")
      }
    KeywordLet =>
      if strict_context() {
        parse_fail(tokens, index, "unexpected token")
      } else {
        (Ident(tok.lexeme, tok.offset), index + 1)
      }
    KeywordOf => (Ident(tok.lexeme, tok.offset), index + 1)
    KeywordStatic =>
      if strict_context() {
        parse_fail(tokens, index, "unexpected token")
      } else {
        (Ident(tok.lexeme, tok.offset), index + 1)
      }
    KeywordImplements
    | KeywordInterface
    | KeywordPackage
    | KeywordPrivate
    | KeywordProtected
    | KeywordPublic =>
      if strict_context() {
        parse_fail(tokens, index, "unexpected token")
      } else {
        (Ident(tok.lexeme, tok.offset), index + 1)
      }
    Ident => {
      if escaped_keyword_disallowed(tok) {
        parse_fail(tokens, index, "unexpected token")
      }
      let ident_has_escape = match tok.raw_lexeme {
        Some(_) => true
        None => false
      }
      if tok.lexeme == "await" && !await_ident_allowed() {
        parse_fail(tokens, index, "unexpected token")
      } else if tok.lexeme == "yield" && !yield_ident_allowed() {
        parse_fail(tokens, index, "unexpected token")
      } else if is_unescaped_ident(tokens, index, "async") &&
        match_kind(tokens, index + 1, KeywordFunction) &&
        !peek(tokens, index + 1).line_break_before {
        let (func, next_i) = parse_function(
          tokens,
          index + 1,
          false,
          true,
          true,
        )
        (FunctionExpr(with_function_offset(func, tok.offset)), next_i)
      } else {
        if tok.lexeme == "import" &&
          ident_has_escape &&
          match_punct(tokens, index + 1, '(') {
          parse_fail(tokens, index, "invalid use of 'import()'")
        }
        if tok.lexeme == "import" &&
          ident_has_escape &&
          match_punct(tokens, index + 1, '.') {
          let next_tok = peek(tokens, index + 2)
          if next_tok.kind == Ident && next_tok.lexeme == "meta" {
            parse_fail(tokens, index, "unexpected token")
          }
        }
        if tok.lexeme == "new" &&
          ident_has_escape &&
          match_punct(tokens, index + 1, '.') {
          let next_tok = peek(tokens, index + 2)
          if next_tok.kind == Ident && next_tok.lexeme == "target" {
            parse_fail(tokens, index, "unexpected token")
          }
        }
        (Ident(tok.lexeme, tok.offset), index + 1)
      }
    }
    KeywordFunction => {
      let (func, next_i) = parse_function(tokens, index, false, false, true)
      (FunctionExpr(func), next_i)
    }
    Punct('{') => parse_object_literal(tokens, index)
    Punct('[') => parse_array_literal(tokens, index)
    Punct('(') => {
      let (expr, next_i) = parse_expression(tokens, index + 1)
      if !match_punct(tokens, next_i, ')') {
        parse_fail(tokens, next_i, "expected ')'")
      }
      (Paren(expr), next_i + 1)
    }
    _ => parse_fail(tokens, index, "unexpected token")
  }
}

///|
fn is_arrow_param_list(tokens : Array[Token], index : Int) -> Bool {
  if !match_punct(tokens, index, '(') {
    return false
  }
  let mut i = index
  let mut depth = 0
  while i < tokens.length() {
    let tok = peek(tokens, i)
    match tok.kind {
      Punct('(') => depth = depth + 1
      Punct(')') => {
        depth = depth - 1
        if depth == 0 {
          return match_kind(tokens, i + 1, Arrow)
        }
      }
      _ => ()
    }
    i = i + 1
  }
  false
}

///|
fn split_regexp_lexeme(lexeme : String) -> (String, String) {
  match lexeme.find("\n") {
    Some(pos) => {
      let len = lexeme.length()
      let pattern = lexeme.unsafe_substring(start=0, end=pos)
      let flags = lexeme.unsafe_substring(start=pos + 1, end=len)
      (pattern, flags)
    }
    None => (lexeme, "")
  }
}

///|
fn advance_index_to_offset(
  tokens : Array[Token],
  index : Int,
  offset : Int,
) -> Int {
  let mut i = index
  while i < tokens.length() && peek(tokens, i).offset < offset {
    i = i + 1
  }
  i
}

///|
fn parse_arguments(
  tokens : Array[Token],
  index : Int,
) -> (Array[Expr], Int) raise {
  if !match_punct(tokens, index, '(') {
    parse_fail(tokens, index, "expected '('")
  }
  let args : Array[Expr] = []
  let mut i = index + 1
  if !match_punct(tokens, i, ')') {
    for {
      if match_kind(tokens, i, Ellipsis) {
        let (expr, next_i) = parse_assignment(tokens, i + 1)
        args.push(Spread(expr))
        i = next_i
      } else {
        let (expr, next_i) = parse_assignment(tokens, i)
        args.push(expr)
        i = next_i
      }
      if match_punct(tokens, i, ',') {
        i = i + 1
        if match_punct(tokens, i, ')') {
          break
        }
        continue
      }
      break
    }
  }
  if !match_punct(tokens, i, ')') {
    parse_fail(tokens, i, "expected ')'")
  }
  (args, i + 1)
}

///|
fn parse_import_call(tokens : Array[Token], index : Int) -> (Expr, Int) raise {
  if !match_punct(tokens, index + 1, '(') {
    parse_fail(tokens, index + 1, "expected '('")
  }
  let call_offset = peek(tokens, index + 1).offset
  let args : Array[Expr] = []
  let mut i = index + 2
  if match_punct(tokens, i, ')') {
    parse_fail(tokens, i, "expected expression")
  }
  if match_kind(tokens, i, Ellipsis) {
    parse_fail(tokens, i, "unexpected token")
  }
  let (first, next_i) = with_in_context(true, fn() raise {
    parse_assignment(tokens, i)
  })
  args.push(first)
  i = next_i
  if match_punct(tokens, i, ',') {
    if match_punct(tokens, i + 1, ')') {
      i = i + 1
    } else {
      i = i + 1
      if match_kind(tokens, i, Ellipsis) {
        parse_fail(tokens, i, "unexpected token")
      }
      let (second, next_i) = with_in_context(true, fn() raise {
        parse_assignment(tokens, i)
      })
      args.push(second)
      i = next_i
      if match_punct(tokens, i, ',') {
        if match_punct(tokens, i + 1, ')') {
          i = i + 1
        } else {
          parse_fail(tokens, i, "unexpected token")
        }
      }
    }
  }
  if !match_punct(tokens, i, ')') {
    parse_fail(tokens, i, "expected ')'")
  }
  let callee = Expr::Ident("__import_dynamic", peek(tokens, index).offset)
  (Call(callee, args, call_offset), i + 1)
}

///|
fn parse_var_decl_with_mode(
  tokens : Array[Token],
  index : Int,
  allow_const_without_init : Bool,
) -> (Stmt, Int) raise {
  let kind = match peek(tokens, index).kind {
    KeywordVar => VarKind::Var
    KeywordLet => VarKind::Let
    KeywordConst => VarKind::ConstDecl
    _ => parse_fail(tokens, index, "expected variable declaration")
  }
  let mut i = index + 1
  let decls : Array[(VarBinding, Expr?)] = []
  for {
    let allow_keyword_ident = kind == VarKind::Var
    let allow_yield_ident = true
    let allow_await_ident = true
    let (binding, next_i) = parse_var_binding(
      tokens, i, allow_keyword_ident, allow_yield_ident, allow_await_ident,
    )
    i = next_i
    let mut init : Expr? = None
    if match_punct(tokens, i, '=') {
      let (expr, next_i) = parse_assignment(tokens, i + 1)
      init = Some(expr)
      i = next_i
    }
    if kind == VarKind::ConstDecl && init is None && !allow_const_without_init {
      parse_fail(tokens, i, "missing initializer in const declaration")
    }
    decls.push((binding, init))
    if match_punct(tokens, i, ',') {
      i = i + 1
      continue
    }
    break
  }
  if match_punct(tokens, i, ';') {
    i = i + 1
  }
  (VarDecl(kind, decls), i)
}

///|
fn parse_var_decl(tokens : Array[Token], index : Int) -> (Stmt, Int) raise {
  parse_var_decl_with_mode(tokens, index, false)
}

///|
fn parse_block(tokens : Array[Token], index : Int) -> (Stmt, Int) raise {
  if !match_punct(tokens, index, '{') {
    parse_fail(tokens, index, "expected '{'")
  }
  let stmts : Array[Stmt] = []
  let mut i = index + 1
  while !match_punct(tokens, i, '}') && !is_eof(tokens, i) {
    let (stmt, next_i) = parse_statement(tokens, i, true, false)
    stmts.push(stmt)
    i = next_i
  }
  if !match_punct(tokens, i, '}') {
    parse_fail(tokens, i, "expected '}'")
  }
  (Block(stmts), i + 1)
}

///|
fn parse_return(tokens : Array[Token], index : Int) -> (Stmt, Int) raise {
  if !return_allowed() {
    parse_fail(tokens, index, "illegal return statement")
  }
  let mut i = index + 1
  if match_punct(tokens, i, ';') {
    return (Return(None), i + 1)
  }
  let next_tok = peek(tokens, i)
  if match_punct(tokens, i, '}') ||
    is_eof(tokens, i) ||
    next_tok.line_break_before {
    return (Return(None), i)
  }
  let (expr, next_i) = parse_expression(tokens, i)
  i = next_i
  if match_punct(tokens, i, ';') {
    i = i + 1
  }
  (Return(Some(expr)), i)
}

///|
fn parse_if(tokens : Array[Token], index : Int) -> (Stmt, Int) raise {
  let mut i = index + 1
  if !match_punct(tokens, i, '(') {
    parse_fail(tokens, i, "expected '('")
  }
  let (cond_expr, next_i) = parse_expression(tokens, i + 1)
  i = next_i
  if !match_punct(tokens, i, ')') {
    parse_fail(tokens, i, "expected ')'")
  }
  i = i + 1
  let (conseq, next_i2) = parse_statement(tokens, i, false, false)
  if strict_context() {
    if stmt_is_function_decl(conseq) {
      parse_fail(tokens, index, "labelled function declaration not allowed")
    }
  } else if stmt_is_labelled_function(conseq) {
    parse_fail(tokens, index, "labelled function declaration not allowed")
  }
  i = next_i2
  let mut alt : Stmt? = None
  if match_kind(tokens, i, KeywordElse) {
    let (stmt, next_i3) = parse_statement(tokens, i + 1, false, false)
    if strict_context() {
      if stmt_is_function_decl(stmt) {
        parse_fail(tokens, index, "labelled function declaration not allowed")
      }
    } else if stmt_is_labelled_function(stmt) {
      parse_fail(tokens, index, "labelled function declaration not allowed")
    }
    alt = Some(stmt)
    i = next_i3
  }
  (If(cond_expr, conseq, alt), i)
}

///|
fn parse_while(tokens : Array[Token], index : Int) -> (Stmt, Int) raise {
  let mut i = index + 1
  if !match_punct(tokens, i, '(') {
    parse_fail(tokens, i, "expected '('")
  }
  let (cond_expr, next_i) = parse_expression(tokens, i + 1)
  i = next_i
  if !match_punct(tokens, i, ')') {
    parse_fail(tokens, i, "expected ')'")
  }
  i = i + 1
  let (body, next_i2) = parse_statement(tokens, i, false, false)
  if stmt_is_function_decl(body) {
    parse_fail(tokens, index, "labelled function declaration not allowed")
  }
  (While(cond_expr, body), next_i2)
}

///|
fn is_simple_assignment_target(expr : Expr) -> Bool {
  match expr {
    Ident(_, _) | Member(_, _, _) => true
    Paren(inner) => is_simple_assignment_target(inner)
    _ => false
  }
}

///|
fn is_call_assignment_target(expr : Expr) -> Bool {
  match expr {
    Call(Ident(name, _), _, _) => name != "__import_dynamic"
    Call(_, _, _) => true
    Paren(inner) => is_call_assignment_target(inner)
    _ => false
  }
}

///|
fn check_for_decl_duplicates(
  tokens : Array[Token],
  index : Int,
  decls : Array[(VarBinding, Expr?)],
) -> Unit raise {
  let names : Array[String] = []
  for decl in decls {
    let (binding, _) = decl
    collect_binding_names(binding, names)
  }
  let seen : Map[String, Bool] = Map::new()
  for name in names {
    if seen.contains(name) {
      parse_fail(tokens, index, "duplicate lexical declaration")
    }
    seen.set(name, true)
  }
}

///|
fn parse_with(tokens : Array[Token], index : Int) -> (Stmt, Int) raise {
  let mut i = index + 1
  if !match_punct(tokens, i, '(') {
    parse_fail(tokens, i, "expected '('")
  }
  let (expr, next_i) = parse_expression(tokens, i + 1)
  i = next_i
  if !match_punct(tokens, i, ')') {
    parse_fail(tokens, i, "expected ')'")
  }
  i = i + 1
  let (body, next_i2) = parse_statement(tokens, i, false, false)
  if stmt_is_function_decl(body) {
    parse_fail(tokens, index, "labelled function declaration not allowed")
  }
  (With(expr, body), next_i2)
}

///|
fn binding_to_assign_expr(binding : VarBinding, offset : Int) -> Expr {
  match binding {
    VarBinding::Name(name) => Expr::Ident(name, offset)
    VarBinding::ArrayPattern(pattern) => Expr::AssignArray(pattern, Expr::Null)
    VarBinding::ObjectPattern(pattern) =>
      Expr::AssignObject(pattern, Expr::Null)
    VarBinding::Target(expr) => expr
  }
}

///|
fn parse_for(tokens : Array[Token], index : Int) -> (Stmt, Int) raise {
  let mut i = index + 1
  let mut is_await = false
  if match_kind(tokens, i, KeywordAwait) {
    if !await_expr_allowed() {
      parse_fail(tokens, i, "for await is only valid in asynchronous functions")
    }
    is_await = true
    i = i + 1
  }
  if !match_punct(tokens, i, '(') {
    parse_fail(tokens, i, "expected '('")
  }
  i = i + 1
  let mut init = ForInit::None
  if match_punct(tokens, i, ';') {
    if is_await {
      parse_fail(tokens, i, "expected 'of'")
    }
    i = i + 1
  } else if match_kind(tokens, i, KeywordVar) ||
    match_kind(tokens, i, KeywordConst) ||
    is_let_decl(tokens, i, true) {
    let (stmt, next_i) = with_in_context(false, fn() raise {
      parse_var_decl_with_mode(tokens, i, true)
    })
    init = match stmt {
      VarDecl(kind, decls) => ForInit::Var(kind, decls)
      _ => parse_fail(tokens, i, "expected var declaration")
    }
    i = next_i
    if match_kind(tokens, i, KeywordOf) || is_unescaped_ident(tokens, i, "of") {
      match init {
        ForInit::Var(kind, decls) =>
          if kind != VarKind::Var {
            check_for_decl_duplicates(tokens, i, decls)
          }
        _ => ()
      }
      let (rhs, next_i2) = parse_assignment(tokens, i + 1)
      i = next_i2
      if !match_punct(tokens, i, ')') {
        parse_fail(tokens, i, "expected ')'")
      }
      i = i + 1
      let (body, next_i2) = parse_statement(tokens, i, false, false)
      if stmt_is_function_decl(body) {
        parse_fail(tokens, index, "labelled function declaration not allowed")
      }
      if is_await {
        return (ForAwaitOf(init, rhs, body), next_i2)
      }
      return (ForOf(init, rhs, body), next_i2)
    }
    if match_kind(tokens, i, KeywordIn) {
      if is_await {
        parse_fail(tokens, i, "expected 'of'")
      }
      match init {
        ForInit::Var(kind, decls) =>
          if kind != VarKind::Var {
            check_for_decl_duplicates(tokens, i, decls)
          }
        _ => ()
      }
      let (rhs, next_i2) = parse_expression(tokens, i + 1)
      i = next_i2
      if !match_punct(tokens, i, ')') {
        parse_fail(tokens, i, "expected ')'")
      }
      i = i + 1
      let (body, next_i2) = parse_statement(tokens, i, false, false)
      if stmt_is_function_decl(body) {
        parse_fail(tokens, index, "labelled function declaration not allowed")
      }
      return (ForIn(init, rhs, body), next_i2)
    }
    if match_punct(tokens, i, ')') {
      if is_await {
        parse_fail(tokens, i, "expected 'of'")
      }
      match init {
        ForInit::Var(kind, decls) =>
          if decls.length() == 1 {
            let (binding, init_expr) = decls[0]
            match init_expr {
              Some(Binary(BinaryOp::In, left, right, _)) => {
                let rewritten : Array[(VarBinding, Expr?)] = [
                  (binding, Some(left)),
                ]
                i = i + 1
                let (body, next_i2) = parse_statement(tokens, i, false, false)
                if stmt_is_function_decl(body) {
                  parse_fail(
                    tokens, index, "labelled function declaration not allowed",
                  )
                }
                return (
                  ForIn(ForInit::Var(kind, rewritten), right, body),
                  next_i2,
                )
              }
              _ => ()
            }
          }
        _ => ()
      }
    }
    match init {
      ForInit::Var(kind, decls) =>
        if kind != VarKind::Var {
          check_for_decl_duplicates(tokens, i, decls)
        }
      _ => ()
    }
    match init {
      ForInit::Var(kind, decls) =>
        if kind != VarKind::Var {
          for decl in decls {
            let (binding, init_expr) = decl
            if init_expr is None {
              if kind == VarKind::ConstDecl {
                parse_fail(
                  tokens, i, "missing initializer in const declaration",
                )
              } else {
                match binding {
                  VarBinding::Name(_) => ()
                  _ =>
                    parse_fail(
                      tokens, i, "missing initializer in lexical declaration",
                    )
                }
              }
            }
          }
        }
      _ => ()
    }
    if is_await {
      parse_fail(tokens, i, "expected 'of'")
    }
  } else {
    let mut parsed_pattern = false
    if match_punct(tokens, i, '{') || match_punct(tokens, i, '[') {
      let (open, close) = if match_punct(tokens, i, '{') {
        ('{', '}')
      } else {
        ('[', ']')
      }
      match find_matching_punct(tokens, i, open, close) {
        Some(end_i) =>
          if match_kind(tokens, end_i + 1, KeywordIn) ||
            match_kind(tokens, end_i + 1, KeywordOf) ||
            is_unescaped_ident(tokens, end_i + 1, "of") {
            let (binding, next_i) = parse_assignment_binding(
              tokens, i, true, true, true,
            )
            let offset = peek(tokens, i).offset
            init = ForInit::Expr(binding_to_assign_expr(binding, offset))
            i = next_i
            parsed_pattern = true
          }
        None => ()
      }
    }
    if parsed_pattern {
      if match_kind(tokens, i, KeywordOf) || is_unescaped_ident(tokens, i, "of") {
        let (rhs, next_i2) = parse_assignment(tokens, i + 1)
        i = next_i2
        if !match_punct(tokens, i, ')') {
          parse_fail(tokens, i, "expected ')'")
        }
        i = i + 1
        let (body, next_i2) = parse_statement(tokens, i, false, false)
        if is_await {
          return (ForAwaitOf(init, rhs, body), next_i2)
        }
        return (ForOf(init, rhs, body), next_i2)
      }
      if match_kind(tokens, i, KeywordIn) {
        if is_await {
          parse_fail(tokens, i, "expected 'of'")
        }
        let (rhs, next_i2) = parse_expression(tokens, i + 1)
        i = next_i2
        if !match_punct(tokens, i, ')') {
          parse_fail(tokens, i, "expected ')'")
        }
        i = i + 1
        let (body, next_i2) = parse_statement(tokens, i, false, false)
        return (ForIn(init, rhs, body), next_i2)
      }
      parse_fail(tokens, i, "expected 'in' or 'of'")
    }
    let expr_start = i
    let (expr, next_i) = with_in_context(false, fn() raise {
      parse_expression(tokens, i)
    })
    init = ForInit::Expr(expr)
    i = next_i
    if match_kind(tokens, i, KeywordOf) || is_unescaped_ident(tokens, i, "of") {
      match expr {
        Ident(name, _) =>
          if name == "async" &&
            !is_await &&
            is_unescaped_ident(tokens, expr_start, "async") {
            parse_fail(tokens, i, "for of expression cannot start with 'async'")
          }
        _ => ()
      }
      if !is_simple_assignment_target(expr) &&
        !(!strict_context() && is_call_assignment_target(expr)) {
        parse_fail(tokens, i, "invalid assignment target")
      }
      let (rhs, next_i2) = parse_assignment(tokens, i + 1)
      i = next_i2
      if !match_punct(tokens, i, ')') {
        parse_fail(tokens, i, "expected ')'")
      }
      i = i + 1
      let (body, next_i2) = parse_statement(tokens, i, false, false)
      if stmt_is_function_decl(body) {
        parse_fail(tokens, index, "labelled function declaration not allowed")
      }
      if is_await {
        return (ForAwaitOf(init, rhs, body), next_i2)
      }
      return (ForOf(init, rhs, body), next_i2)
    }
    if match_kind(tokens, i, KeywordIn) {
      if is_await {
        parse_fail(tokens, i, "expected 'of'")
      }
      if !is_simple_assignment_target(expr) &&
        !(!strict_context() && is_call_assignment_target(expr)) {
        parse_fail(tokens, i, "invalid assignment target")
      }
      let (rhs, next_i2) = parse_expression(tokens, i + 1)
      i = next_i2
      if !match_punct(tokens, i, ')') {
        parse_fail(tokens, i, "expected ')'")
      }
      i = i + 1
      let (body, next_i2) = parse_statement(tokens, i, false, false)
      if stmt_is_function_decl(body) {
        parse_fail(tokens, index, "labelled function declaration not allowed")
      }
      return (ForIn(init, rhs, body), next_i2)
    }
    if match_punct(tokens, i, ')') {
      if is_await {
        parse_fail(tokens, i, "expected 'of'")
      }
      match expr {
        Binary(BinaryOp::In, lhs, rhs, _) => {
          if !is_simple_assignment_target(lhs) &&
            !(!strict_context() && is_call_assignment_target(lhs)) {
            parse_fail(tokens, i, "invalid assignment target")
          }
          i = i + 1
          let (body, next_i2) = parse_statement(tokens, i, false, false)
          if stmt_is_function_decl(body) {
            parse_fail(
              tokens, index, "labelled function declaration not allowed",
            )
          }
          return (ForIn(ForInit::Expr(lhs), rhs, body), next_i2)
        }
        Sequence(exprs) =>
          if exprs.length() >= 2 {
            match exprs[0] {
              Binary(BinaryOp::In, lhs, rhs, _) => {
                if !is_simple_assignment_target(lhs) &&
                  !(!strict_context() && is_call_assignment_target(lhs)) {
                  parse_fail(tokens, i, "invalid assignment target")
                }
                let rhs_exprs : Array[Expr] = [rhs]
                let mut idx = 1
                while idx < exprs.length() {
                  rhs_exprs.push(exprs[idx])
                  idx = idx + 1
                }
                let rhs_expr = if rhs_exprs.length() == 1 {
                  rhs_exprs[0]
                } else {
                  Sequence(rhs_exprs)
                }
                i = i + 1
                let (body, next_i2) = parse_statement(tokens, i, false, false)
                if stmt_is_function_decl(body) {
                  parse_fail(
                    tokens, index, "labelled function declaration not allowed",
                  )
                }
                return (ForIn(ForInit::Expr(lhs), rhs_expr, body), next_i2)
              }
              _ => ()
            }
          }
        _ => parse_fail(tokens, i, "expected ';'")
      }
    }
    if is_await {
      parse_fail(tokens, i, "expected 'of'")
    }
    if !match_punct(tokens, i, ';') {
      parse_fail(tokens, i, "expected ';'")
    }
    i = i + 1
  }
  if for_init_contains_private_in(init) {
    parse_fail(tokens, i, "unexpected token")
  }
  let mut test_expr : Expr? = None
  if match_punct(tokens, i, ';') {
    i = i + 1
  } else {
    let (expr, next_i) = parse_expression(tokens, i)
    test_expr = Some(expr)
    i = next_i
    if !match_punct(tokens, i, ';') {
      parse_fail(tokens, i, "expected ';'")
    }
    i = i + 1
  }
  let mut update_expr : Expr? = None
  if match_punct(tokens, i, ')') {
    i = i + 1
  } else {
    let (expr, next_i) = parse_expression(tokens, i)
    update_expr = Some(expr)
    i = next_i
    if !match_punct(tokens, i, ')') {
      parse_fail(tokens, i, "expected ')'")
    }
    i = i + 1
  }
  let (body, next_i2) = parse_statement(tokens, i, false, false)
  if stmt_is_function_decl(body) {
    parse_fail(tokens, index, "labelled function declaration not allowed")
  }
  (For(init, test_expr, update_expr, body), next_i2)
}

///|
fn parse_do_while(tokens : Array[Token], index : Int) -> (Stmt, Int) raise {
  let (body, next_i) = parse_statement(tokens, index + 1, false, false)
  if stmt_is_function_decl(body) {
    parse_fail(tokens, index, "labelled function declaration not allowed")
  }
  let mut i = next_i
  if !match_kind(tokens, i, KeywordWhile) {
    parse_fail(tokens, i, "expected 'while'")
  }
  i = i + 1
  if !match_punct(tokens, i, '(') {
    parse_fail(tokens, i, "expected '('")
  }
  let (cond_expr, next_i2) = parse_expression(tokens, i + 1)
  i = next_i2
  if !match_punct(tokens, i, ')') {
    parse_fail(tokens, i, "expected ')'")
  }
  i = i + 1
  if match_punct(tokens, i, ';') {
    i = i + 1
  }
  (DoWhile(body, cond_expr), i)
}

///|
fn parse_switch(tokens : Array[Token], index : Int) -> (Stmt, Int) raise {
  let mut i = index + 1
  if !match_punct(tokens, i, '(') {
    parse_fail(tokens, i, "expected '('")
  }
  let (discriminant, next_i) = parse_expression(tokens, i + 1)
  i = next_i
  if !match_punct(tokens, i, ')') {
    parse_fail(tokens, i, "expected ')'")
  }
  i = i + 1
  if !match_punct(tokens, i, '{') {
    parse_fail(tokens, i, "expected '{'")
  }
  i = i + 1
  let cases : Array[SwitchCase] = []
  let mut saw_default = false
  while !match_punct(tokens, i, '}') && !is_eof(tokens, i) {
    if match_kind(tokens, i, KeywordCase) {
      let (expr, next_i2) = parse_expression(tokens, i + 1)
      i = next_i2
      if !match_punct(tokens, i, ':') {
        parse_fail(tokens, i, "expected ':'")
      }
      i = i + 1
      let (stmts, next_i3) = parse_case_body(tokens, i)
      cases.push(SwitchCase::Case(expr, stmts))
      i = next_i3
      continue
    }
    if match_kind(tokens, i, KeywordDefault) {
      if saw_default {
        parse_fail(tokens, i, "duplicate default clause")
      }
      saw_default = true
      i = i + 1
      if !match_punct(tokens, i, ':') {
        parse_fail(tokens, i, "expected ':'")
      }
      i = i + 1
      let (stmts, next_i3) = parse_case_body(tokens, i)
      cases.push(SwitchCase::Default(stmts))
      i = next_i3
      continue
    }
    parse_fail(tokens, i, "expected case or default")
  }
  if !match_punct(tokens, i, '}') {
    parse_fail(tokens, i, "expected '}'")
  }
  (Switch(discriminant, cases), i + 1)
}

///|
fn parse_throw(tokens : Array[Token], index : Int) -> (Stmt, Int) raise {
  let mut i = index + 1
  if i < tokens.length() {
    let next_tok = peek(tokens, i)
    if next_tok.line_break_before {
      parse_fail(tokens, i, "line terminator not allowed after throw")
    }
  }
  let (expr, next_i) = parse_expression(tokens, i)
  i = next_i
  if match_punct(tokens, i, ';') {
    i = i + 1
  }
  (Throw(expr), i)
}

///|
fn parse_try(tokens : Array[Token], index : Int) -> (Stmt, Int) raise {
  let (try_body, next_i) = parse_block(tokens, index + 1)
  let mut i = next_i
  let mut catch_clause : CatchClause? = None
  if match_kind(tokens, i, KeywordCatch) {
    i = i + 1
    let mut name : String? = None
    let mut binding : VarBinding? = None
    let mut binding_offset = peek(tokens, i).offset
    if match_punct(tokens, i, '(') {
      let tok = peek(tokens, i + 1)
      binding_offset = tok.offset
      let allow_let = !strict_context()
      let (parsed, next_i) = parse_var_binding(
        tokens,
        i + 1,
        allow_let,
        true,
        true,
      )
      binding = Some(parsed)
      if !match_punct(tokens, next_i, ')') {
        parse_fail(tokens, next_i, "expected ')'")
      }
      i = next_i + 1
    }
    let (catch_body, next_i2) = parse_block(tokens, i)
    match binding {
      Some(value) => {
        let bound_names : Array[String] = []
        collect_binding_names(value, bound_names)
        if !bound_names.is_empty() {
          let lex_seen : Map[String, Bool] = Map::new()
          match catch_body {
            Block(stmts) =>
              for stmt in stmts {
                let lex_names : Array[String] = []
                collect_block_lex_names(stmt, lex_names, true)
                for name in lex_names {
                  lex_seen.set(name, true)
                }
              }
            _ => ()
          }
          for name in bound_names {
            if lex_seen.contains(name) {
              parse_fail(
                tokens, binding_offset, "duplicate lexical declaration",
              )
            }
          }
        }
      }
      None => ()
    }
    let mut body = catch_body
    match binding {
      Some(value) =>
        match value {
          VarBinding::Name(name_value) => name = Some(name_value)
          VarBinding::Target(_) => parse_fail(tokens, i, "expected identifier")
          _ => {
            let temp_name = "__catch" + Int::to_string(binding_offset)
            name = Some(temp_name)
            let init_expr = Expr::Ident(temp_name, binding_offset)
            let decl = Stmt::VarDecl(VarKind::Let, [(value, Some(init_expr))])
            body = match catch_body {
              Block(stmts) => {
                let merged : Array[Stmt] = [decl]
                for stmt in stmts {
                  merged.push(stmt)
                }
                Block(merged)
              }
              _ => catch_body
            }
          }
        }
      None => ()
    }
    catch_clause = Some(CatchClause::{ name, body })
    i = next_i2
  }
  let mut finally_body : Stmt? = None
  if match_kind(tokens, i, KeywordFinally) {
    let (final_block, next_i2) = parse_block(tokens, i + 1)
    finally_body = Some(final_block)
    i = next_i2
  }
  if catch_clause is None && finally_body is None {
    parse_fail(tokens, i, "expected catch or finally")
  }
  (Try(try_body, catch_clause, finally_body), i)
}

///|
fn parse_case_body(
  tokens : Array[Token],
  index : Int,
) -> (Array[Stmt], Int) raise {
  let stmts : Array[Stmt] = []
  let mut i = index
  while !is_eof(tokens, i) &&
        !match_punct(tokens, i, '}') &&
        !match_kind(tokens, i, KeywordCase) &&
        !match_kind(tokens, i, KeywordDefault) {
    let (stmt, next_i) = parse_statement(tokens, i, true, false)
    stmts.push(stmt)
    i = next_i
  }
  (stmts, i)
}

///|
fn parse_computed_obj_key(
  tokens : Array[Token],
  index : Int,
) -> (ObjKey, Int) raise {
  if !match_punct(tokens, index, '[') {
    parse_fail(tokens, index, "expected '['")
  }
  let (expr, next_i) = with_in_context(true, fn() raise {
    parse_assignment(tokens, index + 1)
  })
  if !match_punct(tokens, next_i, ']') {
    parse_fail(tokens, next_i, "expected ']'")
  }
  (ObjKey::Computed(expr), next_i + 1)
}

///|
fn parse_object_literal(
  tokens : Array[Token],
  index : Int,
) -> (Expr, Int) raise {
  let mut i = index + 1
  let props : Array[ObjectProp] = []
  let mut has_proto = false
  if !match_punct(tokens, i, '}') {
    for {
      let tok = peek(tokens, i)
      let mut handled = false
      if is_unescaped_ident(tokens, i, "async") &&
        !peek(tokens, i + 1).line_break_before {
        if match_punct(tokens, i + 1, '*') {
          let name_index = i + 2
          if match_punct(tokens, name_index, '[') {
            let (key, key_i) = parse_computed_obj_key(tokens, name_index)
            if match_punct(tokens, key_i, '(') {
              let (func, next_i) = parse_method_body(
                tokens,
                key_i,
                None,
                true,
                true,
                false,
                tok.offset,
              )
              props.push(ObjectProp::Method(key, func))
              i = next_i
              handled = true
            }
          } else if (
              is_identifier_name(peek(tokens, name_index).kind) ||
              match_kind(tokens, name_index, String) ||
              match_kind(tokens, name_index, Number) ||
              match_kind(tokens, name_index, PrivateName)
            ) &&
            match_punct(tokens, name_index + 1, '(') {
            let key_tok = peek(tokens, name_index)
            let key = match key_tok.kind {
              Ident => ObjKey::Ident(key_tok.lexeme)
              _ if is_identifier_name(key_tok.kind) =>
                ObjKey::Ident(key_tok.lexeme)
              String => ObjKey::String(key_tok.lexeme)
              Number => parse_numeric_obj_key(key_tok.lexeme, key_tok.offset)
              _ => parse_fail(tokens, name_index, "expected property name")
            }
            let key_name = obj_key_name(key)
            let (func, next_i) = parse_method_body(
              tokens,
              name_index + 1,
              Some(key_name),
              true,
              true,
              false,
              tok.offset,
            )
            props.push(ObjectProp::Method(key, func))
            i = next_i
            handled = true
          }
        } else if match_punct(tokens, i + 1, '[') {
          let (key, key_i) = parse_computed_obj_key(tokens, i + 1)
          if match_punct(tokens, key_i, '(') {
            let (func, next_i) = parse_method_body(
              tokens,
              key_i,
              None,
              false,
              true,
              false,
              tok.offset,
            )
            props.push(ObjectProp::Method(key, func))
            i = next_i
            handled = true
          }
        } else if (
            is_identifier_name(peek(tokens, i + 1).kind) ||
            match_kind(tokens, i + 1, String) ||
            match_kind(tokens, i + 1, Number) ||
            match_kind(tokens, i + 1, PrivateName)
          ) &&
          match_punct(tokens, i + 2, '(') {
          let key_tok = peek(tokens, i + 1)
          let key = match key_tok.kind {
            Ident => ObjKey::Ident(key_tok.lexeme)
            _ if is_identifier_name(key_tok.kind) =>
              ObjKey::Ident(key_tok.lexeme)
            String => ObjKey::String(key_tok.lexeme)
            Number => parse_numeric_obj_key(key_tok.lexeme, key_tok.offset)
            _ => parse_fail(tokens, i + 1, "expected property name")
          }
          let key_name = obj_key_name(key)
          let (func, next_i) = parse_method_body(
            tokens,
            i + 2,
            Some(key_name),
            false,
            true,
            false,
            tok.offset,
          )
          props.push(ObjectProp::Method(key, func))
          i = next_i
          handled = true
        }
        if handled {
          ()
        }
      }
      if !handled {
        let mut is_generator = false
        let mut method_offset = tok.offset
        if match_punct(tokens, i, '*') {
          is_generator = true
          method_offset = tok.offset
          i = i + 1
        }
        let tok = peek(tokens, i)
        match tok.kind {
          Ellipsis => {
            if is_generator {
              parse_fail(tokens, i, "expected method")
            }
            let (expr, next_i) = parse_assignment(tokens, i + 1)
            props.push(ObjectProp::Spread(expr))
            i = next_i
          }
          Ident => {
            let name = tok.lexeme
            let mut handled = false
            let is_get = !is_generator && is_unescaped_ident(tokens, i, "get")
            let is_set = !is_generator && is_unescaped_ident(tokens, i, "set")
            if is_get || is_set {
              if match_punct(tokens, i + 1, '[') {
                let (key, key_i) = parse_computed_obj_key(tokens, i + 1)
                if match_punct(tokens, key_i, '(') {
                  let is_getter = is_get
                  let (func, next_i) = parse_accessor(
                    tokens,
                    key_i,
                    None,
                    is_getter,
                    tok.offset,
                  )
                  if is_getter {
                    props.push(ObjectProp::Getter(key, func))
                  } else {
                    props.push(ObjectProp::Setter(key, func))
                  }
                  i = next_i
                  handled = true
                }
              } else if (
                  is_identifier_name(peek(tokens, i + 1).kind) ||
                  match_kind(tokens, i + 1, String) ||
                  match_kind(tokens, i + 1, Number)
                ) &&
                match_punct(tokens, i + 2, '(') {
                let key_tok = peek(tokens, i + 1)
                let key = match key_tok.kind {
                  Ident => ObjKey::Ident(key_tok.lexeme)
                  _ if is_identifier_name(key_tok.kind) =>
                    ObjKey::Ident(key_tok.lexeme)
                  String => ObjKey::String(key_tok.lexeme)
                  Number =>
                    parse_numeric_obj_key(key_tok.lexeme, key_tok.offset)
                  _ => parse_fail(tokens, i + 1, "expected property name")
                }
                let key_name = obj_key_name(key)
                let is_getter = is_get
                let (func, next_i) = parse_accessor(
                  tokens,
                  i + 2,
                  Some(key_name),
                  is_getter,
                  tok.offset,
                )
                if is_getter {
                  props.push(ObjectProp::Getter(key, func))
                } else {
                  props.push(ObjectProp::Setter(key, func))
                }
                i = next_i
                handled = true
              }
            }
            if handled {
              ()
            } else if match_punct(tokens, i + 1, '(') {
              let (func, next_i) = parse_method_body(
                tokens,
                i + 1,
                Some(name),
                is_generator,
                false,
                false,
                method_offset,
              )
              props.push(ObjectProp::Method(ObjKey::Ident(name), func))
              i = next_i
            } else {
              if is_generator {
                parse_fail(tokens, i, "expected method")
              }
              if match_punct(tokens, i + 1, ':') {
                let (value, next_i) = parse_assignment(tokens, i + 2)
                if name == "__proto__" {
                  if has_proto {
                    parse_fail(tokens, i, "duplicate __proto__ property name")
                  }
                  has_proto = true
                }
                props.push(ObjectProp::KeyValue(ObjKey::Ident(name), value))
                i = next_i
              } else {
                props.push(ObjectProp::Shorthand(name))
                i = i + 1
              }
            }
          }
          _ if is_identifier_name(tok.kind) => {
            let name = tok.lexeme
            if match_punct(tokens, i + 1, '(') {
              let (func, next_i) = parse_method_body(
                tokens,
                i + 1,
                Some(name),
                is_generator,
                false,
                false,
                method_offset,
              )
              props.push(ObjectProp::Method(ObjKey::Ident(name), func))
              i = next_i
            } else {
              if is_generator {
                parse_fail(tokens, i, "expected method")
              }
              if match_punct(tokens, i + 1, ':') {
                let (value, next_i) = parse_assignment(tokens, i + 2)
                if name == "__proto__" {
                  if has_proto {
                    parse_fail(tokens, i, "duplicate __proto__ property name")
                  }
                  has_proto = true
                }
                props.push(ObjectProp::KeyValue(ObjKey::Ident(name), value))
                i = next_i
              } else if tok.kind == KeywordLet ||
                tok.kind == KeywordStatic ||
                (tok.kind == KeywordYield && yield_ident_allowed()) ||
                (tok.kind == KeywordAwait && await_ident_allowed()) ||
                tok.kind == KeywordOf {
                props.push(ObjectProp::Shorthand(name))
                i = i + 1
              } else {
                parse_fail(tokens, i + 1, "expected ':'")
              }
            }
          }
          String => {
            let key = tok.lexeme
            if match_punct(tokens, i + 1, '(') {
              let (func, next_i) = parse_method_body(
                tokens,
                i + 1,
                Some(key),
                is_generator,
                false,
                false,
                method_offset,
              )
              props.push(ObjectProp::Method(ObjKey::String(key), func))
              i = next_i
            } else {
              if is_generator {
                parse_fail(tokens, i, "expected method")
              }
              if !match_punct(tokens, i + 1, ':') {
                parse_fail(tokens, i + 1, "expected ':'")
              }
              let (value, next_i) = parse_assignment(tokens, i + 2)
              if key == "__proto__" {
                if has_proto {
                  parse_fail(tokens, i, "duplicate __proto__ property name")
                }
                has_proto = true
              }
              props.push(ObjectProp::KeyValue(ObjKey::String(key), value))
              i = next_i
            }
          }
          Number => {
            let key = parse_numeric_obj_key(tok.lexeme, tok.offset)
            if match_punct(tokens, i + 1, '(') {
              let key_name = obj_key_name(key)
              let (func, next_i) = parse_method_body(
                tokens,
                i + 1,
                Some(key_name),
                is_generator,
                false,
                false,
                method_offset,
              )
              props.push(ObjectProp::Method(key, func))
              i = next_i
            } else {
              if is_generator {
                parse_fail(tokens, i, "expected method")
              }
              if !match_punct(tokens, i + 1, ':') {
                parse_fail(tokens, i + 1, "expected ':'")
              }
              let (value, next_i) = parse_assignment(tokens, i + 2)
              props.push(ObjectProp::KeyValue(key, value))
              i = next_i
            }
          }
          Punct('[') => {
            let (key, key_i) = parse_computed_obj_key(tokens, i)
            if match_punct(tokens, key_i, '(') {
              let (func, next_i) = parse_method_body(
                tokens,
                key_i,
                None,
                is_generator,
                false,
                false,
                method_offset,
              )
              props.push(ObjectProp::Method(key, func))
              i = next_i
            } else {
              if is_generator {
                parse_fail(tokens, i, "expected method")
              }
              if !match_punct(tokens, key_i, ':') {
                parse_fail(tokens, key_i, "expected ':'")
              }
              let (value, next_i) = parse_assignment(tokens, key_i + 1)
              props.push(ObjectProp::KeyValue(key, value))
              i = next_i
            }
          }
          _ => parse_fail(tokens, i, "expected property")
        }
      }
      if match_punct(tokens, i, ',') {
        i = i + 1
        if match_punct(tokens, i, '}') {
          break
        }
        continue
      }
      break
    }
  }
  if !match_punct(tokens, i, '}') {
    parse_fail(tokens, i, "expected '}'")
  }
  (ObjectLiteral(props), i + 1)
}

///|
fn is_binding_identifier_token(
  kind : TokenKind,
  allow_let : Bool,
  allow_yield : Bool,
  allow_await : Bool,
) -> Bool {
  match kind {
    Ident => true
    KeywordLet => allow_let
    KeywordYield => allow_yield && yield_ident_allowed()
    KeywordAwait => allow_await && await_ident_allowed()
    KeywordOf => true
    KeywordStatic => true
    KeywordImplements
    | KeywordInterface
    | KeywordPackage
    | KeywordPrivate
    | KeywordProtected
    | KeywordPublic => true
    _ => false
  }
}

///|
fn parse_binding_identifier(
  tokens : Array[Token],
  index : Int,
  allow_let : Bool,
  allow_yield : Bool,
  allow_await : Bool,
) -> (String, Int) raise {
  let tok = peek(tokens, index)
  let kind = binding_identifier_kind(tok)
  let allow_yield_here = allow_yield && yield_ident_allowed()
  let allow_await_here = allow_await && await_ident_allowed()
  if is_binding_identifier_token(
      kind, allow_let, allow_yield_here, allow_await_here,
    ) {
    (tok.lexeme, index + 1)
  } else {
    parse_fail(tokens, index, "expected identifier")
  }
}

///|
fn parse_array_pattern(
  tokens : Array[Token],
  index : Int,
  allow_let : Bool,
  allow_yield : Bool,
  allow_await : Bool,
  allow_assignment_target : Bool,
) -> (Array[ArrayPatternElem], Int) raise {
  if !match_punct(tokens, index, '[') {
    parse_fail(tokens, index, "expected '['")
  }
  let elems : Array[ArrayPatternElem] = []
  let mut i = index + 1
  let mut saw_rest = false
  let parse_binding = if allow_assignment_target {
    parse_assignment_binding
  } else {
    parse_var_binding
  }
  if !match_punct(tokens, i, ']') {
    for {
      if match_punct(tokens, i, ',') {
        elems.push(ArrayPatternElem::Hole)
        i = i + 1
        if match_punct(tokens, i, ']') {
          break
        }
        continue
      }
      if match_kind(tokens, i, Ellipsis) {
        if saw_rest {
          parse_fail(tokens, i, "duplicate rest element")
        }
        let (binding, next_i) = parse_binding(
          tokens,
          i + 1,
          allow_let,
          allow_yield,
          allow_await,
        )
        elems.push(ArrayPatternElem::Rest(binding))
        i = next_i
        saw_rest = true
      } else {
        let (binding, next_i) = parse_binding(
          tokens, i, allow_let, allow_yield, allow_await,
        )
        i = next_i
        let mut init : Expr? = None
        if match_punct(tokens, i, '=') {
          let (expr, next_i) = parse_assignment(tokens, i + 1)
          init = Some(expr)
          i = next_i
        }
        elems.push(ArrayPatternElem::Bind(binding, init))
      }
      if match_punct(tokens, i, ',') {
        if saw_rest {
          parse_fail(tokens, i, "rest element must be last")
        }
        i = i + 1
        if match_punct(tokens, i, ']') {
          break
        }
        continue
      }
      break
    }
  }
  if !match_punct(tokens, i, ']') {
    parse_fail(tokens, i, "expected ']'")
  }
  (elems, i + 1)
}

///|
fn parse_object_pattern(
  tokens : Array[Token],
  index : Int,
  allow_let : Bool,
  allow_yield : Bool,
  allow_await : Bool,
  allow_assignment_target : Bool,
) -> (Array[ObjectPatternElem], Int) raise {
  if !match_punct(tokens, index, '{') {
    parse_fail(tokens, index, "expected '{'")
  }
  let elems : Array[ObjectPatternElem] = []
  let mut i = index + 1
  let mut saw_rest = false
  let parse_binding = if allow_assignment_target {
    parse_assignment_binding
  } else {
    parse_var_binding
  }
  if !match_punct(tokens, i, '}') {
    for {
      if match_kind(tokens, i, Ellipsis) {
        if saw_rest {
          parse_fail(tokens, i, "duplicate rest element")
        }
        let (binding, next_i) = parse_binding(
          tokens,
          i + 1,
          allow_let,
          allow_yield,
          allow_await,
        )
        if !allow_assignment_target {
          match binding {
            VarBinding::Name(_) => ()
            _ => parse_fail(tokens, i + 1, "expected identifier")
          }
        }
        elems.push(ObjectPatternElem::Rest(binding))
        i = next_i
        saw_rest = true
      } else {
        let tok = peek(tokens, i)
        let mut key = ObjKey::Ident("")
        let mut binding : VarBinding? = None
        if match_punct(tokens, i, '[') {
          let (parsed_key, next_i) = parse_computed_obj_key(tokens, i)
          key = parsed_key
          i = next_i
          if !match_punct(tokens, i, ':') {
            parse_fail(tokens, i, "expected ':'")
          }
          let (next_binding, next_i) = parse_binding(
            tokens,
            i + 1,
            allow_let,
            allow_yield,
            allow_await,
          )
          binding = Some(next_binding)
          i = next_i
        } else {
          key = match tok.kind {
            String => ObjKey::String(tok.lexeme)
            Number => parse_numeric_obj_key(tok.lexeme, tok.offset)
            _ if is_identifier_name(tok.kind) => ObjKey::Ident(tok.lexeme)
            _ => parse_fail(tokens, i, "expected property name")
          }
          i = i + 1
          if match_punct(tokens, i, ':') {
            let (next_binding, next_i) = parse_binding(
              tokens,
              i + 1,
              allow_let,
              allow_yield,
              allow_await,
            )
            binding = Some(next_binding)
            i = next_i
          } else {
            match key {
              ObjKey::String(_) | ObjKey::Number(_) =>
                parse_fail(tokens, i, "expected ':'")
              _ => ()
            }
            let name_kind = binding_identifier_kind(tok)
            if !is_binding_identifier_token(
                name_kind, allow_let, allow_yield, allow_await,
              ) {
              parse_fail(tokens, i - 1, "expected identifier")
            }
            binding = Some(VarBinding::Name(obj_key_name(key)))
          }
        }
        let bound = match binding {
          Some(value) => value
          None => parse_fail(tokens, i, "expected identifier")
        }
        let mut init : Expr? = None
        if match_punct(tokens, i, '=') {
          let (expr, next_i) = parse_assignment(tokens, i + 1)
          init = Some(expr)
          i = next_i
        }
        elems.push(ObjectPatternElem::Bind(key, bound, init))
      }
      if match_punct(tokens, i, ',') {
        if saw_rest {
          parse_fail(tokens, i, "rest element must be last")
        }
        i = i + 1
        if match_punct(tokens, i, '}') {
          break
        }
        continue
      }
      break
    }
  }
  if !match_punct(tokens, i, '}') {
    parse_fail(tokens, i, "expected '}'")
  }
  (elems, i + 1)
}

///|
fn parse_var_binding(
  tokens : Array[Token],
  index : Int,
  allow_let : Bool,
  allow_yield : Bool,
  allow_await : Bool,
) -> (VarBinding, Int) raise {
  let tok = peek(tokens, index)
  let kind = binding_identifier_kind(tok)
  let allow_yield_here = allow_yield && yield_ident_allowed()
  let allow_await_here = allow_await && await_ident_allowed()
  if is_binding_identifier_token(
      kind, allow_let, allow_yield_here, allow_await_here,
    ) {
    let (name, next_i) = parse_binding_identifier(
      tokens, index, allow_let, allow_yield, allow_await,
    )
    (VarBinding::Name(name), next_i)
  } else {
    match tok.kind {
      Punct('[') => {
        let (pattern, next_i) = parse_array_pattern(
          tokens, index, allow_let, allow_yield, allow_await, false,
        )
        (VarBinding::ArrayPattern(pattern), next_i)
      }
      Punct('{') => {
        let (pattern, next_i) = parse_object_pattern(
          tokens, index, allow_let, allow_yield, allow_await, false,
        )
        (VarBinding::ObjectPattern(pattern), next_i)
      }
      _ => parse_fail(tokens, index, "expected identifier")
    }
  }
}

///|
fn parse_assignment_binding(
  tokens : Array[Token],
  index : Int,
  allow_let : Bool,
  allow_yield : Bool,
  allow_await : Bool,
) -> (VarBinding, Int) raise {
  let tok = peek(tokens, index)
  match tok.kind {
    Punct('[') =>
      match find_matching_punct(tokens, index, '[', ']') {
        Some(end_i) =>
          if match_punct(tokens, end_i + 1, '.') ||
            match_punct(tokens, end_i + 1, '[') {
            let (expr, next_i) = parse_member(tokens, index)
            if !is_simple_assignment_target(expr) {
              parse_fail(tokens, index, "invalid assignment target")
            }
            (VarBinding::Target(expr), next_i)
          } else {
            let (pattern, next_i) = parse_array_pattern(
              tokens, index, allow_let, allow_yield, allow_await, true,
            )
            (VarBinding::ArrayPattern(pattern), next_i)
          }
        None => {
          let (pattern, next_i) = parse_array_pattern(
            tokens, index, allow_let, allow_yield, allow_await, true,
          )
          (VarBinding::ArrayPattern(pattern), next_i)
        }
      }
    Punct('{') =>
      match find_matching_punct(tokens, index, '{', '}') {
        Some(end_i) =>
          if match_punct(tokens, end_i + 1, '.') ||
            match_punct(tokens, end_i + 1, '[') {
            let (expr, next_i) = parse_member(tokens, index)
            if !is_simple_assignment_target(expr) {
              parse_fail(tokens, index, "invalid assignment target")
            }
            (VarBinding::Target(expr), next_i)
          } else {
            let (pattern, next_i) = parse_object_pattern(
              tokens, index, allow_let, allow_yield, allow_await, true,
            )
            (VarBinding::ObjectPattern(pattern), next_i)
          }
        None => {
          let (pattern, next_i) = parse_object_pattern(
            tokens, index, allow_let, allow_yield, allow_await, true,
          )
          (VarBinding::ObjectPattern(pattern), next_i)
        }
      }
    Ident | KeywordLet | KeywordYield | KeywordAwait | KeywordOf =>
      if match_punct(tokens, index + 1, '.') ||
        match_punct(tokens, index + 1, '[') ||
        match_punct(tokens, index + 1, '(') ||
        match_kind(tokens, index + 1, QuestionMarkDot) {
        let (expr, next_i) = parse_left_hand_side_expr(tokens, index)
        if !is_simple_assignment_target(expr) {
          parse_fail(tokens, index, "invalid assignment target")
        }
        match expr {
          Ident(name, _) => (VarBinding::Name(name), next_i)
          _ => (VarBinding::Target(expr), next_i)
        }
      } else {
        let (name, next_i) = parse_binding_identifier(
          tokens, index, allow_let, allow_yield, allow_await,
        )
        (VarBinding::Name(name), next_i)
      }
    _ => {
      let (expr, next_i) = parse_left_hand_side_expr(tokens, index)
      if !is_simple_assignment_target(expr) {
        parse_fail(tokens, index, "invalid assignment target")
      }
      match expr {
        Ident(name, _) => (VarBinding::Name(name), next_i)
        _ => (VarBinding::Target(expr), next_i)
      }
    }
  }
}

///|
fn find_matching_punct(
  tokens : Array[Token],
  index : Int,
  open : Char,
  close : Char,
) -> Int? {
  let mut depth = 0
  let mut i = index
  while i < tokens.length() {
    if match_punct(tokens, i, open) {
      depth = depth + 1
    } else if match_punct(tokens, i, close) {
      depth = depth - 1
      if depth == 0 {
        return Some(i)
      }
    }
    i = i + 1
  }
  None
}

///|
fn parse_accessor(
  tokens : Array[Token],
  index : Int,
  name : String?,
  is_getter : Bool,
  offset : Int,
) -> (FunctionDef, Int) raise {
  let (params, param_inits, next_i) = with_await_context(false, true, fn() raise {
    with_yield_context(false, true, fn() raise {
      parse_parameters(tokens, index, true, true, false)
    })
  })
  let i = next_i
  if is_getter && !params.is_empty() {
    parse_fail(tokens, i, "invalid getter")
  }
  if !is_getter && (params.length() != 1 || params[0].is_rest) {
    parse_fail(tokens, i, "invalid setter")
  }
  let outer_strict = strict_context()
  let parse_body = fn() raise {
    with_return_context(true, fn() raise {
      with_await_context(false, true, fn() raise {
        with_yield_context(false, true, fn() raise { parse_block(tokens, i) })
      })
    })
  }
  let (body_stmt, next_i0) = with_strict_context(outer_strict, fn() raise {
    parse_body()
  })
  let mut next_i = next_i0
  let mut body = match body_stmt {
    Block(stmts) => stmts
    _ => parse_fail(tokens, i, "expected block")
  }
  let mut use_strict = has_use_strict(body)
  if use_strict && !outer_strict {
    let (strict_body_stmt, strict_next_i) = with_strict_context(true, fn() raise {
      parse_body()
    })
    body = match strict_body_stmt {
      Block(stmts) => stmts
      _ => parse_fail(tokens, i, "expected block")
    }
    next_i = strict_next_i
    use_strict = has_use_strict(body)
  }
  let is_strict = use_strict || outer_strict
  if is_strict && stmts_contain_with(body) {
    parse_fail(tokens, index, "with not allowed in strict mode")
  }
  check_function_params(
    tokens, index, name, params, param_inits, is_strict, use_strict, false, true,
    false, false,
  )
  check_param_lexical_conflicts(tokens, index, params, body)
  let end_offset = if next_i > 0 {
    token_end_offset(tokens, next_i - 1)
  } else {
    offset
  }
  (
    FunctionDef::{
      name,
      params,
      param_inits,
      body,
      is_strict,
      is_generator: false,
      is_async: false,
      is_arrow: false,
      offset,
      end_offset,
    },
    next_i,
  )
}

///|
fn parse_param_binding(
  tokens : Array[Token],
  index : Int,
  allow_yield : Bool,
  allow_await : Bool,
) -> (VarBinding, Int) raise {
  let tok = peek(tokens, index)
  match tok.kind {
    Punct('[') => {
      let (pattern, next_i) = parse_array_pattern(
        tokens, index, false, allow_yield, allow_await, false,
      )
      (VarBinding::ArrayPattern(pattern), next_i)
    }
    Punct('{') => {
      let (pattern, next_i) = parse_object_pattern(
        tokens, index, false, allow_yield, allow_await, false,
      )
      (VarBinding::ObjectPattern(pattern), next_i)
    }
    _ => {
      let allow_let = !strict_context()
      let (name, next_i) = parse_binding_identifier(
        tokens, index, allow_let, allow_yield, allow_await,
      )
      (VarBinding::Name(name), next_i)
    }
  }
}

///|
fn parse_parameters(
  tokens : Array[Token],
  index : Int,
  allow_yield : Bool,
  allow_await : Bool,
  allow_await_override_in_init : Bool,
) -> (Array[ParamBinding], Array[Expr?], Int) raise {
  if !match_punct(tokens, index, '(') {
    parse_fail(tokens, index, "expected '('")
  }
  let params : Array[ParamBinding] = []
  let param_inits : Array[Expr?] = []
  let mut i = index + 1
  let mut saw_rest = false
  if !match_punct(tokens, i, ')') {
    for {
      if match_kind(tokens, i, Ellipsis) {
        if saw_rest {
          parse_fail(tokens, i, "duplicate rest element")
        }
        let (binding, next_i) = parse_param_binding(
          tokens,
          i + 1,
          allow_yield,
          allow_await,
        )
        if match_punct(tokens, next_i, '=') {
          parse_fail(tokens, next_i, "rest element cannot have initializer")
        }
        params.push(ParamBinding::{ binding, is_rest: true })
        param_inits.push(None)
        i = next_i
        saw_rest = true
      } else {
        let (binding, next_i) = parse_param_binding(
          tokens, i, allow_yield, allow_await,
        )
        i = next_i
        let mut init : Expr? = None
        if match_punct(tokens, i, '=') {
          let (expr, after_init) = if allow_await_override_in_init {
            with_await_param_override(true, fn() raise {
              parse_assignment(tokens, i + 1)
            })
          } else {
            parse_assignment(tokens, i + 1)
          }
          init = Some(expr)
          i = after_init
        }
        params.push(ParamBinding::{ binding, is_rest: false })
        param_inits.push(init)
      }
      if match_punct(tokens, i, ',') {
        if saw_rest {
          parse_fail(tokens, i, "rest element must be last")
        }
        i = i + 1
        if match_punct(tokens, i, ')') {
          break
        }
        continue
      }
      break
    }
  }
  if !match_punct(tokens, i, ')') {
    parse_fail(tokens, i, "expected ')'")
  }
  (params, param_inits, i + 1)
}

///|
fn is_strict_future_keyword(name : String) -> Bool {
  name == "implements" ||
  name == "interface" ||
  name == "let" ||
  name == "package" ||
  name == "private" ||
  name == "protected" ||
  name == "public" ||
  name == "static" ||
  name == "yield"
}

///|
fn is_strict_reserved_name(name : String) -> Bool {
  name == "eval" ||
  name == "arguments" ||
  name == "let" ||
  name == "static" ||
  name == "yield" ||
  is_strict_future_keyword(name)
}

///|
fn var_binding_has_default(binding : VarBinding) -> Bool {
  match binding {
    VarBinding::Name(_) => false
    VarBinding::ArrayPattern(pattern) => {
      for elem in pattern {
        match elem {
          ArrayPatternElem::Bind(_, Some(_)) => return true
          ArrayPatternElem::Bind(inner, None) =>
            if var_binding_has_default(inner) {
              return true
            }
          ArrayPatternElem::Rest(inner) =>
            if var_binding_has_default(inner) {
              return true
            }
          ArrayPatternElem::Hole => ()
        }
      }
      false
    }
    VarBinding::ObjectPattern(pattern) => {
      for elem in pattern {
        match elem {
          ObjectPatternElem::Bind(_, _, Some(_)) => return true
          ObjectPatternElem::Bind(_, inner, None) =>
            if var_binding_has_default(inner) {
              return true
            }
          ObjectPatternElem::Rest(inner) =>
            if var_binding_has_default(inner) {
              return true
            }
        }
      }
      false
    }
    VarBinding::Target(_) => false
  }
}

///|
fn is_simple_parameter_list(
  params : Array[ParamBinding],
  param_inits : Array[Expr?],
) -> Bool {
  for i = 0; i < params.length(); i = i + 1 {
    let param = params[i]
    if param.is_rest {
      return false
    }
    match param.binding {
      VarBinding::Name(_) => ()
      _ => return false
    }
    match param_inits[i] {
      Some(_) => return false
      None => ()
    }
    if var_binding_has_default(param.binding) {
      return false
    }
  }
  true
}

///|
fn collect_binding_names(binding : VarBinding, out : Array[String]) -> Unit {
  match binding {
    VarBinding::Name(name) => out.push(name)
    VarBinding::ArrayPattern(pattern) =>
      for elem in pattern {
        match elem {
          ArrayPatternElem::Bind(inner, _) => collect_binding_names(inner, out)
          ArrayPatternElem::Rest(inner) => collect_binding_names(inner, out)
          ArrayPatternElem::Hole => ()
        }
      }
    VarBinding::ObjectPattern(pattern) =>
      for elem in pattern {
        match elem {
          ObjectPatternElem::Bind(_, inner, _) =>
            collect_binding_names(inner, out)
          ObjectPatternElem::Rest(inner) => collect_binding_names(inner, out)
        }
      }
    VarBinding::Target(_) => ()
  }
}

///|
fn params_have_duplicates(names : Array[String]) -> Bool {
  let seen : Map[String, Bool] = Map::new()
  for name in names {
    if name.is_empty() {
      continue
    }
    if seen.contains(name) {
      return true
    }
    seen.set(name, true)
  }
  false
}

///|
fn obj_key_contains_await(key : ObjKey) -> Bool {
  match key {
    ObjKey::Computed(expr) => expr_contains_await(expr)
    _ => false
  }
}

///|
fn member_key_contains_await(key : MemberKey) -> Bool {
  match key {
    MemberKey::Computed(expr) => expr_contains_await(expr)
    _ => false
  }
}

///|
fn expr_contains_await(expr : Expr) -> Bool {
  match expr {
    Await(_, _) => true
    Yield(inner, _) =>
      match inner {
        Some(value) => expr_contains_await(value)
        None => false
      }
    Unary(_, inner, _) => expr_contains_await(inner)
    Update(_, inner, _, _) => expr_contains_await(inner)
    New(callee, args) =>
      if expr_contains_await(callee) {
        true
      } else {
        for arg in args {
          if expr_contains_await(arg) {
            return true
          }
        }
        false
      }
    Binary(_, left, right, _) =>
      expr_contains_await(left) || expr_contains_await(right)
    Conditional(test_expr, conseq, alt) =>
      expr_contains_await(test_expr) ||
      expr_contains_await(conseq) ||
      expr_contains_await(alt)
    Sequence(exprs) => {
      for value in exprs {
        if expr_contains_await(value) {
          return true
        }
      }
      false
    }
    Paren(inner) => expr_contains_await(inner)
    Assign(_, rhs, _, _) => expr_contains_await(rhs)
    AssignInvalid(lhs, rhs, _) =>
      expr_contains_await(lhs) || expr_contains_await(rhs)
    AssignArray(pattern, rhs) =>
      array_pattern_contains_await(pattern) || expr_contains_await(rhs)
    AssignObject(pattern, rhs) =>
      object_pattern_contains_await(pattern) || expr_contains_await(rhs)
    AssignOp(_, left, right, _) =>
      expr_contains_await(left) || expr_contains_await(right)
    AssignMember(obj, key, rhs, _) =>
      expr_contains_await(obj) ||
      member_key_contains_await(key) ||
      expr_contains_await(rhs)
    Call(callee, args, _) =>
      if expr_contains_await(callee) {
        true
      } else {
        for arg in args {
          if expr_contains_await(arg) {
            return true
          }
        }
        false
      }
    Member(obj, key, _) =>
      expr_contains_await(obj) || member_key_contains_await(key)
    OptionalChain(base, segments) => {
      if expr_contains_await(base) {
        return true
      }
      for segment in segments {
        match segment {
          ChainSegment::Member(key, _, _) =>
            if member_key_contains_await(key) {
              return true
            }
          ChainSegment::Call(args, _, _) =>
            for arg in args {
              if expr_contains_await(arg) {
                return true
              }
            }
        }
      }
      false
    }
    FunctionExpr(func) =>
      if func.is_arrow {
        params_contain_await_expr(func.params, func.param_inits)
      } else {
        false
      }
    ClassExpr(class_def) => class_def_contains_await(class_def)
    ObjectLiteral(props) => {
      for prop in props {
        match prop {
          ObjectProp::KeyValue(key, value) =>
            if obj_key_contains_await(key) || expr_contains_await(value) {
              return true
            }
          ObjectProp::Method(key, _) =>
            if obj_key_contains_await(key) {
              return true
            }
          ObjectProp::Getter(key, _) =>
            if obj_key_contains_await(key) {
              return true
            }
          ObjectProp::Setter(key, _) =>
            if obj_key_contains_await(key) {
              return true
            }
          ObjectProp::Spread(inner) =>
            if expr_contains_await(inner) {
              return true
            }
          ObjectProp::Shorthand(_) => ()
        }
      }
      false
    }
    ArrayLiteral(elems) => {
      for elem in elems {
        match elem {
          Some(value) => if expr_contains_await(value) { return true }
          None => ()
        }
      }
      false
    }
    Spread(inner) => expr_contains_await(inner)
    TemplateLiteral(_, exprs) => {
      for value in exprs {
        if expr_contains_await(value) {
          return true
        }
      }
      false
    }
    TaggedTemplate(callee, _, exprs) => {
      if expr_contains_await(callee) {
        return true
      }
      for value in exprs {
        if expr_contains_await(value) {
          return true
        }
      }
      false
    }
    _ => false
  }
}

///|
fn class_def_contains_await(class_def : ClassDef) -> Bool {
  match class_def.name {
    Some(name) => if name == "await" { return true }
    None => ()
  }
  match class_def.super_class {
    Some(expr) => if expr_contains_await(expr) { return true }
    None => ()
  }
  for class_member in class_def.body {
    match class_member {
      ClassMember::Method(key, _, _)
      | ClassMember::Getter(key, _, _)
      | ClassMember::Setter(key, _, _) =>
        if obj_key_contains_await(key) {
          return true
        }
      ClassMember::Field(key, init, _) => {
        if obj_key_contains_await(key) {
          return true
        }
        match init {
          Some(value) => if expr_contains_await(value) { return true }
          None => ()
        }
      }
      ClassMember::StaticBlock(stmts) =>
        if stmts_contain_await(stmts) {
          return true
        }
    }
  }
  false
}

///|
fn class_def_contains_yield(class_def : ClassDef) -> Bool {
  match class_def.name {
    Some(name) => if name == "yield" { return true }
    None => ()
  }
  match class_def.super_class {
    Some(expr) => if expr_contains_yield(expr) { return true }
    None => ()
  }
  for class_member in class_def.body {
    match class_member {
      ClassMember::Method(key, _, _)
      | ClassMember::Getter(key, _, _)
      | ClassMember::Setter(key, _, _) =>
        if obj_key_contains_yield(key) {
          return true
        }
      ClassMember::Field(key, init, _) => {
        if obj_key_contains_yield(key) {
          return true
        }
        match init {
          Some(value) => if expr_contains_yield(value) { return true }
          None => ()
        }
      }
      ClassMember::StaticBlock(stmts) =>
        if stmts_contain_yield(stmts) {
          return true
        }
    }
  }
  false
}

///|
fn stmts_contain_await(stmts : Array[Stmt]) -> Bool {
  for stmt in stmts {
    if stmt_contains_await(stmt) {
      return true
    }
  }
  false
}

///|
fn stmt_contains_await(stmt : Stmt) -> Bool {
  match stmt {
    ExprStmt(expr) => expr_contains_await(expr)
    VarDecl(_, decls) => {
      for decl in decls {
        let (binding, init) = decl
        if var_binding_contains_await(binding) {
          return true
        }
        match init {
          Some(expr) => if expr_contains_await(expr) { return true }
          None => ()
        }
      }
      false
    }
    Block(stmts) => stmts_contain_await(stmts)
    Label(_, body) => stmt_contains_await(body)
    FunctionDecl(func) =>
      match func.name {
        Some(name) => name == "await"
        None => false
      }
    ClassDecl(class_def) => class_def_contains_await(class_def)
    Return(expr_opt) =>
      match expr_opt {
        Some(expr) => expr_contains_await(expr)
        None => false
      }
    If(cond_expr, conseq, alt) =>
      if expr_contains_await(cond_expr) || stmt_contains_await(conseq) {
        true
      } else {
        match alt {
          Some(value) => stmt_contains_await(value)
          None => false
        }
      }
    With(expr, body) => expr_contains_await(expr) || stmt_contains_await(body)
    While(expr, body) => expr_contains_await(expr) || stmt_contains_await(body)
    For(init, test_expr, update, body) =>
      for_init_contains_await(init) ||
      (match test_expr {
        Some(expr) => expr_contains_await(expr)
        None => false
      }) ||
      (match update {
        Some(expr) => expr_contains_await(expr)
        None => false
      }) ||
      stmt_contains_await(body)
    ForIn(init, expr, body) =>
      for_init_contains_await(init) ||
      expr_contains_await(expr) ||
      stmt_contains_await(body)
    ForOf(init, expr, body) =>
      for_init_contains_await(init) ||
      expr_contains_await(expr) ||
      stmt_contains_await(body)
    ForAwaitOf(init, expr, body) =>
      for_init_contains_arguments(init) ||
      expr_contains_arguments(expr) ||
      stmt_contains_arguments(body)
    DoWhile(body, expr) =>
      stmt_contains_await(body) || expr_contains_await(expr)
    Switch(discriminant, cases) => {
      if expr_contains_await(discriminant) {
        return true
      }
      for clause in cases {
        match clause {
          SwitchCase::Case(expr, body) =>
            if expr_contains_await(expr) || stmts_contain_await(body) {
              return true
            }
          SwitchCase::Default(body) =>
            if stmts_contain_await(body) {
              return true
            }
        }
      }
      false
    }
    Throw(expr) => expr_contains_await(expr)
    Try(try_body, catch_clause, finally_body) =>
      if stmt_contains_await(try_body) {
        true
      } else {
        let catch_has_await = match catch_clause {
          Some(clause) => stmt_contains_await(clause.body)
          None => false
        }
        if catch_has_await {
          true
        } else {
          match finally_body {
            Some(value) => stmt_contains_await(value)
            None => false
          }
        }
      }
    _ => false
  }
}

///|
fn stmts_contain_yield(stmts : Array[Stmt]) -> Bool {
  for stmt in stmts {
    if stmt_contains_yield(stmt) {
      return true
    }
  }
  false
}

///|
fn stmts_contain_return(stmts : Array[Stmt]) -> Bool {
  for stmt in stmts {
    if stmt_contains_return(stmt) {
      return true
    }
  }
  false
}

///|
fn stmt_contains_yield(stmt : Stmt) -> Bool {
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
    Return(expr_opt) =>
      match expr_opt {
        Some(expr) => expr_contains_yield(expr)
        None => false
      }
    If(cond_expr, conseq, alt) =>
      if expr_contains_yield(cond_expr) || stmt_contains_yield(conseq) {
        true
      } else {
        match alt {
          Some(value) => stmt_contains_yield(value)
          None => false
        }
      }
    With(expr, body) => expr_contains_yield(expr) || stmt_contains_yield(body)
    While(expr, body) => expr_contains_yield(expr) || stmt_contains_yield(body)
    For(init, test_expr, update, body) =>
      for_init_contains_yield(init) ||
      (match test_expr {
        Some(expr) => expr_contains_yield(expr)
        None => false
      }) ||
      (match update {
        Some(expr) => expr_contains_yield(expr)
        None => false
      }) ||
      stmt_contains_yield(body)
    ForIn(init, expr, body) =>
      for_init_contains_yield(init) ||
      expr_contains_yield(expr) ||
      stmt_contains_yield(body)
    ForOf(init, expr, body) =>
      for_init_contains_yield(init) ||
      expr_contains_yield(expr) ||
      stmt_contains_yield(body)
    ForAwaitOf(_, _, _) => true
    DoWhile(body, expr) =>
      stmt_contains_yield(body) || expr_contains_yield(expr)
    Switch(discriminant, cases) => {
      if expr_contains_yield(discriminant) {
        return true
      }
      for clause in cases {
        match clause {
          SwitchCase::Case(expr, body) =>
            if expr_contains_yield(expr) || stmts_contain_yield(body) {
              return true
            }
          SwitchCase::Default(body) =>
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
            Some(value) => stmt_contains_yield(value)
            None => false
          }
        }
      }
    _ => false
  }
}

///|
fn stmt_contains_return(stmt : Stmt) -> Bool {
  match stmt {
    Return(_) => true
    Block(stmts) => stmts_contain_return(stmts)
    Label(_, body) => stmt_contains_return(body)
    FunctionDecl(_) => false
    ClassDecl(_) => false
    If(_, conseq, alt) =>
      if stmt_contains_return(conseq) {
        true
      } else {
        match alt {
          Some(value) => stmt_contains_return(value)
          None => false
        }
      }
    With(_, body) => stmt_contains_return(body)
    While(_, body) => stmt_contains_return(body)
    For(_, _, _, body) => stmt_contains_return(body)
    ForIn(_, _, body) => stmt_contains_return(body)
    ForOf(_, _, body) => stmt_contains_return(body)
    ForAwaitOf(_, _, body) => stmt_contains_return(body)
    DoWhile(body, _) => stmt_contains_return(body)
    Switch(_, cases) => {
      for clause in cases {
        let body = match clause {
          SwitchCase::Case(_, stmts) => stmts
          SwitchCase::Default(stmts) => stmts
        }
        if stmts_contain_return(body) {
          return true
        }
      }
      false
    }
    Try(try_body, catch_clause, finally_body) =>
      if stmt_contains_return(try_body) {
        true
      } else {
        let catch_has_return = match catch_clause {
          Some(clause) => stmt_contains_return(clause.body)
          None => false
        }
        if catch_has_return {
          true
        } else {
          match finally_body {
            Some(body) => stmt_contains_return(body)
            None => false
          }
        }
      }
    _ => false
  }
}

///|
fn for_init_contains_await(init : ForInit) -> Bool {
  match init {
    ForInit::None => false
    ForInit::Expr(expr) => expr_contains_await(expr)
    ForInit::Var(_, decls) => {
      for decl in decls {
        let (binding, init) = decl
        if var_binding_contains_await(binding) {
          return true
        }
        match init {
          Some(expr) => if expr_contains_await(expr) { return true }
          None => ()
        }
      }
      false
    }
  }
}

///|
fn for_init_contains_yield(init : ForInit) -> Bool {
  match init {
    ForInit::None => false
    ForInit::Expr(expr) => expr_contains_yield(expr)
    ForInit::Var(_, decls) => {
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
  }
}

///|
fn obj_key_contains_arguments(key : ObjKey) -> Bool {
  match key {
    ObjKey::Computed(expr) => expr_contains_arguments(expr)
    _ => false
  }
}

///|
fn member_key_contains_arguments(key : MemberKey) -> Bool {
  match key {
    MemberKey::Computed(expr) => expr_contains_arguments(expr)
    _ => false
  }
}

///|
fn expr_contains_arguments(expr : Expr) -> Bool {
  match expr {
    Ident(name, _) => name == "arguments"
    TemplateLiteral(_, exprs) => expr_list_contains_arguments(exprs)
    TaggedTemplate(callee, _, exprs) =>
      expr_contains_arguments(callee) || expr_list_contains_arguments(exprs)
    Await(inner, _) => expr_contains_arguments(inner)
    Unary(_, inner, _) => expr_contains_arguments(inner)
    Update(_, inner, _, _) => expr_contains_arguments(inner)
    Yield(expr_opt, _) =>
      match expr_opt {
        Some(value) => expr_contains_arguments(value)
        None => false
      }
    New(callee, args) =>
      expr_contains_arguments(callee) || expr_list_contains_arguments(args)
    Binary(_, left, right, _) =>
      expr_contains_arguments(left) || expr_contains_arguments(right)
    PrivateIn(_, rhs, _) => expr_contains_arguments(rhs)
    Conditional(test_expr, conseq, alt) =>
      expr_contains_arguments(test_expr) ||
      expr_contains_arguments(conseq) ||
      expr_contains_arguments(alt)
    Sequence(exprs) => expr_list_contains_arguments(exprs)
    Paren(inner) => expr_contains_arguments(inner)
    Assign(_, rhs, _, _) => expr_contains_arguments(rhs)
    AssignInvalid(lhs, rhs, _) =>
      expr_contains_arguments(lhs) || expr_contains_arguments(rhs)
    AssignArray(pattern, rhs) =>
      array_pattern_contains_arguments(pattern) || expr_contains_arguments(rhs)
    AssignObject(pattern, rhs) =>
      object_pattern_contains_arguments(pattern) || expr_contains_arguments(rhs)
    AssignOp(_, lhs, rhs, _) =>
      expr_contains_arguments(lhs) || expr_contains_arguments(rhs)
    AssignMember(obj, key, rhs, _) =>
      expr_contains_arguments(obj) ||
      member_key_contains_arguments(key) ||
      expr_contains_arguments(rhs)
    Call(callee, args, _) =>
      expr_contains_arguments(callee) || expr_list_contains_arguments(args)
    Member(obj, key, _) =>
      expr_contains_arguments(obj) || member_key_contains_arguments(key)
    OptionalChain(base, segments) =>
      expr_contains_arguments(base) ||
      chain_segments_contain_arguments(segments)
    FunctionExpr(func) =>
      if func.is_arrow {
        params_contain_arguments_expr(func.params, func.param_inits) ||
        stmts_contain_arguments(func.body)
      } else {
        false
      }
    ClassExpr(class_def) => class_def_contains_arguments(class_def)
    ObjectLiteral(props) => object_props_contain_arguments(props)
    ArrayLiteral(elems) => expr_option_list_contains_arguments(elems)
    Spread(inner) => expr_contains_arguments(inner)
    _ => false
  }
}

///|
fn class_def_contains_arguments(class_def : ClassDef) -> Bool {
  match class_def.super_class {
    Some(expr) => if expr_contains_arguments(expr) { return true }
    None => ()
  }
  for class_member in class_def.body {
    match class_member {
      ClassMember::Method(key, _, _)
      | ClassMember::Getter(key, _, _)
      | ClassMember::Setter(key, _, _) =>
        if obj_key_contains_arguments(key) {
          return true
        }
      ClassMember::Field(key, init, _) => {
        if obj_key_contains_arguments(key) {
          return true
        }
        match init {
          Some(value) => if expr_contains_arguments(value) { return true }
          None => ()
        }
      }
      ClassMember::StaticBlock(stmts) =>
        if stmts_contain_arguments(stmts) {
          return true
        }
    }
  }
  false
}

///|
fn stmts_contain_arguments(stmts : Array[Stmt]) -> Bool {
  for stmt in stmts {
    if stmt_contains_arguments(stmt) {
      return true
    }
  }
  false
}

///|
fn stmt_contains_arguments(stmt : Stmt) -> Bool {
  match stmt {
    ExprStmt(expr) => expr_contains_arguments(expr)
    VarDecl(_, decls) => {
      for decl in decls {
        let (binding, init) = decl
        if var_binding_contains_arguments(binding) {
          return true
        }
        match init {
          Some(expr) => if expr_contains_arguments(expr) { return true }
          None => ()
        }
      }
      false
    }
    Block(stmts) => stmts_contain_arguments(stmts)
    Label(_, body) => stmt_contains_arguments(body)
    FunctionDecl(_) => false
    ClassDecl(class_def) => class_def_contains_arguments(class_def)
    Return(expr_opt) =>
      match expr_opt {
        Some(expr) => expr_contains_arguments(expr)
        None => false
      }
    If(cond_expr, conseq, alt) =>
      if expr_contains_arguments(cond_expr) || stmt_contains_arguments(conseq) {
        true
      } else {
        match alt {
          Some(value) => stmt_contains_arguments(value)
          None => false
        }
      }
    With(expr, body) =>
      expr_contains_arguments(expr) || stmt_contains_arguments(body)
    While(expr, body) =>
      expr_contains_arguments(expr) || stmt_contains_arguments(body)
    For(init, test_expr, update, body) =>
      for_init_contains_arguments(init) ||
      (match test_expr {
        Some(expr) => expr_contains_arguments(expr)
        None => false
      }) ||
      (match update {
        Some(expr) => expr_contains_arguments(expr)
        None => false
      }) ||
      stmt_contains_arguments(body)
    ForIn(init, expr, body) =>
      for_init_contains_arguments(init) ||
      expr_contains_arguments(expr) ||
      stmt_contains_arguments(body)
    ForOf(init, expr, body) =>
      for_init_contains_arguments(init) ||
      expr_contains_arguments(expr) ||
      stmt_contains_arguments(body)
    ForAwaitOf(_, _, _) => true
    DoWhile(body, expr) =>
      stmt_contains_arguments(body) || expr_contains_arguments(expr)
    Switch(discriminant, cases) => {
      if expr_contains_arguments(discriminant) {
        return true
      }
      for clause in cases {
        match clause {
          SwitchCase::Case(expr, body) =>
            if expr_contains_arguments(expr) || stmts_contain_arguments(body) {
              return true
            }
          SwitchCase::Default(body) =>
            if stmts_contain_arguments(body) {
              return true
            }
        }
      }
      false
    }
    Throw(expr) => expr_contains_arguments(expr)
    Try(try_body, catch_clause, finally_body) =>
      if stmt_contains_arguments(try_body) {
        true
      } else {
        let catch_has_arguments = match catch_clause {
          Some(clause) => stmt_contains_arguments(clause.body)
          None => false
        }
        if catch_has_arguments {
          true
        } else {
          match finally_body {
            Some(value) => stmt_contains_arguments(value)
            None => false
          }
        }
      }
    _ => false
  }
}

///|
fn for_init_contains_arguments(init : ForInit) -> Bool {
  match init {
    ForInit::None => false
    ForInit::Expr(expr) => expr_contains_arguments(expr)
    ForInit::Var(_, decls) => {
      for decl in decls {
        let (binding, init) = decl
        if var_binding_contains_arguments(binding) {
          return true
        }
        match init {
          Some(expr) => if expr_contains_arguments(expr) { return true }
          None => ()
        }
      }
      false
    }
  }
}

///|
fn expr_list_contains_arguments(exprs : Array[Expr]) -> Bool {
  for expr in exprs {
    if expr_contains_arguments(expr) {
      return true
    }
  }
  false
}

///|
fn expr_option_list_contains_arguments(exprs : Array[Expr?]) -> Bool {
  for expr in exprs {
    match expr {
      Some(value) => if expr_contains_arguments(value) { return true }
      None => ()
    }
  }
  false
}

///|
fn chain_segments_contain_arguments(segments : Array[ChainSegment]) -> Bool {
  for segment in segments {
    match segment {
      ChainSegment::Member(key, _, _) =>
        if member_key_contains_arguments(key) {
          return true
        }
      ChainSegment::Call(args, _, _) =>
        if expr_list_contains_arguments(args) {
          return true
        }
    }
  }
  false
}

///|
fn object_props_contain_arguments(props : Array[ObjectProp]) -> Bool {
  for prop in props {
    match prop {
      ObjectProp::KeyValue(key, value) =>
        if obj_key_contains_arguments(key) || expr_contains_arguments(value) {
          return true
        }
      ObjectProp::Method(key, _) =>
        if obj_key_contains_arguments(key) {
          return true
        }
      ObjectProp::Getter(key, _) =>
        if obj_key_contains_arguments(key) {
          return true
        }
      ObjectProp::Setter(key, _) =>
        if obj_key_contains_arguments(key) {
          return true
        }
      ObjectProp::Spread(inner) =>
        if expr_contains_arguments(inner) {
          return true
        }
      ObjectProp::Shorthand(_) => ()
    }
  }
  false
}

///|
fn array_pattern_contains_arguments(pattern : Array[ArrayPatternElem]) -> Bool {
  for elem in pattern {
    match elem {
      ArrayPatternElem::Bind(inner, init) => {
        if var_binding_contains_arguments(inner) {
          return true
        }
        match init {
          Some(value) => if expr_contains_arguments(value) { return true }
          None => ()
        }
      }
      ArrayPatternElem::Rest(inner) =>
        if var_binding_contains_arguments(inner) {
          return true
        }
      ArrayPatternElem::Hole => ()
    }
  }
  false
}

///|
fn object_pattern_contains_arguments(
  pattern : Array[ObjectPatternElem],
) -> Bool {
  for elem in pattern {
    match elem {
      ObjectPatternElem::Bind(key, inner, init) => {
        if obj_key_contains_arguments(key) ||
          var_binding_contains_arguments(inner) {
          return true
        }
        match init {
          Some(value) => if expr_contains_arguments(value) { return true }
          None => ()
        }
      }
      ObjectPatternElem::Rest(inner) =>
        if var_binding_contains_arguments(inner) {
          return true
        }
    }
  }
  false
}

///|
fn var_binding_contains_arguments(binding : VarBinding) -> Bool {
  match binding {
    VarBinding::Name(_) => false
    VarBinding::ArrayPattern(pattern) =>
      array_pattern_contains_arguments(pattern)
    VarBinding::ObjectPattern(pattern) =>
      object_pattern_contains_arguments(pattern)
    VarBinding::Target(expr) => expr_contains_arguments(expr)
  }
}

///|
fn params_contain_arguments_expr(
  params : Array[ParamBinding],
  param_inits : Array[Expr?],
) -> Bool {
  for i = 0; i < params.length(); i = i + 1 {
    let param = params[i]
    if var_binding_contains_arguments(param.binding) {
      return true
    }
    match param_inits[i] {
      Some(value) => if expr_contains_arguments(value) { return true }
      None => ()
    }
  }
  false
}

///|
fn obj_key_contains_yield(key : ObjKey) -> Bool {
  match key {
    ObjKey::Computed(expr) => expr_contains_yield(expr)
    _ => false
  }
}

///|
fn member_key_contains_yield(key : MemberKey) -> Bool {
  match key {
    MemberKey::Computed(expr) => expr_contains_yield(expr)
    _ => false
  }
}

///|
fn expr_contains_yield(expr : Expr) -> Bool {
  match expr {
    Yield(_, _) => true
    Await(inner, _) => expr_contains_yield(inner)
    Unary(_, inner, _) => expr_contains_yield(inner)
    Update(_, inner, _, _) => expr_contains_yield(inner)
    New(callee, args) =>
      if expr_contains_yield(callee) {
        true
      } else {
        for arg in args {
          if expr_contains_yield(arg) {
            return true
          }
        }
        false
      }
    Binary(_, left, right, _) =>
      expr_contains_yield(left) || expr_contains_yield(right)
    Conditional(test_expr, conseq, alt) =>
      expr_contains_yield(test_expr) ||
      expr_contains_yield(conseq) ||
      expr_contains_yield(alt)
    Sequence(exprs) => {
      for value in exprs {
        if expr_contains_yield(value) {
          return true
        }
      }
      false
    }
    Paren(inner) => expr_contains_yield(inner)
    Assign(_, rhs, _, _) => expr_contains_yield(rhs)
    AssignInvalid(lhs, rhs, _) =>
      expr_contains_yield(lhs) || expr_contains_yield(rhs)
    AssignArray(pattern, rhs) =>
      array_pattern_contains_yield(pattern) || expr_contains_yield(rhs)
    AssignObject(pattern, rhs) =>
      object_pattern_contains_yield(pattern) || expr_contains_yield(rhs)
    AssignOp(_, left, right, _) =>
      expr_contains_yield(left) || expr_contains_yield(right)
    AssignMember(obj, key, rhs, _) =>
      expr_contains_yield(obj) ||
      member_key_contains_yield(key) ||
      expr_contains_yield(rhs)
    Call(callee, args, _) =>
      if expr_contains_yield(callee) {
        true
      } else {
        for arg in args {
          if expr_contains_yield(arg) {
            return true
          }
        }
        false
      }
    Member(obj, key, _) =>
      expr_contains_yield(obj) || member_key_contains_yield(key)
    OptionalChain(base, segments) => {
      if expr_contains_yield(base) {
        return true
      }
      for segment in segments {
        match segment {
          ChainSegment::Member(key, _, _) =>
            if member_key_contains_yield(key) {
              return true
            }
          ChainSegment::Call(args, _, _) =>
            for arg in args {
              if expr_contains_yield(arg) {
                return true
              }
            }
        }
      }
      false
    }
    FunctionExpr(func) =>
      if func.is_arrow {
        params_contain_yield_expr(func.params, func.param_inits)
      } else {
        false
      }
    ClassExpr(class_def) => class_def_contains_yield(class_def)
    ObjectLiteral(props) => {
      for prop in props {
        match prop {
          ObjectProp::KeyValue(key, value) =>
            if obj_key_contains_yield(key) || expr_contains_yield(value) {
              return true
            }
          ObjectProp::Method(key, _) =>
            if obj_key_contains_yield(key) {
              return true
            }
          ObjectProp::Getter(key, _) =>
            if obj_key_contains_yield(key) {
              return true
            }
          ObjectProp::Setter(key, _) =>
            if obj_key_contains_yield(key) {
              return true
            }
          ObjectProp::Spread(inner) =>
            if expr_contains_yield(inner) {
              return true
            }
          ObjectProp::Shorthand(_) => ()
        }
      }
      false
    }
    ArrayLiteral(elems) => {
      for elem in elems {
        match elem {
          Some(value) => if expr_contains_yield(value) { return true }
          None => ()
        }
      }
      false
    }
    Spread(inner) => expr_contains_yield(inner)
    TemplateLiteral(_, exprs) => {
      for value in exprs {
        if expr_contains_yield(value) {
          return true
        }
      }
      false
    }
    TaggedTemplate(callee, _, exprs) => {
      if expr_contains_yield(callee) {
        return true
      }
      for value in exprs {
        if expr_contains_yield(value) {
          return true
        }
      }
      false
    }
    _ => false
  }
}

///|
fn array_pattern_contains_await(pattern : Array[ArrayPatternElem]) -> Bool {
  for elem in pattern {
    match elem {
      ArrayPatternElem::Bind(inner, init) => {
        if var_binding_contains_await(inner) {
          return true
        }
        match init {
          Some(value) => if expr_contains_await(value) { return true }
          None => ()
        }
      }
      ArrayPatternElem::Rest(inner) =>
        if var_binding_contains_await(inner) {
          return true
        }
      ArrayPatternElem::Hole => ()
    }
  }
  false
}

///|
fn array_pattern_contains_yield(pattern : Array[ArrayPatternElem]) -> Bool {
  for elem in pattern {
    match elem {
      ArrayPatternElem::Bind(inner, init) => {
        if var_binding_contains_yield(inner) {
          return true
        }
        match init {
          Some(value) => if expr_contains_yield(value) { return true }
          None => ()
        }
      }
      ArrayPatternElem::Rest(inner) =>
        if var_binding_contains_yield(inner) {
          return true
        }
      ArrayPatternElem::Hole => ()
    }
  }
  false
}

///|
fn object_pattern_contains_await(pattern : Array[ObjectPatternElem]) -> Bool {
  for elem in pattern {
    match elem {
      ObjectPatternElem::Bind(key, inner, init) => {
        if obj_key_contains_await(key) || var_binding_contains_await(inner) {
          return true
        }
        match init {
          Some(value) => if expr_contains_await(value) { return true }
          None => ()
        }
      }
      ObjectPatternElem::Rest(inner) =>
        if var_binding_contains_await(inner) {
          return true
        }
    }
  }
  false
}

///|
fn object_pattern_contains_yield(pattern : Array[ObjectPatternElem]) -> Bool {
  for elem in pattern {
    match elem {
      ObjectPatternElem::Bind(key, inner, init) => {
        if obj_key_contains_yield(key) || var_binding_contains_yield(inner) {
          return true
        }
        match init {
          Some(value) => if expr_contains_yield(value) { return true }
          None => ()
        }
      }
      ObjectPatternElem::Rest(inner) =>
        if var_binding_contains_yield(inner) {
          return true
        }
    }
  }
  false
}

///|
fn var_binding_contains_await(binding : VarBinding) -> Bool {
  match binding {
    VarBinding::Name(name) => name == "await"
    VarBinding::ArrayPattern(pattern) => array_pattern_contains_await(pattern)
    VarBinding::ObjectPattern(pattern) => object_pattern_contains_await(pattern)
    VarBinding::Target(expr) => expr_contains_await(expr)
  }
}

///|
fn var_binding_contains_yield(binding : VarBinding) -> Bool {
  match binding {
    VarBinding::Name(name) => name == "yield"
    VarBinding::ArrayPattern(pattern) => array_pattern_contains_yield(pattern)
    VarBinding::ObjectPattern(pattern) => object_pattern_contains_yield(pattern)
    VarBinding::Target(expr) => expr_contains_yield(expr)
  }
}

///|
fn params_contain_await_expr(
  params : Array[ParamBinding],
  param_inits : Array[Expr?],
) -> Bool {
  for i = 0; i < params.length(); i = i + 1 {
    let param = params[i]
    if var_binding_contains_await(param.binding) {
      return true
    }
    match param_inits[i] {
      Some(value) => if expr_contains_await(value) { return true }
      None => ()
    }
  }
  false
}

///|
fn params_contain_yield_expr(
  params : Array[ParamBinding],
  param_inits : Array[Expr?],
) -> Bool {
  for i = 0; i < params.length(); i = i + 1 {
    let param = params[i]
    if var_binding_contains_yield(param.binding) {
      return true
    }
    match param_inits[i] {
      Some(value) => if expr_contains_yield(value) { return true }
      None => ()
    }
  }
  false
}

///|
fn stmts_contain_with(stmts : Array[Stmt]) -> Bool {
  for stmt in stmts {
    if stmt_contains_with(stmt) {
      return true
    }
  }
  false
}

///|
fn stmt_contains_with(stmt : Stmt) -> Bool {
  match stmt {
    With(_, _) => true
    Block(stmts) => stmts_contain_with(stmts)
    Label(_, inner) => stmt_contains_with(inner)
    If(_, conseq, alt) =>
      if stmt_contains_with(conseq) {
        true
      } else {
        match alt {
          Some(value) => stmt_contains_with(value)
          None => false
        }
      }
    While(_, body) => stmt_contains_with(body)
    For(_, _, _, body) => stmt_contains_with(body)
    ForIn(_, _, body) => stmt_contains_with(body)
    ForOf(_, _, body) => stmt_contains_with(body)
    ForAwaitOf(_, _, body) => stmt_contains_with(body)
    DoWhile(body, _) => stmt_contains_with(body)
    Switch(_, cases) => {
      for entry in cases {
        match entry {
          SwitchCase::Case(_, body) =>
            if stmts_contain_with(body) {
              return true
            }
          SwitchCase::Default(body) =>
            if stmts_contain_with(body) {
              return true
            }
        }
      }
      false
    }
    Try(body, catch_clause, final_clause) =>
      if stmt_contains_with(body) {
        true
      } else {
        let catch_has = match catch_clause {
          Some(clause) => stmt_contains_with(clause.body)
          None => false
        }
        if catch_has {
          true
        } else {
          match final_clause {
            Some(value) => stmt_contains_with(value)
            None => false
          }
        }
      }
    _ => false
  }
}

///|
fn stmt_is_labelled_function(stmt : Stmt) -> Bool {
  match stmt {
    Label(_, inner) => stmt_is_function_decl(inner)
    _ => false
  }
}

///|
fn check_function_params(
  tokens : Array[Token],
  index : Int,
  name : String?,
  params : Array[ParamBinding],
  param_inits : Array[Expr?],
  is_strict : Bool,
  has_use_strict : Bool,
  is_arrow : Bool,
  is_method : Bool,
  is_generator : Bool,
  is_async : Bool,
) -> Unit raise {
  let simple = is_simple_parameter_list(params, param_inits)
  if has_use_strict && !simple {
    parse_fail(
      tokens, index, "\"use strict\" not allowed in function with default or destructuring parameter",
    )
  }
  if !is_method {
    match name {
      Some(func_name) =>
        if is_strict && is_strict_reserved_name(func_name) {
          parse_fail(tokens, index, "invalid function name in strict code")
        }
      None => ()
    }
  }
  let names : Array[String] = []
  for param in params {
    collect_binding_names(param.binding, names)
  }
  if is_async {
    for param_name in names {
      if param_name == "await" {
        parse_fail(tokens, index, "invalid argument name")
      }
    }
    if params_contain_await_expr(params, param_inits) {
      parse_fail(tokens, index, "invalid await expression in parameters")
    }
  }
  if is_generator {
    for param_name in names {
      if param_name == "yield" {
        parse_fail(tokens, index, "invalid argument name")
      }
    }
    if params_contain_yield_expr(params, param_inits) {
      parse_fail(tokens, index, "invalid yield expression in parameters")
    }
  }
  if is_strict {
    for param_name in names {
      if is_strict_reserved_name(param_name) {
        parse_fail(tokens, index, "invalid argument name in strict code")
      }
    }
  }
  if is_strict || !simple || is_arrow || is_method {
    if params_have_duplicates(names) {
      parse_fail(
        tokens, index, "duplicate argument names not allowed in this context",
      )
    }
  }
}

///|
fn check_param_lexical_conflicts(
  tokens : Array[Token],
  index : Int,
  params : Array[ParamBinding],
  body : Array[Stmt],
) -> Unit raise {
  let param_names : Array[String] = []
  for param in params {
    collect_binding_names(param.binding, param_names)
  }
  if param_names.is_empty() {
    return
  }
  let lex_seen : Map[String, Bool] = Map::new()
  for stmt in body {
    let lex_names : Array[String] = []
    collect_block_lex_names(stmt, lex_names, false)
    for name in lex_names {
      lex_seen.set(name, true)
    }
  }
  for name in param_names {
    if lex_seen.contains(name) {
      parse_fail(tokens, index, "duplicate lexical declaration")
    }
  }
}

///|
fn parse_method_body(
  tokens : Array[Token],
  index : Int,
  name : String?,
  is_generator : Bool,
  is_async : Bool,
  force_strict : Bool,
  offset : Int,
) -> (FunctionDef, Int) raise {
  let (params, param_inits, next_i) = with_await_context(false, !is_async, fn() raise {
    with_yield_context(false, !is_generator, fn() raise {
      parse_parameters(tokens, index, !is_generator, !is_async, false)
    })
  })
  let i = next_i
  let outer_strict = force_strict || strict_context()
  let parse_body = fn() raise {
    with_return_context(true, fn() raise {
      with_await_context(is_async, !is_async, fn() raise {
        with_yield_context(is_generator, !is_generator, fn() raise {
          parse_block(tokens, i)
        })
      })
    })
  }
  let (body_stmt, next_i0) = with_strict_context(outer_strict, fn() raise {
    parse_body()
  })
  let mut next_i = next_i0
  let mut body = match body_stmt {
    Block(stmts) => stmts
    _ => parse_fail(tokens, i, "expected block")
  }
  let mut use_strict = has_use_strict(body)
  if use_strict && !outer_strict {
    let (strict_body_stmt, strict_next_i) = with_strict_context(true, fn() raise {
      parse_body()
    })
    body = match strict_body_stmt {
      Block(stmts) => stmts
      _ => parse_fail(tokens, i, "expected block")
    }
    next_i = strict_next_i
    use_strict = has_use_strict(body)
  }
  let is_strict = force_strict || use_strict || strict_context()
  if is_strict && stmts_contain_with(body) {
    parse_fail(tokens, index, "with not allowed in strict mode")
  }
  check_function_params(
    tokens, index, name, params, param_inits, is_strict, use_strict, false, true,
    is_generator, is_async,
  )
  check_param_lexical_conflicts(tokens, index, params, body)
  let end_offset = if next_i > 0 {
    token_end_offset(tokens, next_i - 1)
  } else {
    offset
  }
  (
    FunctionDef::{
      name,
      params,
      param_inits,
      body,
      is_strict,
      is_generator,
      is_async,
      is_arrow: false,
      offset,
      end_offset,
    },
    next_i,
  )
}

///|
fn parse_left_hand_side_expr(
  tokens : Array[Token],
  index : Int,
) -> (Expr, Int) raise {
  let (expr, next_i) = parse_postfix(tokens, index)
  match expr {
    Update(_, _, UpdateKind::Postfix, _) =>
      parse_fail(tokens, index, "invalid class heritage")
    _ => ()
  }
  (expr, next_i)
}

///|
fn parse_class(
  tokens : Array[Token],
  index : Int,
  require_name : Bool,
) -> (ClassDef, Int) raise {
  let offset = peek(tokens, index).offset
  let mut i = index + 1
  let mut name : String? = None
  let name_index = i
  let name_kind = binding_identifier_kind(peek(tokens, i))
  if is_binding_identifier_token(name_kind, false, true, true) {
    let (parsed, next_i) = parse_binding_identifier(
      tokens, i, false, true, true,
    )
    if is_strict_reserved_name(parsed) {
      parse_fail(tokens, name_index, "invalid binding name in strict code")
    }
    name = Some(parsed)
    i = next_i
  } else if require_name {
    parse_fail(tokens, i, "expected class name")
  }
  let mut super_class : Expr? = None
  let members : Array[ClassMember] = []
  let mut has_constructor = false
  with_strict_context(true, fn() raise {
    if match_kind(tokens, i, KeywordExtends) {
      let (expr, next_i) = parse_left_hand_side_expr(tokens, i + 1)
      super_class = Some(expr)
      i = next_i
    }
    if !match_punct(tokens, i, '{') {
      parse_fail(tokens, i, "expected '{'")
    }
    i = i + 1
    while !match_punct(tokens, i, '}') {
      if match_punct(tokens, i, ';') {
        i = i + 1
        continue
      }
      if match_kind(tokens, i, KeywordStatic) && match_punct(tokens, i + 1, '{') {
        let allow_yield_ident = yield_ident_allowed()
        let allow_await_ident = false
        let (block_stmt, next_i) = with_yield_context(
          false,
          allow_yield_ident,
          fn() raise {
            with_await_context(false, allow_await_ident, fn() raise {
              parse_block(tokens, i + 1)
            })
          },
        )
        let block_stmts = match block_stmt {
          Block(stmts) => stmts
          _ => ([] : Array[Stmt])
        }
        if stmts_contain_await(block_stmts) {
          parse_fail(tokens, i + 1, "await not allowed in class static block")
        }
        if stmts_contain_return(block_stmts) {
          parse_fail(tokens, i + 1, "return not allowed in class static block")
        }
        if stmts_contain_arguments(block_stmts) {
          parse_fail(
            tokens,
            i + 1,
            "arguments not allowed in class static block",
          )
        }
        members.push(ClassMember::StaticBlock(block_stmts))
        i = next_i
        continue
      }
      let mut is_static = false
      if match_kind(tokens, i, KeywordStatic) {
        if !match_punct(tokens, i + 1, '(') &&
          !match_punct(tokens, i + 1, ';') &&
          !match_punct(tokens, i + 1, '=') &&
          !match_punct(tokens, i + 1, '}') {
          is_static = true
          i = i + 1
        }
      }
      let tok = peek(tokens, i)
      if is_unescaped_ident(tokens, i, "async") &&
        !peek(tokens, i + 1).line_break_before {
        let mut handled = false
        if match_punct(tokens, i + 1, '*') {
          let name_index = i + 2
          if match_punct(tokens, name_index, '[') {
            let (key, key_i) = parse_computed_obj_key(tokens, name_index)
            if match_punct(tokens, key_i, '(') {
              let (func, next_i) = parse_method_body(
                tokens,
                key_i,
                None,
                true,
                true,
                true,
                tok.offset,
              )
              members.push(ClassMember::Method(key, func, is_static))
              i = next_i
              handled = true
            }
          } else if (
              is_identifier_name(peek(tokens, name_index).kind) ||
              match_kind(tokens, name_index, String) ||
              match_kind(tokens, name_index, Number) ||
              match_kind(tokens, name_index, PrivateName)
            ) &&
            match_punct(tokens, name_index + 1, '(') {
            let key_tok = peek(tokens, name_index)
            let key = match key_tok.kind {
              String => ObjKey::String(key_tok.lexeme)
              Number => parse_numeric_obj_key(key_tok.lexeme, key_tok.offset)
              PrivateName => ObjKey::Private(key_tok.lexeme)
              _ if is_identifier_name(key_tok.kind) =>
                ObjKey::Ident(key_tok.lexeme)
              _ => parse_fail(tokens, name_index, "expected property name")
            }
            let key_name = obj_key_name(key)
            if obj_key_is_private_constructor(key) ||
              (!is_static && obj_key_is_public_constructor(key)) ||
              (is_static && obj_key_is_public_prototype(key)) {
              parse_fail(tokens, name_index, "invalid method name")
            }
            let (func, next_i) = parse_method_body(
              tokens,
              name_index + 1,
              Some(key_name),
              true,
              true,
              true,
              tok.offset,
            )
            members.push(ClassMember::Method(key, func, is_static))
            i = next_i
            handled = true
          }
        } else if match_punct(tokens, i + 1, '[') {
          let (key, key_i) = parse_computed_obj_key(tokens, i + 1)
          if match_punct(tokens, key_i, '(') {
            let (func, next_i) = parse_method_body(
              tokens,
              key_i,
              None,
              false,
              true,
              true,
              tok.offset,
            )
            members.push(ClassMember::Method(key, func, is_static))
            i = next_i
            handled = true
          }
        } else if (
            is_identifier_name(peek(tokens, i + 1).kind) ||
            match_kind(tokens, i + 1, String) ||
            match_kind(tokens, i + 1, Number) ||
            match_kind(tokens, i + 1, PrivateName)
          ) &&
          match_punct(tokens, i + 2, '(') {
          let key_tok = peek(tokens, i + 1)
          let key = match key_tok.kind {
            String => ObjKey::String(key_tok.lexeme)
            Number => parse_numeric_obj_key(key_tok.lexeme, key_tok.offset)
            PrivateName => ObjKey::Private(key_tok.lexeme)
            _ if is_identifier_name(key_tok.kind) =>
              ObjKey::Ident(key_tok.lexeme)
            _ => parse_fail(tokens, i + 1, "expected property name")
          }
          let key_name = obj_key_name(key)
          if obj_key_is_private_constructor(key) ||
            (!is_static && obj_key_is_public_constructor(key)) ||
            (is_static && obj_key_is_public_prototype(key)) {
            parse_fail(tokens, i + 1, "invalid method name")
          }
          let (func, next_i) = parse_method_body(
            tokens,
            i + 2,
            Some(key_name),
            false,
            true,
            true,
            tok.offset,
          )
          members.push(ClassMember::Method(key, func, is_static))
          i = next_i
          handled = true
        }
        if handled {
          continue
        }
      }
      let mut is_generator = false
      let mut method_offset = tok.offset
      if match_punct(tokens, i, '*') {
        is_generator = true
        method_offset = tok.offset
        i = i + 1
      }
      let tok = peek(tokens, i)
      let mut handled = false
      let is_get = is_unescaped_ident(tokens, i, "get")
      let is_set = is_unescaped_ident(tokens, i, "set")
      if is_get || is_set {
        if match_punct(tokens, i + 1, '[') {
          let (key, key_i) = parse_computed_obj_key(tokens, i + 1)
          if match_punct(tokens, key_i, '(') {
            if obj_key_is_private_constructor(key) ||
              (!is_static && obj_key_is_public_constructor(key)) ||
              (is_static && obj_key_is_public_prototype(key)) {
              parse_fail(tokens, i + 1, "invalid method name")
            }
            let is_getter = is_get
            let (func, next_i) = parse_accessor(
              tokens,
              key_i,
              None,
              is_getter,
              tok.offset,
            )
            if is_getter {
              members.push(ClassMember::Getter(key, func, is_static))
            } else {
              members.push(ClassMember::Setter(key, func, is_static))
            }
            i = next_i
            handled = true
          }
        } else if (
            is_identifier_name(peek(tokens, i + 1).kind) ||
            match_kind(tokens, i + 1, String) ||
            match_kind(tokens, i + 1, Number) ||
            match_kind(tokens, i + 1, PrivateName)
          ) &&
          match_punct(tokens, i + 2, '(') {
          let key_tok = peek(tokens, i + 1)
          let key = match key_tok.kind {
            String => ObjKey::String(key_tok.lexeme)
            Number => parse_numeric_obj_key(key_tok.lexeme, key_tok.offset)
            PrivateName => ObjKey::Private(key_tok.lexeme)
            _ if is_identifier_name(key_tok.kind) =>
              ObjKey::Ident(key_tok.lexeme)
            _ => parse_fail(tokens, i + 1, "expected property name")
          }
          let key_name = obj_key_name(key)
          if obj_key_is_private_constructor(key) ||
            (!is_static && obj_key_is_public_constructor(key)) ||
            (is_static && obj_key_is_public_prototype(key)) {
            parse_fail(tokens, i + 1, "invalid method name")
          }
          let is_getter = is_get
          let func_name = if is_getter {
            "get " + key_name
          } else {
            "set " + key_name
          }
          let (func, next_i) = parse_accessor(
            tokens,
            i + 2,
            Some(func_name),
            is_getter,
            tok.offset,
          )
          if is_getter {
            members.push(ClassMember::Getter(key, func, is_static))
          } else {
            members.push(ClassMember::Setter(key, func, is_static))
          }
          i = next_i
          handled = true
        }
      }
      if handled {
        continue
      }
      let mut key = ObjKey::Ident("")
      let mut key_name : String? = None
      let mut key_i = i + 1
      if match_punct(tokens, i, '[') {
        let (parsed_key, next_i) = parse_computed_obj_key(tokens, i)
        key = parsed_key
        key_i = next_i
      } else {
        key = match tok.kind {
          String => ObjKey::String(tok.lexeme)
          Number => parse_numeric_obj_key(tok.lexeme, tok.offset)
          PrivateName => ObjKey::Private(tok.lexeme)
          _ if is_identifier_name(tok.kind) => ObjKey::Ident(tok.lexeme)
          _ => parse_fail(tokens, i, "expected property name")
        }
        key_name = Some(obj_key_name(key))
      }
      if match_punct(tokens, key_i, '(') {
        if obj_key_is_private_constructor(key) ||
          (is_static && obj_key_is_public_prototype(key)) {
          parse_fail(tokens, i, "invalid method name")
        }
        if !is_static && obj_key_is_public_constructor(key) {
          if is_generator {
            parse_fail(tokens, i, "invalid method name")
          }
          if has_constructor {
            parse_fail(tokens, i, "property constructor appears more than once")
          }
          has_constructor = true
        }
        let (func, next_i) = parse_method_body(
          tokens, key_i, key_name, is_generator, false, true, method_offset,
        )
        members.push(ClassMember::Method(key, func, is_static))
        i = next_i
      } else {
        if obj_key_is_private_constructor(key) ||
          obj_key_is_public_constructor(key) ||
          obj_key_is_public_prototype(key) {
          parse_fail(tokens, key_i, "invalid field name")
        }
        let mut init : Expr? = None
        if match_punct(tokens, key_i, '=') {
          let (value, next_i) = with_await_context(false, true, fn() raise {
            parse_assignment(tokens, key_i + 1)
          })
          if expr_contains_arguments(value) {
            parse_fail(
              tokens,
              key_i + 1,
              "arguments not allowed in class field initializer",
            )
          }
          init = Some(value)
          key_i = next_i
        }
        let mut has_semicolon = false
        if match_punct(tokens, key_i, ';') {
          key_i = key_i + 1
          has_semicolon = true
        }
        if !has_semicolon && !match_punct(tokens, key_i, '}') {
          let next_tok = peek(tokens, key_i)
          if !next_tok.line_break_before {
            parse_fail(tokens, key_i, "expected ';'")
          }
        }
        members.push(ClassMember::Field(key, init, is_static))
        i = key_i
      }
    }
  })
  let end_offset = if i > 0 { token_end_offset(tokens, i) } else { offset }
  (ClassDef::{ name, super_class, body: members, offset, end_offset }, i + 1)
}

///|
fn parse_array_literal(tokens : Array[Token], index : Int) -> (Expr, Int) raise {
  let mut i = index + 1
  let elems : Array[Expr?] = []
  if !match_punct(tokens, i, ']') {
    for {
      if match_punct(tokens, i, ',') {
        elems.push(None)
        i = i + 1
        if match_punct(tokens, i, ']') {
          break
        }
        continue
      }
      if match_kind(tokens, i, Ellipsis) {
        let (expr, next_i) = parse_assignment(tokens, i + 1)
        elems.push(Some(Spread(expr)))
        i = next_i
      } else {
        let (expr, next_i) = parse_assignment(tokens, i)
        elems.push(Some(expr))
        i = next_i
      }
      if match_punct(tokens, i, ',') {
        i = i + 1
        if match_punct(tokens, i, ']') {
          break
        }
        continue
      }
      break
    }
  }
  if !match_punct(tokens, i, ']') {
    parse_fail(tokens, i, "expected ']'")
  }
  (ArrayLiteral(elems), i + 1)
}

///|
fn parse_break(tokens : Array[Token], index : Int) -> (Stmt, Int) raise {
  let mut i = index + 1
  if match_punct(tokens, i, ';') {
    return (Break(None), i + 1)
  }
  let next_tok = peek(tokens, i)
  if match_punct(tokens, i, '}') ||
    is_eof(tokens, i) ||
    next_tok.line_break_before {
    return (Break(None), i)
  }
  let mut label : String? = None
  if match_kind(tokens, i, Ident) {
    label = Some(next_tok.lexeme)
    i = i + 1
  } else {
    parse_fail(tokens, i, "expected label")
  }
  if match_punct(tokens, i, ';') {
    i = i + 1
  } else {
    ensure_asi(tokens, i)
  }
  (Break(label), i)
}

///|
fn parse_continue(tokens : Array[Token], index : Int) -> (Stmt, Int) raise {
  let mut i = index + 1
  if match_punct(tokens, i, ';') {
    return (Continue(None), i + 1)
  }
  let next_tok = peek(tokens, i)
  if match_punct(tokens, i, '}') ||
    is_eof(tokens, i) ||
    next_tok.line_break_before {
    return (Continue(None), i)
  }
  let mut label : String? = None
  if match_kind(tokens, i, Ident) {
    label = Some(next_tok.lexeme)
    i = i + 1
  } else {
    parse_fail(tokens, i, "expected label")
  }
  if match_punct(tokens, i, ';') {
    i = i + 1
  } else {
    ensure_asi(tokens, i)
  }
  (Continue(label), i)
}

///|
fn parse_function(
  tokens : Array[Token],
  index : Int,
  require_name : Bool,
  is_async : Bool,
  is_expr : Bool,
) -> (FunctionDef, Int) raise {
  let offset = peek(tokens, index).offset
  let mut i = index + 1
  let mut is_generator = false
  if match_punct(tokens, i, '*') {
    is_generator = true
    i = i + 1
  }
  let mut name : String? = None
  let allow_yield_name = !(is_expr && is_generator) || !yield_ident_allowed()
  let allow_await_name = !(is_expr && is_async)
  let allow_let = !strict_context()
  let name_index = i
  let name_kind = binding_identifier_kind(peek(tokens, i))
  if !is_expr && !yield_ident_allowed() && name_kind == KeywordYield {
    parse_fail(tokens, name_index, "invalid binding name in generator")
  }
  let allow_yield_override = allow_yield_name && name_kind == KeywordYield
  let allow_await_override = allow_await_name &&
    name_kind == KeywordAwait &&
    !await_expr_allowed_raw()
  if allow_yield_override ||
    allow_await_override ||
    is_binding_identifier_token(
      name_kind, allow_let, allow_yield_name, allow_await_name,
    ) {
    let (parsed, next_i) = if allow_yield_name && !yield_ident_allowed() {
      with_yield_context(false, true, fn() raise {
        parse_binding_identifier(
          tokens, i, allow_let, allow_yield_name, allow_await_name,
        )
      })
    } else if allow_await_override && !await_ident_allowed() {
      with_await_context(false, true, fn() raise {
        parse_binding_identifier(
          tokens, i, allow_let, allow_yield_name, allow_await_name,
        )
      })
    } else {
      parse_binding_identifier(
        tokens, i, allow_let, allow_yield_name, allow_await_name,
      )
    }
    name = Some(parsed)
    i = next_i
  } else if require_name {
    parse_fail(tokens, i, "expected function name")
  }
  if !match_punct(tokens, i, '(') {
    parse_fail(tokens, i, "expected '('")
  }
  let (params, param_inits, next_params) = with_await_context(
    false,
    !is_async,
    fn() raise {
      with_yield_context(false, !is_generator, fn() raise {
        parse_parameters(tokens, i, !is_generator, !is_async, false)
      })
    },
  )
  i = next_params
  let outer_strict = strict_context()
  let parse_body = fn() raise {
    with_return_context(true, fn() raise {
      with_await_context(is_async, !is_async, fn() raise {
        with_yield_context(is_generator, !is_generator, fn() raise {
          parse_block(tokens, i)
        })
      })
    })
  }
  let (body_stmt, next_i0) = with_strict_context(outer_strict, fn() raise {
    parse_body()
  })
  let mut next_i = next_i0
  let mut body = match body_stmt {
    Block(stmts) => stmts
    _ => parse_fail(tokens, i, "expected block")
  }
  let mut use_strict = has_use_strict(body)
  if use_strict && !outer_strict {
    let (strict_body_stmt, strict_next_i) = with_strict_context(true, fn() raise {
      parse_body()
    })
    body = match strict_body_stmt {
      Block(stmts) => stmts
      _ => parse_fail(tokens, i, "expected block")
    }
    next_i = strict_next_i
    use_strict = has_use_strict(body)
  }
  let is_strict = use_strict || outer_strict
  if is_strict && stmts_contain_with(body) {
    parse_fail(tokens, index, "with not allowed in strict mode")
  }
  check_function_params(
    tokens, index, name, params, param_inits, is_strict, use_strict, false, false,
    is_generator, is_async,
  )
  check_param_lexical_conflicts(tokens, index, params, body)
  let end_offset = if next_i > 0 {
    token_end_offset(tokens, next_i - 1)
  } else {
    offset
  }
  (
    FunctionDef::{
      name,
      params,
      param_inits,
      body,
      is_strict,
      is_generator,
      is_async,
      is_arrow: false,
      offset,
      end_offset,
    },
    next_i,
  )
}

///|
fn with_function_offset(func : FunctionDef, offset : Int) -> FunctionDef {
  FunctionDef::{
    name: func.name,
    params: func.params,
    param_inits: func.param_inits,
    body: func.body,
    is_strict: func.is_strict,
    is_generator: func.is_generator,
    is_async: func.is_async,
    is_arrow: func.is_arrow,
    offset,
    end_offset: func.end_offset,
  }
}

///|
fn parse_number_literal(lexeme : String, offset : Int) -> Double raise {
  let lexeme = strip_numeric_separators(lexeme, offset)
  if lexeme.has_prefix("0x") || lexeme.has_prefix("0X") {
    let digits = lexeme.op_as_view(start=2)
    let value = @strconv.parse_uint64(digits, base=16)
    return Double::convert_uint64(value)
  }
  if lexeme.has_prefix("0o") || lexeme.has_prefix("0O") {
    let digits = lexeme.op_as_view(start=2)
    let value = @strconv.parse_uint64(digits, base=8)
    return Double::convert_uint64(value)
  }
  if lexeme.has_prefix("0b") || lexeme.has_prefix("0B") {
    let digits = lexeme.op_as_view(start=2)
    let value = @strconv.parse_uint64(digits, base=2)
    return Double::convert_uint64(value)
  }
  if lexeme.length() > 1 &&
    lexeme.has_prefix("0") &&
    !lexeme.contains(".") &&
    !lexeme.contains("e") &&
    !lexeme.contains("E") {
    let mut is_octal = true
    let mut i = 1
    while i < lexeme.length() {
      let unit = UInt16::to_int(lexeme.code_unit_at(i))
      if unit < Char::to_int('0') || unit > Char::to_int('7') {
        is_octal = false
        break
      }
      i = i + 1
    }
    if is_octal {
      let value = @strconv.parse_uint64(lexeme.op_as_view(), base=8)
      return Double::convert_uint64(value)
    }
  }
  try @strconv.parse_double(lexeme) catch {
    _ =>
      match decimal_overflow_literal(lexeme) {
        Some(value) => value
        None => parse_fail_offset(offset, "invalid number")
      }
  } noraise {
    value => value
  }
}

///|
fn is_legacy_decimal_literal(lexeme : String, offset : Int) -> Bool raise {
  let lexeme = strip_numeric_separators(lexeme, offset)
  if lexeme.length() < 2 {
    return false
  }
  let zero = Char::to_int('0')
  let first = UInt16::to_int(lexeme.code_unit_at(0))
  if first != zero {
    return false
  }
  if lexeme.has_prefix("0x") ||
    lexeme.has_prefix("0X") ||
    lexeme.has_prefix("0o") ||
    lexeme.has_prefix("0O") ||
    lexeme.has_prefix("0b") ||
    lexeme.has_prefix("0B") {
    return false
  }
  if lexeme.has_suffix("n") {
    return false
  }
  let dot = Char::to_int('.')
  let lower_e = Char::to_int('e')
  let upper_e = Char::to_int('E')
  let second = UInt16::to_int(lexeme.code_unit_at(1))
  if second == dot || second == lower_e || second == upper_e {
    return false
  }
  let nine = Char::to_int('9')
  second >= zero && second <= nine
}

///|
fn bigint_has_leading_zero(lexeme : String, offset : Int) -> Bool raise {
  if !lexeme.has_suffix("n") {
    return false
  }
  let len = lexeme.length()
  if len < 3 {
    return false
  }
  let digits = lexeme.unsafe_substring(start=0, end=len - 1)
  let stripped = strip_numeric_separators(digits, offset)
  if stripped.has_prefix("0x") ||
    stripped.has_prefix("0X") ||
    stripped.has_prefix("0o") ||
    stripped.has_prefix("0O") ||
    stripped.has_prefix("0b") ||
    stripped.has_prefix("0B") {
    return false
  }
  if stripped.length() < 2 {
    return false
  }
  let zero = Char::to_int('0')
  let first = UInt16::to_int(stripped.code_unit_at(0))
  first == zero
}

///|
fn inf() -> Double {
  1.0 / 0.0
}

///|
fn find_exponent_index(lexeme : String, start : Int) -> Int? {
  let len = lexeme.length()
  let mut index = start
  let lower_e = Char::to_int('e')
  let upper_e = Char::to_int('E')
  while index < len {
    let unit = UInt16::to_int(lexeme.code_unit_at(index))
    if unit == lower_e || unit == upper_e {
      return Some(index)
    }
    index = index + 1
  }
  None
}

///|
fn scan_mantissa_digits(
  lexeme : String,
  start : Int,
  end : Int,
) -> (Bool, Bool) {
  let mut has_digit = false
  let mut has_non_zero = false
  let mut index = start
  let zero = Char::to_int('0')
  let nine = Char::to_int('9')
  while index < end {
    let unit = UInt16::to_int(lexeme.code_unit_at(index))
    if unit >= zero && unit <= nine {
      has_digit = true
      if unit != zero {
        has_non_zero = true
        break
      }
    }
    index = index + 1
  }
  (has_digit, has_non_zero)
}

///|
fn decimal_overflow_literal(lexeme : String) -> Double? {
  let len = lexeme.length()
  if len == 0 {
    return None
  }
  let exp_index = match find_exponent_index(lexeme, 0) {
    Some(pos) => pos
    None => return None
  }
  let (has_digit, has_non_zero) = scan_mantissa_digits(lexeme, 0, exp_index)
  if !has_digit {
    return None
  }
  if !has_non_zero {
    return Some(0.0)
  }
  let mut exp_start = exp_index + 1
  if exp_start >= len {
    return None
  }
  let mut exp_negative = false
  let exp_sign = UInt16::to_int(lexeme.code_unit_at(exp_start))
  if exp_sign == Char::to_int('+') || exp_sign == Char::to_int('-') {
    exp_negative = exp_sign == Char::to_int('-')
    exp_start = exp_start + 1
  }
  if exp_start >= len {
    return None
  }
  let mut exp_value = 0
  let mut idx = exp_start
  let zero = Char::to_int('0')
  let nine = Char::to_int('9')
  while idx < len {
    let unit = UInt16::to_int(lexeme.code_unit_at(idx))
    if unit < zero || unit > nine {
      return None
    }
    exp_value = exp_value * 10 + (unit - zero)
    idx = idx + 1
  }
  let exp = if exp_negative { -exp_value } else { exp_value }
  if exp >= 308 {
    return Some(inf())
  }
  if exp <= -324 {
    return Some(0.0)
  }
  None
}

///|
fn digit_value(code : Int) -> Int {
  if code >= Char::to_int('0') && code <= Char::to_int('9') {
    return code - Char::to_int('0')
  }
  if code >= Char::to_int('a') && code <= Char::to_int('f') {
    return code - Char::to_int('a') + 10
  }
  if code >= Char::to_int('A') && code <= Char::to_int('F') {
    return code - Char::to_int('A') + 10
  }
  -1
}

///|
fn strip_digits_with_separators(
  lexeme : String,
  start : Int,
  end : Int,
  radix : Int,
  offset : Int,
  allow_empty : Bool,
) -> (String, Bool) raise {
  let mut index = start
  let mut any = false
  let mut prev_digit = false
  let underscore = Char::to_int('_')
  let sb = StringBuilder::new()
  while index < end {
    let unit = lexeme.code_unit_at(index)
    let code = UInt16::to_int(unit)
    if code == underscore {
      if !prev_digit || index + 1 >= end {
        parse_fail_offset(offset, "invalid number")
      }
      let next_unit = lexeme.code_unit_at(index + 1)
      let next_digit = digit_value(UInt16::to_int(next_unit))
      if next_digit < 0 || next_digit >= radix {
        parse_fail_offset(offset, "invalid number")
      }
      prev_digit = false
      index = index + 1
      continue
    }
    let digit = digit_value(code)
    if digit < 0 || digit >= radix {
      parse_fail_offset(offset, "invalid number")
    }
    sb.write_char(UInt16::unsafe_to_char(unit))
    any = true
    prev_digit = true
    index = index + 1
  }
  if !any {
    if allow_empty {
      return ("", false)
    }
    parse_fail_offset(offset, "invalid number")
  }
  if !prev_digit {
    parse_fail_offset(offset, "invalid number")
  }
  (sb.to_string(), true)
}

///|
fn strip_numeric_separators(lexeme : String, offset : Int) -> String raise {
  let len = lexeme.length()
  if len == 0 {
    parse_fail_offset(offset, "invalid number")
  }
  if len > 1 &&
    lexeme.has_prefix("0") &&
    !lexeme.has_prefix("0x") &&
    !lexeme.has_prefix("0X") &&
    !lexeme.has_prefix("0o") &&
    !lexeme.has_prefix("0O") &&
    !lexeme.has_prefix("0b") &&
    !lexeme.has_prefix("0B") &&
    !lexeme.contains(".") &&
    !lexeme.contains("e") &&
    !lexeme.contains("E") &&
    lexeme.contains("_") {
    parse_fail_offset(offset, "invalid number")
  }
  if lexeme.has_prefix("0x") || lexeme.has_prefix("0X") {
    let (digits, _) = strip_digits_with_separators(
      lexeme, 2, len, 16, offset, false,
    )
    return "0x" + digits
  }
  if lexeme.has_prefix("0o") || lexeme.has_prefix("0O") {
    let (digits, _) = strip_digits_with_separators(
      lexeme, 2, len, 8, offset, false,
    )
    return "0o" + digits
  }
  if lexeme.has_prefix("0b") || lexeme.has_prefix("0B") {
    let (digits, _) = strip_digits_with_separators(
      lexeme, 2, len, 2, offset, false,
    )
    return "0b" + digits
  }
  let exp_index = find_exponent_index(lexeme, 0)
  let mantissa_end = match exp_index {
    Some(index) => index
    None => len
  }
  let mut dot_index : Int? = None
  let dot = Char::to_int('.')
  let mut i = 0
  while i < mantissa_end {
    let unit = UInt16::to_int(lexeme.code_unit_at(i))
    if unit == dot {
      dot_index = Some(i)
      break
    }
    i = i + 1
  }
  let sb = StringBuilder::new()
  match dot_index {
    Some(dot_pos) => {
      let (int_digits, has_int) = strip_digits_with_separators(
        lexeme, 0, dot_pos, 10, offset, true,
      )
      let (frac_digits, has_frac) = strip_digits_with_separators(
        lexeme,
        dot_pos + 1,
        mantissa_end,
        10,
        offset,
        true,
      )
      if !has_int && !has_frac {
        parse_fail_offset(offset, "invalid number")
      }
      sb.write_string(int_digits)
      sb.write_char('.')
      sb.write_string(frac_digits)
    }
    None => {
      let (digits, _) = strip_digits_with_separators(
        lexeme, 0, mantissa_end, 10, offset, false,
      )
      sb.write_string(digits)
    }
  }
  match exp_index {
    Some(exp_pos) => {
      sb.write_char(UInt16::unsafe_to_char(lexeme.code_unit_at(exp_pos)))
      let mut exp_start = exp_pos + 1
      if exp_start >= len {
        parse_fail_offset(offset, "invalid number")
      }
      let sign = UInt16::to_int(lexeme.code_unit_at(exp_start))
      if sign == Char::to_int('+') || sign == Char::to_int('-') {
        sb.write_char(UInt16::unsafe_to_char(lexeme.code_unit_at(exp_start)))
        exp_start = exp_start + 1
      }
      let (exp_digits, _) = strip_digits_with_separators(
        lexeme, exp_start, len, 10, offset, false,
      )
      sb.write_string(exp_digits)
    }
    None => ()
  }
  sb.to_string()
}

///|
fn parse_bigint_literal_key(lexeme : String, offset : Int) -> String raise {
  let len = lexeme.length()
  if len < 2 || !lexeme.has_suffix("n") {
    parse_fail_offset(offset, "invalid bigint literal")
  }
  let mut radix = 10
  let mut index = 0
  if lexeme.has_prefix("0x") || lexeme.has_prefix("0X") {
    radix = 16
    index = 2
  } else if lexeme.has_prefix("0o") || lexeme.has_prefix("0O") {
    radix = 8
    index = 2
  } else if lexeme.has_prefix("0b") || lexeme.has_prefix("0B") {
    radix = 2
    index = 2
  }
  let end = len - 1
  if index >= end {
    parse_fail_offset(offset, "invalid bigint literal")
  }
  let (digits, _) = strip_digits_with_separators(
    lexeme, index, end, radix, offset, false,
  )
  let view = digits.view(start_offset=0, end_offset=digits.length())
  let mut value = @bigint.BigInt::from_int(0)
  let base = @bigint.BigInt::from_int(radix)
  for ch in view {
    let digit = digit_value(Char::to_int(ch))
    if digit < 0 || digit >= radix {
      parse_fail_offset(offset, "invalid bigint literal")
    }
    value = value.mul(base).add(@bigint.BigInt::from_int(digit))
  }
  value.to_string()
}

///|
fn parse_numeric_obj_key(lexeme : String, offset : Int) -> ObjKey raise {
  if lexeme.has_suffix("n") {
    ObjKey::String(parse_bigint_literal_key(lexeme, offset))
  } else {
    ObjKey::Number(parse_number_literal(lexeme, offset))
  }
}

///|
fn number_key_name(value : Double) -> String {
  if value == 0.0 {
    return "0"
  }
  let text = Double::to_string(value)
  if text.has_suffix(".0") {
    text.unsafe_substring(start=0, end=text.length() - 2)
  } else {
    text
  }
}

///|
fn obj_key_name(key : ObjKey) -> String {
  match key {
    ObjKey::Ident(id) => id
    ObjKey::Private(id) => "#" + id
    ObjKey::String(s) => s
    ObjKey::Number(value) => number_key_name(value)
    ObjKey::Computed(_) => ""
  }
}

///|
fn obj_key_is_public_constructor(key : ObjKey) -> Bool {
  match key {
    ObjKey::Ident(name) => name == "constructor"
    ObjKey::String(name) => name == "constructor"
    _ => false
  }
}

///|
fn obj_key_is_public_prototype(key : ObjKey) -> Bool {
  match key {
    ObjKey::Ident(name) => name == "prototype"
    ObjKey::String(name) => name == "prototype"
    _ => false
  }
}

///|
fn obj_key_is_private_constructor(key : ObjKey) -> Bool {
  match key {
    ObjKey::Private(name) => name == "constructor"
    _ => false
  }
}

///|
fn is_unary_expr(expr : Expr) -> Bool {
  match expr {
    Unary(_, _, _) => true
    Await(_, _) => true
    _ => false
  }
}

///|
fn is_update_target(expr : Expr) -> Bool {
  match expr {
    Ident(_, _) => true
    Member(_, _, _) => true
    Paren(inner) => is_update_target(inner)
    _ => false
  }
}

///|
fn expr_has_private_member(expr : Expr) -> Bool {
  match expr {
    Member(_, MemberKey::Private(_), _) => true
    OptionalChain(_, segments) => chain_segments_have_private(segments)
    Paren(inner) => expr_has_private_member(inner)
    _ => false
  }
}

///|
fn chain_segments_have_private(segments : Array[ChainSegment]) -> Bool {
  for segment in segments {
    match segment {
      ChainSegment::Member(MemberKey::Private(_), _, _) => return true
      _ => ()
    }
  }
  false
}

///|
fn can_insert_semicolon(tokens : Array[Token], index : Int) -> Bool {
  if is_eof(tokens, index) || match_punct(tokens, index, '}') {
    return true
  }
  peek(tokens, index).line_break_before
}

///|
fn ensure_asi(tokens : Array[Token], index : Int) -> Unit raise {
  if !can_insert_semicolon(tokens, index) {
    parse_fail(tokens, index, "expected ';'")
  }
}

///|
fn peek(tokens : Array[Token], index : Int) -> Token {
  match tokens.get(index) {
    Some(tok) => tok
    None => Token::eof(index, false)
  }
}

///|
fn match_punct(tokens : Array[Token], index : Int, ch : Char) -> Bool {
  match peek(tokens, index).kind {
    Punct(c) => c == ch
    _ => false
  }
}

///|
fn match_kind(tokens : Array[Token], index : Int, kind : TokenKind) -> Bool {
  peek(tokens, index).kind == kind
}

///|
fn is_eof(tokens : Array[Token], index : Int) -> Bool {
  match tokens.get(index) {
    Some(tok) => tok.kind == Eof
    None => true
  }
}
