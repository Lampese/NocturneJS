///|
fn function_property_get_with_receiver(
  func : FunctionValue,
  name : String,
  receiver : Value,
) -> Value raise {
  if name == "prototype" && !func.props.contains(name) {
    ensure_function_prototype(func)
  }
  property_get_from_chain(func.props, func.proto, name, receiver)
}

///|
fn function_debug_location(this_value : Value?, want_column : Bool) -> Value {
  match this_value {
    Some(Function(func)) => {
      let slot = if want_column { func.column_number } else { func.line_number }
      match slot {
        Some(value) => Number(Double::from_int(value))
        None => Undefined
      }
    }
    _ => Undefined
  }
}

///|
fn ensure_function_prototype(func : FunctionValue) -> Unit {
  if !func.is_constructor && !func.is_generator {
    return
  }
  if func.props.contains("prototype") {
    return
  }
  let proto = if func.is_generator {
    let base_proto = if func.is_async {
      match value_from_object(async_generator_proto_for_env(func.env)) {
        Some(value) => Some(value)
        None => value_from_object(async_generator_proto())
      }
    } else {
      match value_from_object(generator_proto_for_env(func.env)) {
        Some(value) => Some(value)
        None => value_from_object(generator_proto())
      }
    }
    new_object_value_with_proto(base_proto)
  } else {
    let default_proto = match
      value_from_object(object_proto_for_env(func.env)) {
      Some(value) => Some(value)
      None => value_from_object(object_proto())
    }
    new_object_value_with_proto(default_proto)
  }
  match proto {
    Object(obj) =>
      if !func.is_generator {
        obj.props.set(
          "constructor",
          property_data_non_enum(Value::Function(func)),
        )
      }
    _ => ()
  }
  let proto_prop = if func.is_class_constructor {
    property_data_const(proto)
  } else {
    property_data_non_enum_non_config(proto)
  }
  func.props.set("prototype", proto_prop)
}

///|
fn to_function_value(
  env : Env,
  func : @engine.FunctionDef,
  local_name_binding : Bool,
) -> FunctionValue {
  let props = Map::new()
  let mut len = 0
  for i = 0; i < func.params.length(); i = i + 1 {
    if func.params[i].is_rest {
      break
    }
    match func.param_inits[i] {
      Some(_) => break
      None => len = len + 1
    }
  }
  props.set("length", property_data_readonly(Number(Double::from_int(len))))
  let name = match func.name {
    Some(value) => value
    None => ""
  }
  props.set("name", property_data_readonly(String(name)))
  let mut source_text : String? = None
  let mut source_root : String? = None
  let mut source_path : String? = None
  let mut line_number : Int? = None
  let mut column_number : Int? = None
  let mut source_value : String? = None
  match current_source() {
    Some(source) => source_value = Some(source)
    None =>
      match current_function() {
        Some(func) => source_value = func.source_root
        None => ()
      }
  }
  match source_value {
    Some(source) => {
      source_root = Some(source)
      let end_offset = if func.end_offset > func.offset {
        func.end_offset
      } else {
        func.offset
      }
      source_text = Some(slice_source_range(source, func.offset, end_offset))
      let (line, column) = offset_to_line_column(source, func.offset)
      line_number = Some(line)
      column_number = Some(column)
    }
    None => ()
  }
  match current_source_path() {
    Some(path) => source_path = Some(path)
    None =>
      match current_function() {
        Some(func) => source_path = func.source_path
        None => ()
      }
  }
  let mut in_class_field_init = false
  let mut home_object : Value? = None
  let mut super_constructor : Value? = None
  if func.is_arrow {
    match current_function() {
      Some(current) => in_class_field_init = current.in_class_field_init
      None => ()
    }
    match current_super_function() {
      Some(current) => {
        home_object = current.home_object
        super_constructor = current.super_constructor
      }
      None => ()
    }
  }
  register_function_value(FunctionValue::{
    name: func.name,
    source: source_text,
    source_root,
    source_path,
    line_number,
    column_number,
    local_name_binding,
    params: func.params,
    param_inits: func.param_inits,
    body: func.body,
    env,
    id: alloc_id(),
    props,
    proto: if func.is_generator && func.is_async {
      match value_from_object(async_generator_function_proto_for_env(env)) {
        Some(value) => Some(value)
        None => value_from_object(async_generator_function_proto())
      }
    } else if func.is_generator {
      match value_from_object(generator_function_proto_for_env(env)) {
        Some(value) => Some(value)
        None => value_from_object(generator_function_proto())
      }
    } else if func.is_async {
      match value_from_object(async_function_proto_for_env(env)) {
        Some(value) => Some(value)
        None => value_from_object(async_function_proto())
      }
    } else {
      match function_proto_for_env(env) {
        Some(value) => Some(value)
        None => function_proto()
      }
    },
    home_object,
    super_constructor,
    instance_fields: [],
    private_instance_fields: [],
    private_instance_props: Map::new(),
    is_constructor: !func.is_generator && !func.is_async && !func.is_arrow,
    is_class_constructor: false,
    is_default_constructor: false,
    extensible: true,
    is_strict: func.is_strict ||
    env.strict ||
    function_body_has_use_strict(func.body),
    is_generator: func.is_generator,
    is_async: func.is_async,
    is_arrow: func.is_arrow,
    in_class_field_init,
  })
}

///|
fn new_bound_function(
  target : Value,
  bound_this : Value,
  bound_args : Array[Value],
) -> Value raise {
  let mut target_len = 0.0
  if has_own_property(target, "length") {
    let len_value = property_get(target, "length")
    match len_value {
      Number(len_num) =>
        if Double::is_nan(len_num) || len_num == 0.0 {
          target_len = 0.0
        } else if Double::is_inf(len_num) {
          if len_num > 0.0 {
            target_len = len_num
          } else {
            target_len = 0.0
          }
        } else {
          let truncated = Double::trunc(len_num)
          if truncated == 0.0 {
            target_len = 0.0
          } else {
            target_len = truncated
          }
        }
      _ => target_len = 0.0
    }
  }
  let mut new_len_num = if Double::is_inf(target_len) {
    target_len
  } else {
    target_len - Double::from_int(bound_args.length())
  }
  if new_len_num < 0.0 {
    new_len_num = 0.0
  }
  let name_value = property_get(target, "name")
  let name = match name_value {
    String(s) => s
    _ => ""
  }
  let props = Map::new()
  props.set("length", property_data_readonly(Number(new_len_num)))
  props.set("name", property_data_readonly(String("bound " + name)))
  let proto = match function_realm_env(target) {
    Some(env) =>
      match function_proto_for_env(env) {
        Some(value) => Some(value)
        None => function_proto()
      }
    None => function_proto()
  }
  let bound = register_bound_function_value(BoundFunctionValue::{
    id: alloc_id(),
    target,
    bound_this,
    bound_args,
    props,
    proto,
    is_constructor: is_constructor_value(target),
    extensible: true,
  })
  BoundFunction(bound)
}

///|
fn has_param_expressions(func : FunctionValue) -> Bool {
  for i = 0; i < func.params.length(); i = i + 1 {
    match func.param_inits[i] {
      Some(_) => return true
      None => ()
    }
    if var_binding_has_default(func.params[i].binding) {
      return true
    }
  }
  false
}

///|
fn has_simple_parameter_list(func : FunctionValue) -> Bool {
  for i = 0; i < func.params.length(); i = i + 1 {
    let param = func.params[i]
    if param.is_rest {
      return false
    }
    match param.binding {
      @engine.VarBinding::Name(_) => ()
      _ => return false
    }
    match func.param_inits[i] {
      Some(_) => return false
      None => ()
    }
  }
  true
}

///|
fn var_binding_has_default(binding : @engine.VarBinding) -> Bool {
  match binding {
    @engine.VarBinding::Name(_) => false
    @engine.VarBinding::ArrayPattern(pattern) => {
      for elem in pattern {
        match elem {
          @engine.ArrayPatternElem::Bind(_, Some(_)) => return true
          @engine.ArrayPatternElem::Bind(inner, None) =>
            if var_binding_has_default(inner) {
              return true
            }
          @engine.ArrayPatternElem::Rest(inner) =>
            if var_binding_has_default(inner) {
              return true
            }
          @engine.ArrayPatternElem::Hole => ()
        }
      }
      false
    }
    @engine.VarBinding::ObjectPattern(pattern) => {
      for elem in pattern {
        match elem {
          @engine.ObjectPatternElem::Bind(_, _, Some(_)) => return true
          @engine.ObjectPatternElem::Bind(_, inner, None) =>
            if var_binding_has_default(inner) {
              return true
            }
          @engine.ObjectPatternElem::Rest(inner) =>
            if var_binding_has_default(inner) {
              return true
            }
        }
      }
      false
    }
    @engine.VarBinding::Target(_) => false
  }
}

///|
fn var_binding_contains_name(
  binding : @engine.VarBinding,
  target : String,
) -> Bool {
  match binding {
    @engine.VarBinding::Name(name) => name == target
    @engine.VarBinding::ArrayPattern(pattern) => {
      for elem in pattern {
        match elem {
          @engine.ArrayPatternElem::Bind(inner, _) =>
            if var_binding_contains_name(inner, target) {
              return true
            }
          @engine.ArrayPatternElem::Rest(inner) =>
            if var_binding_contains_name(inner, target) {
              return true
            }
          @engine.ArrayPatternElem::Hole => ()
        }
      }
      false
    }
    @engine.VarBinding::ObjectPattern(pattern) => {
      for elem in pattern {
        match elem {
          @engine.ObjectPatternElem::Bind(_, inner, _) =>
            if var_binding_contains_name(inner, target) {
              return true
            }
          @engine.ObjectPatternElem::Rest(inner) =>
            if var_binding_contains_name(inner, target) {
              return true
            }
        }
      }
      false
    }
    @engine.VarBinding::Target(_) => false
  }
}

///|
fn params_contains_name(
  params : Array[@engine.ParamBinding],
  target : String,
) -> Bool {
  for param in params {
    if var_binding_contains_name(param.binding, target) {
      return true
    }
  }
  false
}

///|
fn collect_binding_names(
  binding : @engine.VarBinding,
  names : @hashmap.HashMap[String, Bool],
) -> Unit {
  match binding {
    @engine.VarBinding::Name(name) => names.set(name, true)
    @engine.VarBinding::ArrayPattern(pattern) =>
      for elem in pattern {
        match elem {
          @engine.ArrayPatternElem::Bind(inner, _) =>
            collect_binding_names(inner, names)
          @engine.ArrayPatternElem::Rest(inner) =>
            collect_binding_names(inner, names)
          @engine.ArrayPatternElem::Hole => ()
        }
      }
    @engine.VarBinding::ObjectPattern(pattern) =>
      for elem in pattern {
        match elem {
          @engine.ObjectPatternElem::Bind(_, inner, _) =>
            collect_binding_names(inner, names)
          @engine.ObjectPatternElem::Rest(inner) =>
            collect_binding_names(inner, names)
        }
      }
    @engine.VarBinding::Target(_) => ()
  }
}

///|
fn annex_b_param_names_for_func(
  func : FunctionValue,
) -> @hashmap.HashMap[String, Bool] {
  let names : @hashmap.HashMap[String, Bool] = @hashmap.new()
  for param in func.params {
    collect_binding_names(param.binding, names)
  }
  names
}

///|
fn init_param_binding_names(
  env : Env,
  func : FunctionValue,
  has_arguments_binding : Bool,
) -> Unit {
  env.is_param_env = true
  for param in func.params {
    collect_binding_names(param.binding, env.param_binding_names)
  }
  match func.name {
    Some(name) =>
      if func.local_name_binding {
        env.param_binding_names.set(name, true)
      }
    None => ()
  }
  if has_arguments_binding {
    env.param_binding_names.set("arguments", true)
  }
}

///|
fn declare_param_bindings_uninitialized(
  env : Env,
  func : FunctionValue,
) -> Unit {
  for param in func.params {
    declare_binding_uninitialized(env, param.binding)
  }
}

///|
fn class_constructor_result(
  func : FunctionValue,
  result : Value,
  frame : Env,
  this_bound : Value,
  new_target : Value,
) -> Value raise {
  if !func.is_class_constructor || new_target is Undefined {
    return result
  }
  let is_derived = match func.super_constructor {
    Some(_) => true
    None => false
  }
  if is_derived {
    if is_object_like(result) {
      return result
    }
    if result is Undefined {
      if frame.this_uninitialized {
        return throw_reference_error("this is not initialized")
      }
      match frame.bindings.get("this") {
        Some(value) => value
        None => Undefined
      }
    } else {
      return throw_type_error("derived constructor returned non-object")
    }
  } else {
    if is_object_like(result) {
      return result
    }
    return this_bound
  }
}

///|
fn init_instance_fields(
  func : FunctionValue,
  env : Env,
  this_value : Value,
) -> Unit raise {
  if !func.private_instance_props.is_empty() {
    for entry in func.private_instance_props {
      let (key, prop) = entry
      define_private_property(this_value, key, prop)
    }
  }
  if !func.instance_fields.is_empty() {
    for field in func.instance_fields {
      let should_set_name = match field.init {
        Some(expr) => is_anonymous_function_definition(expr)
        None => false
      }
      let value = match field.init {
        Some(expr) => eval_class_field_initializer(env, expr)
        None => Undefined
      }
      if should_set_name {
        set_anonymous_function_name(value, field.display_name, None)
      }
      if field.is_private {
        define_private_property(this_value, field.name, property_data(value))
      } else {
        let _ = create_data_property_or_throw(this_value, field.name, value)

      }
    }
  }
}

///|
fn prepare_function_body_env(var_env : Env, func : FunctionValue) -> Env raise {
  let body_env = if func.is_strict {
    var_env
  } else {
    let lex_env = Env::new(Some(var_env))
    lex_env.var_env = Some(var_env)
    lex_env.strict = var_env.strict
    lex_env
  }
  eval_predeclare_lexical_decls(body_env, func.body)
  body_env
}

///|
fn function_from_args(args : Array[Value]) -> Value raise {
  let base_env = match root_env() {
    Some(root) => root
    None => Env::new(None)
  }
  function_from_args_with_env(args, base_env)
}

///|
fn generator_function_from_args(args : Array[Value]) -> Value raise {
  let base_env = match root_env() {
    Some(root) => root
    None => Env::new(None)
  }
  function_from_args_with_env_kind(args, base_env, true, false)
}

///|
fn async_generator_function_from_args(args : Array[Value]) -> Value raise {
  let base_env = match root_env() {
    Some(root) => root
    None => Env::new(None)
  }
  function_from_args_with_env_kind(args, base_env, true, true)
}

///|
fn async_function_from_args(args : Array[Value]) -> Value raise {
  let base_env = match root_env() {
    Some(root) => root
    None => Env::new(None)
  }
  function_from_args_with_env_kind(args, base_env, false, true)
}

///|
fn function_from_args_with_env(
  args : Array[Value],
  base_env : Env,
) -> Value raise {
  function_from_args_with_env_kind(args, base_env, false, false)
}

///|
fn function_from_args_with_env_kind(
  args : Array[Value],
  base_env : Env,
  is_generator : Bool,
  is_async : Bool,
) -> Value raise {
  let mut params_source = ""
  if args.length() > 1 {
    let parts : Array[String] = []
    for i = 0; i < args.length() - 1; i = i + 1 {
      parts.push(to_string_strict(args[i]))
    }
    params_source = parts.join(",")
  }
  let body = if args.is_empty() {
    ""
  } else {
    to_string_strict(args[args.length() - 1])
  }
  let mut header = "("
  if is_async {
    header = header + "async "
  }
  header = header + "function"
  if is_generator {
    header = header + "*"
  }
  header = header + " anonymous("
  let source = header + params_source + "\n) {\n" + body + "\n})"
  let script = parse_script_with_pos(source)
  if script.body.is_empty() {
    return throw_type_error("invalid function body")
  }
  let func_def = match script.body[0] {
    FunctionDecl(func) => func
    ExprStmt(expr) =>
      match expr {
        @engine.Expr::FunctionExpr(func) => func
        @engine.Expr::Paren(inner) =>
          match inner {
            @engine.Expr::FunctionExpr(func) => func
            _ => return throw_type_error("invalid function body")
          }
        _ => return throw_type_error("invalid function body")
      }
    _ => return throw_type_error("invalid function body")
  }
  let func_env = Env::new(Some(base_env))
  func_env.strict = false
  with_source_value(source, fn() {
    let func_value = to_function_value(func_env, func_def, false)
    match func_def.name {
      Some(name) =>
        env_define_readonly(func_env, name, Value::Function(func_value), false)
      None => ()
    }
    if !is_async || is_generator {
      ensure_function_prototype(func_value)
    }
    Value::Function(func_value)
  })
}

///|
fn new_arguments_value(
  func : FunctionValue,
  env : Env,
  args : Array[Value],
  is_strict : Bool,
  mapped_allowed : Bool,
) -> Value {
  let param_names = if mapped_allowed {
    let names : Array[String] = []
    for param in func.params {
      if param.is_rest {
        break
      }
      match param.binding {
        @engine.VarBinding::Name(name) => names.push(name)
        _ => ()
      }
    }
    names
  } else {
    []
  }
  let elements : Array[Value?] = []
  for value in args {
    elements.push(Some(value))
  }
  let mapped : Array[Bool] = []
  let param_len = param_names.length()
  let arg_len = elements.length()
  for i = 0; i < arg_len; i = i + 1 {
    mapped.push(false)
  }
  if !is_strict && mapped_allowed && param_len > 0 {
    let seen : Map[String, Bool] = Map::new()
    let mut i = param_len - 1
    while i >= 0 {
      let name = param_names[i]
      if !seen.contains(name) {
        seen.set(name, true)
        if i < arg_len {
          mapped[i] = true
        }
      }
      if i == 0 {
        break
      }
      i = i - 1
    }
  }
  let props : Map[String, Property] = Map::new()
  props.set("length", property_data_non_enum(Number(Double::from_int(arg_len))))
  if is_strict || !mapped_allowed {
    let thrower = throw_type_error_value_for_env(env)
    props.set("callee", property_accessor_non_config(thrower, thrower))
  } else {
    props.set("callee", property_data_non_enum(Value::Function(func)))
  }
  let mut array_iterator_value : Value? = None
  match array_proto_for_env(env) {
    Some(array_proto_obj) => {
      match symbol_iterator_key() {
        Some(key) =>
          match array_proto_obj.props.get(key) {
            Some(prop) => array_iterator_value = Some(prop.value)
            None => ()
          }
        None => ()
      }
      if array_iterator_value is None {
        match array_proto_obj.props.get("values") {
          Some(prop) => array_iterator_value = Some(prop.value)
          None => ()
        }
      }
    }
    None => ()
  }
  match symbol_iterator_key() {
    Some(key) =>
      match array_iterator_value {
        Some(value) => props.set(key, property_data_non_enum(value))
        None =>
          props.set(
            key,
            property_data_non_enum(
              new_builtin_value(BuiltinFunction::ArrayValues),
            ),
          )
      }
    None => ()
  }
  let args_value = register_arguments_value(ArgumentsValue::{
    id: alloc_id(),
    env,
    params: param_names,
    mapped,
    elements,
    props,
    proto: value_from_object(object_proto()),
    extensible: true,
  })
  Arguments(args_value)
}

///|
fn function_name_from_key(
  name : String,
  key_value : Value?,
  prefix : String?,
) -> String {
  let base = match key_value {
    Some(value) =>
      match symbol_from_value(value) {
        Some(symbol) =>
          match symbol.description {
            Some(desc) => "[" + desc + "]"
            None => ""
          }
        None => name
      }
    None => name
  }
  match prefix {
    Some(value) => value + base
    None => base
  }
}

///|
fn set_function_name(func : FunctionValue, name : String) -> Unit {
  func.props.set("name", property_data_readonly(String(name)))
}

///|
fn set_anonymous_function_name(
  value : Value,
  name : String,
  key_value : Value?,
) -> Unit {
  match value {
    Function(func) =>
      if func.name is None {
        let mut allow = true
        match func.props.get("name") {
          Some(prop) =>
            match prop.value {
              String(value) => allow = value == ""
              _ => allow = false
            }
          None => ()
        }
        if allow {
          set_function_name(func, function_name_from_key(name, key_value, None))
        }
      }
    _ => ()
  }
}

///|
fn eval_named_expr(
  env : Env,
  expr : @engine.Expr,
  name : String,
  key_value : Value?,
) -> Value raise {
  let hint_name = function_name_from_key(name, key_value, None)
  match unwrap_paren(expr) {
    @engine.Expr::ClassExpr(class_def) =>
      if class_def.name is None {
        return eval_class(env, class_def, name_hint=Some(hint_name))
      }
    _ => ()
  }
  let value = eval_expr(env, expr)
  if is_anonymous_function_definition(expr) {
    set_anonymous_function_name(value, name, key_value)
  }
  value
}

///|
fn is_anonymous_function_definition(expr : @engine.Expr) -> Bool {
  match expr {
    @engine.Expr::FunctionExpr(func) => func.name is None
    @engine.Expr::ClassExpr(class_def) => class_def.name is None
    @engine.Expr::Paren(inner) => is_anonymous_function_definition(inner)
    _ => false
  }
}

///|
fn slice_source_range(source : String, start : Int, end : Int) -> String {
  let len = source.length()
  let start_idx = if start < 0 { 0 } else if start > len { len } else { start }
  let end_idx = if end < start_idx {
    start_idx
  } else if end > len {
    len
  } else {
    end
  }
  source.view(start_offset=start_idx, end_offset=end_idx).to_string()
}

///|
fn function_body_has_use_strict(body : Array[@engine.Stmt]) -> Bool {
  for stmt in body {
    match stmt {
      @engine.Stmt::ExprStmt(@engine.Expr::String(value, has_escape, _)) =>
        if value == "use strict" && !has_escape {
          return true
        }
      @engine.Stmt::ExprStmt(_) => break
      _ => break
    }
  }
  false
}
