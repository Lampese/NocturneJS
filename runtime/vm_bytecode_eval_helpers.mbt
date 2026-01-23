///|
fn bytecode_stack_pop(stack : Array[Value]) -> Value {
  match stack.pop() {
    Some(value) => value
    None => Undefined
  }
}

///|
fn bytecode_stack_peek(stack : Array[Value]) -> Value {
  if stack.is_empty() {
    Undefined
  } else {
    stack[stack.length() - 1]
  }
}

///|
fn bytecode_fast_get_prop(target : Value, name : String) -> Value? raise {
  match target {
    Object(obj) =>
      if obj.date_data is Some(_) && obj.props.is_empty() {
        match obj.proto {
          Some(Object(proto_obj)) => property_get_from_props(
            proto_obj.props,
            name,
            target,
          )
          _ => None
        }
      } else {
        None
      }
    _ => None
  }
}

///|
fn bytecode_date_get_timezone_offset(this_value : Value) -> Value raise {
  let ms = date_value_of(this_value)
  if !date_is_valid(ms) {
    Number(nan())
  } else {
    Number(0.0)
  }
}

///|
fn bytecode_date_set_time(this_value : Value, arg : Value?) -> Value raise {
  let obj = require_date_object(this_value)
  let value = match arg {
    Some(Number(num)) => num
    Some(other) => to_number(other)
    None => to_number(Undefined)
  }
  let new_ms = date_time_clip(value)
  obj.date_data = Some(new_ms)
  Number(new_ms)
}

///|
fn bytecode_call_with_this_noargs(
  callee : Value,
  this_value : Value,
) -> Value raise {
  match callee {
    Builtin(builtin) => {
      let actual_this = match builtin.this_value {
        Some(value) => value
        None => this_value
      }
      match builtin.kind {
        BuiltinFunction::DateGetTimezoneOffset =>
          bytecode_date_get_timezone_offset(actual_this)
        BuiltinFunction::DateSetTime => bytecode_date_set_time(actual_this, None)
        _ => call_value_with_this(callee, [], this_value)
      }
    }
    _ => call_value_with_this(callee, [], this_value)
  }
}

///|
fn bytecode_call_with_this_onearg(
  callee : Value,
  this_value : Value,
  arg : Value,
) -> Value raise {
  match callee {
    Builtin(builtin) => {
      let actual_this = match builtin.this_value {
        Some(value) => value
        None => this_value
      }
      match builtin.kind {
        BuiltinFunction::DateSetTime =>
          bytecode_date_set_time(actual_this, Some(arg))
        _ => call_value_with_this(callee, [arg], this_value)
      }
    }
    _ => call_value_with_this(callee, [arg], this_value)
  }
}

///|
let bytecode_class_eval_ref : Ref[Bool] = Ref::new(false)

///|
fn bytecode_class_eval_enabled() -> Bool {
  let mut value = false
  bytecode_class_eval_ref.update(fn(current) {
    value = current
    current
  })
  value
}

///|
fn set_bytecode_class_eval_enabled(enabled : Bool) -> Unit {
  bytecode_class_eval_ref.update(fn(_) { enabled })
}

///|
fn with_bytecode_class_eval(f : () -> Value raise) -> Value raise {
  let prev = bytecode_class_eval_enabled()
  set_bytecode_class_eval_enabled(true)
  try f() catch {
    err => {
      set_bytecode_class_eval_enabled(prev)
      raise err
    }
  } noraise {
    value => {
      set_bytecode_class_eval_enabled(prev)
      value
    }
  }
}

///|
fn with_bytecode_class_eval_step(f : () -> GenStep raise) -> GenStep raise {
  let prev = bytecode_class_eval_enabled()
  set_bytecode_class_eval_enabled(true)
  let step = f() catch {
    err => {
      set_bytecode_class_eval_enabled(prev)
      raise err
    }
  }
  set_bytecode_class_eval_enabled(prev)
  match step {
    Yield(value, next_step) =>
      Yield(value, fn(resume_state) raise {
        with_bytecode_class_eval_step(fn() raise {
          next_step(resume_state)
        })
      })
    Await(value, next_step) =>
      Await(value, fn(resume_state) raise {
        with_bytecode_class_eval_step(fn() raise {
          next_step(resume_state)
        })
      })
    Done(control) => Done(control)
  }
}

///|
fn bytecode_eval_expr(env : Env, expr : @engine.Expr) -> Value raise {
  let allow_await = match current_function() {
    Some(func) => func.is_async
    None => false
  }
  match bytecode_compile_expr(expr, false, allow_await) {
    Some(bytecode) => bytecode_exec(env, bytecode)
    None => throw_type_error("invalid bytecode expression")
  }
}

///|
fn bytecode_eval_block(
  env : Env,
  stmts : Array[@engine.Stmt],
) -> Value raise {
  let allow_await = match current_function() {
    Some(func) => func.is_async
    None => false
  }
  match bytecode_compile_block(stmts, false, allow_await) {
    Some(bytecode) => bytecode_exec(env, bytecode)
    None => throw_type_error("invalid bytecode block")
  }
}

///|
fn bytecode_collect_active_roots() -> Array[Value] {
  let roots : Array[Value] = []
  current_bytecode_state_ref.update(fn(stack) {
    for state in stack {
      for value in state.stack {
        roots.push(value)
      }
      for entry in state.for_in_stack {
        let (value, _, _) = entry
        roots.push(value)
      }
      for entry in state.for_of_stack {
        let (iterator, next_method, _) = entry
        roots.push(iterator)
        roots.push(next_method)
      }
      for entry in state.finally_stack {
        roots.push(entry.value)
      }
      for entry in state.try_stack {
        let (_, _, _, _, _, _, _, last_value) = entry
        roots.push(last_value)
      }
      for target in state.binding_stack {
        match target {
          BindingTarget::WithObject(obj) => roots.push(obj)
          _ => ()
        }
      }
    }
    stack
  })
  roots
}

///|
fn bytecode_merge_roots(
  base_roots : Array[Value],
  state : BytecodeExecState,
) -> Array[Value] {
  let roots : Array[Value] = []
  for value in base_roots {
    roots.push(value)
  }
  for value in bytecode_collect_active_roots() {
    roots.push(value)
  }
  for value in bytecode_collect_roots(state) {
    roots.push(value)
  }
  roots
}

///|
fn gen_bytecode_wrap_step(
  step : GenStep,
  base_roots : Array[Value],
  state : BytecodeExecState,
) -> GenStep {
  match step {
    Yield(value, next_step) => {
      gen_root_replace(bytecode_merge_roots(base_roots, state))
      Yield(value, fn(resume_state) raise {
        gen_bytecode_wrap_step(
          next_step(resume_state),
          base_roots,
          state,
        )
      })
    }
    Await(value, next_step) => {
      gen_root_replace(bytecode_merge_roots(base_roots, state))
      Await(value, fn(resume_state) raise {
        gen_bytecode_wrap_step(
          next_step(resume_state),
          base_roots,
          state,
        )
      })
    }
    Done(control) => {
      gen_root_replace(base_roots)
      Done(control)
    }
  }
}

///|
fn gen_bytecode_eval_expr_cont(
  env : Env,
  expr : @engine.Expr,
  k : (Value) -> GenStep raise,
) -> GenStep raise {
  if !expr_contains_yield(expr) {
    let value = bytecode_eval_expr(env, expr)
    return k(value)
  }
  let allow_await = match current_function() {
    Some(func) => func.is_async
    None => false
  }
  let allow_yield = current_function_is_generator()
  let base_roots = gen_root_snapshot()
  match bytecode_compile_expr(expr, allow_yield, allow_await) {
    Some(bytecode) => {
      let state = bytecode_exec_state_new(env, bytecode)
      let step = gen_bytecode_wrap_step(
        bytecode_exec_step(state, GenResume::Next(Undefined)),
        base_roots,
        state,
      )
      gen_bind(step, fn(control) raise {
        match control {
          Normal(value) => k(value)
          Return(value) => k(value)
          Throw(value) => Done(Throw(value))
          Break(label, value_opt) => Done(Break(label, value_opt))
          Continue(label, value_opt) => Done(Continue(label, value_opt))
        }
      })
    }
    None => {
      let _ = throw_type_error("invalid bytecode expression")
      Done(Throw(Undefined))
    }
  }
}

///|
fn gen_bytecode_eval_block(
  env : Env,
  stmts : Array[@engine.Stmt],
) -> GenStep raise {
  let allow_await = match current_function() {
    Some(func) => func.is_async
    None => false
  }
  let allow_yield = current_function_is_generator()
  let base_roots = gen_root_snapshot()
  match bytecode_compile_block(stmts, allow_yield, allow_await) {
    Some(bytecode) => {
      let state = bytecode_exec_state_new(env, bytecode)
      let step = gen_bytecode_wrap_step(
        bytecode_exec_step(state, GenResume::Next(Undefined)),
        base_roots,
        state,
      )
      gen_bind(step, fn(control) raise {
        match control {
          Normal(value) => Done(Normal(value))
          Return(value) => Done(Normal(value))
          Throw(value) => Done(Throw(value))
          Break(label, value_opt) => Done(Break(label, value_opt))
          Continue(label, value_opt) => Done(Continue(label, value_opt))
        }
      })
    }
    None => {
      let _ = throw_type_error("invalid bytecode block")
      Done(Throw(Undefined))
    }
  }
}

///|
fn bytecode_call_direct_eval(
  env : Env,
  callee : Value,
  argv : Array[Value],
  this_value : Value,
) -> Value raise {
  match callee {
    Builtin(builtin) if builtin.kind == BuiltinFunction::Eval =>
      match function_realm_env(callee) {
        Some(realm_env) =>
          if realm_env_from_env(realm_env).id == realm_env_from_env(env).id {
            return eval_with_env(env, argv)
          }
        None => ()
      }
    _ => ()
  }
  call_value_with_this(callee, argv, this_value)
}

///|
fn bytecode_target_pop(
  stack : Array[(Value) -> Unit raise],
) -> (Value) -> Unit raise {
  match stack.pop() {
    Some(value) => value
    None =>
      fn(_) raise {
        let _ = throw_type_error("invalid bytecode assignment")
        ()
      }
  }
}

///|
fn bytecode_binding_pop(stack : Array[BindingTarget]) -> BindingTarget raise {
  match stack.pop() {
    Some(value) => value
    None => {
      let _ = throw_type_error("invalid bytecode binding")
      BindingTarget::Unbound
    }
  }
}

///|
fn bytecode_binding_peek(stack : Array[BindingTarget]) -> BindingTarget raise {
  if stack.is_empty() {
    let _ = throw_type_error("invalid bytecode binding")
    BindingTarget::Unbound
  } else {
    stack[stack.length() - 1]
  }
}

///|
fn bytecode_error_to_value(err : Error) -> Value {
  match err {
    ThrowSignal(value) => value
    _ => type_error_value("unexpected error")
  }
}

///|
fn bytecode_close_for_of_stack(
  for_of_stack : Array[(Value, Value, Bool)],
  on_error : Bool,
) -> Unit raise {
  let entries : Array[(Value, Bool)] = []
  let mut i = for_of_stack.length()
  while i > 0 {
    i = i - 1
    let (iterator, _, is_async) = for_of_stack[i]
    entries.push((iterator, is_async))
  }
  for_of_stack.clear()
  let mut index = 0
  while index < entries.length() {
    let (iterator, is_async) = entries[index]
    if !(iterator is Undefined) {
      if on_error {
        if is_async {
          async_iterator_close_on_error(iterator)
        } else {
          iterator_close_on_error(iterator)
        }
      } else {
        if is_async {
          let _ = async_iterator_close(iterator)
        } else {
          iterator_close(iterator)
        }
      }
    }
    index = index + 1
  }
}

///|
fn bytecode_close_for_of_stack_to_depth(
  for_of_stack : Array[(Value, Value, Bool)],
  depth : Int,
  on_error : Bool,
) -> Unit raise {
  let mut target = depth
  if target < 0 {
    target = 0
  }
  while for_of_stack.length() > target {
    match for_of_stack.pop() {
      Some((iterator, _, is_async)) =>
        if !(iterator is Undefined) {
          if on_error {
            if is_async {
              async_iterator_close_on_error(iterator)
            } else {
              iterator_close_on_error(iterator)
            }
          } else {
            if is_async {
              let _ = async_iterator_close(iterator)
            } else {
              iterator_close(iterator)
            }
          }
        }
      None => ()
    }
  }
}

///|
type BytecodeTryEntry = (Int, Int, Int, Int, Int, Int, Int, Value)

///|
priv struct BytecodeFinallyState {
  mut code : Int
  mut value : Value
  start_pc : Int
  dispatch_pc : Int
  mut entered : Bool
}

///|
fn bytecode_finally_info_for_pc(
  func : BytecodeFunction,
  enter_pc : Int,
) -> (Int, Int) {
  for entry in func.finally_pc_pool {
    let (enter, start, dispatch) = entry
    if enter == enter_pc {
      return (start, dispatch)
    }
  }
  (-1, -1)
}

///|
fn bytecode_is_finally_start(func : BytecodeFunction, target : Int) -> Bool {
  for entry in func.finally_pc_pool {
    let (_, start, _) = entry
    if start == target {
      return true
    }
  }
  false
}

///|
fn bytecode_note_finally_enter(
  finally_stack : Array[BytecodeFinallyState],
  target_pc : Int,
  finally_body_depth : Int,
) -> Int {
  let mut i = finally_stack.length()
  while i > 0 {
    i = i - 1
    if finally_stack[i].start_pc == target_pc {
      if !finally_stack[i].entered {
        finally_stack[i].entered = true
        return finally_body_depth + 1
      }
      return finally_body_depth
    }
  }
  finally_body_depth
}

///|
fn bytecode_jump_target(code : Array[BytecodeInstr], target : Int) -> Int raise {
  if target < 0 || target >= code.length() {
    let _ = throw_type_error("invalid bytecode jump")
  }
  target
}

///|
fn bytecode_exec(env : Env, func : BytecodeFunction) -> Value raise {
  with_bytecode_class_eval(fn() raise {
    let state = bytecode_exec_state_new(env, func)
    with_current_bytecode_state_value(state, fn() raise {
      let mut env = env
      let stack = state.stack
      let target_stack = state.target_stack
      let binding_stack = state.binding_stack
      let for_in_stack = state.for_in_stack
      let for_of_stack = state.for_of_stack
      let finally_stack = state.finally_stack
      let try_stack = state.try_stack
      let env_stack = state.env_stack
      let code = func.code
      let cpool = func.cpool
      let func_pool = func.func_pool
      let class_pool = func.class_pool
      let chain_pool = func.optional_chain_pool
      let tagged_template_pool = func.tagged_template_pool
      let super_member_pool = func.super_member_pool
      let super_member_call_pool = func.super_member_call_pool
      let super_update_pool = func.super_update_pool
      let super_delete_pool = func.super_delete_pool
      let assign_array_pool = func.assign_array_pool
      let assign_object_pool = func.assign_object_pool
      let assign_invalid_pool = func.assign_invalid_pool
      let assign_for_in_pool = func.assign_for_in_pool
      let var_decl_array_pool = func.var_decl_array_pool
      let var_decl_object_pool = func.var_decl_object_pool
      let declare_binding_pool = func.declare_binding_pool
      let block_env_pool = func.block_env_pool
      let pop_envs = fn() {
        while !env_stack.is_empty() {
          let _ = env_stack.pop()
          pop_env()
        }
      }
      let mut pc = 0
      let mut finally_body_depth = 0
      let result = try {
      while pc < code.length() {
        let instr = code[pc]
        pc = pc + 1
        try {
          match instr {
            BytecodeInstr::Op(op) =>
              match op {
            BytecodeOp::Nop => ()
            BytecodeOp::Pop => {
              let _ = stack.pop()
            }
            BytecodeOp::Dup => {
              let value = bytecode_stack_pop(stack)
              stack.push(value)
              stack.push(value)
            }
            BytecodeOp::Dup2 => {
              if stack.length() < 2 {
                let _ = throw_type_error("invalid bytecode dup2")
              } else {
                let b = bytecode_stack_pop(stack)
                let a = bytecode_stack_pop(stack)
                stack.push(a)
                stack.push(b)
                stack.push(a)
                stack.push(b)
              }
            }
            BytecodeOp::Swap => {
              if stack.length() < 2 {
                let _ = throw_type_error("invalid bytecode swap")
              } else {
                let a = bytecode_stack_pop(stack)
                let b = bytecode_stack_pop(stack)
                stack.push(a)
                stack.push(b)
              }
            }
            BytecodeOp::GetElem => {
              let key_value = bytecode_stack_pop(stack)
              let target = bytecode_stack_pop(stack)
              let target_obj = to_object(target)
              let name = property_key_name(key_value)
              stack.push(property_get_with_receiver(target_obj, name, target))
            }
            BytecodeOp::GetElemRef => {
              let key_value = bytecode_stack_pop(stack)
              let target = bytecode_stack_pop(stack)
              let target_obj = to_object(target)
              let name = property_key_name(key_value)
              let current = property_get_with_receiver(target_obj, name, target)
              stack.push(target)
              stack.push(String(name))
              stack.push(current)
            }
            BytecodeOp::ToPropertyKey => {
              let key_value = bytecode_stack_pop(stack)
              stack.push(to_property_key_value(key_value))
            }
            BytecodeOp::ToStringStrict => {
              let input = bytecode_stack_pop(stack)
              stack.push(String(to_string_strict(input)))
            }
            BytecodeOp::EnterWithEnv =>
              if env.strict {
                let _ = throw_syntax_error("with not allowed in strict mode")
              } else {
                let value = bytecode_stack_pop(stack)
                let obj = to_object(value)
                let with_env = Env::{
                  id: alloc_id(),
                  parent: Some(env),
                  bindings: @hashmap.new(),
                  readonly_bindings: @hashmap.new(),
                  deletable_bindings: @hashmap.new(),
                  private_bindings: @hashmap.new(),
                  strict: env.strict,
                  with_object: Some(obj),
                  var_object: None,
                  new_target: env.new_target,
                  allow_new_target: env.allow_new_target,
                  allow_super: env.allow_super,
                  allow_super_call: env.allow_super_call,
                  this_uninitialized: false,
                  is_param_env: false,
                  is_catch_env: false,
                  param_binding_names: @hashmap.new(),
                  uninitialized_bindings: @hashmap.new(),
                  var_env: None,
                }
                register_env(with_env)
                env_stack.push(env)
                push_env(with_env)
                env = with_env
              }
            BytecodeOp::MarkCatchEnv => {
              env.is_catch_env = true
            }
            BytecodeOp::EnterFinally => {
              let enter_pc = pc - 1
              let (start_pc, dispatch_pc) =
                bytecode_finally_info_for_pc(func, enter_pc)
              if start_pc < 0 || dispatch_pc < 0 {
                let _ = throw_type_error("invalid bytecode finally start")
              }
              finally_stack.push(BytecodeFinallyState::{
                code: 0,
                value: Undefined,
                start_pc,
                dispatch_pc,
                entered: false,
              })
            }
            BytecodeOp::LeaveFinally => {
              match finally_stack.pop() {
                Some(entry) =>
                  if entry.entered && finally_body_depth > 0 {
                    finally_body_depth = finally_body_depth - 1
                  }
                None => ()
              }
            }
            BytecodeOp::SetFinallyValue =>
              if !finally_stack.is_empty() {
                let top_index = finally_stack.length() - 1
                let value = bytecode_stack_pop(stack)
                finally_stack[top_index].value = value
              }
            BytecodeOp::GetFinallyCode =>
              if finally_stack.is_empty() {
                stack.push(Number(0.0))
              } else {
                let top_index = finally_stack.length() - 1
                let code = finally_stack[top_index].code
                stack.push(Number(Double::from_int(code)))
              }
            BytecodeOp::PushFinallyValue =>
              if finally_stack.is_empty() {
                stack.push(Undefined)
              } else {
                let top_index = finally_stack.length() - 1
                stack.push(finally_stack[top_index].value)
              }
            BytecodeOp::UpdateTryLast =>
              if !try_stack.is_empty() {
                let top_index = try_stack.length() - 1
                let (
                  catch_pc,
                  stack_len,
                  target_len,
                  binding_len,
                  env_depth,
                  for_in_depth,
                  for_of_depth,
                  _,
                ) = try_stack[top_index]
                let value = bytecode_stack_peek(stack)
                try_stack[top_index] = (
                  catch_pc,
                  stack_len,
                  target_len,
                  binding_len,
                  env_depth,
                  for_in_depth,
                  for_of_depth,
                  value,
                )
              }
            BytecodeOp::PushTryLast =>
              if try_stack.is_empty() {
                stack.push(Undefined)
              } else {
                let top_index = try_stack.length() - 1
                let (_, _, _, _, _, _, _, last_value) = try_stack[top_index]
                stack.push(last_value)
              }
            BytecodeOp::PopTryLast =>
              if !try_stack.is_empty() {
                let top_index = try_stack.length() - 1
                let (
                  catch_pc,
                  stack_len,
                  target_len,
                  binding_len,
                  env_depth,
                  for_in_depth,
                  for_of_depth,
                  _,
                ) = try_stack[top_index]
                try_stack[top_index] = (
                  catch_pc,
                  stack_len,
                  target_len,
                  binding_len,
                  env_depth,
                  for_in_depth,
                  for_of_depth,
                  Undefined,
                )
              }
            BytecodeOp::SetElem => {
              let value = bytecode_stack_pop(stack)
              let key_value = bytecode_stack_pop(stack)
              let target = bytecode_stack_pop(stack)
              let _ = to_object(target)
              let name = property_key_name(key_value)
              stack.push(set_property_for_value(target, name, value, env.strict))
            }
            BytecodeOp::CheckSuperThis => {
              let _ = env_get(env, "this")
            }
            BytecodeOp::SuperSetElem => {
              let value = bytecode_stack_pop(stack)
              let key_value = bytecode_stack_pop(stack)
              let (base, receiver) = super_base_and_receiver(env)
              let name = property_key_name(key_value)
              super_set_property(env, base, receiver, name, value)
              stack.push(value)
            }
            BytecodeOp::SuperGetElem => {
              let key_value = bytecode_stack_pop(stack)
              let (base, receiver) = super_base_and_receiver(env)
              let name = property_key_name(key_value)
              let base_obj = to_object(base)
              stack.push(property_get_with_receiver(base_obj, name, receiver))
            }
            BytecodeOp::DeleteElem => {
              let key_value = bytecode_stack_pop(stack)
              let target = bytecode_stack_pop(stack)
              let name = property_key_name(key_value)
              let result = delete_property(env, target, name)
              stack.push(result)
            }
            BytecodeOp::NewObject => {
              stack.push(new_object_value())
            }
            BytecodeOp::InitPropComputed => {
              let value = bytecode_stack_pop(stack)
              let key_value = bytecode_stack_pop(stack)
              let target = bytecode_stack_pop(stack)
              let name = property_key_name(key_value)
              match target {
                Object(obj) => props_set(obj.props, name, property_data(value))
                _ => {
                  let _ = throw_type_error("invalid bytecode object")
                }
              }
              stack.push(value)
            }
            BytecodeOp::InitPropComputedNamed => {
              let value = bytecode_stack_pop(stack)
              let key_value = bytecode_stack_pop(stack)
              let target = bytecode_stack_pop(stack)
              let name = property_key_name(key_value)
              set_anonymous_function_name(value, name, Some(key_value))
              match target {
                Object(obj) => props_set(obj.props, name, property_data(value))
                _ => {
                  let _ = throw_type_error("invalid bytecode object")
                }
              }
              stack.push(value)
            }
            BytecodeOp::InitMethodComputed => {
              let func_value = bytecode_stack_pop(stack)
              let key_value = bytecode_stack_pop(stack)
              let target = bytecode_stack_pop(stack)
              let name = property_key_name(key_value)
              match (target, func_value) {
                (Object(obj), Function(func)) => {
                  func.is_constructor = false
                  set_function_name(
                    func,
                    function_name_from_key(name, Some(key_value), None),
                  )
                  set_function_home_object(func, Some(Value::Object(obj)))
                  let method_value = Value::Function(func)
                  props_set(obj.props, name, property_data(method_value))
                  stack.push(method_value)
                }
                _ => {
                  let _ = throw_type_error("invalid bytecode method")
                }
              }
            }
            BytecodeOp::InitGetterComputed => {
              let func_value = bytecode_stack_pop(stack)
              let key_value = bytecode_stack_pop(stack)
              let target = bytecode_stack_pop(stack)
              let name = property_key_name(key_value)
              match (target, func_value) {
                (Object(obj), Function(func)) => {
                  func.is_constructor = false
                  set_function_name(
                    func,
                    function_name_from_key(name, Some(key_value), Some("get ")),
                  )
                  set_function_home_object(func, Some(Value::Object(obj)))
                  let getter_value = Value::Function(func)
                  set_accessor_property(obj.props, name, Some(getter_value), None)
                  stack.push(getter_value)
                }
                _ => {
                  let _ = throw_type_error("invalid bytecode getter")
                }
              }
            }
            BytecodeOp::InitSetterComputed => {
              let func_value = bytecode_stack_pop(stack)
              let key_value = bytecode_stack_pop(stack)
              let target = bytecode_stack_pop(stack)
              let name = property_key_name(key_value)
              match (target, func_value) {
                (Object(obj), Function(func)) => {
                  func.is_constructor = false
                  set_function_name(
                    func,
                    function_name_from_key(name, Some(key_value), Some("set ")),
                  )
                  set_function_home_object(func, Some(Value::Object(obj)))
                  let setter_value = Value::Function(func)
                  set_accessor_property(obj.props, name, None, Some(setter_value))
                  stack.push(setter_value)
                }
                _ => {
                  let _ = throw_type_error("invalid bytecode setter")
                }
              }
            }
            BytecodeOp::ObjectSpread => {
              let spread_value = bytecode_stack_pop(stack)
              let target = bytecode_stack_pop(stack)
              match target {
                Object(obj) => {
                  copy_data_properties(obj, spread_value)
                  stack.push(Object(obj))
                }
                _ => {
                  let _ = throw_type_error("invalid bytecode object")
                }
              }
            }
            BytecodeOp::SetProto => {
              let proto_value = bytecode_stack_pop(stack)
              let target = bytecode_stack_pop(stack)
              match target {
                Object(obj) => {
                  match proto_value {
                    Null => {
                      rc_replace_optional_value(obj.proto, None)
                      obj.proto = None
                    }
                    _ =>
                      if is_object_like(proto_value) {
                        rc_replace_optional_value(obj.proto, Some(proto_value))
                        obj.proto = Some(proto_value)
                      }
                  }
                  stack.push(Object(obj))
                }
                _ => {
                  let _ = throw_type_error("invalid bytecode object")
                }
              }
            }
            BytecodeOp::CallArgs => {
              let args_value = bytecode_stack_pop(stack)
              let callee = bytecode_stack_pop(stack)
              let argv = build_arg_list(args_value)
              stack.push(call_value(callee, argv))
            }
            BytecodeOp::NewArgs => {
              let args_value = bytecode_stack_pop(stack)
              let callee = bytecode_stack_pop(stack)
              let argv = build_arg_list(args_value)
              stack.push(call_constructor(callee, argv))
            }
            BytecodeOp::CallWithThisArgs => {
              let args_value = bytecode_stack_pop(stack)
              let callee = bytecode_stack_pop(stack)
              let this_value = bytecode_stack_pop(stack)
              let argv = build_arg_list(args_value)
              stack.push(call_value_with_this(callee, argv, this_value))
            }
            BytecodeOp::CallWithThisEvalArgs => {
              let args_value = bytecode_stack_pop(stack)
              let callee = bytecode_stack_pop(stack)
              let this_value = bytecode_stack_pop(stack)
              let argv = build_arg_list(args_value)
              stack.push(bytecode_call_direct_eval(env, callee, argv, this_value))
            }
            BytecodeOp::SuperCallArgs => {
              let args_value = bytecode_stack_pop(stack)
              let argv = build_arg_list(args_value)
              stack.push(super_call_with_args(env, argv))
            }
            BytecodeOp::ArrayPush => {
              let value = bytecode_stack_pop(stack)
              let target = bytecode_stack_pop(stack)
              match target {
                Array(arr) => {
                  rc_incref_value(value)
                  arr.elements.push(Some(value))
                  let len = Int64::from_int(arr.elements.length())
                  set_length_prop(arr.props, Number(Int64::to_double(len)))
                  stack.push(Array(arr))
                }
                _ => {
                  let _ = throw_type_error("invalid bytecode array")
                }
              }
            }
            BytecodeOp::ArraySpread => {
              let spread_value = bytecode_stack_pop(stack)
              let target = bytecode_stack_pop(stack)
              match target {
                Array(arr) => {
                  for item in build_spread_list(spread_value) {
                    rc_incref_value(item)
                    arr.elements.push(Some(item))
                  }
                  let len = Int64::from_int(arr.elements.length())
                  set_length_prop(arr.props, Number(Int64::to_double(len)))
                  stack.push(Array(arr))
                }
                _ => {
                  let _ = throw_type_error("invalid bytecode array")
                }
              }
            }
            BytecodeOp::UpdateElemPrefixInc
            | BytecodeOp::UpdateElemPrefixDec
            | BytecodeOp::UpdateElemPostfixInc
            | BytecodeOp::UpdateElemPostfixDec => {
              let (is_inc, is_prefix) = match op {
                BytecodeOp::UpdateElemPrefixInc => (true, true)
                BytecodeOp::UpdateElemPrefixDec => (false, true)
                BytecodeOp::UpdateElemPostfixInc => (true, false)
                _ => (false, false)
              }
              let key_value = bytecode_stack_pop(stack)
              let target = bytecode_stack_pop(stack)
              let target_obj = to_object(target)
              let name = property_key_name(key_value)
              let current = property_get_with_receiver(target_obj, name, target)
              let (old_value, new_value) =
                update_values_with_flags(is_inc, current)
              let _ = set_property_for_value(
                target,
                name,
                new_value,
                env.strict,
              )
              let result =
                update_result_with_flags(is_prefix, old_value, new_value)
              stack.push(result)
            }
            BytecodeOp::ForInInit => {
              let target_value = bytecode_stack_pop(stack)
              let mut target = target_value
              let mut keys : Array[String] = []
              if !(target_value is Null || target_value is Undefined) {
                target = to_object(target_value)
                keys = enumerable_string_keys_in_chain(target)
              }
              for_in_stack.push((target, keys, 0))
            }
            BytecodeOp::ForInNext => {
              if for_in_stack.is_empty() {
                stack.push(Undefined)
              } else {
                let top_index = for_in_stack.length() - 1
                let (target, keys, start_index) = for_in_stack[top_index]
                let mut index = start_index
                let mut next_key : String? = None
                while index < keys.length() {
                  let key = keys[index]
                  index = index + 1
                  if is_enumerable_key_in_chain(target, key) {
                    next_key = Some(key)
                    break
                  }
                }
                for_in_stack[top_index] = (target, keys, index)
                match next_key {
                  Some(key) => stack.push(String(key))
                  None => stack.push(Undefined)
                }
              }
            }
            BytecodeOp::ForInEnd => {
              let _ = for_in_stack.pop()
            }
            BytecodeOp::ForOfInit => {
              let iterable = bytecode_stack_pop(stack)
              let (iterator, next_method) = get_iterator_from_value(iterable)
              for_of_stack.push((iterator, next_method, false))
            }
            BytecodeOp::ForAwaitInit => {
              let iterable = bytecode_stack_pop(stack)
              let (iterator, next_method) =
                get_async_iterator_from_value(iterable)
              for_of_stack.push((iterator, next_method, true))
            }
            BytecodeOp::ForOfNext => {
              if for_of_stack.is_empty() {
                stack.push(Undefined)
                stack.push(Bool(true))
              } else {
                let top_index = for_of_stack.length() - 1
                let (iterator, next_method, is_async) = for_of_stack[top_index]
                if iterator is Undefined {
                  stack.push(Undefined)
                  stack.push(Bool(true))
                } else {
                  let (done, value) = try
                    (if is_async {
                      async_iterator_step_value(iterator, next_method)
                    } else {
                      iterator_step_value(iterator, next_method)
                    })
                  catch {
                    err => {
                      for_of_stack[top_index] = (Undefined, Undefined, is_async)
                      raise err
                    }
                  } noraise {
                    pair => pair
                  }
                  if done {
                    for_of_stack[top_index] = (Undefined, Undefined, is_async)
                    stack.push(Undefined)
                    stack.push(Bool(true))
                  } else {
                    stack.push(value)
                    stack.push(Bool(false))
                  }
                }
              }
            }
            BytecodeOp::PushTrue => stack.push(Bool(true))
            BytecodeOp::PushFalse => stack.push(Bool(false))
            BytecodeOp::PushNull => stack.push(Null)
            BytecodeOp::PushUndefined => stack.push(Undefined)
            BytecodeOp::NewTarget => stack.push(env.new_target)
            BytecodeOp::ImportMeta => stack.push(import_meta_value())
            BytecodeOp::Await => {
              let value = bytecode_stack_pop(stack)
              let result = match current_function() {
                Some(func) => if func.is_async { await_value(value) } else { value }
                None => value
              }
              stack.push(result)
            }
            BytecodeOp::LeaveTry => {
              let _ = try_stack.pop()
            }
            BytecodeOp::Throw => raise ThrowSignal(bytecode_stack_pop(stack))
            BytecodeOp::Return => {
              let value = bytecode_stack_pop(stack)
              if !finally_stack.is_empty() {
                let top_index = finally_stack.length() - 1
                finally_stack[top_index].code = 2
                finally_stack[top_index].value = value
                if finally_stack[top_index].entered {
                  pc =
                    bytecode_jump_target(code, finally_stack[top_index].dispatch_pc)
                } else {
                  pc =
                    bytecode_jump_target(code, finally_stack[top_index].start_pc)
                  finally_stack[top_index].entered = true
                  finally_body_depth = finally_body_depth + 1
                }
                continue
              }
              bytecode_close_for_of_stack(for_of_stack, false)
              pop_envs()
              return value
            }
            BytecodeOp::ReturnNoAwait => {
              let value = bytecode_stack_pop(stack)
              if !finally_stack.is_empty() {
                let top_index = finally_stack.length() - 1
                finally_stack[top_index].code = 3
                finally_stack[top_index].value = value
                if finally_stack[top_index].entered {
                  pc =
                    bytecode_jump_target(code, finally_stack[top_index].dispatch_pc)
                } else {
                  pc =
                    bytecode_jump_target(code, finally_stack[top_index].start_pc)
                  finally_stack[top_index].entered = true
                  finally_body_depth = finally_body_depth + 1
                }
                continue
              }
              bytecode_close_for_of_stack(for_of_stack, false)
              pop_envs()
              return value
            }
            _ => ()
          }
          BytecodeInstr::I32(op, value) =>
            match op {
            BytecodeOp::EnterTry => {
              let entry = (
                value,
                stack.length(),
                target_stack.length(),
                binding_stack.length(),
                env_stack.length(),
                for_in_stack.length(),
                for_of_stack.length(),
                Undefined,
              )
              try_stack.push(entry)
            }
            BytecodeOp::SetFinallyCode =>
              if !finally_stack.is_empty() {
                let top_index = finally_stack.length() - 1
                finally_stack[top_index].code = value
              }
            BytecodeOp::PushI32 => {
              stack.push(Number(Double::from_int(value)))
            }
            BytecodeOp::Unary => {
              let input = bytecode_stack_pop(stack)
              stack.push(eval_unary_code(value, input))
            }
            BytecodeOp::Binary => {
              let right = bytecode_stack_pop(stack)
              let left = bytecode_stack_pop(stack)
              stack.push(eval_binary_code(value, left, right))
            }
            BytecodeOp::ToStringStrict => {
              let input = bytecode_stack_pop(stack)
              stack.push(String(to_string_strict(input)))
            }
            BytecodeOp::Call => {
              let argc = if value < 0 { 0 } else { value }
              let argv : Array[Value] = Array::make(argc, Undefined)
              let mut i = 0
              while i < argc {
                let index = argc - 1 - i
                argv[index] = bytecode_stack_pop(stack)
                i = i + 1
              }
              let callee = bytecode_stack_pop(stack)
              stack.push(call_value(callee, argv))
            }
            BytecodeOp::CallWithThis => {
              let argc = if value < 0 { 0 } else { value }
              if argc == 0 {
                let callee = bytecode_stack_pop(stack)
                let this_value = bytecode_stack_pop(stack)
                stack.push(bytecode_call_with_this_noargs(callee, this_value))
              } else if argc == 1 {
                let arg = bytecode_stack_pop(stack)
                let callee = bytecode_stack_pop(stack)
                let this_value = bytecode_stack_pop(stack)
                stack.push(
                  bytecode_call_with_this_onearg(callee, this_value, arg),
                )
              } else {
                let argv : Array[Value] = Array::make(argc, Undefined)
                let mut i = 0
                while i < argc {
                  let index = argc - 1 - i
                  argv[index] = bytecode_stack_pop(stack)
                  i = i + 1
                }
                let callee = bytecode_stack_pop(stack)
                let this_value = bytecode_stack_pop(stack)
                stack.push(call_value_with_this(callee, argv, this_value))
              }
            }
            BytecodeOp::CallWithThisEval => {
              let argc = if value < 0 { 0 } else { value }
              let argv : Array[Value] = Array::make(argc, Undefined)
              let mut i = 0
              while i < argc {
                let index = argc - 1 - i
                argv[index] = bytecode_stack_pop(stack)
                i = i + 1
              }
              let callee = bytecode_stack_pop(stack)
              let this_value = bytecode_stack_pop(stack)
              stack.push(bytecode_call_direct_eval(env, callee, argv, this_value))
            }
            BytecodeOp::PopEnv => {
              let mut count = if value < 0 { 0 } else { value }
              while count > 0 {
                match env_stack.pop() {
                  Some(prev) => {
                    pop_env()
                    env = prev
                  }
                  None => {
                    let _ = throw_type_error("invalid bytecode env pop")
                  }
                }
                count = count - 1
              }
            }
            BytecodeOp::New => {
              let argc = if value < 0 { 0 } else { value }
              let argv : Array[Value] = Array::make(argc, Undefined)
              let mut i = 0
              while i < argc {
                let index = argc - 1 - i
                argv[index] = bytecode_stack_pop(stack)
                i = i + 1
              }
              let callee = bytecode_stack_pop(stack)
              stack.push(call_constructor(callee, argv))
            }
            BytecodeOp::NewRegexp => {
              let flags_value = bytecode_stack_pop(stack)
              let pattern_value = bytecode_stack_pop(stack)
              let pattern = match pattern_value {
                String(text) => text
                _ => {
                  let _ = throw_type_error("invalid bytecode regexp")
                  ""
                }
              }
              let flags = match flags_value {
                String(text) => text
                _ => {
                  let _ = throw_type_error("invalid bytecode regexp")
                  ""
                }
              }
              let proto = match value_from_object(regexp_proto_for_env(env)) {
                Some(value) => Some(value)
                None => value_from_object(regexp_proto())
              }
              let regex_value = new_regexp_value(
                pattern,
                flags,
                proto,
                offset=value,
              )
              stack.push(regex_value)
            }
            BytecodeOp::NewArray => {
              let count = if value < 0 { 0 } else { value }
              let elements : Array[Value?] = Array::make(count, None)
              let mut index = count
              while index > 0 {
                index = index - 1
                let elem = bytecode_stack_pop(stack)
                elements[index] = Some(elem)
              }
              let array_proto_value = match value_from_object(
                array_proto_for_env(env),
              ) {
                Some(value) => Some(value)
                None => value_from_object(array_proto())
              }
              stack.push(
                new_array_value_with_proto(elements, proto=array_proto_value),
              )
            }
            BytecodeOp::ArrayPushHole => {
              let count = if value < 0 { 0 } else { value }
              let target = bytecode_stack_pop(stack)
              match target {
                Array(arr) => {
                  let mut i = 0
                  while i < count {
                    arr.elements.push(None)
                    i = i + 1
                  }
                  let len = Int64::from_int(arr.elements.length())
                  set_length_prop(arr.props, Number(Int64::to_double(len)))
                  stack.push(Array(arr))
                }
                _ => {
                  let _ = throw_type_error("invalid bytecode array")
                }
              }
            }
            BytecodeOp::Jmp => {
              let target_pc = bytecode_jump_target(code, value)
              pc = target_pc
              if bytecode_is_finally_start(func, target_pc) {
                finally_body_depth = bytecode_note_finally_enter(
                  finally_stack,
                  target_pc,
                  finally_body_depth,
                )
              }
            }
            BytecodeOp::JmpIfTrue => {
              if is_truthy(bytecode_stack_peek(stack)) {
                let target_pc = bytecode_jump_target(code, value)
                pc = target_pc
                if bytecode_is_finally_start(func, target_pc) {
                  finally_body_depth = bytecode_note_finally_enter(
                    finally_stack,
                    target_pc,
                    finally_body_depth,
                  )
                }
              }
            }
            BytecodeOp::JmpIfFalse => {
              if !is_truthy(bytecode_stack_peek(stack)) {
                let target_pc = bytecode_jump_target(code, value)
                pc = target_pc
                if bytecode_is_finally_start(func, target_pc) {
                  finally_body_depth = bytecode_note_finally_enter(
                    finally_stack,
                    target_pc,
                    finally_body_depth,
                  )
                }
              }
            }
            BytecodeOp::JmpIfNullish => {
              if is_nullish(bytecode_stack_peek(stack)) {
                let target_pc = bytecode_jump_target(code, value)
                pc = target_pc
                if bytecode_is_finally_start(func, target_pc) {
                  finally_body_depth = bytecode_note_finally_enter(
                    finally_stack,
                    target_pc,
                    finally_body_depth,
                  )
                }
              }
            }
            BytecodeOp::ForOfClose => {
              let mut count = if value < 0 { 0 } else { value }
              while count > 0 && !for_of_stack.is_empty() {
                match for_of_stack.pop() {
                  Some((iterator, _, is_async)) =>
                    if !(iterator is Undefined) {
                      if is_async {
                        let _ = async_iterator_close(iterator)
                      } else {
                        iterator_close(iterator)
                      }
                    }
                  None => ()
                }
                count = count - 1
              }
            }
            _ => ()
          }
          BytecodeInstr::Const(op, index) =>
            match op {
            BytecodeOp::EnterBlockEnv =>
              if index >= 0 && index < block_env_pool.length() {
                let def = block_env_pool[index]
                let block_env = Env::new(Some(env))
                for func_index in def.func_indices {
                  if func_index >= 0 && func_index < func_pool.length() {
                    let func_def = func_pool[func_index]
                    match func_def.name {
                      Some(name) => {
                        let value =
                          Value::Function(to_function_value(block_env, func_def, false))
                        env_set_local(block_env, name, value)
                      }
                      None => ()
                    }
                  } else {
                    let _ = throw_type_error("invalid bytecode block function")
                  }
                }
                for binding in def.lex_bindings {
                  declare_binding_uninitialized(block_env, binding)
                }
                for name in def.class_names {
                  env_declare_uninitialized(block_env, name)
                }
                env_stack.push(env)
                push_env(block_env)
                env = block_env
              } else {
                let _ = throw_type_error("invalid bytecode block env")
              }
            BytecodeOp::FunctionDecl =>
              if index >= 0 && index < func_pool.length() {
                let func_def = func_pool[index]
                eval_function_decl(env, func_def)
              } else {
                let _ = throw_type_error("invalid bytecode function decl")
              }
            BytecodeOp::NewFunction =>
              if index >= 0 && index < func_pool.length() {
                let func_def = func_pool[index]
                let value = match func_def.name {
                  Some(name) => {
                    let name_env = Env::new(Some(env))
                    let func_value = to_function_value(name_env, func_def, false)
                    env_define_readonly(
                      name_env,
                      name,
                      Value::Function(func_value),
                      false,
                    )
                    Value::Function(func_value)
                  }
                  None => Value::Function(to_function_value(env, func_def, false))
                }
                stack.push(value)
              } else {
                let _ = throw_type_error("invalid bytecode function")
              }
            BytecodeOp::NewClass =>
              if index >= 0 && index < class_pool.length() {
                let class_entry = class_pool[index]
                let value = with_bytecode_class_eval(fn() raise {
                  eval_class(
                    env,
                    class_entry.class_def,
                    name_hint=class_entry.name_hint,
                  )
                })
                stack.push(value)
              } else {
                let _ = throw_type_error("invalid bytecode class")
              }
            BytecodeOp::OptionalChain =>
              if index >= 0 && index < chain_pool.length() {
                let _ = chain_pool[index]
                let _ =
                  throw_type_error("unexpected bytecode optional chain opcode")
              } else {
                let _ = throw_type_error("invalid bytecode optional chain")
              }
            BytecodeOp::OptionalChainCallRef =>
              if index >= 0 && index < chain_pool.length() {
                let _ = chain_pool[index]
                let _ =
                  throw_type_error("unexpected bytecode optional chain opcode")
              } else {
                let _ = throw_type_error("invalid bytecode optional chain")
              }
            BytecodeOp::DeleteOptionalChain =>
              if index >= 0 && index < chain_pool.length() {
                let _ = chain_pool[index]
                let _ =
                  throw_type_error("unexpected bytecode optional chain opcode")
              } else {
                let _ = throw_type_error("invalid bytecode optional chain")
              }
            BytecodeOp::TaggedTemplate =>
              if index >= 0 && index < tagged_template_pool.length() {
                let def = tagged_template_pool[index]
                let value = build_template_object(env, def.parts)
                stack.push(value)
              } else {
                let _ = throw_type_error("invalid bytecode tagged template")
              }
            BytecodeOp::SuperMember =>
              if index >= 0 && index < super_member_pool.length() {
                let def = super_member_pool[index]
                match def.key {
                  @engine.MemberKey::Private(_) =>
                    return throw_syntax_error(
                      "private class field forbidden after super",
                    )
                  @engine.MemberKey::Computed(_) => {
                    let key_value = bytecode_stack_pop(stack)
                    let (base, receiver) = super_base_and_receiver(env)
                    let name = property_key_name(key_value)
                    let base_obj = to_object(base)
                    stack.push(
                      property_get_with_receiver(base_obj, name, receiver),
                    )
                  }
                  @engine.MemberKey::Ident(name) => {
                    let (base, receiver) = super_base_and_receiver(env)
                    let base_obj = to_object(base)
                    stack.push(
                      property_get_with_receiver(base_obj, name, receiver),
                    )
                  }
                }
              } else {
                let _ = throw_type_error("invalid bytecode super member")
              }
            BytecodeOp::SuperMemberCall =>
              if index >= 0 && index < super_member_call_pool.length() {
                let def = super_member_call_pool[index]
                match def.key {
                  @engine.MemberKey::Private(_) =>
                    return throw_syntax_error(
                      "private class field forbidden after super",
                    )
                  @engine.MemberKey::Computed(_) => {
                    let key_value = bytecode_stack_pop(stack)
                    let (base, receiver) = super_base_and_receiver(env)
                    let name = property_key_name(key_value)
                    let base_obj = to_object(base)
                    let callee =
                      property_get_with_receiver(base_obj, name, receiver)
                    stack.push(receiver)
                    stack.push(callee)
                  }
                  @engine.MemberKey::Ident(name) => {
                    let (base, receiver) = super_base_and_receiver(env)
                    let base_obj = to_object(base)
                    let callee =
                      property_get_with_receiver(base_obj, name, receiver)
                    stack.push(receiver)
                    stack.push(callee)
                  }
                }
              } else {
                let _ = throw_type_error("invalid bytecode super member call")
              }
            BytecodeOp::SuperCall =>
              {
                let _ = throw_type_error("invalid bytecode super call")
              }
            BytecodeOp::SuperUpdate =>
              if index >= 0 && index < super_update_pool.length() {
                let def = super_update_pool[index]
                match def.key {
                  @engine.MemberKey::Private(_) =>
                    return throw_syntax_error(
                      "private class field forbidden after super",
                    )
                  _ => ()
                }
                let (is_inc, is_prefix) = match def.op {
                  @engine.UpdateOp::Inc =>
                    match def.kind {
                      @engine.UpdateKind::Prefix => (true, true)
                      _ => (true, false)
                    }
                  _ =>
                    match def.kind {
                      @engine.UpdateKind::Prefix => (false, true)
                      _ => (false, false)
                    }
                }
                let (base, receiver) = super_base_and_receiver(env)
                let name = member_key_name(env, def.key)
                let base_obj = to_object(base)
                let current = property_get_with_receiver(base_obj, name, receiver)
                let (old_value, new_value) =
                  update_values_with_flags(is_inc, current)
                super_set_property(env, base, receiver, name, new_value)
                let result =
                  update_result_with_flags(is_prefix, old_value, new_value)
                stack.push(result)
              } else {
                let _ = throw_type_error("invalid bytecode super update")
              }
            BytecodeOp::DeleteSuper =>
              if index >= 0 && index < super_delete_pool.length() {
                let def = super_delete_pool[index]
                match def.key {
                  @engine.MemberKey::Private(_) =>
                    return throw_syntax_error(
                      "cannot delete a private class field",
                    )
                  @engine.MemberKey::Computed(expr) => {
                    let _ = super_base_and_receiver(env)
                    let _ = bytecode_eval_expr(env, expr)
                    return throw_reference_error(
                      "unsupported reference to 'super'",
                    )
                  }
                  _ => {
                    let _ = super_base_and_receiver(env)
                    return throw_reference_error(
                      "unsupported reference to 'super'",
                    )
                  }
                }
              } else {
                let _ = throw_type_error("invalid bytecode delete super")
              }
            BytecodeOp::AssignOpEval =>
              {
                let _ = throw_type_error("invalid bytecode assign op")
              }
            BytecodeOp::AssignArray =>
              if index >= 0 && index < assign_array_pool.length() {
                let def = assign_array_pool[index]
                let value = with_gc_frame(fn() raise {
                  let value = bytecode_stack_pop(stack)
                  let _ = gc_root_push(value)
                  destructure_array_pattern(env, def.pattern, value, fn(name, v) raise {
                    env_set(env, name, v)
                  })
                  value
                })
                stack.push(value)
              } else {
                let _ = throw_type_error("invalid bytecode assign array")
              }
            BytecodeOp::AssignObject =>
              if index >= 0 && index < assign_object_pool.length() {
                let def = assign_object_pool[index]
                let value = with_gc_frame(fn() raise {
                  let value = bytecode_stack_pop(stack)
                  let _ = gc_root_push(value)
                  destructure_object_pattern(env, def.pattern, value, fn(name, v) raise {
                    env_set(env, name, v)
                  })
                  value
                })
                stack.push(value)
              } else {
                let _ = throw_type_error("invalid bytecode assign object")
              }
            BytecodeOp::AssignInvalid =>
              if index >= 0 && index < assign_invalid_pool.length() {
                let def = assign_invalid_pool[index]
                let _ = with_expr_pos(def.offset, fn() raise {
                  let _ = bytecode_stack_pop(stack)
                  throw_reference_error("invalid assignment target")
                })
                stack.push(Undefined)
              } else {
                let _ = throw_type_error("invalid bytecode assign invalid")
              }
            BytecodeOp::AssignForInExpr =>
              if index >= 0 && index < assign_for_in_pool.length() {
                let def = assign_for_in_pool[index]
                let _ = with_gc_frame(fn() raise {
                  let value = bytecode_stack_pop(stack)
                  let _ = gc_root_push(value)
                  assign_for_in_expr(env, def.expr, value)
                  Undefined
                })
              } else {
                let _ = throw_type_error("invalid bytecode assign for-in")
              }
            BytecodeOp::YieldStar => {
              let _ = throw_type_error("invalid bytecode yield star")
            }
            BytecodeOp::VarDeclArray =>
              if index >= 0 && index < var_decl_array_pool.length() {
                let def = var_decl_array_pool[index]
                let _ = with_gc_frame(fn() raise {
                  let value = bytecode_stack_pop(stack)
                  let _ = gc_root_push(value)
                  let assign = fn(name : String, v : Value) raise {
                    match def.kind {
                      @engine.VarKind::ConstDecl =>
                        env_define_readonly(env, name, v, true)
                      @engine.VarKind::Var => env_set(env, name, v)
                      _ => env_initialize_binding(env, name, v)
                    }
                  }
                  destructure_array_pattern(env, def.pattern, value, assign)
                  Undefined
                })
              } else {
                let _ = throw_type_error("invalid bytecode var decl array")
              }
            BytecodeOp::VarDeclObject =>
              if index >= 0 && index < var_decl_object_pool.length() {
                let def = var_decl_object_pool[index]
                let _ = with_gc_frame(fn() raise {
                  let value = bytecode_stack_pop(stack)
                  let _ = gc_root_push(value)
                  let assign = fn(name : String, v : Value) raise {
                    match def.kind {
                      @engine.VarKind::ConstDecl =>
                        env_define_readonly(env, name, v, true)
                      @engine.VarKind::Var => env_set(env, name, v)
                      _ => env_initialize_binding(env, name, v)
                    }
                  }
                  destructure_object_pattern(env, def.pattern, value, assign)
                  Undefined
                })
              } else {
                let _ = throw_type_error("invalid bytecode var decl object")
              }
            BytecodeOp::DeclareBinding =>
              if index >= 0 && index < declare_binding_pool.length() {
                let def = declare_binding_pool[index]
                declare_binding_names(env, def.binding)
              } else {
                let _ = throw_type_error("invalid bytecode declare binding")
              }
            _ =>
              if index >= 0 && index < cpool.length() {
                let const_value = cpool[index]
                match op {
                  BytecodeOp::PushConst => stack.push(const_value)
                  BytecodeOp::GetName =>
                    match const_value {
                      String(name) => stack.push(env_get(env, name))
                      _ => stack.push(Undefined)
                    }
                  BytecodeOp::TypeofName =>
                    match const_value {
                      String(name) =>
                        if env_has(env, name) {
                          stack.push(String(typeof_value(env_get(env, name))))
                        } else {
                          stack.push(String("undefined"))
                        }
                      _ => {
                        let _ = throw_type_error("invalid bytecode typeof name")
                      }
                    }
                  BytecodeOp::DeleteName =>
                    match const_value {
                      String(name) =>
                        stack.push(delete_ident(env, name, env.strict))
                      _ => {
                        let _ = throw_type_error("invalid bytecode delete name")
                      }
                    }
                  BytecodeOp::ThrowSyntaxError =>
                    match const_value {
                      String(message) => {
                        let _ = throw_syntax_error(message)
                      }
                      _ => {
                        let _ = throw_type_error(
                          "invalid bytecode throw syntax error",
                        )
                      }
                    }
                  BytecodeOp::ThrowReferenceError =>
                    match const_value {
                      String(message) => {
                        let _ = throw_reference_error(message)
                      }
                      _ => {
                        let _ = throw_type_error(
                          "invalid bytecode throw reference error",
                        )
                      }
                    }
                  BytecodeOp::ThrowTypeError =>
                    match const_value {
                      String(message) => {
                        let _ = throw_type_error(message)
                      }
                      _ => {
                        let _ = throw_type_error(
                          "invalid bytecode throw type error",
                        )
                      }
                    }
                  BytecodeOp::PrivateIn =>
                    match const_value {
                      String(name) => {
                        let target = bytecode_stack_pop(stack)
                        stack.push(Bool(eval_private_in(env, name, target)))
                      }
                      _ => {
                        let _ = throw_type_error("invalid bytecode private in")
                      }
                    }
                  BytecodeOp::BindName =>
                    match const_value {
                      String(name) => {
                        let assigner = prepare_identifier_assignment_target(
                          env,
                          name,
                          env.strict,
                        )
                        target_stack.push(assigner)
                      }
                      _ => {
                        let _ = throw_type_error("invalid bytecode binding name")
                      }
                    }
                  BytecodeOp::AssignName =>
                    match const_value {
                      String(name) => {
                        let value = bytecode_stack_peek(stack)
                        set_anonymous_function_name(value, name, None)
                        let assigner = bytecode_target_pop(target_stack)
                        assigner(value)
                      }
                      _ => {
                        let _ = throw_type_error("invalid bytecode assignment name")
                      }
                    }
                  BytecodeOp::AssignNameNoName =>
                    match const_value {
                      String(_) => {
                        let value = bytecode_stack_peek(stack)
                        let assigner = bytecode_target_pop(target_stack)
                        assigner(value)
                      }
                      _ => {
                        let _ = throw_type_error("invalid bytecode assignment name")
                      }
                    }
                  BytecodeOp::BindNameTarget =>
                    match const_value {
                      String(name) => {
                        let target = resolve_binding_target(env, name)
                        binding_stack.push(target)
                      }
                      _ => {
                        let _ = throw_type_error("invalid bytecode binding target")
                      }
                    }
                  BytecodeOp::GetNameFromTarget =>
                    match const_value {
                      String(name) => {
                        let target = bytecode_binding_peek(binding_stack)
                        stack.push(
                          get_binding_value_from_target(
                            env,
                            name,
                            target,
                            env.strict,
                          ),
                        )
                      }
                      _ => {
                        let _ = throw_type_error("invalid bytecode binding read")
                      }
                    }
                  BytecodeOp::AssignFromTarget =>
                    match const_value {
                      String(name) => {
                        let value = bytecode_stack_peek(stack)
                        let target = bytecode_binding_pop(binding_stack)
                        assign_binding_value_to_target(
                          env,
                          name,
                          target,
                          value,
                          env.strict,
                        )
                      }
                      _ => {
                        let _ = throw_type_error("invalid bytecode binding write")
                      }
                    }
                  BytecodeOp::PopTarget => {
                    let _ = bytecode_binding_pop(binding_stack)
                  }
                  BytecodeOp::SetAnonName =>
                    match const_value {
                      String(name) => {
                        let value = bytecode_stack_peek(stack)
                        set_anonymous_function_name(value, name, None)
                      }
                      _ => {
                        let _ = throw_type_error("invalid bytecode anon name")
                      }
                    }
                  BytecodeOp::VarInitName =>
                    match const_value {
                      String(name) => {
                        let value = bytecode_stack_pop(stack)
                        let target = resolve_binding_target(env, name)
                        match target {
                          BindingTarget::WithObject(obj) => {
                            let _ = property_set(obj, name, value, env.strict)
                          }
                          BindingTarget::VarObject(obj) => {
                            let _ = property_set(obj, name, value, env.strict)
                          }
                          BindingTarget::Env(target_env) =>
                            env_set_local(target_env, name, value)
                          BindingTarget::Unbound =>
                            env_set_with_strict_no_with(
                              var_env_for(env),
                              name,
                              value,
                              env.strict,
                            )
                        }
                      }
                      _ => {
                        let _ = throw_type_error("invalid bytecode var init name")
                      }
                    }
                  BytecodeOp::InitName =>
                    match const_value {
                      String(name) => {
                        let value = bytecode_stack_pop(stack)
                        env_initialize_binding(env, name, value)
                      }
                      _ => {
                        let _ = throw_type_error("invalid bytecode init name")
                      }
                    }
                  BytecodeOp::InitConstName =>
                    match const_value {
                      String(name) => {
                        let value = bytecode_stack_pop(stack)
                        env_define_readonly(env, name, value, true)
                      }
                      _ => {
                        let _ = throw_type_error("invalid bytecode const init name")
                      }
                    }
                  BytecodeOp::GetProp =>
                    match const_value {
                      String(name) => {
                        let target = bytecode_stack_pop(stack)
                        match bytecode_fast_get_prop(target, name) {
                          Some(value) => stack.push(value)
                          None => {
                            let target_obj = to_object(target)
                            stack.push(
                              property_get_with_receiver(target_obj, name, target),
                            )
                          }
                        }
                      }
                      _ => {
                        let _ = throw_type_error("invalid bytecode property name")
                      }
                    }
                  BytecodeOp::GetPrivate =>
                    match const_value {
                      String(name) => {
                        let target = bytecode_stack_pop(stack)
                        let key_name = env_private_key_checked(env, name)
                        stack.push(private_property_get(target, key_name, name))
                      }
                      _ => {
                        let _ = throw_type_error("invalid bytecode private name")
                      }
                    }
                  BytecodeOp::SetProp =>
                    match const_value {
                      String(name) => {
                        let value = bytecode_stack_pop(stack)
                        let target = bytecode_stack_pop(stack)
                        stack.push(
                          set_property_for_value(target, name, value, env.strict),
                        )
                      }
                      _ => {
                        let _ = throw_type_error("invalid bytecode property name")
                      }
                    }
                  BytecodeOp::SetPrivate =>
                    match const_value {
                      String(name) => {
                        let value = bytecode_stack_pop(stack)
                        let target = bytecode_stack_pop(stack)
                        let key_name = env_private_key_checked(env, name)
                        private_property_set(target, key_name, name, value)
                        stack.push(value)
                      }
                      _ => {
                        let _ = throw_type_error("invalid bytecode private name")
                      }
                    }
                  BytecodeOp::SuperSetProp =>
                    match const_value {
                      String(name) => {
                        let value = bytecode_stack_pop(stack)
                        let (base, receiver) = super_base_and_receiver(env)
                        super_set_property(env, base, receiver, name, value)
                        stack.push(value)
                      }
                      _ => {
                        let _ = throw_type_error("invalid bytecode property name")
                      }
                    }
                  BytecodeOp::InitProp =>
                    match const_value {
                      String(name) => {
                        let value = bytecode_stack_pop(stack)
                        let target = bytecode_stack_pop(stack)
                        match target {
                          Object(obj) => props_set(obj.props, name, property_data(value))
                          _ => {
                            let _ = throw_type_error("invalid bytecode object")
                          }
                        }
                        stack.push(value)
                      }
                      _ => {
                        let _ = throw_type_error("invalid bytecode property name")
                      }
                    }
                  BytecodeOp::InitPropNamed =>
                    match const_value {
                      String(name) => {
                        let value = bytecode_stack_pop(stack)
                        let target = bytecode_stack_pop(stack)
                        set_anonymous_function_name(value, name, None)
                        match target {
                          Object(obj) => props_set(obj.props, name, property_data(value))
                          _ => {
                            let _ = throw_type_error("invalid bytecode object")
                          }
                        }
                        stack.push(value)
                      }
                      _ => {
                        let _ = throw_type_error("invalid bytecode property name")
                      }
                    }
                  BytecodeOp::InitMethod =>
                    match const_value {
                      String(name) => {
                        let func_value = bytecode_stack_pop(stack)
                        let target = bytecode_stack_pop(stack)
                        match (target, func_value) {
                          (Object(obj), Function(func)) => {
                            func.is_constructor = false
                            set_function_name(
                              func,
                              function_name_from_key(name, None, None),
                            )
                            set_function_home_object(func, Some(Value::Object(obj)))
                            let method_value = Value::Function(func)
                            props_set(obj.props, name, property_data(method_value))
                            stack.push(method_value)
                          }
                          _ => {
                            let _ = throw_type_error("invalid bytecode method")
                          }
                        }
                      }
                      _ => {
                        let _ = throw_type_error("invalid bytecode method name")
                      }
                    }
                  BytecodeOp::InitGetter =>
                    match const_value {
                      String(name) => {
                        let func_value = bytecode_stack_pop(stack)
                        let target = bytecode_stack_pop(stack)
                        match (target, func_value) {
                          (Object(obj), Function(func)) => {
                            func.is_constructor = false
                            set_function_name(
                              func,
                              function_name_from_key(name, None, Some("get ")),
                            )
                            set_function_home_object(func, Some(Value::Object(obj)))
                            let getter_value = Value::Function(func)
                            set_accessor_property(
                              obj.props,
                              name,
                              Some(getter_value),
                              None,
                            )
                            stack.push(getter_value)
                          }
                          _ => {
                            let _ = throw_type_error("invalid bytecode getter")
                          }
                        }
                      }
                      _ => {
                        let _ = throw_type_error("invalid bytecode getter name")
                      }
                    }
                  BytecodeOp::InitSetter =>
                    match const_value {
                      String(name) => {
                        let func_value = bytecode_stack_pop(stack)
                        let target = bytecode_stack_pop(stack)
                        match (target, func_value) {
                          (Object(obj), Function(func)) => {
                            func.is_constructor = false
                            set_function_name(
                              func,
                              function_name_from_key(name, None, Some("set ")),
                            )
                            set_function_home_object(func, Some(Value::Object(obj)))
                            let setter_value = Value::Function(func)
                            set_accessor_property(
                              obj.props,
                              name,
                              None,
                              Some(setter_value),
                            )
                            stack.push(setter_value)
                          }
                          _ => {
                            let _ = throw_type_error("invalid bytecode setter")
                          }
                        }
                      }
                      _ => {
                        let _ = throw_type_error("invalid bytecode setter name")
                      }
                    }
                  BytecodeOp::DeleteProp =>
                    match const_value {
                      String(name) => {
                        let target = bytecode_stack_pop(stack)
                        stack.push(delete_property(env, target, name))
                      }
                      _ => {
                        let _ = throw_type_error("invalid bytecode property name")
                      }
                    }
                  BytecodeOp::UpdateNamePrefixInc
                  | BytecodeOp::UpdateNamePrefixDec
                  | BytecodeOp::UpdateNamePostfixInc
                  | BytecodeOp::UpdateNamePostfixDec =>
                    match const_value {
                      String(name) => {
                        let (is_inc, is_prefix) = match op {
                          BytecodeOp::UpdateNamePrefixInc => (true, true)
                          BytecodeOp::UpdateNamePrefixDec => (false, true)
                          BytecodeOp::UpdateNamePostfixInc => (true, false)
                          _ => (false, false)
                        }
                        let target = resolve_binding_target(env, name)
                        let current = get_binding_value_from_target(
                          env,
                          name,
                          target,
                          env.strict,
                        )
                        let (old_value, new_value) =
                          update_values_with_flags(is_inc, current)
                        assign_binding_value_to_target(
                          env,
                          name,
                          target,
                          new_value,
                          env.strict,
                        )
                        let result =
                          update_result_with_flags(is_prefix, old_value, new_value)
                        stack.push(result)
                      }
                      _ => {
                        let _ = throw_type_error("invalid bytecode update name")
                      }
                    }
                  BytecodeOp::UpdatePropPrefixInc
                  | BytecodeOp::UpdatePropPrefixDec
                  | BytecodeOp::UpdatePropPostfixInc
                  | BytecodeOp::UpdatePropPostfixDec =>
                    match const_value {
                      String(name) => {
                        let (is_inc, is_prefix) = match op {
                          BytecodeOp::UpdatePropPrefixInc => (true, true)
                          BytecodeOp::UpdatePropPrefixDec => (false, true)
                          BytecodeOp::UpdatePropPostfixInc => (true, false)
                          _ => (false, false)
                        }
                        let target = bytecode_stack_pop(stack)
                        let target_obj = to_object(target)
                        let current =
                          property_get_with_receiver(target_obj, name, target)
                        let (old_value, new_value) =
                          update_values_with_flags(is_inc, current)
                        let _ = set_property_for_value(
                          target,
                          name,
                          new_value,
                          env.strict,
                        )
                        let result =
                          update_result_with_flags(is_prefix, old_value, new_value)
                        stack.push(result)
                      }
                      _ => {
                        let _ = throw_type_error("invalid bytecode update property")
                      }
                    }
                  BytecodeOp::UpdatePrivatePrefixInc
                  | BytecodeOp::UpdatePrivatePrefixDec
                  | BytecodeOp::UpdatePrivatePostfixInc
                  | BytecodeOp::UpdatePrivatePostfixDec =>
                    match const_value {
                      String(name) => {
                        let (is_inc, is_prefix) = match op {
                          BytecodeOp::UpdatePrivatePrefixInc => (true, true)
                          BytecodeOp::UpdatePrivatePrefixDec => (false, true)
                          BytecodeOp::UpdatePrivatePostfixInc => (true, false)
                          _ => (false, false)
                        }
                        let target = bytecode_stack_pop(stack)
                        let key_name = env_private_key_checked(env, name)
                        let current = private_property_get(target, key_name, name)
                        let (old_value, new_value) =
                          update_values_with_flags(is_inc, current)
                        private_property_set(target, key_name, name, new_value)
                        let result =
                          update_result_with_flags(is_prefix, old_value, new_value)
                        stack.push(result)
                      }
                      _ => {
                        let _ = throw_type_error(
                          "invalid bytecode update private",
                        )
                      }
                    }
                  BytecodeOp::GetCallRef =>
                    match const_value {
                      String(name) => {
                        let (callee_value, this_value) =
                          env_get_call_reference(env, name)
                        stack.push(this_value)
                        stack.push(callee_value)
                      }
                      _ => {
                        let _ = throw_type_error("invalid bytecode call reference")
                      }
                    }
                  _ => ()
                }
              }
            }
        }
      } catch {
        ThrowSignal(value) => {
          let mut handled = false
          while !try_stack.is_empty() {
            let top_index = try_stack.length() - 1
            let (
              catch_pc,
              stack_len,
              target_len,
              binding_len,
              env_depth,
              for_in_depth,
              for_of_depth,
              last_value,
            ) = try_stack[top_index]
            if catch_pc < 0 {
              let _ = try_stack.pop()
              continue
            }
            handled = true
            while stack.length() > stack_len {
              let _ = stack.pop()
            }
            while target_stack.length() > target_len {
              let _ = target_stack.pop()
            }
            while binding_stack.length() > binding_len {
              let _ = binding_stack.pop()
            }
            while for_in_stack.length() > for_in_depth {
              let _ = for_in_stack.pop()
            }
            bytecode_close_for_of_stack_to_depth(
              for_of_stack,
              for_of_depth,
              true,
            )
            while env_stack.length() > env_depth {
              match env_stack.pop() {
                Some(prev) => {
                  pop_env()
                  env = prev
                }
                None => {
                  let _ = throw_type_error("invalid bytecode env pop")
                }
              }
            }
            let target_pc = bytecode_jump_target(code, catch_pc)
            try_stack[top_index] = (
              -1,
              stack_len,
              target_len,
              binding_len,
              env_depth,
              for_in_depth,
              for_of_depth,
              last_value,
            )
            stack.push(value)
            pc = target_pc
            break
          }
          if handled {
            continue
          }
          raise ThrowSignal(value)
        }
        err => raise err
      }
    }
    bytecode_close_for_of_stack(for_of_stack, false)
    pop_envs()
    Undefined
  } catch {
    err => {
      bytecode_close_for_of_stack(for_of_stack, true)
      pop_envs()
      raise err
    }
  }
  result
  })
  })
}

///|
priv struct BytecodeExecState {
  func : BytecodeFunction
  mut env : Env
  stack : Array[Value]
  target_stack : Array[(Value) -> Unit raise]
  binding_stack : Array[BindingTarget]
  for_in_stack : Array[(Value, Array[String], Int)]
  for_of_stack : Array[(Value, Value, Bool)]
  finally_stack : Array[BytecodeFinallyState]
  try_stack : Array[BytecodeTryEntry]
  env_stack : Array[Env]
  mut pc : Int
  mut resume_pending : Bool
  mut finally_body_depth : Int
}

///|
let current_bytecode_state_ref : Ref[Array[BytecodeExecState]] = Ref::new([])

///|
fn with_current_bytecode_state_value(
  state : BytecodeExecState,
  f : () -> Value raise,
) -> Value raise {
  current_bytecode_state_ref.update(fn(stack) {
    stack.push(state)
    stack
  })
  try {
    let result = f()
    current_bytecode_state_ref.update(fn(stack) {
      if !stack.is_empty() {
        let _ = stack.pop()
      }
      stack
    })
    result
  } catch {
    err => {
      current_bytecode_state_ref.update(fn(stack) {
        if !stack.is_empty() {
          let _ = stack.pop()
        }
        stack
      })
      raise err
    }
  }
}

///|
fn with_current_bytecode_state_step(
  state : BytecodeExecState,
  f : () -> GenStep raise,
) -> GenStep raise {
  current_bytecode_state_ref.update(fn(stack) {
    stack.push(state)
    stack
  })
  try {
    let result = f()
    current_bytecode_state_ref.update(fn(stack) {
      if !stack.is_empty() {
        let _ = stack.pop()
      }
      stack
    })
    result
  } catch {
    err => {
      current_bytecode_state_ref.update(fn(stack) {
        if !stack.is_empty() {
          let _ = stack.pop()
        }
        stack
      })
      raise err
    }
  }
}

///|
fn bytecode_visit_roots(visit_value : (Value) -> Unit) -> Unit {
  current_bytecode_state_ref.update(fn(stack) {
    for state in stack {
      for value in state.stack {
        visit_value(value)
      }
      for entry in state.for_in_stack {
        let (value, _, _) = entry
        visit_value(value)
      }
      for entry in state.for_of_stack {
        let (iterator, next_method, _) = entry
        visit_value(iterator)
        visit_value(next_method)
      }
      for entry in state.finally_stack {
        visit_value(entry.value)
      }
      for entry in state.try_stack {
        let (_, _, _, _, _, _, _, last_value) = entry
        visit_value(last_value)
      }
      for target in state.binding_stack {
        match target {
          BindingTarget::WithObject(obj) => visit_value(obj)
          _ => ()
        }
      }
    }
    stack
  })
}

///|
fn bytecode_exec_state_new(env : Env, func : BytecodeFunction) -> BytecodeExecState {
  BytecodeExecState::{
    func,
    env,
    stack: [],
    target_stack: [],
    binding_stack: [],
    for_in_stack: [],
    for_of_stack: [],
    finally_stack: [],
    try_stack: [],
    env_stack: [],
    pc: 0,
    resume_pending: false,
    finally_body_depth: 0,
  }
}

///|
fn bytecode_exec_pop_envs(env_stack : Array[Env]) -> Unit {
  while !env_stack.is_empty() {
    let _ = env_stack.pop()
    pop_env()
  }
}

///|
fn bytecode_collect_roots(state : BytecodeExecState) -> Array[Value] {
  let roots : Array[Value] = []
  for value in state.stack {
    roots.push(value)
  }
  for entry in state.for_in_stack {
    let (value, _, _) = entry
    roots.push(value)
  }
  for entry in state.for_of_stack {
    let (iterator, next_method, _) = entry
    roots.push(iterator)
    roots.push(next_method)
  }
  for entry in state.finally_stack {
    roots.push(entry.value)
  }
  for entry in state.try_stack {
    let (_, _, _, _, _, _, _, last_value) = entry
    roots.push(last_value)
  }
  for target in state.binding_stack {
    match target {
      BindingTarget::WithObject(obj) => roots.push(obj)
      _ => ()
    }
  }
  roots
}

///|
fn bytecode_update_gen_roots(state : BytecodeExecState) -> Unit {
  let roots = bytecode_collect_roots(state)
  gen_root_replace(roots)
}

fn bytecode_async_return(
  state : BytecodeExecState,
  env : Env,
  pc : Int,
  finally_body_depth : Int,
  value : Value,
) -> GenStep {
  state.env = env
  state.pc = pc
  state.resume_pending = true
  state.finally_body_depth = finally_body_depth
  bytecode_update_gen_roots(state)
  Await(value, fn(next_resume) raise {
    match next_resume {
      GenResume::Next(awaited) =>
        bytecode_exec_step(state, GenResume::Return(awaited))
      GenResume::Return(arg) => bytecode_exec_step(state, GenResume::Return(arg))
      GenResume::Throw(err) => bytecode_exec_step(state, GenResume::Throw(err))
    }
  })
}


fn bytecode_exec_step(state : BytecodeExecState, resume_state : GenResume) -> GenStep raise {
  with_current_bytecode_state_step(state, fn() raise {
  let func = state.func
  let code = func.code
  let cpool = func.cpool
  let func_pool = func.func_pool
  let class_pool = func.class_pool
  let chain_pool = func.optional_chain_pool
  let tagged_template_pool = func.tagged_template_pool
  let super_member_pool = func.super_member_pool
  let super_member_call_pool = func.super_member_call_pool
  let super_update_pool = func.super_update_pool
  let super_delete_pool = func.super_delete_pool
  let assign_array_pool = func.assign_array_pool
  let assign_object_pool = func.assign_object_pool
  let assign_invalid_pool = func.assign_invalid_pool
  let assign_for_in_pool = func.assign_for_in_pool
  let yield_star_pool = func.yield_star_pool
  let var_decl_array_pool = func.var_decl_array_pool
  let var_decl_object_pool = func.var_decl_object_pool
  let declare_binding_pool = func.declare_binding_pool
  let block_env_pool = func.block_env_pool
  let stack = state.stack
  let target_stack = state.target_stack
  let binding_stack = state.binding_stack
  let for_in_stack = state.for_in_stack
  let for_of_stack = state.for_of_stack
  let finally_stack = state.finally_stack
  let try_stack = state.try_stack
  let env_stack = state.env_stack
  let mut env = state.env
  let pop_envs = fn() {
    bytecode_exec_pop_envs(env_stack)
  }
  let mut pc = state.pc
  let mut finally_body_depth = state.finally_body_depth
  let mut resume_throw : Value? = None
  let mut resume_return : Value? = None
  if state.resume_pending {
    state.resume_pending = false
    match resume_state {
      GenResume::Next(value) => stack.push(value)
      GenResume::Throw(value) => resume_throw = Some(value)
      GenResume::Return(value) => resume_return = Some(value)
    }
  }
  let result = try {
    while pc < code.length() || resume_throw is Some(_) || resume_return is Some(_) {
      match resume_return {
        Some(value) => {
          resume_return = None
          if finally_stack.is_empty() {
            bytecode_close_for_of_stack(for_of_stack, false)
            pop_envs()
            state.env = env
            state.pc = pc
            state.finally_body_depth = finally_body_depth
            gen_root_replace([])
            return Done(Return(value))
          }
          let top_index = finally_stack.length() - 1
          finally_stack[top_index].code = 2
          finally_stack[top_index].value = value
          if finally_stack[top_index].entered {
            pc =
              bytecode_jump_target(code, finally_stack[top_index].dispatch_pc)
          } else {
            pc = bytecode_jump_target(code, finally_stack[top_index].start_pc)
            finally_stack[top_index].entered = true
            finally_body_depth = finally_body_depth + 1
          }
          continue
        }
        None => ()
      }
      let mut pending_throw : Value? = None
      match resume_throw {
        Some(value) => {
          resume_throw = None
          pending_throw = Some(value)
        }
        None => ()
      }
      let mut has_instr = false
      let mut instr_index = pc
      if pending_throw is None && pc < code.length() {
        has_instr = true
        instr_index = pc
        pc = pc + 1
      }
      if pending_throw is None && !has_instr {
        break
      }
      try {
        match pending_throw {
          Some(value) => raise ThrowSignal(value)
          None => ()
        }
        if has_instr {
          let instr = code[instr_index]
          match instr {
            BytecodeInstr::Op(op) =>
              match op {
              BytecodeOp::Nop => ()
              BytecodeOp::Pop => {
                let _ = stack.pop()
              }
              BytecodeOp::Dup => {
                let value = bytecode_stack_pop(stack)
                stack.push(value)
                stack.push(value)
              }
              BytecodeOp::Dup2 => {
                if stack.length() < 2 {
                  let _ = throw_type_error("invalid bytecode dup2")
                } else {
                  let b = bytecode_stack_pop(stack)
                  let a = bytecode_stack_pop(stack)
                  stack.push(a)
                  stack.push(b)
                  stack.push(a)
                  stack.push(b)
                }
              }
              BytecodeOp::Swap => {
                if stack.length() < 2 {
                  let _ = throw_type_error("invalid bytecode swap")
                } else {
                  let a = bytecode_stack_pop(stack)
                  let b = bytecode_stack_pop(stack)
                  stack.push(a)
                  stack.push(b)
                }
              }
              BytecodeOp::GetElem => {
                let key_value = bytecode_stack_pop(stack)
                let target = bytecode_stack_pop(stack)
                let target_obj = to_object(target)
                let name = property_key_name(key_value)
                stack.push(property_get_with_receiver(target_obj, name, target))
              }
              BytecodeOp::GetElemRef => {
                let key_value = bytecode_stack_pop(stack)
                let target = bytecode_stack_pop(stack)
                let target_obj = to_object(target)
                let name = property_key_name(key_value)
                let current = property_get_with_receiver(target_obj, name, target)
                stack.push(target)
                stack.push(String(name))
                stack.push(current)
              }
              BytecodeOp::ToPropertyKey => {
                let key_value = bytecode_stack_pop(stack)
                stack.push(to_property_key_value(key_value))
              }
              BytecodeOp::EnterWithEnv =>
                if env.strict {
                  let _ = throw_syntax_error("with not allowed in strict mode")
                } else {
                  let value = bytecode_stack_pop(stack)
                  let obj = to_object(value)
                  let with_env = Env::{
                    id: alloc_id(),
                    parent: Some(env),
                    bindings: @hashmap.new(),
                    readonly_bindings: @hashmap.new(),
                    deletable_bindings: @hashmap.new(),
                    private_bindings: @hashmap.new(),
                    strict: env.strict,
                    with_object: Some(obj),
                    var_object: None,
                    new_target: env.new_target,
                    allow_new_target: env.allow_new_target,
                    allow_super: env.allow_super,
                    allow_super_call: env.allow_super_call,
                    this_uninitialized: false,
                    is_param_env: false,
                    is_catch_env: false,
                    param_binding_names: @hashmap.new(),
                    uninitialized_bindings: @hashmap.new(),
                    var_env: None,
                  }
                  register_env(with_env)
                  env_stack.push(env)
                  push_env(with_env)
                  env = with_env
                }
              BytecodeOp::MarkCatchEnv => {
                env.is_catch_env = true
              }
              BytecodeOp::EnterFinally => {
                let enter_pc = pc - 1
                let (start_pc, dispatch_pc) =
                  bytecode_finally_info_for_pc(func, enter_pc)
                if start_pc < 0 || dispatch_pc < 0 {
                  let _ = throw_type_error("invalid bytecode finally start")
                }
                finally_stack.push(BytecodeFinallyState::{
                  code: 0,
                  value: Undefined,
                  start_pc,
                  dispatch_pc,
                  entered: false,
                })
              }
              BytecodeOp::LeaveFinally => {
                match finally_stack.pop() {
                  Some(entry) =>
                    if entry.entered && finally_body_depth > 0 {
                      finally_body_depth = finally_body_depth - 1
                    }
                  None => ()
                }
              }
              BytecodeOp::SetFinallyValue =>
                if !finally_stack.is_empty() {
                  let top_index = finally_stack.length() - 1
                  let value = bytecode_stack_pop(stack)
                  finally_stack[top_index].value = value
                }
              BytecodeOp::GetFinallyCode =>
                if finally_stack.is_empty() {
                  stack.push(Number(0.0))
                } else {
                  let top_index = finally_stack.length() - 1
                  let code = finally_stack[top_index].code
                  stack.push(Number(Double::from_int(code)))
                }
              BytecodeOp::PushFinallyValue =>
                if finally_stack.is_empty() {
                  stack.push(Undefined)
                } else {
                  let top_index = finally_stack.length() - 1
                  stack.push(finally_stack[top_index].value)
                }
              BytecodeOp::UpdateTryLast =>
                if !try_stack.is_empty() {
                  let top_index = try_stack.length() - 1
                  let (
                    catch_pc,
                    stack_len,
                    target_len,
                    binding_len,
                    env_depth,
                    for_in_depth,
                    for_of_depth,
                    _,
                  ) = try_stack[top_index]
                  let value = bytecode_stack_peek(stack)
                  try_stack[top_index] = (
                    catch_pc,
                    stack_len,
                    target_len,
                    binding_len,
                    env_depth,
                    for_in_depth,
                    for_of_depth,
                    value,
                  )
                }
              BytecodeOp::PushTryLast =>
                if try_stack.is_empty() {
                  stack.push(Undefined)
                } else {
                  let top_index = try_stack.length() - 1
                  let (_, _, _, _, _, _, _, last_value) = try_stack[top_index]
                  stack.push(last_value)
                }
              BytecodeOp::PopTryLast =>
                if !try_stack.is_empty() {
                  let top_index = try_stack.length() - 1
                  let (
                    catch_pc,
                    stack_len,
                    target_len,
                    binding_len,
                    env_depth,
                    for_in_depth,
                    for_of_depth,
                    _,
                  ) = try_stack[top_index]
                  try_stack[top_index] = (
                    catch_pc,
                    stack_len,
                    target_len,
                    binding_len,
                    env_depth,
                    for_in_depth,
                    for_of_depth,
                    Undefined,
                  )
                }
              BytecodeOp::SetElem => {
                let value = bytecode_stack_pop(stack)
                let key_value = bytecode_stack_pop(stack)
                let target = bytecode_stack_pop(stack)
                let _ = to_object(target)
                let name = property_key_name(key_value)
                stack.push(
                  set_property_for_value(target, name, value, env.strict),
                )
              }
              BytecodeOp::CheckSuperThis => {
                let _ = env_get(env, "this")
              }
              BytecodeOp::SuperSetElem => {
                let value = bytecode_stack_pop(stack)
                let key_value = bytecode_stack_pop(stack)
                let (base, receiver) = super_base_and_receiver(env)
                let name = property_key_name(key_value)
                super_set_property(env, base, receiver, name, value)
                stack.push(value)
              }
              BytecodeOp::SuperGetElem => {
                let key_value = bytecode_stack_pop(stack)
                let (base, receiver) = super_base_and_receiver(env)
                let name = property_key_name(key_value)
                let base_obj = to_object(base)
                stack.push(property_get_with_receiver(base_obj, name, receiver))
              }
              BytecodeOp::DeleteElem => {
                let key_value = bytecode_stack_pop(stack)
                let target = bytecode_stack_pop(stack)
                let name = property_key_name(key_value)
                let result = delete_property(env, target, name)
                stack.push(result)
              }
              BytecodeOp::NewObject => {
                stack.push(new_object_value())
              }
              BytecodeOp::InitPropComputed => {
                let value = bytecode_stack_pop(stack)
                let key_value = bytecode_stack_pop(stack)
                let target = bytecode_stack_pop(stack)
                let name = property_key_name(key_value)
                match target {
                  Object(obj) => props_set(obj.props, name, property_data(value))
                  _ => {
                    let _ = throw_type_error("invalid bytecode object")
                  }
                }
                stack.push(value)
              }
              BytecodeOp::InitPropComputedNamed => {
                let value = bytecode_stack_pop(stack)
                let key_value = bytecode_stack_pop(stack)
                let target = bytecode_stack_pop(stack)
                let name = property_key_name(key_value)
                set_anonymous_function_name(value, name, Some(key_value))
                match target {
                  Object(obj) => props_set(obj.props, name, property_data(value))
                  _ => {
                    let _ = throw_type_error("invalid bytecode object")
                  }
                }
                stack.push(value)
              }
              BytecodeOp::InitMethodComputed => {
                let func_value = bytecode_stack_pop(stack)
                let key_value = bytecode_stack_pop(stack)
                let target = bytecode_stack_pop(stack)
                let name = property_key_name(key_value)
                match (target, func_value) {
                  (Object(obj), Function(func)) => {
                    func.is_constructor = false
                    set_function_name(
                      func,
                      function_name_from_key(name, Some(key_value), None),
                    )
                    set_function_home_object(func, Some(Value::Object(obj)))
                    let method_value = Value::Function(func)
                    props_set(obj.props, name, property_data(method_value))
                    stack.push(method_value)
                  }
                  _ => {
                    let _ = throw_type_error("invalid bytecode method")
                  }
                }
              }
              BytecodeOp::InitGetterComputed => {
                let func_value = bytecode_stack_pop(stack)
                let key_value = bytecode_stack_pop(stack)
                let target = bytecode_stack_pop(stack)
                let name = property_key_name(key_value)
                match (target, func_value) {
                  (Object(obj), Function(func)) => {
                    func.is_constructor = false
                    set_function_name(
                      func,
                      function_name_from_key(name, Some(key_value), Some("get ")),
                    )
                    set_function_home_object(func, Some(Value::Object(obj)))
                    let getter_value = Value::Function(func)
                    set_accessor_property(obj.props, name, Some(getter_value), None)
                    stack.push(getter_value)
                  }
                  _ => {
                    let _ = throw_type_error("invalid bytecode getter")
                  }
                }
              }
              BytecodeOp::InitSetterComputed => {
                let func_value = bytecode_stack_pop(stack)
                let key_value = bytecode_stack_pop(stack)
                let target = bytecode_stack_pop(stack)
                let name = property_key_name(key_value)
                match (target, func_value) {
                  (Object(obj), Function(func)) => {
                    func.is_constructor = false
                    set_function_name(
                      func,
                      function_name_from_key(name, Some(key_value), Some("set ")),
                    )
                    set_function_home_object(func, Some(Value::Object(obj)))
                    let setter_value = Value::Function(func)
                    set_accessor_property(obj.props, name, None, Some(setter_value))
                    stack.push(setter_value)
                  }
                  _ => {
                    let _ = throw_type_error("invalid bytecode setter")
                  }
                }
              }
              BytecodeOp::ObjectSpread => {
                let spread_value = bytecode_stack_pop(stack)
                let target = bytecode_stack_pop(stack)
                match target {
                  Object(obj) => {
                    copy_data_properties(obj, spread_value)
                    stack.push(Object(obj))
                  }
                  _ => {
                    let _ = throw_type_error("invalid bytecode object")
                  }
                }
              }
              BytecodeOp::SetProto => {
                let proto_value = bytecode_stack_pop(stack)
                let target = bytecode_stack_pop(stack)
                match target {
                  Object(obj) => {
                    match proto_value {
                      Null => {
                        rc_replace_optional_value(obj.proto, None)
                        obj.proto = None
                      }
                      _ =>
                        if is_object_like(proto_value) {
                          rc_replace_optional_value(obj.proto, Some(proto_value))
                          obj.proto = Some(proto_value)
                        }
                    }
                    stack.push(Object(obj))
                  }
                  _ => {
                    let _ = throw_type_error("invalid bytecode object")
                  }
                }
              }
              BytecodeOp::CallArgs => {
                let args_value = bytecode_stack_pop(stack)
                let callee = bytecode_stack_pop(stack)
                let argv = build_arg_list(args_value)
                stack.push(call_value(callee, argv))
              }
              BytecodeOp::NewArgs => {
                let args_value = bytecode_stack_pop(stack)
                let callee = bytecode_stack_pop(stack)
                let argv = build_arg_list(args_value)
                stack.push(call_constructor(callee, argv))
              }
              BytecodeOp::CallWithThisArgs => {
                let args_value = bytecode_stack_pop(stack)
                let callee = bytecode_stack_pop(stack)
                let this_value = bytecode_stack_pop(stack)
                let argv = build_arg_list(args_value)
                stack.push(call_value_with_this(callee, argv, this_value))
              }
              BytecodeOp::CallWithThisEvalArgs => {
                let args_value = bytecode_stack_pop(stack)
                let callee = bytecode_stack_pop(stack)
                let this_value = bytecode_stack_pop(stack)
                let argv = build_arg_list(args_value)
                stack.push(bytecode_call_direct_eval(env, callee, argv, this_value))
              }
              BytecodeOp::SuperCallArgs => {
                let args_value = bytecode_stack_pop(stack)
                let argv = build_arg_list(args_value)
                stack.push(super_call_with_args(env, argv))
              }
              BytecodeOp::ArrayPush => {
                let value = bytecode_stack_pop(stack)
                let target = bytecode_stack_pop(stack)
              match target {
                Array(arr) => {
                  rc_incref_value(value)
                  arr.elements.push(Some(value))
                  let len = Int64::from_int(arr.elements.length())
                  set_length_prop(arr.props, Number(Int64::to_double(len)))
                  stack.push(Array(arr))
                  }
                  _ => {
                    let _ = throw_type_error("invalid bytecode array")
                  }
                }
              }
              BytecodeOp::ArraySpread => {
                let spread_value = bytecode_stack_pop(stack)
                let target = bytecode_stack_pop(stack)
              match target {
                Array(arr) => {
                  for item in build_spread_list(spread_value) {
                    rc_incref_value(item)
                    arr.elements.push(Some(item))
                  }
                    let len = Int64::from_int(arr.elements.length())
                    set_length_prop(arr.props, Number(Int64::to_double(len)))
                    stack.push(Array(arr))
                  }
                  _ => {
                    let _ = throw_type_error("invalid bytecode array")
                  }
                }
              }
              BytecodeOp::UpdateElemPrefixInc
              | BytecodeOp::UpdateElemPrefixDec
              | BytecodeOp::UpdateElemPostfixInc
              | BytecodeOp::UpdateElemPostfixDec => {
                let (is_inc, is_prefix) = match op {
                  BytecodeOp::UpdateElemPrefixInc => (true, true)
                  BytecodeOp::UpdateElemPrefixDec => (false, true)
                  BytecodeOp::UpdateElemPostfixInc => (true, false)
                  _ => (false, false)
                }
                let key_value = bytecode_stack_pop(stack)
                let target = bytecode_stack_pop(stack)
                let target_obj = to_object(target)
                let name = property_key_name(key_value)
                let current = property_get_with_receiver(target_obj, name, target)
                let (old_value, new_value) =
                  update_values_with_flags(is_inc, current)
                let _ = set_property_for_value(
                  target,
                  name,
                  new_value,
                  env.strict,
                )
                let result =
                  update_result_with_flags(is_prefix, old_value, new_value)
                stack.push(result)
              }
              BytecodeOp::ForInInit => {
                let target_value = bytecode_stack_pop(stack)
                let mut target = target_value
                let mut keys : Array[String] = []
                if !(target_value is Null || target_value is Undefined) {
                  target = to_object(target_value)
                  keys = enumerable_string_keys_in_chain(target)
                }
                for_in_stack.push((target, keys, 0))
              }
              BytecodeOp::ForInNext => {
                if for_in_stack.is_empty() {
                  stack.push(Undefined)
                } else {
                  let top_index = for_in_stack.length() - 1
                  let (target, keys, start_index) = for_in_stack[top_index]
                  let mut index = start_index
                  let mut next_key : String? = None
                  while index < keys.length() {
                    let key = keys[index]
                    index = index + 1
                    if is_enumerable_key_in_chain(target, key) {
                      next_key = Some(key)
                      break
                    }
                  }
                  for_in_stack[top_index] = (target, keys, index)
                  match next_key {
                    Some(key) => stack.push(String(key))
                    None => stack.push(Undefined)
                  }
                }
              }
              BytecodeOp::ForInEnd => {
                let _ = for_in_stack.pop()
              }
              BytecodeOp::ForOfInit => {
                let iterable = bytecode_stack_pop(stack)
                let (iterator, next_method) = get_iterator_from_value(iterable)
                for_of_stack.push((iterator, next_method, false))
              }
              BytecodeOp::ForAwaitInit => {
                let iterable = bytecode_stack_pop(stack)
                let (iterator, next_method) =
                  get_async_iterator_from_value(iterable)
                for_of_stack.push((iterator, next_method, true))
              }
              BytecodeOp::ForOfNext => {
                if for_of_stack.is_empty() {
                  stack.push(Undefined)
                  stack.push(Bool(true))
                } else {
                  let top_index = for_of_stack.length() - 1
                  let (iterator, next_method, is_async) = for_of_stack[top_index]
                  if iterator is Undefined {
                    stack.push(Undefined)
                    stack.push(Bool(true))
                  } else {
                    if is_async {
                      let result = try call_value_with_this(
                        next_method,
                        [],
                        iterator,
                      ) catch {
                        err => {
                          for_of_stack[top_index] = (
                            Undefined,
                            Undefined,
                            is_async,
                          )
                          raise err
                        }
                      } noraise {
                        value => value
                      }
                      state.env = env
                      state.pc = pc
                      state.resume_pending = true
                      state.finally_body_depth = finally_body_depth
                      bytecode_update_gen_roots(state)
                      return Await(result, fn(next_resume) raise {
                        match next_resume {
                          GenResume::Next(awaited) => {
                            if !is_object_like(awaited) {
                              for_of_stack[top_index] = (
                                Undefined,
                                Undefined,
                                is_async,
                              )
                              state.env = env
                              state.pc = pc
                              state.resume_pending = true
                              state.finally_body_depth = finally_body_depth
                              bytecode_update_gen_roots(state)
                              return bytecode_exec_step(
                                state,
                                GenResume::Throw(type_error_value(
                                  "iterator must return an object",
                                )),
                              )
                            }
                            let done_result : Result[Value, Error] =
                              try? property_get(awaited, "done")
                            let done = match done_result {
                              Err(err) => {
                                let throw_value = bytecode_error_to_value(err)
                                for_of_stack[top_index] = (
                                  Undefined,
                                  Undefined,
                                  is_async,
                                )
                                state.env = env
                                state.pc = pc
                                state.resume_pending = true
                                state.finally_body_depth = finally_body_depth
                                bytecode_update_gen_roots(state)
                                return bytecode_exec_step(
                                  state,
                                  GenResume::Throw(throw_value),
                                )
                              }
                              Ok(value) => is_truthy(value)
                            }
                            if done {
                              for_of_stack[top_index] = (
                                Undefined,
                                Undefined,
                                is_async,
                              )
                              stack.push(Undefined)
                              stack.push(Bool(true))
                            } else {
                              let value_result : Result[Value, Error] =
                                try? property_get(awaited, "value")
                              match value_result {
                                Err(err) => {
                                  let throw_value = bytecode_error_to_value(err)
                                  for_of_stack[top_index] = (
                                    Undefined,
                                    Undefined,
                                    is_async,
                                  )
                                  state.env = env
                                  state.pc = pc
                                  state.resume_pending = true
                                  state.finally_body_depth = finally_body_depth
                                  bytecode_update_gen_roots(state)
                                  return bytecode_exec_step(
                                    state,
                                    GenResume::Throw(throw_value),
                                  )
                                }
                                Ok(value) => {
                                  stack.push(value)
                                  stack.push(Bool(false))
                                }
                              }
                            }
                            state.env = env
                            state.pc = pc
                            state.resume_pending = false
                            state.finally_body_depth = finally_body_depth
                            bytecode_update_gen_roots(state)
                            bytecode_exec_step(state, GenResume::Next(Undefined))
                          }
                          GenResume::Throw(err) => {
                            for_of_stack[top_index] = (
                              Undefined,
                              Undefined,
                              is_async,
                            )
                            state.env = env
                            state.pc = pc
                            state.resume_pending = true
                            state.finally_body_depth = finally_body_depth
                            bytecode_update_gen_roots(state)
                            bytecode_exec_step(state, GenResume::Throw(err))
                          }
                          GenResume::Return(arg) => {
                            state.env = env
                            state.pc = pc
                            state.resume_pending = true
                            state.finally_body_depth = finally_body_depth
                            bytecode_update_gen_roots(state)
                            bytecode_exec_step(state, GenResume::Return(arg))
                          }
                        }
                      })
                    } else {
                      let (done, value) = try
                        iterator_step_value(iterator, next_method)
                      catch {
                        err => {
                          for_of_stack[top_index] = (
                            Undefined,
                            Undefined,
                            is_async,
                          )
                          raise err
                        }
                      } noraise {
                        pair => pair
                      }
                      if done {
                        for_of_stack[top_index] = (Undefined, Undefined, is_async)
                        stack.push(Undefined)
                        stack.push(Bool(true))
                      } else {
                        stack.push(value)
                        stack.push(Bool(false))
                      }
                    }
                  }
                }
              }
              BytecodeOp::PushTrue => stack.push(Bool(true))
              BytecodeOp::PushFalse => stack.push(Bool(false))
              BytecodeOp::PushNull => stack.push(Null)
              BytecodeOp::PushUndefined => stack.push(Undefined)
              BytecodeOp::NewTarget => stack.push(env.new_target)
              BytecodeOp::ImportMeta => stack.push(import_meta_value())
              BytecodeOp::Await => {
                let value = bytecode_stack_pop(stack)
                state.env = env
                state.pc = pc
                state.resume_pending = true
                state.finally_body_depth = finally_body_depth
                bytecode_update_gen_roots(state)
                return Await(value, fn(next_resume) raise {
                  bytecode_exec_step(state, next_resume)
                })
              }
              BytecodeOp::Yield => {
                let value = bytecode_stack_pop(stack)
                let is_async_gen = current_function_is_async_generator()
                state.env = env
                state.pc = pc
                state.finally_body_depth = finally_body_depth
                bytecode_update_gen_roots(state)
                if is_async_gen {
                  state.resume_pending = true
                  return Await(value, fn(next_resume) {
                    match next_resume {
                      GenResume::Next(arg) => {
                        state.env = env
                        state.pc = pc
                        state.resume_pending = true
                        state.finally_body_depth = finally_body_depth
                        bytecode_update_gen_roots(state)
                        Yield(arg, fn(resume_state) raise {
                          match resume_state {
                            GenResume::Next(_) =>
                              bytecode_exec_step(state, resume_state)
                            GenResume::Return(ret) =>
                              bytecode_async_return(
                                state,
                                env,
                                pc,
                                finally_body_depth,
                                ret,
                              )
                            GenResume::Throw(err) =>
                              bytecode_exec_step(state, GenResume::Throw(err))
                          }
                        })
                      }
                      GenResume::Return(arg) =>
                        bytecode_async_return(
                          state,
                          env,
                          pc,
                          finally_body_depth,
                          arg,
                        )
                      GenResume::Throw(arg) =>
                        bytecode_exec_step(state, GenResume::Throw(arg))
                    }
                  })
                } else {
                  state.resume_pending = true
                  return Yield(value, fn(next_resume) raise {
                    bytecode_exec_step(state, next_resume)
                  })
                }
              }
              BytecodeOp::LeaveTry => {
                let _ = try_stack.pop()
              }
              BytecodeOp::Throw => raise ThrowSignal(bytecode_stack_pop(stack))
              BytecodeOp::Return => {
                let value = bytecode_stack_pop(stack)
                if current_function_is_async_generator() {
                  return bytecode_async_return(
                    state,
                    env,
                    pc,
                    finally_body_depth,
                    value,
                  )
                }
                if !finally_stack.is_empty() {
                  let top_index = finally_stack.length() - 1
                  finally_stack[top_index].code = 2
                  finally_stack[top_index].value = value
                  if finally_stack[top_index].entered {
                    pc =
                      bytecode_jump_target(code, finally_stack[top_index].dispatch_pc)
                  } else {
                    pc =
                      bytecode_jump_target(code, finally_stack[top_index].start_pc)
                    finally_stack[top_index].entered = true
                    finally_body_depth = finally_body_depth + 1
                  }
                  continue
                }
                bytecode_close_for_of_stack(for_of_stack, false)
                pop_envs()
                state.env = env
                state.pc = pc
                state.finally_body_depth = finally_body_depth
                gen_root_replace([])
                return Done(Return(value))
              }
              BytecodeOp::ReturnNoAwait => {
                let value = bytecode_stack_pop(stack)
                if !finally_stack.is_empty() {
                  let top_index = finally_stack.length() - 1
                  finally_stack[top_index].code = 3
                  finally_stack[top_index].value = value
                  if finally_stack[top_index].entered {
                    pc =
                      bytecode_jump_target(code, finally_stack[top_index].dispatch_pc)
                  } else {
                    pc =
                      bytecode_jump_target(code, finally_stack[top_index].start_pc)
                    finally_stack[top_index].entered = true
                    finally_body_depth = finally_body_depth + 1
                  }
                  continue
                }
                bytecode_close_for_of_stack(for_of_stack, false)
                pop_envs()
                state.env = env
                state.pc = pc
                state.finally_body_depth = finally_body_depth
                gen_root_replace([])
                return Done(Return(value))
              }
              _ => ()
            }
            BytecodeInstr::I32(op, value) =>
              match op {
              BytecodeOp::EnterTry => {
                let entry = (
                  value,
                  stack.length(),
                  target_stack.length(),
                  binding_stack.length(),
                  env_stack.length(),
                  for_in_stack.length(),
                  for_of_stack.length(),
                  Undefined,
                )
                try_stack.push(entry)
              }
              BytecodeOp::SetFinallyCode =>
                if !finally_stack.is_empty() {
                  let top_index = finally_stack.length() - 1
                  finally_stack[top_index].code = value
                }
              BytecodeOp::PushI32 => {
                stack.push(Number(Double::from_int(value)))
              }
              BytecodeOp::Unary => {
                let input = bytecode_stack_pop(stack)
                stack.push(eval_unary_code(value, input))
              }
              BytecodeOp::Binary => {
                let right = bytecode_stack_pop(stack)
                let left = bytecode_stack_pop(stack)
                stack.push(eval_binary_code(value, left, right))
              }
              BytecodeOp::ToStringStrict => {
                let input = bytecode_stack_pop(stack)
                stack.push(String(to_string_strict(input)))
              }
              BytecodeOp::Call => {
                let argc = if value < 0 { 0 } else { value }
                let argv : Array[Value] = Array::make(argc, Undefined)
                let mut i = 0
                while i < argc {
                  let index = argc - 1 - i
                  argv[index] = bytecode_stack_pop(stack)
                  i = i + 1
                }
                let callee = bytecode_stack_pop(stack)
                stack.push(call_value(callee, argv))
              }
              BytecodeOp::CallWithThis => {
                let argc = if value < 0 { 0 } else { value }
                if argc == 0 {
                  let callee = bytecode_stack_pop(stack)
                  let this_value = bytecode_stack_pop(stack)
                  stack.push(bytecode_call_with_this_noargs(callee, this_value))
                } else if argc == 1 {
                  let arg = bytecode_stack_pop(stack)
                  let callee = bytecode_stack_pop(stack)
                  let this_value = bytecode_stack_pop(stack)
                  stack.push(
                    bytecode_call_with_this_onearg(callee, this_value, arg),
                  )
                } else {
                  let argv : Array[Value] = Array::make(argc, Undefined)
                  let mut i = 0
                  while i < argc {
                    let index = argc - 1 - i
                    argv[index] = bytecode_stack_pop(stack)
                    i = i + 1
                  }
                  let callee = bytecode_stack_pop(stack)
                  let this_value = bytecode_stack_pop(stack)
                  stack.push(call_value_with_this(callee, argv, this_value))
                }
              }
              BytecodeOp::CallWithThisEval => {
                let argc = if value < 0 { 0 } else { value }
                let argv : Array[Value] = Array::make(argc, Undefined)
                let mut i = 0
                while i < argc {
                  let index = argc - 1 - i
                  argv[index] = bytecode_stack_pop(stack)
                  i = i + 1
                }
                let callee = bytecode_stack_pop(stack)
                let this_value = bytecode_stack_pop(stack)
                stack.push(bytecode_call_direct_eval(env, callee, argv, this_value))
              }
              BytecodeOp::PopEnv => {
                let mut count = if value < 0 { 0 } else { value }
                while count > 0 {
                  match env_stack.pop() {
                    Some(prev) => {
                      pop_env()
                      env = prev
                    }
                    None => {
                      let _ = throw_type_error("invalid bytecode env pop")
                    }
                  }
                  count = count - 1
                }
              }
              BytecodeOp::New => {
                let argc = if value < 0 { 0 } else { value }
                let argv : Array[Value] = Array::make(argc, Undefined)
                let mut i = 0
                while i < argc {
                  let index = argc - 1 - i
                  argv[index] = bytecode_stack_pop(stack)
                  i = i + 1
                }
                let callee = bytecode_stack_pop(stack)
                stack.push(call_constructor(callee, argv))
              }
              BytecodeOp::NewRegexp => {
                let flags_value = bytecode_stack_pop(stack)
                let pattern_value = bytecode_stack_pop(stack)
                let pattern = match pattern_value {
                  String(text) => text
                  _ => {
                    let _ = throw_type_error("invalid bytecode regexp")
                    ""
                  }
                }
                let flags = match flags_value {
                  String(text) => text
                  _ => {
                    let _ = throw_type_error("invalid bytecode regexp")
                    ""
                  }
                }
                let proto = match value_from_object(regexp_proto_for_env(env)) {
                  Some(value) => Some(value)
                  None => value_from_object(regexp_proto())
                }
                let regex_value = new_regexp_value(
                  pattern,
                  flags,
                  proto,
                  offset=value,
                )
                stack.push(regex_value)
              }
              BytecodeOp::NewArray => {
                let count = if value < 0 { 0 } else { value }
                let elements : Array[Value?] = Array::make(count, None)
                let mut index = count
                while index > 0 {
                  index = index - 1
                  let elem = bytecode_stack_pop(stack)
                  elements[index] = Some(elem)
                }
                let array_proto_value = match value_from_object(
                  array_proto_for_env(env),
                ) {
                  Some(value) => Some(value)
                  None => value_from_object(array_proto())
                }
                stack.push(
                  new_array_value_with_proto(elements, proto=array_proto_value),
                )
              }
              BytecodeOp::ArrayPushHole => {
                let count = if value < 0 { 0 } else { value }
                let target = bytecode_stack_pop(stack)
                match target {
                  Array(arr) => {
                    let mut i = 0
                    while i < count {
                      arr.elements.push(None)
                      i = i + 1
                    }
                    let len = Int64::from_int(arr.elements.length())
                    set_length_prop(arr.props, Number(Int64::to_double(len)))
                    stack.push(Array(arr))
                  }
                  _ => {
                    let _ = throw_type_error("invalid bytecode array")
                  }
                }
              }
              BytecodeOp::Jmp => {
                let target_pc = bytecode_jump_target(code, value)
                pc = target_pc
                if bytecode_is_finally_start(func, target_pc) {
                  finally_body_depth = bytecode_note_finally_enter(
                    finally_stack,
                    target_pc,
                    finally_body_depth,
                  )
                }
              }
              BytecodeOp::JmpIfTrue => {
                if is_truthy(bytecode_stack_peek(stack)) {
                  let target_pc = bytecode_jump_target(code, value)
                  pc = target_pc
                  if bytecode_is_finally_start(func, target_pc) {
                    finally_body_depth = bytecode_note_finally_enter(
                      finally_stack,
                      target_pc,
                      finally_body_depth,
                    )
                  }
                }
              }
              BytecodeOp::JmpIfFalse => {
                if !is_truthy(bytecode_stack_peek(stack)) {
                  let target_pc = bytecode_jump_target(code, value)
                  pc = target_pc
                  if bytecode_is_finally_start(func, target_pc) {
                    finally_body_depth = bytecode_note_finally_enter(
                      finally_stack,
                      target_pc,
                      finally_body_depth,
                    )
                  }
                }
              }
              BytecodeOp::JmpIfNullish => {
                if is_nullish(bytecode_stack_peek(stack)) {
                  let target_pc = bytecode_jump_target(code, value)
                  pc = target_pc
                  if bytecode_is_finally_start(func, target_pc) {
                    finally_body_depth = bytecode_note_finally_enter(
                      finally_stack,
                      target_pc,
                      finally_body_depth,
                    )
                  }
                }
              }
              BytecodeOp::ForOfClose => {
                let mut count = if value < 0 { 0 } else { value }
                while count > 0 && !for_of_stack.is_empty() {
                  match for_of_stack.pop() {
                    Some((iterator, _, is_async)) =>
                      if !(iterator is Undefined) {
                        if is_async {
                          let _ = async_iterator_close(iterator)
                        } else {
                          iterator_close(iterator)
                        }
                      }
                    None => ()
                  }
                  count = count - 1
                }
              }
              _ => ()
            }
            BytecodeInstr::Const(op, index) =>
              match op {
              BytecodeOp::EnterBlockEnv =>
                if index >= 0 && index < block_env_pool.length() {
                  let def = block_env_pool[index]
                  let block_env = Env::new(Some(env))
                  for func_index in def.func_indices {
                    if func_index >= 0 && func_index < func_pool.length() {
                      let func_def = func_pool[func_index]
                      match func_def.name {
                        Some(name) => {
                          let value =
                            Value::Function(to_function_value(block_env, func_def, false))
                          env_set_local(block_env, name, value)
                        }
                        None => ()
                      }
                    } else {
                      let _ = throw_type_error("invalid bytecode block function")
                    }
                  }
                  for binding in def.lex_bindings {
                    declare_binding_uninitialized(block_env, binding)
                  }
                  for name in def.class_names {
                    env_declare_uninitialized(block_env, name)
                  }
                  env_stack.push(env)
                  push_env(block_env)
                  env = block_env
                } else {
                  let _ = throw_type_error("invalid bytecode block env")
                }
              BytecodeOp::FunctionDecl =>
                if index >= 0 && index < func_pool.length() {
                  let func_def = func_pool[index]
                  eval_function_decl(env, func_def)
                } else {
                  let _ = throw_type_error("invalid bytecode function decl")
                }
              BytecodeOp::NewFunction =>
                if index >= 0 && index < func_pool.length() {
                  let func_def = func_pool[index]
                  let value = match func_def.name {
                    Some(name) => {
                      let name_env = Env::new(Some(env))
                      let func_value = to_function_value(name_env, func_def, false)
                      env_define_readonly(
                        name_env,
                        name,
                        Value::Function(func_value),
                        false,
                      )
                      Value::Function(func_value)
                    }
                    None => Value::Function(to_function_value(env, func_def, false))
                  }
                  stack.push(value)
                } else {
                  let _ = throw_type_error("invalid bytecode function")
                }
              BytecodeOp::NewClass =>
                if index >= 0 && index < class_pool.length() {
                  let class_entry = class_pool[index]
                  state.env = env
                  state.pc = pc
                  state.finally_body_depth = finally_body_depth
                  bytecode_update_gen_roots(state)
                  return gen_bind(
                    with_bytecode_class_eval_step(fn() raise {
                      gen_eval_class(
                        env,
                        class_entry.class_def,
                        name_hint=class_entry.name_hint,
                      )
                    }),
                    fn(control) raise {
                    match control {
                      Normal(value) => {
                        stack.push(value)
                        bytecode_exec_step(state, GenResume::Next(Undefined))
                      }
                      Return(value) => Done(Return(value))
                      Throw(value) => Done(Throw(value))
                      Break(label, value_opt) => Done(Break(label, value_opt))
                      Continue(label, value_opt) =>
                        Done(Continue(label, value_opt))
                    }
                  })
                } else {
                  let _ = throw_type_error("invalid bytecode class")
                }
              BytecodeOp::OptionalChain =>
                if index >= 0 && index < chain_pool.length() {
                  let _ = chain_pool[index]
                  let _ =
                    throw_type_error("unexpected bytecode optional chain opcode")
                } else {
                  let _ = throw_type_error("invalid bytecode optional chain")
                }
              BytecodeOp::OptionalChainCallRef =>
                if index >= 0 && index < chain_pool.length() {
                  let _ = chain_pool[index]
                  let _ =
                    throw_type_error("unexpected bytecode optional chain opcode")
                } else {
                  let _ = throw_type_error("invalid bytecode optional chain")
                }
              BytecodeOp::DeleteOptionalChain =>
                if index >= 0 && index < chain_pool.length() {
                  let _ = chain_pool[index]
                  let _ =
                    throw_type_error("unexpected bytecode optional chain opcode")
                } else {
                  let _ = throw_type_error("invalid bytecode optional chain")
                }
              BytecodeOp::TaggedTemplate =>
                if index >= 0 && index < tagged_template_pool.length() {
                  let def = tagged_template_pool[index]
                  let value = build_template_object(env, def.parts)
                  stack.push(value)
                } else {
                  let _ = throw_type_error("invalid bytecode tagged template")
                }
              BytecodeOp::SuperMember =>
                if index >= 0 && index < super_member_pool.length() {
                  let def = super_member_pool[index]
                  match def.key {
                    @engine.MemberKey::Private(_) =>
                      {
                        let _ = throw_syntax_error(
                          "private class field forbidden after super",
                        )
                      }
                    @engine.MemberKey::Computed(_) => {
                      let key_value = bytecode_stack_pop(stack)
                      let (base, receiver) = super_base_and_receiver(env)
                      let name = property_key_name(key_value)
                      let base_obj = to_object(base)
                      stack.push(
                        property_get_with_receiver(base_obj, name, receiver),
                      )
                    }
                    @engine.MemberKey::Ident(name) => {
                      let (base, receiver) = super_base_and_receiver(env)
                      let base_obj = to_object(base)
                      stack.push(
                        property_get_with_receiver(base_obj, name, receiver),
                      )
                    }
                  }
                } else {
                  let _ = throw_type_error("invalid bytecode super member")
                }
              BytecodeOp::SuperMemberCall =>
                if index >= 0 && index < super_member_call_pool.length() {
                  let def = super_member_call_pool[index]
                  match def.key {
                    @engine.MemberKey::Private(_) =>
                      {
                        let _ = throw_syntax_error(
                          "private class field forbidden after super",
                        )
                      }
                    @engine.MemberKey::Computed(_) => {
                      let key_value = bytecode_stack_pop(stack)
                      let (base, receiver) = super_base_and_receiver(env)
                      let name = property_key_name(key_value)
                      let base_obj = to_object(base)
                      let callee =
                        property_get_with_receiver(base_obj, name, receiver)
                      stack.push(receiver)
                      stack.push(callee)
                    }
                    @engine.MemberKey::Ident(name) => {
                      let (base, receiver) = super_base_and_receiver(env)
                      let base_obj = to_object(base)
                      let callee =
                        property_get_with_receiver(base_obj, name, receiver)
                      stack.push(receiver)
                      stack.push(callee)
                    }
                  }
                } else {
                  let _ = throw_type_error("invalid bytecode super member call")
                }
              BytecodeOp::SuperCall =>
                {
                  let _ = throw_type_error("invalid bytecode super call")
                }
              BytecodeOp::SuperUpdate =>
                if index >= 0 && index < super_update_pool.length() {
                  let def = super_update_pool[index]
                  match def.key {
                    @engine.MemberKey::Private(_) =>
                      {
                        let _ = throw_syntax_error(
                          "private class field forbidden after super",
                        )
                        return Done(Throw(Undefined))
                      }
                    _ => ()
                  }
                  let (is_inc, is_prefix) = match def.op {
                    @engine.UpdateOp::Inc =>
                      match def.kind {
                        @engine.UpdateKind::Prefix => (true, true)
                        _ => (true, false)
                      }
                    _ =>
                      match def.kind {
                        @engine.UpdateKind::Prefix => (false, true)
                        _ => (false, false)
                      }
                  }
                  let (base, receiver) = super_base_and_receiver(env)
                  let name = member_key_name(env, def.key)
                  let base_obj = to_object(base)
                  let current = property_get_with_receiver(base_obj, name, receiver)
                  let (old_value, new_value) =
                    update_values_with_flags(is_inc, current)
                  super_set_property(env, base, receiver, name, new_value)
                  let result =
                    update_result_with_flags(is_prefix, old_value, new_value)
                  stack.push(result)
                } else {
                  let _ = throw_type_error("invalid bytecode super update")
                }
              BytecodeOp::DeleteSuper =>
                if index >= 0 && index < super_delete_pool.length() {
                  let def = super_delete_pool[index]
                  match def.key {
                    @engine.MemberKey::Private(_) =>
                      {
                        let _ = throw_syntax_error(
                          "cannot delete a private class field",
                        )
                        return Done(Throw(Undefined))
                      }
                    @engine.MemberKey::Computed(expr) => {
                      let _ = super_base_and_receiver(env)
                      state.env = env
                      state.pc = pc
                      state.finally_body_depth = finally_body_depth
                      bytecode_update_gen_roots(state)
                      return gen_bytecode_eval_expr_cont(env, expr, fn(_) raise {
                        let throw_value = try {
                          let _ = throw_reference_error(
                            "unsupported reference to 'super'",
                          )
                          Undefined
                        } catch {
                          ThrowSignal(value) => value
                          err => raise err
                        }
                        state.resume_pending = true
                        bytecode_exec_step(
                          state,
                          GenResume::Throw(throw_value),
                        )
                      })
                    }
                    _ => {
                      let _ = super_base_and_receiver(env)
                      {
                        let _ = throw_reference_error(
                          "unsupported reference to 'super'",
                        )
                        return Done(Throw(Undefined))
                      }
                    }
                  }
                } else {
                  let _ = throw_type_error("invalid bytecode delete super")
                }
              BytecodeOp::AssignOpEval =>
                {
                  let _ = throw_type_error("invalid bytecode assign op")
                }
              BytecodeOp::AssignArray =>
                if index >= 0 && index < assign_array_pool.length() {
                  let def = assign_array_pool[index]
                  let value = bytecode_stack_pop(stack)
                  state.env = env
                  state.pc = pc
                  state.finally_body_depth = finally_body_depth
                  bytecode_update_gen_roots(state)
                  return gen_with_root_frame(fn() raise {
                    let _ = gen_root_push(value)
                    let assign = fn(name : String, v : Value) raise {
                      env_set(env, name, v)
                    }
                    let step =
                      gen_destructure_array_pattern(env, def.pattern, value, assign, fn() {
                        Done(Normal(Undefined))
                      })
                    gen_bind(step, fn(control) raise {
                      match control {
                        Normal(_) => {
                          stack.push(value)
                          bytecode_exec_step(state, GenResume::Next(Undefined))
                        }
                        _ => Done(control)
                      }
                    })
                  })
                } else {
                  let _ = throw_type_error("invalid bytecode assign array")
                }
              BytecodeOp::AssignObject =>
                if index >= 0 && index < assign_object_pool.length() {
                  let def = assign_object_pool[index]
                  let value = bytecode_stack_pop(stack)
                  state.env = env
                  state.pc = pc
                  state.finally_body_depth = finally_body_depth
                  bytecode_update_gen_roots(state)
                  return gen_with_root_frame(fn() raise {
                    let _ = gen_root_push(value)
                    let obj = to_object(value)
                    let _ = gen_root_push(obj)
                    let assign = fn(name : String, v : Value) raise {
                      env_set(env, name, v)
                    }
                    let step =
                      gen_destructure_object_pattern(
                        env,
                        def.pattern,
                        obj,
                        [],
                        0,
                        assign,
                        fn() { Done(Normal(Undefined)) },
                      )
                    gen_bind(step, fn(control) raise {
                      match control {
                        Normal(_) => {
                          stack.push(value)
                          bytecode_exec_step(state, GenResume::Next(Undefined))
                        }
                        _ => Done(control)
                      }
                    })
                  })
                } else {
                  let _ = throw_type_error("invalid bytecode assign object")
                }
              BytecodeOp::AssignInvalid =>
                if index >= 0 && index < assign_invalid_pool.length() {
                  let def = assign_invalid_pool[index]
                  let _ = with_expr_pos(def.offset, fn() raise {
                    let _ = bytecode_stack_pop(stack)
                    throw_reference_error("invalid assignment target")
                  })
                  stack.push(Undefined)
                } else {
                  let _ = throw_type_error("invalid bytecode assign invalid")
                }
              BytecodeOp::AssignForInExpr =>
                if index >= 0 && index < assign_for_in_pool.length() {
                  let def = assign_for_in_pool[index]
                  let value = bytecode_stack_pop(stack)
                  state.env = env
                  state.pc = pc
                  state.finally_body_depth = finally_body_depth
                  bytecode_update_gen_roots(state)
                  return gen_with_root_frame(fn() raise {
                    let _ = gen_root_push(value)
                    let step =
                      gen_assign_for_in_expr(env, def.expr, value, fn() {
                        Done(Normal(Undefined))
                      })
                    gen_bind(step, fn(control) raise {
                      match control {
                        Normal(_) =>
                          bytecode_exec_step(state, GenResume::Next(Undefined))
                        _ => Done(control)
                      }
                    })
                  })
                } else {
                  let _ = throw_type_error("invalid bytecode assign for-in")
                }
              BytecodeOp::YieldStar =>
                if index >= 0 && index < yield_star_pool.length() {
                  let def = yield_star_pool[index]
                  state.env = env
                  state.pc = pc
                  state.finally_body_depth = finally_body_depth
                  bytecode_update_gen_roots(state)
                  let step = gen_eval_yield_star(env, def.expr, fn(value) raise {
                    stack.push(value)
                    bytecode_exec_step(state, GenResume::Next(Undefined))
                  })
                  return gen_bind(step, fn(control) raise {
                    match control {
                      Normal(value) => {
                        stack.push(value)
                        bytecode_exec_step(state, GenResume::Next(Undefined))
                      }
                      Return(value) => {
                        state.resume_pending = true
                        bytecode_exec_step(state, GenResume::Return(value))
                      }
                      Throw(value) => {
                        state.resume_pending = true
                        bytecode_exec_step(state, GenResume::Throw(value))
                      }
                      Break(label, value_opt) => Done(Break(label, value_opt))
                      Continue(label, value_opt) =>
                        Done(Continue(label, value_opt))
                    }
                  })
                } else {
                  let _ = throw_type_error("invalid bytecode yield star")
                }
              BytecodeOp::VarDeclArray =>
                if index >= 0 && index < var_decl_array_pool.length() {
                  let def = var_decl_array_pool[index]
                  let value = bytecode_stack_pop(stack)
                  state.env = env
                  state.pc = pc
                  state.finally_body_depth = finally_body_depth
                  bytecode_update_gen_roots(state)
                  return gen_with_root_frame(fn() raise {
                    let _ = gen_root_push(value)
                    let assign = fn(name : String, v : Value) raise {
                      match def.kind {
                        @engine.VarKind::ConstDecl =>
                          env_define_readonly(env, name, v, true)
                        @engine.VarKind::Var => env_set(env, name, v)
                        _ => env_initialize_binding(env, name, v)
                      }
                    }
                    let step =
                      gen_destructure_array_pattern(env, def.pattern, value, assign, fn() {
                        Done(Normal(Undefined))
                      })
                    gen_bind(step, fn(control) raise {
                      match control {
                        Normal(_) =>
                          bytecode_exec_step(state, GenResume::Next(Undefined))
                        _ => Done(control)
                      }
                    })
                  })
                } else {
                  let _ = throw_type_error("invalid bytecode var decl array")
                }
              BytecodeOp::VarDeclObject =>
                if index >= 0 && index < var_decl_object_pool.length() {
                  let def = var_decl_object_pool[index]
                  let value = bytecode_stack_pop(stack)
                  state.env = env
                  state.pc = pc
                  state.finally_body_depth = finally_body_depth
                  bytecode_update_gen_roots(state)
                  return gen_with_root_frame(fn() raise {
                    let _ = gen_root_push(value)
                    let obj = to_object(value)
                    let _ = gen_root_push(obj)
                    let assign = fn(name : String, v : Value) raise {
                      match def.kind {
                        @engine.VarKind::ConstDecl =>
                          env_define_readonly(env, name, v, true)
                        @engine.VarKind::Var => env_set(env, name, v)
                        _ => env_initialize_binding(env, name, v)
                      }
                    }
                    let step =
                      gen_destructure_object_pattern(
                        env,
                        def.pattern,
                        obj,
                        [],
                        0,
                        assign,
                        fn() { Done(Normal(Undefined)) },
                      )
                    gen_bind(step, fn(control) raise {
                      match control {
                        Normal(_) =>
                          bytecode_exec_step(state, GenResume::Next(Undefined))
                        _ => Done(control)
                      }
                    })
                  })
                } else {
                  let _ = throw_type_error("invalid bytecode var decl object")
                }
              BytecodeOp::DeclareBinding =>
                if index >= 0 && index < declare_binding_pool.length() {
                  let def = declare_binding_pool[index]
                  declare_binding_names(env, def.binding)
                } else {
                  let _ = throw_type_error("invalid bytecode declare binding")
                }
              _ =>
                if index >= 0 && index < cpool.length() {
                  let const_value = cpool[index]
                  match op {
                    BytecodeOp::PushConst => stack.push(const_value)
                    BytecodeOp::GetName =>
                      match const_value {
                        String(name) => stack.push(env_get(env, name))
                        _ => stack.push(Undefined)
                      }
                    BytecodeOp::TypeofName =>
                      match const_value {
                        String(name) =>
                          if env_has(env, name) {
                            stack.push(String(typeof_value(env_get(env, name))))
                          } else {
                            stack.push(String("undefined"))
                          }
                        _ => {
                          let _ = throw_type_error("invalid bytecode typeof name")
                        }
                      }
                    BytecodeOp::DeleteName =>
                      match const_value {
                        String(name) =>
                          stack.push(delete_ident(env, name, env.strict))
                        _ => {
                          let _ = throw_type_error("invalid bytecode delete name")
                        }
                      }
                    BytecodeOp::ThrowSyntaxError =>
                      match const_value {
                        String(message) => {
                          let _ = throw_syntax_error(message)
                        }
                        _ => {
                          let _ = throw_type_error(
                            "invalid bytecode throw syntax error",
                          )
                        }
                      }
                    BytecodeOp::ThrowReferenceError =>
                      match const_value {
                        String(message) => {
                          let _ = throw_reference_error(message)
                        }
                        _ => {
                          let _ = throw_type_error(
                            "invalid bytecode throw reference error",
                          )
                        }
                      }
                    BytecodeOp::ThrowTypeError =>
                      match const_value {
                        String(message) => {
                          let _ = throw_type_error(message)
                        }
                        _ => {
                          let _ = throw_type_error(
                            "invalid bytecode throw type error",
                          )
                        }
                      }
                    BytecodeOp::PrivateIn =>
                      match const_value {
                        String(name) => {
                          let target = bytecode_stack_pop(stack)
                          stack.push(Bool(eval_private_in(env, name, target)))
                        }
                        _ => {
                          let _ = throw_type_error("invalid bytecode private in")
                        }
                      }
                    BytecodeOp::BindName =>
                      match const_value {
                        String(name) => {
                          let assigner = prepare_identifier_assignment_target(
                            env,
                            name,
                            env.strict,
                          )
                          target_stack.push(assigner)
                        }
                        _ => {
                          let _ = throw_type_error("invalid bytecode binding name")
                        }
                      }
                    BytecodeOp::AssignName =>
                      match const_value {
                        String(name) => {
                          let value = bytecode_stack_peek(stack)
                          set_anonymous_function_name(value, name, None)
                          let assigner = bytecode_target_pop(target_stack)
                          assigner(value)
                        }
                        _ => {
                          let _ = throw_type_error("invalid bytecode assignment name")
                        }
                      }
                    BytecodeOp::AssignNameNoName =>
                      match const_value {
                        String(_) => {
                          let value = bytecode_stack_peek(stack)
                          let assigner = bytecode_target_pop(target_stack)
                          assigner(value)
                        }
                        _ => {
                          let _ = throw_type_error("invalid bytecode assignment name")
                        }
                      }
                    BytecodeOp::BindNameTarget =>
                      match const_value {
                        String(name) => {
                          let target = resolve_binding_target(env, name)
                          binding_stack.push(target)
                        }
                        _ => {
                          let _ = throw_type_error("invalid bytecode binding target")
                        }
                      }
                    BytecodeOp::GetNameFromTarget =>
                      match const_value {
                        String(name) => {
                          let target = bytecode_binding_peek(binding_stack)
                          stack.push(
                            get_binding_value_from_target(
                              env,
                              name,
                              target,
                              env.strict,
                            ),
                          )
                        }
                        _ => {
                          let _ = throw_type_error("invalid bytecode binding read")
                        }
                      }
                    BytecodeOp::AssignFromTarget =>
                      match const_value {
                        String(name) => {
                          let value = bytecode_stack_peek(stack)
                          let target = bytecode_binding_pop(binding_stack)
                          assign_binding_value_to_target(
                            env,
                            name,
                            target,
                            value,
                            env.strict,
                          )
                        }
                        _ => {
                          let _ = throw_type_error("invalid bytecode binding write")
                        }
                      }
                    BytecodeOp::PopTarget => {
                      let _ = bytecode_binding_pop(binding_stack)
                    }
                    BytecodeOp::SetAnonName =>
                      match const_value {
                        String(name) => {
                          let value = bytecode_stack_peek(stack)
                          set_anonymous_function_name(value, name, None)
                        }
                        _ => {
                          let _ = throw_type_error("invalid bytecode anon name")
                        }
                      }
                    BytecodeOp::VarInitName =>
                      match const_value {
                        String(name) => {
                          let value = bytecode_stack_pop(stack)
                          let target = resolve_binding_target(env, name)
                          match target {
                            BindingTarget::WithObject(obj) => {
                              let _ = property_set(obj, name, value, env.strict)
                            }
                            BindingTarget::VarObject(obj) => {
                              let _ = property_set(obj, name, value, env.strict)
                            }
                            BindingTarget::Env(target_env) =>
                              env_set_local(target_env, name, value)
                            BindingTarget::Unbound =>
                              env_set_with_strict_no_with(
                                var_env_for(env),
                                name,
                                value,
                                env.strict,
                              )
                          }
                        }
                        _ => {
                          let _ = throw_type_error("invalid bytecode var init name")
                        }
                      }
                    BytecodeOp::InitName =>
                      match const_value {
                        String(name) => {
                          let value = bytecode_stack_pop(stack)
                          env_initialize_binding(env, name, value)
                        }
                        _ => {
                          let _ = throw_type_error("invalid bytecode init name")
                        }
                      }
                    BytecodeOp::InitConstName =>
                      match const_value {
                        String(name) => {
                          let value = bytecode_stack_pop(stack)
                          env_define_readonly(env, name, value, true)
                        }
                        _ => {
                          let _ = throw_type_error("invalid bytecode const init name")
                        }
                      }
                    BytecodeOp::GetProp =>
                      match const_value {
                        String(name) => {
                          let target = bytecode_stack_pop(stack)
                          match bytecode_fast_get_prop(target, name) {
                            Some(value) => stack.push(value)
                            None => {
                              let target_obj = to_object(target)
                              stack.push(
                                property_get_with_receiver(target_obj, name, target),
                              )
                            }
                          }
                        }
                        _ => {
                          let _ = throw_type_error("invalid bytecode property name")
                        }
                      }
                    BytecodeOp::GetPrivate =>
                      match const_value {
                        String(name) => {
                          let target = bytecode_stack_pop(stack)
                          let key_name = env_private_key_checked(env, name)
                          stack.push(private_property_get(target, key_name, name))
                        }
                        _ => {
                          let _ = throw_type_error("invalid bytecode private name")
                        }
                      }
                    BytecodeOp::SetProp =>
                      match const_value {
                        String(name) => {
                          let value = bytecode_stack_pop(stack)
                          let target = bytecode_stack_pop(stack)
                          stack.push(
                            set_property_for_value(
                              target,
                              name,
                              value,
                              env.strict,
                            ),
                          )
                        }
                        _ => {
                          let _ = throw_type_error("invalid bytecode property name")
                        }
                      }
                    BytecodeOp::SetPrivate =>
                      match const_value {
                        String(name) => {
                          let value = bytecode_stack_pop(stack)
                          let target = bytecode_stack_pop(stack)
                          let key_name = env_private_key_checked(env, name)
                          private_property_set(target, key_name, name, value)
                          stack.push(value)
                        }
                        _ => {
                          let _ = throw_type_error("invalid bytecode private name")
                        }
                      }
                    BytecodeOp::SuperSetProp =>
                      match const_value {
                        String(name) => {
                          let value = bytecode_stack_pop(stack)
                          let (base, receiver) = super_base_and_receiver(env)
                          super_set_property(env, base, receiver, name, value)
                          stack.push(value)
                        }
                        _ => {
                          let _ = throw_type_error("invalid bytecode property name")
                        }
                      }
                    BytecodeOp::InitProp =>
                      match const_value {
                        String(name) => {
                          let value = bytecode_stack_pop(stack)
                          let target = bytecode_stack_pop(stack)
                          match target {
                            Object(obj) => props_set(obj.props, name, property_data(value))
                            _ => {
                              let _ = throw_type_error("invalid bytecode object")
                            }
                          }
                          stack.push(value)
                        }
                        _ => {
                          let _ = throw_type_error("invalid bytecode property name")
                        }
                      }
                    BytecodeOp::InitPropNamed =>
                      match const_value {
                        String(name) => {
                          let value = bytecode_stack_pop(stack)
                          let target = bytecode_stack_pop(stack)
                          set_anonymous_function_name(value, name, None)
                          match target {
                            Object(obj) => props_set(obj.props, name, property_data(value))
                            _ => {
                              let _ = throw_type_error("invalid bytecode object")
                            }
                          }
                          stack.push(value)
                        }
                        _ => {
                          let _ = throw_type_error("invalid bytecode property name")
                        }
                      }
                    BytecodeOp::InitMethod =>
                      match const_value {
                        String(name) => {
                          let func_value = bytecode_stack_pop(stack)
                          let target = bytecode_stack_pop(stack)
                          match (target, func_value) {
                            (Object(obj), Function(func)) => {
                              func.is_constructor = false
                              set_function_name(
                                func,
                                function_name_from_key(name, None, None),
                              )
                              set_function_home_object(func, Some(Value::Object(obj)))
                              let method_value = Value::Function(func)
                              props_set(obj.props, name, property_data(method_value))
                              stack.push(method_value)
                            }
                            _ => {
                              let _ = throw_type_error("invalid bytecode method")
                            }
                          }
                        }
                        _ => {
                          let _ = throw_type_error("invalid bytecode method name")
                        }
                      }
                    BytecodeOp::InitGetter =>
                      match const_value {
                        String(name) => {
                          let func_value = bytecode_stack_pop(stack)
                          let target = bytecode_stack_pop(stack)
                          match (target, func_value) {
                            (Object(obj), Function(func)) => {
                              func.is_constructor = false
                              set_function_name(
                                func,
                                function_name_from_key(name, None, Some("get ")),
                              )
                              set_function_home_object(func, Some(Value::Object(obj)))
                              let getter_value = Value::Function(func)
                              set_accessor_property(
                                obj.props,
                                name,
                                Some(getter_value),
                                None,
                              )
                              stack.push(getter_value)
                            }
                            _ => {
                              let _ = throw_type_error("invalid bytecode getter")
                            }
                          }
                        }
                        _ => {
                          let _ = throw_type_error("invalid bytecode getter name")
                        }
                      }
                    BytecodeOp::InitSetter =>
                      match const_value {
                        String(name) => {
                          let func_value = bytecode_stack_pop(stack)
                          let target = bytecode_stack_pop(stack)
                          match (target, func_value) {
                            (Object(obj), Function(func)) => {
                              func.is_constructor = false
                              set_function_name(
                                func,
                                function_name_from_key(name, None, Some("set ")),
                              )
                              set_function_home_object(func, Some(Value::Object(obj)))
                              let setter_value = Value::Function(func)
                              set_accessor_property(
                                obj.props,
                                name,
                                None,
                                Some(setter_value),
                              )
                              stack.push(setter_value)
                            }
                            _ => {
                              let _ = throw_type_error("invalid bytecode setter")
                            }
                          }
                        }
                        _ => {
                          let _ = throw_type_error("invalid bytecode setter name")
                        }
                      }
                    BytecodeOp::DeleteProp =>
                      match const_value {
                        String(name) => {
                          let target = bytecode_stack_pop(stack)
                          stack.push(delete_property(env, target, name))
                        }
                        _ => {
                          let _ = throw_type_error("invalid bytecode property name")
                        }
                      }
                    BytecodeOp::UpdateNamePrefixInc
                    | BytecodeOp::UpdateNamePrefixDec
                    | BytecodeOp::UpdateNamePostfixInc
                    | BytecodeOp::UpdateNamePostfixDec =>
                      match const_value {
                        String(name) => {
                          let (is_inc, is_prefix) = match op {
                            BytecodeOp::UpdateNamePrefixInc => (true, true)
                            BytecodeOp::UpdateNamePrefixDec => (false, true)
                            BytecodeOp::UpdateNamePostfixInc => (true, false)
                            _ => (false, false)
                          }
                          let target = resolve_binding_target(env, name)
                          let current = get_binding_value_from_target(
                            env,
                            name,
                            target,
                            env.strict,
                          )
                          let (old_value, new_value) =
                            update_values_with_flags(is_inc, current)
                          assign_binding_value_to_target(
                            env,
                            name,
                            target,
                            new_value,
                            env.strict,
                          )
                          let result =
                            update_result_with_flags(is_prefix, old_value, new_value)
                          stack.push(result)
                        }
                        _ => {
                          let _ = throw_type_error("invalid bytecode update name")
                        }
                      }
                    BytecodeOp::UpdatePropPrefixInc
                    | BytecodeOp::UpdatePropPrefixDec
                    | BytecodeOp::UpdatePropPostfixInc
                    | BytecodeOp::UpdatePropPostfixDec =>
                      match const_value {
                        String(name) => {
                          let (is_inc, is_prefix) = match op {
                            BytecodeOp::UpdatePropPrefixInc => (true, true)
                            BytecodeOp::UpdatePropPrefixDec => (false, true)
                            BytecodeOp::UpdatePropPostfixInc => (true, false)
                            _ => (false, false)
                          }
                          let target = bytecode_stack_pop(stack)
                          let target_obj = to_object(target)
                          let current =
                            property_get_with_receiver(target_obj, name, target)
                          let (old_value, new_value) =
                            update_values_with_flags(is_inc, current)
                          let _ = set_property_for_value(
                            target,
                            name,
                            new_value,
                            env.strict,
                          )
                          let result =
                            update_result_with_flags(is_prefix, old_value, new_value)
                          stack.push(result)
                        }
                        _ => {
                          let _ = throw_type_error("invalid bytecode update property")
                        }
                      }
                    BytecodeOp::UpdatePrivatePrefixInc
                    | BytecodeOp::UpdatePrivatePrefixDec
                    | BytecodeOp::UpdatePrivatePostfixInc
                    | BytecodeOp::UpdatePrivatePostfixDec =>
                      match const_value {
                        String(name) => {
                          let (is_inc, is_prefix) = match op {
                            BytecodeOp::UpdatePrivatePrefixInc => (true, true)
                            BytecodeOp::UpdatePrivatePrefixDec => (false, true)
                            BytecodeOp::UpdatePrivatePostfixInc => (true, false)
                            _ => (false, false)
                          }
                          let target = bytecode_stack_pop(stack)
                          let key_name = env_private_key_checked(env, name)
                          let current = private_property_get(target, key_name, name)
                          let (old_value, new_value) =
                            update_values_with_flags(is_inc, current)
                          private_property_set(target, key_name, name, new_value)
                          let result =
                            update_result_with_flags(is_prefix, old_value, new_value)
                          stack.push(result)
                        }
                        _ => {
                          let _ = throw_type_error(
                            "invalid bytecode update private",
                          )
                        }
                      }
                    BytecodeOp::GetCallRef =>
                      match const_value {
                        String(name) => {
                          let (callee_value, this_value) =
                            env_get_call_reference(env, name)
                          stack.push(this_value)
                          stack.push(callee_value)
                        }
                        _ => {
                          let _ = throw_type_error("invalid bytecode call reference")
                        }
                      }
                    _ => ()
                  }
                }
              }
          }
        }
      } catch {
        ThrowSignal(value) => {
          let mut handled = false
          while !try_stack.is_empty() {
            let top_index = try_stack.length() - 1
            let (
              catch_pc,
              stack_len,
              target_len,
              binding_len,
              env_depth,
              for_in_depth,
              for_of_depth,
              last_value,
            ) = try_stack[top_index]
            if catch_pc < 0 {
              let _ = try_stack.pop()
              continue
            }
            handled = true
            while stack.length() > stack_len {
              let _ = stack.pop()
            }
            while target_stack.length() > target_len {
              let _ = target_stack.pop()
            }
            while binding_stack.length() > binding_len {
              let _ = binding_stack.pop()
            }
            while for_in_stack.length() > for_in_depth {
              let _ = for_in_stack.pop()
            }
            bytecode_close_for_of_stack_to_depth(
              for_of_stack,
              for_of_depth,
              true,
            )
            while env_stack.length() > env_depth {
              match env_stack.pop() {
                Some(prev) => {
                  pop_env()
                  env = prev
                }
                None => {
                  let _ = throw_type_error("invalid bytecode env pop")
                }
              }
            }
            let target_pc = bytecode_jump_target(code, catch_pc)
            try_stack[top_index] = (
              -1,
              stack_len,
              target_len,
              binding_len,
              env_depth,
              for_in_depth,
              for_of_depth,
              last_value,
            )
            stack.push(value)
            pc = target_pc
            break
          }
          if handled {
            continue
          }
          bytecode_close_for_of_stack(for_of_stack, true)
          pop_envs()
          state.env = env
          state.pc = pc
          state.finally_body_depth = finally_body_depth
          gen_root_replace([])
          return Done(Throw(value))
        }
        err => raise err
      }
    }
    bytecode_close_for_of_stack(for_of_stack, false)
    pop_envs()
    state.env = env
    state.pc = pc
    state.finally_body_depth = finally_body_depth
    gen_root_replace([])
    Done(Normal(Undefined))
  } catch {
    err => {
      bytecode_close_for_of_stack(for_of_stack, true)
      pop_envs()
      state.env = env
      state.pc = pc
      state.finally_body_depth = finally_body_depth
      gen_root_replace([])
      raise err
    }
  }
  result
  })
}
