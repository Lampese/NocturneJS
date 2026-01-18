///|
fn eval_block_control(env : Env, body : @engine.Stmt) -> GenControl raise {
  try {
    let result = with_env_value(env, fn() raise { eval_stmt(env, body) })
    Normal(result)
  } catch {
    ThrowSignal(value) => Throw(value)
    ReturnSignal(value) => Return(value)
    BreakSignal((label, value_opt)) => Break(label, value_opt)
    ContinueSignal((label, value_opt)) => Continue(label, value_opt)
    err => raise err
  }
}

///|
fn eval_block_control_stmts(
  env : Env,
  body : Array[@engine.Stmt],
) -> GenControl raise {
  try {
    let result = with_env_value(env, fn() raise { eval_block(env, body) })
    Normal(result)
  } catch {
    ThrowSignal(value) => Throw(value)
    ReturnSignal(value) => Return(value)
    BreakSignal((label, value_opt)) => Break(label, value_opt)
    ContinueSignal((label, value_opt)) => Continue(label, value_opt)
    err => raise err
  }
}

///|
fn eval_block_raw_control_with_last(
  env : Env,
  body : Array[@engine.Stmt],
) -> (GenControl, Value) raise {
  let mut last = Undefined
  for stmt in body {
    try {
      let value = eval_stmt(env, stmt)
      if stmt_updates_completion(stmt) {
        last = value
      }
    } catch {
      BreakSignal((label, value_opt)) => {
        let updated = update_empty(value_opt, last)
        return (Break(label, Some(updated)), last)
      }
      ContinueSignal((label, value_opt)) => {
        let updated = update_empty(value_opt, last)
        return (Continue(label, Some(updated)), last)
      }
      ReturnSignal(value) => return (Return(value), last)
      ThrowSignal(value) => return (Throw(value), last)
      err => raise err
    }
  }
  (Normal(last), last)
}

///|
fn eval_block_control_with_last_stmts(
  env : Env,
  body : Array[@engine.Stmt],
) -> (GenControl, Value) raise {
  if current_module_env_id() is Some(_) && block_is_export_wrapper(body) {
    return eval_block_raw_control_with_last(env, body)
  }
  if block_needs_lex_env(body, env.strict) {
    let block_env = Env::new(Some(env))
    hoist_block_functions(block_env, body)
    eval_predeclare_lexical_decls(block_env, body)
    push_env(block_env)
    try {
      let result = eval_block_raw_control_with_last(block_env, body)
      pop_env()
      return result
    } catch {
      err => {
        pop_env()
        raise err
      }
    }
  }
  eval_block_raw_control_with_last(env, body)
}

///|
fn apply_finally_control(
  pending : GenControl,
  final_control : GenControl,
) -> GenControl {
  match final_control {
    Normal(_) => pending
    _ => final_control
  }
}
