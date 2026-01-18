///|
fn label_matches(signal_label : String?, target_label : String?) -> Bool {
  match signal_label {
    None => true
    Some(name) =>
      match target_label {
        Some(target) => target == name
        None => false
      }
  }
}

///|
fn update_empty(value_opt : Value?, fallback : Value) -> Value {
  match value_opt {
    Some(value) => value
    None => fallback
  }
}

///|
fn eval_while_with_label(
  env : Env,
  label : String?,
  cond_expr : @engine.Expr,
  body : @engine.Stmt,
) -> Value raise {
  let mut last = Undefined
  let mut gc_tick = 0
  while is_truthy(eval_expr(env, cond_expr)) {
    try {
      last = eval_stmt(env, body)
    } catch {
      BreakSignal((signal_label, value_opt)) => {
        let updated = update_empty(value_opt, last)
        if label_matches(signal_label, label) {
          last = updated
          gc_tick = loop_gc_maybe(env, last, gc_tick)
          break
        } else {
          raise BreakSignal((signal_label, Some(updated)))
        }
      }
      ContinueSignal((signal_label, value_opt)) => {
        let updated = update_empty(value_opt, last)
        if label_matches(signal_label, label) {
          last = updated
          gc_tick = loop_gc_maybe(env, last, gc_tick)
          continue
        } else {
          raise ContinueSignal((signal_label, Some(updated)))
        }
      }
      err => raise err
    }
    gc_tick = loop_gc_maybe(env, last, gc_tick)
  }
  last
}

///|
fn for_init_binding_names(init : @engine.ForInit) -> Array[String] {
  let names : @hashmap.HashMap[String, Bool] = @hashmap.new()
  match init {
    @engine.ForInit::Var(_, decls) =>
      for decl in decls {
        let (binding, _) = decl
        collect_binding_names(binding, names)
      }
    _ => ()
  }
  let result : Array[String] = []
  for name, _ in names {
    result.push(name)
  }
  result
}

///|
fn create_for_iteration_env(
  source_env : Env,
  outer_env : Env,
  names : Array[String],
  is_const : Bool,
) -> Env raise {
  let iter_env = Env::new(Some(outer_env))
  for name in names {
    let value = env_get(source_env, name)
    if is_const {
      env_define_readonly(iter_env, name, value, true)
    } else {
      env_initialize_binding(iter_env, name, value)
    }
  }
  iter_env
}

///|
fn eval_for_with_label(
  env : Env,
  label : String?,
  init : @engine.ForInit,
  test_expr : @engine.Expr?,
  update_expr : @engine.Expr?,
  body : @engine.Stmt,
) -> Value raise {
  let mut last = Undefined
  let (use_loop_env, loop_is_const) = match init {
    @engine.ForInit::Var(kind, _) =>
      if kind is @engine.VarKind::Var {
        (false, false)
      } else {
        (true, kind is @engine.VarKind::ConstDecl)
      }
    _ => (false, false)
  }
  let loop_env = if use_loop_env { Env::new(Some(env)) } else { env }
  let loop_names = if use_loop_env {
    for_init_binding_names(init)
  } else {
    ([] : Array[String])
  }
  let reuse_iter_env = use_loop_env &&
    !loop_is_const &&
    !for_loop_needs_per_iteration_env(init, test_expr, update_expr, body)
  if use_loop_env {
    hoist_functions(loop_env, body, false, false)
  }
  let _ = with_env_value(loop_env, fn() raise {
    eval_for_init(loop_env, init)
    Undefined
  })
  let mut iter_env = if use_loop_env {
    if reuse_iter_env {
      loop_env
    } else {
      create_for_iteration_env(loop_env, loop_env, loop_names, loop_is_const)
    }
  } else {
    loop_env
  }
  let mut gc_tick = 0
  while true {
    let current_env = if use_loop_env { iter_env } else { loop_env }
    let should_continue = match test_expr {
      Some(expr) =>
        is_truthy(
          with_env_value(current_env, fn() raise {
            eval_expr(current_env, expr)
          }),
        )
      None => true
    }
    if !should_continue {
      break
    }
    let mut do_update = false
    try {
      last = with_env_value(current_env, fn() raise {
        eval_stmt(current_env, body)
      })
      do_update = true
    } catch {
      BreakSignal((signal_label, value_opt)) => {
        let updated = update_empty(value_opt, last)
        if label_matches(signal_label, label) {
          last = updated
          gc_tick = loop_gc_maybe(current_env, last, gc_tick)
          break
        } else {
          raise BreakSignal((signal_label, Some(updated)))
        }
      }
      ContinueSignal((signal_label, value_opt)) => {
        let updated = update_empty(value_opt, last)
        if label_matches(signal_label, label) {
          last = updated
          do_update = true
        } else {
          raise ContinueSignal((signal_label, Some(updated)))
        }
      }
      err => raise err
    }
    if do_update {
      let update_env = if use_loop_env {
        if reuse_iter_env {
          loop_env
        } else {
          let created = create_for_iteration_env(
            current_env, loop_env, loop_names, loop_is_const,
          )
          iter_env = created
          created
        }
      } else {
        loop_env
      }
      match update_expr {
        Some(expr) => {
          let _ = with_env_value(update_env, fn() raise {
            eval_expr(update_env, expr)
          })

        }
        None => ()
      }
      gc_tick = loop_gc_maybe(current_env, last, gc_tick)
    }
  }
  last
}

///|
fn eval_for_in_with_label(
  env : Env,
  label : String?,
  init : @engine.ForInit,
  expr : @engine.Expr,
  body : @engine.Stmt,
) -> Value raise {
  let mut last = Undefined
  let use_loop_env = match init {
    @engine.ForInit::Var(kind, _) => !(kind is @engine.VarKind::Var)
    _ => false
  }
  let loop_is_const = match init {
    @engine.ForInit::Var(kind, _) => kind is @engine.VarKind::ConstDecl
    _ => false
  }
  let reuse_iter_env = use_loop_env &&
    !loop_is_const &&
    !for_init_contains_function_like(init) &&
    !stmt_contains_function_like(body)
  let mut pre_eval_var_init = false
  if !use_loop_env {
    match init {
      @engine.ForInit::Var(kind, decls) =>
        if kind is @engine.VarKind::Var {
          let mut has_init = false
          for decl in decls {
            let (_, init_expr) = decl
            if init_expr is Some(_) {
              has_init = true
              break
            }
          }
          if has_init {
            eval_var_decls_no_name(env, kind, decls)
            pre_eval_var_init = true
          }
        }
      _ => ()
    }
  }
  let mut expr_env = env
  if use_loop_env {
    let tdz_env = Env::new(Some(env))
    match init {
      @engine.ForInit::Var(kind, decls) =>
        if !(kind is @engine.VarKind::Var) {
          for decl in decls {
            let (binding, _) = decl
            declare_binding_uninitialized(tdz_env, binding)
          }
        }
      _ => ()
    }
    expr_env = tdz_env
  }
  let target_value = with_env_value(expr_env, fn() raise {
    eval_expr(expr_env, expr)
  })
  let mut target = target_value
  let mut keys : Array[String] = []
  if !(target_value is Null || target_value is Undefined) {
    target = to_object(target_value)
    keys = enumerable_string_keys_in_chain(target)
  }
  let loop_env = if use_loop_env { Env::new(Some(env)) } else { env }
  if use_loop_env {
    hoist_functions(loop_env, body, false, false)
  }
  if use_loop_env {
    match init {
      @engine.ForInit::Var(kind, decls) => {
        let mut has_init = false
        for decl in decls {
          let (_, init_expr) = decl
          if init_expr is Some(_) {
            has_init = true
            break
          }
        }
        if kind is @engine.VarKind::ConstDecl && !has_init {
          for decl in decls {
            let (binding, _) = decl
            declare_binding_names(loop_env, binding)
          }
        } else {
          eval_var_decls(loop_env, kind, decls)
        }
      }
      _ => ()
    }
  } else {
    match init {
      @engine.ForInit::Var(kind, decls) => {
        let mut has_init = false
        for decl in decls {
          let (_, init_expr) = decl
          if init_expr is Some(_) {
            has_init = true
            break
          }
        }
        if kind is @engine.VarKind::ConstDecl && !has_init {
          for decl in decls {
            let (binding, _) = decl
            declare_binding_names(env, binding)
          }
        } else if !(pre_eval_var_init && kind is @engine.VarKind::Var) {
          eval_var_decls(env, kind, decls)
        }
      }
      _ => ()
    }
  }
  let result = with_gc_frame(fn() raise {
    let _ = gc_root_push(target)
    let mut gc_tick = 0
    for key in keys {
      if !is_enumerable_key_in_chain(target, key) {
        continue
      }
      let iter_env = if use_loop_env {
        if reuse_iter_env {
          loop_env
        } else {
          Env::new(Some(loop_env))
        }
      } else {
        loop_env
      }
      try {
        last = with_env_value(iter_env, fn() raise {
          assign_for_in_target(iter_env, init, String(key))
          eval_stmt(iter_env, body)
        })
      } catch {
        BreakSignal((signal_label, value_opt)) => {
          let updated = update_empty(value_opt, last)
          if label_matches(signal_label, label) {
            last = updated
            gc_tick = loop_gc_maybe(loop_env, last, gc_tick)
            break
          } else {
            raise BreakSignal((signal_label, Some(updated)))
          }
        }
        ContinueSignal((signal_label, value_opt)) => {
          let updated = update_empty(value_opt, last)
          if label_matches(signal_label, label) {
            last = updated
            gc_tick = loop_gc_maybe(loop_env, last, gc_tick)
          } else {
            raise ContinueSignal((signal_label, Some(updated)))
          }
        }
        err => raise err
      }
      gc_tick = loop_gc_maybe(loop_env, last, gc_tick)
    }
    last
  })
  result
}

///|
fn eval_for_of_with_label(
  env : Env,
  label : String?,
  init : @engine.ForInit,
  expr : @engine.Expr,
  body : @engine.Stmt,
) -> Value raise {
  let mut last = Undefined
  let use_loop_env = match init {
    @engine.ForInit::Var(kind, _) => !(kind is @engine.VarKind::Var)
    _ => false
  }
  let loop_is_const = match init {
    @engine.ForInit::Var(kind, _) => kind is @engine.VarKind::ConstDecl
    _ => false
  }
  let reuse_iter_env = use_loop_env &&
    !loop_is_const &&
    !for_init_contains_function_like(init) &&
    !stmt_contains_function_like(body)
  let mut expr_env = env
  if use_loop_env {
    let tdz_env = Env::new(Some(env))
    match init {
      @engine.ForInit::Var(kind, decls) =>
        if !(kind is @engine.VarKind::Var) {
          for decl in decls {
            let (binding, _) = decl
            declare_binding_uninitialized(tdz_env, binding)
          }
        }
      _ => ()
    }
    expr_env = tdz_env
  }
  let iterable = with_env_value(expr_env, fn() raise {
    eval_expr(expr_env, expr)
  })
  let loop_env = if use_loop_env { Env::new(Some(env)) } else { env }
  if use_loop_env {
    hoist_functions(loop_env, body, false, false)
  }
  if use_loop_env {
    match init {
      @engine.ForInit::Var(kind, decls) => {
        let mut has_init = false
        for decl in decls {
          let (_, init_expr) = decl
          if init_expr is Some(_) {
            has_init = true
            break
          }
        }
        if kind is @engine.VarKind::ConstDecl && !has_init {
          for decl in decls {
            let (binding, _) = decl
            declare_binding_names(loop_env, binding)
          }
        } else {
          eval_var_decls(loop_env, kind, decls)
        }
      }
      _ => ()
    }
  } else {
    match init {
      @engine.ForInit::Var(kind, decls) => {
        let mut has_init = false
        for decl in decls {
          let (_, init_expr) = decl
          if init_expr is Some(_) {
            has_init = true
            break
          }
        }
        if kind is @engine.VarKind::ConstDecl && !has_init {
          for decl in decls {
            let (binding, _) = decl
            declare_binding_names(env, binding)
          }
        } else {
          eval_var_decls(env, kind, decls)
        }
      }
      _ => ()
    }
  }
  let (iterator, next_method) = get_iterator_from_value(iterable)
  let result = with_gc_frame(fn() raise {
    let _ = gc_root_push(iterator)
    let _ = gc_root_push(next_method)
    let mut gc_tick = 0
    while true {
      let (done, value) = iterator_step_value(iterator, next_method)
      if done {
        break
      }
      try {
        let iter_env = if use_loop_env {
          if reuse_iter_env {
            loop_env
          } else {
            Env::new(Some(loop_env))
          }
        } else {
          loop_env
        }
        last = with_env_value(iter_env, fn() raise {
          assign_for_in_target(iter_env, init, value)
          eval_stmt(iter_env, body)
        })
      } catch {
        BreakSignal((signal_label, value_opt)) => {
          let updated = update_empty(value_opt, last)
          if label_matches(signal_label, label) {
            last = updated
            gc_tick = loop_gc_maybe(loop_env, last, gc_tick)
            iterator_close(iterator)
            break
          } else {
            iterator_close(iterator)
            raise BreakSignal((signal_label, Some(updated)))
          }
        }
        ContinueSignal((signal_label, value_opt)) => {
          let updated = update_empty(value_opt, last)
          if label_matches(signal_label, label) {
            last = updated
            gc_tick = loop_gc_maybe(loop_env, last, gc_tick)
          } else {
            iterator_close(iterator)
            raise ContinueSignal((signal_label, Some(updated)))
          }
        }
        ReturnSignal(value) => {
          iterator_close(iterator)
          raise ReturnSignal(value)
        }
        ThrowSignal(value) => {
          iterator_close_on_error(iterator)
          raise ThrowSignal(value)
        }
        err => {
          iterator_close_on_error(iterator)
          raise err
        }
      }
      gc_tick = loop_gc_maybe(loop_env, last, gc_tick)
    }
    last
  })
  result
}

///|
fn eval_for_await_of_with_label(
  env : Env,
  label : String?,
  init : @engine.ForInit,
  expr : @engine.Expr,
  body : @engine.Stmt,
) -> Value raise {
  let mut last = Undefined
  let use_loop_env = match init {
    @engine.ForInit::Var(kind, _) => !(kind is @engine.VarKind::Var)
    _ => false
  }
  let loop_is_const = match init {
    @engine.ForInit::Var(kind, _) => kind is @engine.VarKind::ConstDecl
    _ => false
  }
  let reuse_iter_env = use_loop_env &&
    !loop_is_const &&
    !for_init_contains_function_like(init) &&
    !stmt_contains_function_like(body)
  let mut expr_env = env
  if use_loop_env {
    let tdz_env = Env::new(Some(env))
    match init {
      @engine.ForInit::Var(kind, decls) =>
        if !(kind is @engine.VarKind::Var) {
          for decl in decls {
            let (binding, _) = decl
            declare_binding_uninitialized(tdz_env, binding)
          }
        }
      _ => ()
    }
    expr_env = tdz_env
  }
  let iterable = with_env_value(expr_env, fn() raise {
    eval_expr(expr_env, expr)
  })
  let loop_env = if use_loop_env { Env::new(Some(env)) } else { env }
  if use_loop_env {
    hoist_functions(loop_env, body, false, false)
  }
  if use_loop_env {
    match init {
      @engine.ForInit::Var(kind, decls) => {
        let mut has_init = false
        for decl in decls {
          let (_, init_expr) = decl
          if init_expr is Some(_) {
            has_init = true
            break
          }
        }
        if kind is @engine.VarKind::ConstDecl && !has_init {
          for decl in decls {
            let (binding, _) = decl
            declare_binding_names(loop_env, binding)
          }
        } else {
          eval_var_decls(loop_env, kind, decls)
        }
      }
      _ => ()
    }
  } else {
    match init {
      @engine.ForInit::Var(kind, decls) => {
        let mut has_init = false
        for decl in decls {
          let (_, init_expr) = decl
          if init_expr is Some(_) {
            has_init = true
            break
          }
        }
        if kind is @engine.VarKind::ConstDecl && !has_init {
          for decl in decls {
            let (binding, _) = decl
            declare_binding_names(env, binding)
          }
        } else {
          eval_var_decls(env, kind, decls)
        }
      }
      _ => ()
    }
  }
  let (iterator, next_method) = get_async_iterator_from_value(iterable)
  let result = with_gc_frame(fn() raise {
    let _ = gc_root_push(iterator)
    let _ = gc_root_push(next_method)
    let mut gc_tick = 0
    while true {
      let (done, value) = async_iterator_step_value(iterator, next_method)
      if done {
        break
      }
      try {
        let iter_env = if use_loop_env {
          if reuse_iter_env {
            loop_env
          } else {
            Env::new(Some(loop_env))
          }
        } else {
          loop_env
        }
        last = with_env_value(iter_env, fn() raise {
          assign_for_in_target(iter_env, init, value)
          eval_stmt(iter_env, body)
        })
      } catch {
        BreakSignal((signal_label, value_opt)) => {
          let updated = update_empty(value_opt, last)
          if label_matches(signal_label, label) {
            last = updated
            gc_tick = loop_gc_maybe(loop_env, last, gc_tick)
            async_iterator_close(iterator)
            break
          } else {
            async_iterator_close(iterator)
            raise BreakSignal((signal_label, Some(updated)))
          }
        }
        ContinueSignal((signal_label, value_opt)) => {
          let updated = update_empty(value_opt, last)
          if label_matches(signal_label, label) {
            last = updated
            gc_tick = loop_gc_maybe(loop_env, last, gc_tick)
          } else {
            async_iterator_close(iterator)
            raise ContinueSignal((signal_label, Some(updated)))
          }
        }
        ReturnSignal(value) => {
          async_iterator_close(iterator)
          raise ReturnSignal(value)
        }
        ThrowSignal(value) => {
          async_iterator_close_on_error(iterator)
          raise ThrowSignal(value)
        }
        err => {
          async_iterator_close_on_error(iterator)
          raise err
        }
      }
      gc_tick = loop_gc_maybe(loop_env, last, gc_tick)
    }
    last
  })
  result
}

///|
fn eval_do_while_with_label(
  env : Env,
  label : String?,
  body : @engine.Stmt,
  cond_expr : @engine.Expr,
) -> Value raise {
  let mut last = Undefined
  let mut gc_tick = 0
  while true {
    try {
      last = eval_stmt(env, body)
    } catch {
      BreakSignal((signal_label, value_opt)) => {
        let updated = update_empty(value_opt, last)
        if label_matches(signal_label, label) {
          last = updated
          gc_tick = loop_gc_maybe(env, last, gc_tick)
          break
        } else {
          raise BreakSignal((signal_label, Some(updated)))
        }
      }
      ContinueSignal((signal_label, value_opt)) => {
        let updated = update_empty(value_opt, last)
        if label_matches(signal_label, label) {
          last = updated
        } else {
          raise ContinueSignal((signal_label, Some(updated)))
        }
      }
      err => raise err
    }
    gc_tick = loop_gc_maybe(env, last, gc_tick)
    if !is_truthy(eval_expr(env, cond_expr)) {
      break
    }
  }
  last
}

///|
fn eval_switch_with_label(
  env : Env,
  label : String?,
  discriminant : @engine.Expr,
  cases : Array[@engine.SwitchCase],
) -> Value raise {
  let mut last = Undefined
  let value = eval_expr(env, discriminant)
  let switch_env = Env::new(Some(env))
  let case_stmts : Array[@engine.Stmt] = []
  for clause in cases {
    let body = match clause {
      @engine.SwitchCase::Case(_, stmts) => stmts
      @engine.SwitchCase::Default(stmts) => stmts
    }
    for stmt in body {
      case_stmts.push(stmt)
    }
  }
  if !case_stmts.is_empty() {
    hoist_block_functions(switch_env, case_stmts)
    eval_predeclare_lexical_decls(switch_env, case_stmts)
  }
  let mut start : Int? = None
  for i, clause in cases {
    match clause {
      @engine.SwitchCase::Case(expr, _) => {
        let case_value = eval_expr(switch_env, expr)
        if strict_eq(value, case_value) {
          start = Some(i)
          break
        }
      }
      @engine.SwitchCase::Default(_) => if start is None { start = Some(i) }
    }
  }
  match start {
    None => ()
    Some(start_index) =>
      for j = start_index; j < cases.length(); j = j + 1 {
        let clause = cases[j]
        let stmts = match clause {
          @engine.SwitchCase::Case(_, body) => body
          @engine.SwitchCase::Default(body) => body
        }
        try {
          for stmt in stmts {
            let value = eval_stmt(switch_env, stmt)
            if stmt_updates_completion(stmt) {
              last = value
            }
          }
        } catch {
          BreakSignal((signal_label, value_opt)) => {
            let updated = update_empty(value_opt, last)
            if label_matches(signal_label, label) {
              last = updated
              break
            } else {
              raise BreakSignal((signal_label, Some(updated)))
            }
          }
          ContinueSignal((signal_label, value_opt)) => {
            let updated = update_empty(value_opt, last)
            raise ContinueSignal((signal_label, Some(updated)))
          }
          err => raise err
        }
      }
  }
  last
}
