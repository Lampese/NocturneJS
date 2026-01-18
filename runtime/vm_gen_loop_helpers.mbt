///|
fn gen_eval_while(
  env : Env,
  label : String?,
  cond_expr : @engine.Expr,
  body : @engine.Stmt,
) -> GenStep raise {
  gen_eval_while_loop(env, label, cond_expr, body)
}

///|
fn gen_eval_while_loop(
  env : Env,
  label : String?,
  cond_expr : @engine.Expr,
  body : @engine.Stmt,
) -> GenStep raise {
  gen_eval_expr_cont(env, cond_expr, fn(cond) raise {
    if !is_truthy(cond) {
      Done(Normal(Undefined))
    } else {
      gen_bind(gen_eval_stmt(env, body), fn(control) raise {
        match control {
          Normal(_) => gen_eval_while_loop(env, label, cond_expr, body)
          Continue(signal_label, value_opt) => {
            let updated = update_empty(value_opt, Undefined)
            if label_matches(signal_label, label) {
              gen_eval_while_loop(env, label, cond_expr, body)
            } else {
              Done(Continue(signal_label, Some(updated)))
            }
          }
          Break(signal_label, value_opt) => {
            let updated = update_empty(value_opt, Undefined)
            if label_matches(signal_label, label) {
              Done(Normal(updated))
            } else {
              Done(Break(signal_label, Some(updated)))
            }
          }
          Return(value) => Done(Return(value))
          Throw(value) => Done(Throw(value))
        }
      })
    }
  })
}

///|
fn gen_eval_for(
  env : Env,
  label : String?,
  init : @engine.ForInit,
  test_expr : @engine.Expr?,
  update_expr : @engine.Expr?,
  body : @engine.Stmt,
) -> GenStep raise {
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
  if use_loop_env {
    hoist_functions(loop_env, body, false, false)
  }
  let start = match init {
    @engine.ForInit::None => Done(Normal(Undefined))
    @engine.ForInit::Var(kind, decls) =>
      gen_eval_var_decls(loop_env, kind, decls, 0)
    @engine.ForInit::Expr(expr) =>
      gen_eval_expr_cont(loop_env, expr, fn(_) { Done(Normal(Undefined)) })
  }
  gen_bind(start, fn(control) raise {
    match control {
      Normal(_) => {
        let iter_env = if use_loop_env {
          create_for_iteration_env(
            loop_env, loop_env, loop_names, loop_is_const,
          )
        } else {
          loop_env
        }
        gen_eval_for_loop(
          loop_env, label, test_expr, update_expr, body, use_loop_env, loop_names,
          loop_is_const, iter_env,
        )
      }
      _ => Done(control)
    }
  })
}

///|
fn gen_eval_for_loop(
  loop_env : Env,
  label : String?,
  test_expr : @engine.Expr?,
  update_expr : @engine.Expr?,
  body : @engine.Stmt,
  use_loop_env : Bool,
  loop_names : Array[String],
  loop_is_const : Bool,
  iter_env : Env,
) -> GenStep raise {
  let current_env = if use_loop_env { iter_env } else { loop_env }
  let test_step = match test_expr {
    Some(expr) =>
      gen_eval_expr_cont(current_env, expr, fn(value) { Done(Normal(value)) })
    None => Done(Normal(Bool(true)))
  }
  gen_bind(test_step, fn(control) {
    match control {
      Normal(value) =>
        if !is_truthy(value) {
          Done(Normal(Undefined))
        } else {
          gen_bind(gen_eval_stmt(current_env, body), fn(body_control) {
            match body_control {
              Normal(_) => {
                let next_iter_env = if use_loop_env {
                  create_for_iteration_env(
                    current_env, loop_env, loop_names, loop_is_const,
                  )
                } else {
                  loop_env
                }
                gen_eval_for_update(
                  loop_env, next_iter_env, label, test_expr, update_expr, body, use_loop_env,
                  loop_names, loop_is_const,
                )
              }
              Continue(signal_label, value_opt) => {
                let updated = update_empty(value_opt, Undefined)
                if label_matches(signal_label, label) {
                  let next_iter_env = if use_loop_env {
                    create_for_iteration_env(
                      current_env, loop_env, loop_names, loop_is_const,
                    )
                  } else {
                    loop_env
                  }
                  gen_eval_for_update(
                    loop_env, next_iter_env, label, test_expr, update_expr, body,
                    use_loop_env, loop_names, loop_is_const,
                  )
                } else {
                  Done(Continue(signal_label, Some(updated)))
                }
              }
              Break(signal_label, value_opt) => {
                let updated = update_empty(value_opt, Undefined)
                if label_matches(signal_label, label) {
                  Done(Normal(updated))
                } else {
                  Done(Break(signal_label, Some(updated)))
                }
              }
              Return(value) => Done(Return(value))
              Throw(value) => Done(Throw(value))
            }
          })
        }
      _ => Done(control)
    }
  })
}

///|
fn gen_eval_for_in(
  env : Env,
  label : String?,
  init : @engine.ForInit,
  expr : @engine.Expr,
  body : @engine.Stmt,
) -> GenStep raise {
  let start = match init {
    @engine.ForInit::None => Done(Normal(Undefined))
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
        Done(Normal(Undefined))
      } else {
        gen_eval_var_decls(env, kind, decls, 0)
      }
    }
    @engine.ForInit::Expr(_) => Done(Normal(Undefined))
  }
  gen_bind(start, fn(control) {
    match control {
      Normal(_) =>
        gen_eval_expr_cont(env, expr, fn(value) raise {
          gen_with_root_frame(fn() raise {
            let target = to_object(value)
            let _ = gen_root_push(target)
            let keys = enumerable_string_keys_in_chain(target)
            gen_eval_for_in_loop(env, label, init, target, keys, 0, body)
          })
        })
      _ => Done(control)
    }
  })
}

///|
fn gen_iterator_close(iterator : Value, control : GenControl) -> GenStep raise {
  try {
    match control {
      Throw(_) => iterator_close_on_error(iterator)
      _ => iterator_close(iterator)
    }
    Done(control)
  } catch {
    ThrowSignal(value) => Done(Throw(value))
    err => raise err
  }
}

///|
fn gen_async_iterator_close(
  iterator : Value,
  control : GenControl,
) -> GenStep raise {
  try {
    match control {
      Throw(_) => async_iterator_close_on_error(iterator)
      _ => async_iterator_close(iterator)
    }
    Done(control)
  } catch {
    ThrowSignal(value) => Done(Throw(value))
    err => raise err
  }
}

///|
fn gen_async_iterator_step_value(
  iterator : Value,
  next_method : Value,
  k : (Bool, Value) -> GenStep raise,
) -> GenStep raise {
  let result = call_value_with_this(next_method, [], iterator) catch {
    ThrowSignal(value) => return Done(Throw(value))
    err => raise err
  }
  gen_await_value(result, fn(awaited) raise {
    if !is_object_like(awaited) {
      return Done(Throw(type_error_value("iterator must return an object")))
    }
    try {
      let done = is_truthy(property_get(awaited, "done"))
      let value = property_get(awaited, "value")
      k(done, value)
    } catch {
      ThrowSignal(value) => Done(Throw(value))
      err => raise err
    }
  })
}

///|
fn gen_eval_for_of(
  env : Env,
  label : String?,
  init : @engine.ForInit,
  expr : @engine.Expr,
  body : @engine.Stmt,
) -> GenStep raise {
  let start = match init {
    @engine.ForInit::None => Done(Normal(Undefined))
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
        Done(Normal(Undefined))
      } else {
        gen_eval_var_decls(env, kind, decls, 0)
      }
    }
    @engine.ForInit::Expr(_) => Done(Normal(Undefined))
  }
  gen_bind(start, fn(control) raise {
    match control {
      Normal(_) =>
        gen_eval_expr_cont(env, expr, fn(value) raise {
          gen_with_root_frame(fn() raise {
            let (iterator, next_method) = get_iterator_from_value(value)
            let _ = gen_root_push(iterator)
            let _ = gen_root_push(next_method)
            gen_eval_for_of_loop(env, label, init, iterator, next_method, body)
          })
        })
      _ => Done(control)
    }
  })
}

///|
fn gen_eval_for_await_of(
  env : Env,
  label : String?,
  init : @engine.ForInit,
  expr : @engine.Expr,
  body : @engine.Stmt,
) -> GenStep raise {
  let start = match init {
    @engine.ForInit::None => Done(Normal(Undefined))
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
        Done(Normal(Undefined))
      } else {
        gen_eval_var_decls(env, kind, decls, 0)
      }
    }
    @engine.ForInit::Expr(_) => Done(Normal(Undefined))
  }
  gen_bind(start, fn(control) raise {
    match control {
      Normal(_) =>
        gen_eval_expr_cont(env, expr, fn(value) raise {
          gen_with_root_frame(fn() raise {
            let (iterator, next_method) = get_async_iterator_from_value(value)
            let _ = gen_root_push(iterator)
            let _ = gen_root_push(next_method)
            gen_eval_for_await_of_loop(
              env, label, init, iterator, next_method, body,
            )
          })
        })
      _ => Done(control)
    }
  })
}

///|
fn gen_eval_for_of_loop(
  env : Env,
  label : String?,
  init : @engine.ForInit,
  iterator : Value,
  next_method : Value,
  body : @engine.Stmt,
) -> GenStep raise {
  let (done, value) = iterator_step_value(iterator, next_method) catch {
    ThrowSignal(value) => return Done(Throw(value))
    err => raise err
  }
  if done {
    return Done(Normal(Undefined))
  }
  gen_bind(
    gen_assign_for_in_target(env, init, value, fn() { Done(Normal(Undefined)) }),
    fn(control) {
      match control {
        Normal(_) =>
          gen_bind(gen_eval_stmt(env, body), fn(control) {
            match control {
              Normal(_) =>
                gen_eval_for_of_loop(
                  env, label, init, iterator, next_method, body,
                )
              Continue(signal_label, value_opt) => {
                let updated = update_empty(value_opt, Undefined)
                if label_matches(signal_label, label) {
                  gen_eval_for_of_loop(
                    env, label, init, iterator, next_method, body,
                  )
                } else {
                  gen_iterator_close(
                    iterator,
                    Continue(signal_label, Some(updated)),
                  )
                }
              }
              Break(signal_label, value_opt) => {
                let updated = update_empty(value_opt, Undefined)
                if label_matches(signal_label, label) {
                  gen_iterator_close(iterator, Normal(updated))
                } else {
                  gen_iterator_close(
                    iterator,
                    Break(signal_label, Some(updated)),
                  )
                }
              }
              Return(value) => gen_iterator_close(iterator, Return(value))
              Throw(value) => gen_iterator_close(iterator, Throw(value))
            }
          })
        Return(value) => gen_iterator_close(iterator, Return(value))
        Break(label_name, value_opt) =>
          gen_iterator_close(iterator, Break(label_name, value_opt))
        Continue(label_name, value_opt) =>
          gen_iterator_close(iterator, Continue(label_name, value_opt))
        Throw(value) => gen_iterator_close(iterator, Throw(value))
      }
    },
  )
}

///|
fn gen_eval_for_await_of_loop(
  env : Env,
  label : String?,
  init : @engine.ForInit,
  iterator : Value,
  next_method : Value,
  body : @engine.Stmt,
) -> GenStep raise {
  gen_async_iterator_step_value(iterator, next_method, fn(done, value) raise {
    if done {
      return Done(Normal(Undefined))
    }
    gen_bind(
      gen_assign_for_in_target(env, init, value, fn() {
        Done(Normal(Undefined))
      }),
      fn(control) {
        match control {
          Normal(_) =>
            gen_bind(gen_eval_stmt(env, body), fn(control) {
              match control {
                Normal(_) =>
                  gen_eval_for_await_of_loop(
                    env, label, init, iterator, next_method, body,
                  )
                Continue(signal_label, value_opt) => {
                  let updated = update_empty(value_opt, Undefined)
                  if label_matches(signal_label, label) {
                    gen_eval_for_await_of_loop(
                      env, label, init, iterator, next_method, body,
                    )
                  } else {
                    gen_async_iterator_close(
                      iterator,
                      Continue(signal_label, Some(updated)),
                    )
                  }
                }
                Break(signal_label, value_opt) => {
                  let updated = update_empty(value_opt, Undefined)
                  if label_matches(signal_label, label) {
                    gen_async_iterator_close(iterator, Normal(updated))
                  } else {
                    gen_async_iterator_close(
                      iterator,
                      Break(signal_label, Some(updated)),
                    )
                  }
                }
                Return(value) =>
                  gen_async_iterator_close(iterator, Return(value))
                Throw(value) => gen_async_iterator_close(iterator, Throw(value))
              }
            })
          Return(value) => gen_async_iterator_close(iterator, Return(value))
          Break(label_name, value_opt) =>
            gen_async_iterator_close(iterator, Break(label_name, value_opt))
          Continue(label_name, value_opt) =>
            gen_async_iterator_close(iterator, Continue(label_name, value_opt))
          Throw(value) => gen_async_iterator_close(iterator, Throw(value))
        }
      },
    )
  })
}

///|
fn gen_eval_for_in_loop(
  env : Env,
  label : String?,
  init : @engine.ForInit,
  target : Value,
  keys : Array[String],
  index : Int,
  body : @engine.Stmt,
) -> GenStep raise {
  if index >= keys.length() {
    return Done(Normal(Undefined))
  }
  if !is_enumerable_key_in_chain(target, keys[index]) {
    return gen_eval_for_in_loop(env, label, init, target, keys, index + 1, body)
  }
  gen_bind(
    gen_assign_for_in_target(env, init, String(keys[index]), fn() {
      Done(Normal(Undefined))
    }),
    fn(control) {
      match control {
        Normal(_) =>
          gen_bind(gen_eval_stmt(env, body), fn(control) {
            match control {
              Normal(_) =>
                gen_eval_for_in_loop(
                  env,
                  label,
                  init,
                  target,
                  keys,
                  index + 1,
                  body,
                )
              Continue(signal_label, value_opt) => {
                let updated = update_empty(value_opt, Undefined)
                if label_matches(signal_label, label) {
                  gen_eval_for_in_loop(
                    env,
                    label,
                    init,
                    target,
                    keys,
                    index + 1,
                    body,
                  )
                } else {
                  Done(Continue(signal_label, Some(updated)))
                }
              }
              Break(signal_label, value_opt) => {
                let updated = update_empty(value_opt, Undefined)
                if label_matches(signal_label, label) {
                  Done(Normal(updated))
                } else {
                  Done(Break(signal_label, Some(updated)))
                }
              }
              Return(value) => Done(Return(value))
              Throw(value) => Done(Throw(value))
            }
          })
        Return(value) => Done(Return(value))
        Break(label_name, value_opt) => Done(Break(label_name, value_opt))
        Continue(label_name, value_opt) => Done(Continue(label_name, value_opt))
        Throw(value) => Done(Throw(value))
      }
    },
  )
}

///|
fn gen_eval_for_update(
  loop_env : Env,
  iter_env : Env,
  label : String?,
  test_expr : @engine.Expr?,
  update_expr : @engine.Expr?,
  body : @engine.Stmt,
  use_loop_env : Bool,
  loop_names : Array[String],
  loop_is_const : Bool,
) -> GenStep raise {
  let update_step = match update_expr {
    Some(expr) =>
      gen_eval_expr_cont(iter_env, expr, fn(_) { Done(Normal(Undefined)) })
    None => Done(Normal(Undefined))
  }
  gen_bind(update_step, fn(control) raise {
    match control {
      Normal(_) =>
        gen_eval_for_loop(
          loop_env, label, test_expr, update_expr, body, use_loop_env, loop_names,
          loop_is_const, iter_env,
        )
      _ => Done(control)
    }
  })
}

///|
fn gen_eval_do_while(
  env : Env,
  label : String?,
  body : @engine.Stmt,
  cond_expr : @engine.Expr,
) -> GenStep raise {
  gen_bind(gen_eval_stmt(env, body), fn(control) raise {
    match control {
      Normal(_) =>
        gen_eval_expr_cont(env, cond_expr, fn(cond) raise {
          if is_truthy(cond) {
            gen_eval_do_while(env, label, body, cond_expr)
          } else {
            Done(Normal(Undefined))
          }
        })
      Continue(signal_label, value_opt) => {
        let updated = update_empty(value_opt, Undefined)
        if label_matches(signal_label, label) {
          gen_eval_expr_cont(env, cond_expr, fn(cond) raise {
            if is_truthy(cond) {
              gen_eval_do_while(env, label, body, cond_expr)
            } else {
              Done(Normal(Undefined))
            }
          })
        } else {
          Done(Continue(signal_label, Some(updated)))
        }
      }
      Break(signal_label, value_opt) => {
        let updated = update_empty(value_opt, Undefined)
        if label_matches(signal_label, label) {
          Done(Normal(updated))
        } else {
          Done(Break(signal_label, Some(updated)))
        }
      }
      Return(value) => Done(Return(value))
      Throw(value) => Done(Throw(value))
    }
  })
}
