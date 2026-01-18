///|
fn unwrap_paren(expr : @engine.Expr) -> @engine.Expr {
  match expr {
    Paren(inner) => unwrap_paren(inner)
    _ => expr
  }
}

///|
fn unwrap_paren_with_cover(expr : @engine.Expr) -> (@engine.Expr, Bool) {
  let mut target = expr
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
  (target, is_cover)
}

///|
fn eval_call(
  env : Env,
  callee : @engine.Expr,
  args : Array[@engine.Expr],
) -> Value raise {
  with_gc_frame(fn() raise {
    match unwrap_paren(callee) {
      @engine.Expr::Ident(name, offset) => {
        let (callee_value, this_value) = with_expr_pos_call_ref(offset, fn() raise {
          env_get_call_reference(env, name)
        })
        let _ = gc_root_push(callee_value)
        let _ = gc_root_push(this_value)
        let argv = if name == "__export" {
          eval_export_args(env, args)
        } else {
          eval_args(env, args)
        }
        for value in argv {
          let _ = gc_root_push(value)

        }
        if name == "eval" {
          match callee_value {
            Builtin(builtin) if builtin.kind == BuiltinFunction::Eval =>
              match function_realm_env(callee_value) {
                Some(realm_env) =>
                  if realm_env_from_env(realm_env).id ==
                    realm_env_from_env(env).id {
                    return eval_with_env(env, argv)
                  }
                None => ()
              }
            _ => ()
          }
        }
        call_value_with_this(callee_value, argv, this_value)
      }
      @engine.Expr::Member(obj_expr, key, _) => {
        let target = eval_expr(env, obj_expr)
        let _ = gc_root_push(target)
        let callee_value = match key {
          @engine.MemberKey::Private(name) => {
            let key_name = env_private_key_checked(env, name)
            private_property_get(target, key_name, name)
          }
          _ => {
            let name = member_key_name(env, key)
            let target_obj = to_object(target)
            property_get_with_receiver(target_obj, name, target)
          }
        }
        let _ = gc_root_push(callee_value)
        let argv = eval_args(env, args)
        for value in argv {
          let _ = gc_root_push(value)

        }
        call_value_with_this(callee_value, argv, target)
      }
      @engine.Expr::OptionalChain(base, segments) => {
        let (callee_value, this_value) = eval_optional_chain_with_this(
          env, base, segments,
        )
        let _ = gc_root_push(callee_value)
        match this_value {
          Some(this_obj) => {
            let _ = gc_root_push(this_obj)

          }
          None => ()
        }
        let argv = eval_args(env, args)
        for value in argv {
          let _ = gc_root_push(value)

        }
        match this_value {
          Some(this_obj) => call_value_with_this(callee_value, argv, this_obj)
          None => call_value(callee_value, argv)
        }
      }
      other => {
        let callee_value = eval_expr(env, other)
        let _ = gc_root_push(callee_value)
        let argv = eval_args(env, args)
        for value in argv {
          let _ = gc_root_push(value)

        }
        call_value(callee_value, argv)
      }
    }
  })
}

///|
fn eval_export_args(
  env : Env,
  args : Array[@engine.Expr],
) -> Array[Value] raise {
  with_gc_frame_values(fn() raise {
    let values : Array[Value] = []
    if args.is_empty() {
      return values
    }
    let mut export_name : String? = None
    match args[0] {
      @engine.Expr::String(name, _, _) => export_name = Some(name)
      _ => ()
    }
    let first = eval_expr(env, args[0])
    values.push(first)
    let _ = gc_root_push(first)
    let mut i = 1
    while i < args.length() {
      let expr = args[i]
      match expr {
        @engine.Expr::Spread(inner) => {
          let spread_value = eval_expr(env, inner)
          let _ = gc_root_push(spread_value)
          for value in build_spread_list(spread_value) {
            values.push(value)
            let _ = gc_root_push(value)

          }
        }
        _ =>
          if i == 1 {
            match export_name {
              Some(name) => {
                let value = eval_named_expr(env, expr, name, None)
                values.push(value)
                let _ = gc_root_push(value)

              }
              None => {
                let value = eval_expr(env, expr)
                values.push(value)
                let _ = gc_root_push(value)

              }
            }
          } else {
            let value = eval_expr(env, expr)
            values.push(value)
            let _ = gc_root_push(value)

          }
      }
      i = i + 1
    }
    values
  })
}

///|
fn eval_args(env : Env, args : Array[@engine.Expr]) -> Array[Value] raise {
  with_gc_frame_values(fn() raise {
    let values : Array[Value] = []
    for expr in args {
      match expr {
        @engine.Expr::Spread(inner) => {
          let spread_value = eval_expr(env, inner)
          let _ = gc_root_push(spread_value)
          for value in build_spread_list(spread_value) {
            values.push(value)
            let _ = gc_root_push(value)

          }
        }
        _ => {
          let value = eval_expr(env, expr)
          values.push(value)
          let _ = gc_root_push(value)

        }
      }
    }
    values
  })
}
