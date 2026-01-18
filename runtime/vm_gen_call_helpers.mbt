///|
fn gen_eval_call(
  env : Env,
  callee : @engine.Expr,
  args : Array[@engine.Expr],
  k : (Value) -> GenStep raise,
) -> GenStep raise {
  gen_with_root_frame(fn() raise {
    match unwrap_paren(callee) {
      @engine.Expr::Ident(name, offset) => {
        let (callee_value, this_value) = with_expr_pos_call_ref(offset, fn() raise {
          env_get_call_reference(env, name)
        })
        let _ = gen_root_push(callee_value)
        let _ = gen_root_push(this_value)
        gen_eval_args(env, args, 0, [], fn(argv) raise {
          if name == "eval" {
            match callee_value {
              Builtin(builtin) if builtin.kind == BuiltinFunction::Eval =>
                match function_realm_env(callee_value) {
                  Some(realm_env) =>
                    if realm_env_from_env(realm_env).id ==
                      realm_env_from_env(env).id {
                      return k(eval_with_env(env, argv))
                    }
                  None => ()
                }
              _ => ()
            }
          }
          k(call_value_with_this(callee_value, argv, this_value))
        })
      }
      @engine.Expr::Member(obj_expr, key, _) =>
        gen_eval_expr_cont(env, obj_expr, fn(target_value) raise {
          let _ = gen_root_push(target_value)
          match key {
            @engine.MemberKey::Private(name) => {
              let key_name = env_private_key_checked(env, name)
              let callee_value = private_property_get(
                target_value, key_name, name,
              )
              let _ = gen_root_push(callee_value)
              gen_eval_args(env, args, 0, [], fn(argv) raise {
                k(call_value_with_this(callee_value, argv, target_value))
              })
            }
            _ =>
              gen_eval_member_key(env, key, fn(name) raise {
                let target_obj = to_object(target_value)
                let callee_value = property_get_with_receiver(
                  target_obj, name, target_value,
                )
                let _ = gen_root_push(callee_value)
                gen_eval_args(env, args, 0, [], fn(argv) raise {
                  k(call_value_with_this(callee_value, argv, target_value))
                })
              })
          }
        })
      @engine.Expr::OptionalChain(base, segments) =>
        gen_eval_optional_chain_with_this(env, base, segments, fn(
          callee_value,
          this_value,
        ) raise {
          let _ = gen_root_push(callee_value)
          match this_value {
            Some(value) => {
              let _ = gen_root_push(value)

            }
            None => ()
          }
          gen_eval_args(env, args, 0, [], fn(argv) raise {
            let result = match this_value {
              Some(this_obj) =>
                call_value_with_this(callee_value, argv, this_obj)
              None => call_value(callee_value, argv)
            }
            k(result)
          })
        })
      _ =>
        gen_eval_expr_cont(env, callee, fn(callee_value) raise {
          let _ = gen_root_push(callee_value)
          gen_eval_args(env, args, 0, [], fn(argv) raise {
            k(call_value(callee_value, argv))
          })
        })
    }
  })
}

///|
fn gen_eval_args(
  env : Env,
  args : Array[@engine.Expr],
  index : Int,
  values : Array[Value],
  k : (Array[Value]) -> GenStep raise,
) -> GenStep raise {
  if index >= args.length() {
    return k(values)
  }
  let expr = args[index]
  match expr {
    @engine.Expr::Spread(inner) =>
      gen_eval_expr_cont(env, inner, fn(value) raise {
        let _ = gen_root_push(value)
        let list = build_spread_list(value)
        for item in list {
          values.push(item)
          let _ = gen_root_push(item)

        }
        gen_eval_args(env, args, index + 1, values, k)
      })
    _ =>
      gen_eval_expr_cont(env, expr, fn(value) raise {
        values.push(value)
        let _ = gen_root_push(value)
        gen_eval_args(env, args, index + 1, values, k)
      })
  }
}

///|
fn gen_eval_member_key(
  env : Env,
  key : @engine.MemberKey,
  k : (String) -> GenStep raise,
) -> GenStep raise {
  match key {
    @engine.MemberKey::Ident(name) => k(name)
    @engine.MemberKey::Private(name) => k(env_private_key_checked(env, name))
    @engine.MemberKey::Computed(expr) =>
      gen_eval_expr_cont(env, expr, fn(value) raise {
        let _ = gen_root_push(value)
        k(property_key_name(value))
      })
  }
}
