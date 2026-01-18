///|
fn gen_eval_optional_chain(
  env : Env,
  base : @engine.Expr,
  segments : Array[@engine.ChainSegment],
  k : (Value) -> GenStep raise,
) -> GenStep raise {
  gen_eval_optional_chain_with_this(env, base, segments, fn(value, _) raise {
    k(value)
  })
}

///|
fn gen_eval_optional_chain_with_this(
  env : Env,
  base : @engine.Expr,
  segments : Array[@engine.ChainSegment],
  k : (Value, Value?) -> GenStep raise,
) -> GenStep raise {
  gen_with_root_frame(fn() raise {
    match unwrap_paren(base) {
      @engine.Expr::Member(obj_expr, key, offset) =>
        match obj_expr {
          Super => {
            let value = with_expr_pos(offset, fn() raise {
              eval_super_member(env, key)
            })
            gen_eval_optional_chain_step(
              env,
              segments,
              0,
              value,
              Some(env_get(env, "this")),
              k,
            )
          }
          _ =>
            gen_eval_expr_cont(env, obj_expr, fn(target) raise {
              let _ = gen_root_push(target)
              match key {
                @engine.MemberKey::Ident(name) => {
                  let next = with_expr_pos(offset, fn() raise {
                    let target_obj = to_object(target)
                    property_get_with_receiver(target_obj, name, target)
                  })
                  gen_eval_optional_chain_step(
                    env,
                    segments,
                    0,
                    next,
                    Some(target),
                    k,
                  )
                }
                @engine.MemberKey::Private(name) => {
                  let next = with_expr_pos(offset, fn() raise {
                    let key_name = env_private_key_checked(env, name)
                    private_property_get(target, key_name, name)
                  })
                  gen_eval_optional_chain_step(
                    env,
                    segments,
                    0,
                    next,
                    Some(target),
                    k,
                  )
                }
                @engine.MemberKey::Computed(expr) =>
                  gen_eval_expr_cont(env, expr, fn(value) raise {
                    let _ = gen_root_push(value)
                    let name = property_key_name(value)
                    let next = with_expr_pos(offset, fn() raise {
                      let target_obj = to_object(target)
                      property_get_with_receiver(target_obj, name, target)
                    })
                    gen_eval_optional_chain_step(
                      env,
                      segments,
                      0,
                      next,
                      Some(target),
                      k,
                    )
                  })
              }
            })
        }
      @engine.Expr::OptionalChain(inner_base, inner_segments) =>
        gen_eval_optional_chain_with_this(env, inner_base, inner_segments, fn(
          value,
          this_value,
        ) raise {
          gen_eval_optional_chain_step(env, segments, 0, value, this_value, k)
        })
      _ =>
        gen_eval_expr_cont(env, base, fn(value) raise {
          gen_eval_optional_chain_step(env, segments, 0, value, None, k)
        })
    }
  })
}

///|
fn gen_eval_optional_chain_step(
  env : Env,
  segments : Array[@engine.ChainSegment],
  index : Int,
  current : Value,
  this_value : Value?,
  k : (Value, Value?) -> GenStep raise,
) -> GenStep raise {
  if index >= segments.length() {
    return k(current, this_value)
  }
  let _ = gen_root_push(current)
  match this_value {
    Some(value) => {
      let _ = gen_root_push(value)

    }
    None => ()
  }
  match segments[index] {
    @engine.ChainSegment::Member(key, optional, offset) => {
      if optional && is_nullish(current) {
        return k(Undefined, None)
      }
      match key {
        @engine.MemberKey::Ident(name) => {
          let next = with_expr_pos(offset, fn() raise {
            let target = to_object(current)
            property_get_with_receiver(target, name, current)
          })
          gen_eval_optional_chain_step(
            env,
            segments,
            index + 1,
            next,
            Some(current),
            k,
          )
        }
        @engine.MemberKey::Private(name) => {
          let next = with_expr_pos(offset, fn() raise {
            let key_name = env_private_key_checked(env, name)
            private_property_get(current, key_name, name)
          })
          gen_eval_optional_chain_step(
            env,
            segments,
            index + 1,
            next,
            Some(current),
            k,
          )
        }
        @engine.MemberKey::Computed(expr) =>
          gen_eval_expr_cont(env, expr, fn(value) raise {
            let _ = gen_root_push(value)
            let name = property_key_name(value)
            let next = with_expr_pos(offset, fn() raise {
              let target = to_object(current)
              property_get_with_receiver(target, name, current)
            })
            gen_eval_optional_chain_step(
              env,
              segments,
              index + 1,
              next,
              Some(current),
              k,
            )
          })
      }
    }
    @engine.ChainSegment::Call(args, optional, offset) => {
      if optional && is_nullish(current) {
        return k(Undefined, None)
      }
      gen_eval_args(env, args, 0, [], fn(argv) raise {
        let result = with_expr_pos(offset, fn() raise {
          match this_value {
            Some(this_obj) => call_value_with_this(current, argv, this_obj)
            None => call_value(current, argv)
          }
        })
        gen_eval_optional_chain_step(env, segments, index + 1, result, None, k)
      })
    }
  }
}
