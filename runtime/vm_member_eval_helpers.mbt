///|
fn eval_member(
  env : Env,
  obj_expr : @engine.Expr,
  key : @engine.MemberKey,
) -> Value raise {
  with_gc_frame(fn() raise {
    match key {
      @engine.MemberKey::Private(name) => {
        let target = eval_expr(env, obj_expr)
        let _ = gc_root_push(target)
        let key_name = env_private_key_checked(env, name)
        private_property_get(target, key_name, name)
      }
      _ => {
        let target_value = eval_expr(env, obj_expr)
        let _ = gc_root_push(target_value)
        let target_obj = to_object(target_value)
        let name = member_key_name(env, key)
        property_get_with_receiver(target_obj, name, target_value)
      }
    }
  })
}

///|
fn eval_optional_chain_with_this(
  env : Env,
  base : @engine.Expr,
  segments : Array[@engine.ChainSegment],
) -> (Value, Value?) raise {
  fn eval_optional_chain_base_with_this(
    env : Env,
    base : @engine.Expr,
  ) -> (Value, Value?) raise {
    match unwrap_paren(base) {
      @engine.Expr::Member(obj_expr, key, offset) =>
        match obj_expr {
          Super => {
            let value = with_expr_pos(offset, fn() raise {
              eval_super_member(env, key)
            })
            (value, Some(env_get(env, "this")))
          }
          _ => {
            let target = eval_expr(env, obj_expr)
            let base_depth = gc_root_depth()
            let _ = gc_root_push(target)
            let value = with_expr_pos(offset, fn() raise {
              match key {
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
            })
            gc_root_pop_to(base_depth)
            (value, Some(target))
          }
        }
      @engine.Expr::OptionalChain(inner_base, inner_segments) =>
        eval_optional_chain_with_this(env, inner_base, inner_segments)
      _ => (eval_expr(env, base), None)
    }
  }

  with_gc_frame_pair(fn() raise {
    let (base_value, base_this) = eval_optional_chain_base_with_this(env, base)
    let mut current = base_value
    let mut this_value : Value? = base_this
    for segment in segments {
      let base_depth = gc_root_depth()
      let _ = gc_root_push(current)
      match this_value {
        Some(this_obj) => {
          let _ = gc_root_push(this_obj)

        }
        None => ()
      }
      match segment {
        @engine.ChainSegment::Member(key, optional, offset) => {
          if optional && is_nullish(current) {
            gc_root_pop_to(base_depth)
            return (Undefined, None)
          }
          let next = with_expr_pos(offset, fn() raise {
            match key {
              @engine.MemberKey::Private(name) => {
                let key_name = env_private_key_checked(env, name)
                private_property_get(current, key_name, name)
              }
              _ => {
                let name = member_key_name(env, key)
                let target = to_object(current)
                property_get_with_receiver(target, name, current)
              }
            }
          })
          this_value = Some(current)
          current = next
        }
        @engine.ChainSegment::Call(args, optional, offset) => {
          if optional && is_nullish(current) {
            gc_root_pop_to(base_depth)
            return (Undefined, None)
          }
          let argv = eval_args(env, args)
          for value in argv {
            let _ = gc_root_push(value)

          }
          let result = with_expr_pos(offset, fn() raise {
            match this_value {
              Some(this_obj) => call_value_with_this(current, argv, this_obj)
              None => call_value(current, argv)
            }
          })
          current = result
          this_value = None
        }
      }
      gc_root_pop_to(base_depth)
    }
    (current, this_value)
  })
}

///|
fn eval_optional_chain(
  env : Env,
  base : @engine.Expr,
  segments : Array[@engine.ChainSegment],
) -> Value raise {
  let (value, _) = eval_optional_chain_with_this(env, base, segments)
  value
}

///|

///|
