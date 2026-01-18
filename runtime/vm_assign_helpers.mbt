///|
fn eval_assign_op(
  env : Env,
  op : @engine.BinaryOp,
  lhs : @engine.Expr,
  rhs : @engine.Expr,
) -> Value raise {
  with_gc_frame(fn() raise {
    let (target, is_cover) = unwrap_paren_with_cover(lhs)
    match target {
      Ident(name, ident_offset) => {
        let target = resolve_binding_target(env, name)
        let current = with_expr_pos(ident_offset, fn() raise {
          get_binding_value_from_target(env, name, target, env.strict)
        })
        let _ = gc_root_push(current)
        match op {
          @engine.BinaryOp::LogicalAnd =>
            if is_truthy(current) {
              let value = eval_expr(env, rhs)
              let _ = gc_root_push(value)
              if is_anonymous_function_definition(rhs) && !is_cover {
                set_anonymous_function_name(value, name, None)
              }
              with_expr_pos(ident_offset, fn() raise {
                assign_binding_value_to_target(
                  env,
                  name,
                  target,
                  value,
                  env.strict,
                )
                value
              })
            } else {
              current
            }
          @engine.BinaryOp::LogicalOr =>
            if is_truthy(current) {
              current
            } else {
              let value = eval_expr(env, rhs)
              let _ = gc_root_push(value)
              if is_anonymous_function_definition(rhs) && !is_cover {
                set_anonymous_function_name(value, name, None)
              }
              with_expr_pos(ident_offset, fn() raise {
                assign_binding_value_to_target(
                  env,
                  name,
                  target,
                  value,
                  env.strict,
                )
                value
              })
            }
          @engine.BinaryOp::Coalesce =>
            if is_nullish(current) {
              let value = eval_expr(env, rhs)
              let _ = gc_root_push(value)
              if is_anonymous_function_definition(rhs) && !is_cover {
                set_anonymous_function_name(value, name, None)
              }
              with_expr_pos(ident_offset, fn() raise {
                assign_binding_value_to_target(
                  env,
                  name,
                  target,
                  value,
                  env.strict,
                )
                value
              })
            } else {
              current
            }
          _ => {
            let value = eval_expr(env, rhs)
            let _ = gc_root_push(value)
            let result = eval_binary(op, current, value)
            let _ = gc_root_push(result)
            with_expr_pos(ident_offset, fn() raise {
              assign_binding_value_to_target(
                env,
                name,
                target,
                result,
                env.strict,
              )
              result
            })
          }
        }
      }
      Member(obj_expr, key, member_offset) =>
        match obj_expr {
          @engine.Expr::Super =>
            match key {
              @engine.MemberKey::Private(_) =>
                throw_syntax_error("private class field forbidden after super")
              @engine.MemberKey::Computed(expr) => {
                let (base, receiver) = super_base_and_receiver(env)
                let key_value = eval_expr(env, expr)
                let _ = gc_root_push(base)
                let _ = gc_root_push(receiver)
                let _ = gc_root_push(key_value)
                let name = property_key_name(key_value)
                let base_obj = with_expr_pos(member_offset, fn() raise {
                  to_object(base)
                })
                let _ = gc_root_push(base_obj)
                let current = with_expr_pos(member_offset, fn() raise {
                  property_get_with_receiver(base_obj, name, receiver)
                })
                let _ = gc_root_push(current)
                match op {
                  @engine.BinaryOp::LogicalAnd =>
                    if is_truthy(current) {
                      let value = eval_expr(env, rhs)
                      let _ = gc_root_push(value)
                      with_expr_pos(member_offset, fn() raise {
                        super_set_property(env, base, receiver, name, value)
                        value
                      })
                    } else {
                      current
                    }
                  @engine.BinaryOp::LogicalOr =>
                    if is_truthy(current) {
                      current
                    } else {
                      let value = eval_expr(env, rhs)
                      let _ = gc_root_push(value)
                      with_expr_pos(member_offset, fn() raise {
                        super_set_property(env, base, receiver, name, value)
                        value
                      })
                    }
                  @engine.BinaryOp::Coalesce =>
                    if is_nullish(current) {
                      let value = eval_expr(env, rhs)
                      let _ = gc_root_push(value)
                      with_expr_pos(member_offset, fn() raise {
                        super_set_property(env, base, receiver, name, value)
                        value
                      })
                    } else {
                      current
                    }
                  _ => {
                    let value = eval_expr(env, rhs)
                    let _ = gc_root_push(value)
                    let result = eval_binary(op, current, value)
                    let _ = gc_root_push(result)
                    with_expr_pos(member_offset, fn() raise {
                      super_set_property(env, base, receiver, name, result)
                      result
                    })
                  }
                }
              }
              _ => {
                let (base, receiver) = super_base_and_receiver(env)
                let name = member_key_name(env, key)
                let _ = gc_root_push(base)
                let _ = gc_root_push(receiver)
                let base_obj = with_expr_pos(member_offset, fn() raise {
                  to_object(base)
                })
                let _ = gc_root_push(base_obj)
                let current = with_expr_pos(member_offset, fn() raise {
                  property_get_with_receiver(base_obj, name, receiver)
                })
                let _ = gc_root_push(current)
                match op {
                  @engine.BinaryOp::LogicalAnd =>
                    if is_truthy(current) {
                      let value = eval_expr(env, rhs)
                      let _ = gc_root_push(value)
                      with_expr_pos(member_offset, fn() raise {
                        super_set_property(env, base, receiver, name, value)
                        value
                      })
                    } else {
                      current
                    }
                  @engine.BinaryOp::LogicalOr =>
                    if is_truthy(current) {
                      current
                    } else {
                      let value = eval_expr(env, rhs)
                      let _ = gc_root_push(value)
                      with_expr_pos(member_offset, fn() raise {
                        super_set_property(env, base, receiver, name, value)
                        value
                      })
                    }
                  @engine.BinaryOp::Coalesce =>
                    if is_nullish(current) {
                      let value = eval_expr(env, rhs)
                      let _ = gc_root_push(value)
                      with_expr_pos(member_offset, fn() raise {
                        super_set_property(env, base, receiver, name, value)
                        value
                      })
                    } else {
                      current
                    }
                  _ => {
                    let value = eval_expr(env, rhs)
                    let _ = gc_root_push(value)
                    let result = eval_binary(op, current, value)
                    let _ = gc_root_push(result)
                    with_expr_pos(member_offset, fn() raise {
                      super_set_property(env, base, receiver, name, result)
                      result
                    })
                  }
                }
              }
            }
          _ =>
            match key {
              @engine.MemberKey::Private(name) => {
                let target = with_expr_pos(member_offset, fn() raise {
                  eval_expr(env, obj_expr)
                })
                let key_name = env_private_key_checked(env, name)
                let _ = gc_root_push(target)
                let current = with_expr_pos(member_offset, fn() raise {
                  private_property_get(target, key_name, name)
                })
                let _ = gc_root_push(current)
                match op {
                  @engine.BinaryOp::LogicalAnd =>
                    if is_truthy(current) {
                      let value = eval_expr(env, rhs)
                      let _ = gc_root_push(value)
                      with_expr_pos(member_offset, fn() raise {
                        private_property_set(target, key_name, name, value)
                        value
                      })
                    } else {
                      current
                    }
                  @engine.BinaryOp::LogicalOr =>
                    if is_truthy(current) {
                      current
                    } else {
                      let value = eval_expr(env, rhs)
                      let _ = gc_root_push(value)
                      with_expr_pos(member_offset, fn() raise {
                        private_property_set(target, key_name, name, value)
                        value
                      })
                    }
                  @engine.BinaryOp::Coalesce =>
                    if is_nullish(current) {
                      let value = eval_expr(env, rhs)
                      let _ = gc_root_push(value)
                      with_expr_pos(member_offset, fn() raise {
                        private_property_set(target, key_name, name, value)
                        value
                      })
                    } else {
                      current
                    }
                  _ => {
                    let value = eval_expr(env, rhs)
                    let _ = gc_root_push(value)
                    let result = eval_binary(op, current, value)
                    let _ = gc_root_push(result)
                    with_expr_pos(member_offset, fn() raise {
                      private_property_set(target, key_name, name, result)
                      result
                    })
                  }
                }
              }
              @engine.MemberKey::Computed(expr) => {
                let target_value = with_expr_pos(member_offset, fn() raise {
                  eval_expr(env, obj_expr)
                })
                let key_value = eval_expr(env, expr)
                let _ = gc_root_push(target_value)
                let _ = gc_root_push(key_value)
                let target_obj = with_expr_pos(member_offset, fn() raise {
                  to_object(target_value)
                })
                let _ = gc_root_push(target_obj)
                let name = property_key_name(key_value)
                let current = with_expr_pos(member_offset, fn() raise {
                  property_get_with_receiver(target_obj, name, target_value)
                })
                let _ = gc_root_push(current)
                match op {
                  @engine.BinaryOp::LogicalAnd =>
                    if is_truthy(current) {
                      let value = eval_expr(env, rhs)
                      let _ = gc_root_push(value)
                      with_expr_pos(member_offset, fn() raise {
                        let _ = set_property_for_value(
                          target_value,
                          name,
                          value,
                          env.strict,
                        )
                        value
                      })
                    } else {
                      current
                    }
                  @engine.BinaryOp::LogicalOr =>
                    if is_truthy(current) {
                      current
                    } else {
                      let value = eval_expr(env, rhs)
                      let _ = gc_root_push(value)
                      with_expr_pos(member_offset, fn() raise {
                        let _ = set_property_for_value(
                          target_value,
                          name,
                          value,
                          env.strict,
                        )
                        value
                      })
                    }
                  @engine.BinaryOp::Coalesce =>
                    if is_nullish(current) {
                      let value = eval_expr(env, rhs)
                      let _ = gc_root_push(value)
                      with_expr_pos(member_offset, fn() raise {
                        let _ = set_property_for_value(
                          target_value,
                          name,
                          value,
                          env.strict,
                        )
                        value
                      })
                    } else {
                      current
                    }
                  _ => {
                    let value = eval_expr(env, rhs)
                    let _ = gc_root_push(value)
                    let result = eval_binary(op, current, value)
                    let _ = gc_root_push(result)
                    with_expr_pos(member_offset, fn() raise {
                      let _ = set_property_for_value(
                        target_value,
                        name,
                        result,
                        env.strict,
                      )
                      result
                    })
                  }
                }
              }
              _ => {
                let target_value = with_expr_pos(member_offset, fn() raise {
                  eval_expr(env, obj_expr)
                })
                let name = member_key_name(env, key)
                let _ = gc_root_push(target_value)
                let current = with_expr_pos(member_offset, fn() raise {
                  let target_obj = to_object(target_value)
                  let _ = gc_root_push(target_obj)
                  property_get_with_receiver(target_obj, name, target_value)
                })
                let _ = gc_root_push(current)
                match op {
                  @engine.BinaryOp::LogicalAnd =>
                    if is_truthy(current) {
                      let value = eval_expr(env, rhs)
                      let _ = gc_root_push(value)
                      with_expr_pos(member_offset, fn() raise {
                        let _ = set_property_for_value(
                          target_value,
                          name,
                          value,
                          env.strict,
                        )
                        value
                      })
                    } else {
                      current
                    }
                  @engine.BinaryOp::LogicalOr =>
                    if is_truthy(current) {
                      current
                    } else {
                      let value = eval_expr(env, rhs)
                      let _ = gc_root_push(value)
                      with_expr_pos(member_offset, fn() raise {
                        let _ = set_property_for_value(
                          target_value,
                          name,
                          value,
                          env.strict,
                        )
                        value
                      })
                    }
                  @engine.BinaryOp::Coalesce =>
                    if is_nullish(current) {
                      let value = eval_expr(env, rhs)
                      let _ = gc_root_push(value)
                      with_expr_pos(member_offset, fn() raise {
                        let _ = set_property_for_value(
                          target_value,
                          name,
                          value,
                          env.strict,
                        )
                        value
                      })
                    } else {
                      current
                    }
                  _ => {
                    let value = eval_expr(env, rhs)
                    let _ = gc_root_push(value)
                    let result = eval_binary(op, current, value)
                    let _ = gc_root_push(result)
                    with_expr_pos(member_offset, fn() raise {
                      let _ = set_property_for_value(
                        target_value,
                        name,
                        result,
                        env.strict,
                      )
                      result
                    })
                  }
                }
              }
            }
        }
      _ =>
        match target {
          Call(_, _, _) =>
            if !env.strict {
              let _ = eval_expr(env, lhs)
              throw_reference_error("invalid assignment target")
            } else {
              fail("invalid assignment target")
            }
          _ => fail("invalid assignment target")
        }
    }
  })
}
