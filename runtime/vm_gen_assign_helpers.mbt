///|
fn gen_assign_binding_value(
  env : Env,
  binding : @engine.VarBinding,
  value : Value,
  assign : (String, Value) -> Unit raise,
  k : () -> GenStep raise,
) -> GenStep raise {
  match binding {
    @engine.VarBinding::Name(name) => {
      assign(name, value)
      k()
    }
    @engine.VarBinding::ArrayPattern(pattern) =>
      gen_destructure_array_pattern(env, pattern, value, assign, k)
    @engine.VarBinding::ObjectPattern(pattern) => {
      let obj = to_object(value)
      gen_destructure_object_pattern(env, pattern, obj, [], 0, assign, k)
    }
    @engine.VarBinding::Target(expr) =>
      gen_assign_for_in_expr(env, expr, value, k)
  }
}

///|
fn gen_prepare_assignment_target(
  env : Env,
  expr : @engine.Expr,
  k : ((Value) -> Unit raise) -> GenStep raise,
) -> GenStep raise {
  match expr {
    @engine.Expr::Ident(name, _) =>
      k(prepare_identifier_assignment_target(env, name, env.strict))
    @engine.Expr::Member(obj_expr, key, _) =>
      gen_prepare_member_assignment_target(env, obj_expr, key, k)
    @engine.Expr::Paren(inner) => gen_prepare_assignment_target(env, inner, k)
    _ => {
      let _ = throw_type_error("invalid assignment target")
      Done(Throw(Undefined))
    }
  }
}

///|
fn gen_prepare_member_assignment_target(
  env : Env,
  obj_expr : @engine.Expr,
  key : @engine.MemberKey,
  k : ((Value) -> Unit raise) -> GenStep raise,
) -> GenStep raise {
  match obj_expr {
    @engine.Expr::Super =>
      match key {
        @engine.MemberKey::Private(_) => {
          let _ = throw_syntax_error(
            "private class field forbidden after super",
          )
          Done(Throw(Undefined))
        }
        @engine.MemberKey::Computed(expr) => {
          let receiver = env_get(env, "this")
          let base = resolve_super_base()
          let _ = gen_root_push(receiver)
          let _ = gen_root_push(base)
          gen_eval_expr_cont(env, expr, fn(key_value) raise {
            let _ = gen_root_push(key_value)
            k(fn(value) raise {
              let name = property_key_name(key_value)
              super_set_property(env, base, receiver, name, value)
            })
          })
        }
        _ => {
          let receiver = env_get(env, "this")
          let base = resolve_super_base()
          let _ = gen_root_push(receiver)
          let _ = gen_root_push(base)
          gen_eval_member_key(env, key, fn(name) raise {
            k(fn(value) raise {
              super_set_property(env, base, receiver, name, value)
            })
          })
        }
      }
    _ =>
      match key {
        @engine.MemberKey::Private(name) =>
          gen_eval_expr_cont(env, obj_expr, fn(target) raise {
            let key_name = env_private_key_checked(env, name)
            let _ = gen_root_push(target)
            k(fn(value) raise {
              private_property_set(target, key_name, name, value)
            })
          })
        @engine.MemberKey::Computed(expr) =>
          gen_eval_expr_cont(env, obj_expr, fn(target) raise {
            let _ = gen_root_push(target)
            gen_eval_expr_cont(env, expr, fn(key_value) raise {
              let _ = gen_root_push(key_value)
              k(fn(value) raise {
                let name = property_key_name(key_value)
                let _ = set_property_for_value(target, name, value, env.strict)

              })
            })
          })
        _ =>
          gen_eval_expr_cont(env, obj_expr, fn(target) raise {
            let _ = gen_root_push(target)
            gen_eval_member_key(env, key, fn(name) raise {
              k(fn(value) raise {
                let _ = set_property_for_value(target, name, value, env.strict)

              })
            })
          })
      }
  }
}

///|
fn gen_apply_assign_target(
  assign_target : (Value) -> Unit raise,
  value : Value,
) -> GenStep raise {
  try {
    assign_target(value)
    Done(Normal(Undefined))
  } catch {
    ThrowSignal(err) => Done(Throw(err))
    err => raise err
  }
}

///|
fn gen_destructure_array_pattern_iter(
  env : Env,
  pattern : Array[@engine.ArrayPatternElem],
  iterator : Value,
  next_method : Value,
  index : Int,
  done : Bool,
  assign : (String, Value) -> Unit raise,
  k : () -> GenStep raise,
) -> GenStep raise {
  if index >= pattern.length() {
    if done {
      return k()
    }
    return gen_bind(gen_iterator_close(iterator, Normal(Undefined)), fn(
      control,
    ) raise {
      match control {
        Normal(_) => k()
        _ => Done(control)
      }
    })
  }
  match pattern[index] {
    @engine.ArrayPatternElem::Hole =>
      if done {
        gen_destructure_array_pattern_iter(
          env,
          pattern,
          iterator,
          next_method,
          index + 1,
          done,
          assign,
          k,
        )
      } else {
        let (next_done, _) = iterator_step_value(iterator, next_method) catch {
          ThrowSignal(value) => return Done(Throw(value))
          err => raise err
        }
        gen_destructure_array_pattern_iter(
          env,
          pattern,
          iterator,
          next_method,
          index + 1,
          next_done,
          assign,
          k,
        )
      }
    @engine.ArrayPatternElem::Bind(binding, default) =>
      match binding {
        @engine.VarBinding::Target(expr) => {
          let mut target_assign : ((Value) -> Unit raise)? = None
          let prep_step = gen_prepare_assignment_target(env, expr, fn(
            assigner,
          ) {
            target_assign = Some(assigner)
            Done(Normal(Undefined))
          })
          gen_bind(prep_step, fn(control) {
            match control {
              Normal(_) => {
                let assigner = match target_assign {
                  Some(value) => value
                  None => fn(_) { () }
                }
                let mut element = Undefined
                let next_done = if done {
                  true
                } else {
                  let (is_done, item) = iterator_step_value(
                    iterator, next_method,
                  ) catch {
                    ThrowSignal(value) => return Done(Throw(value))
                    err => raise err
                  }
                  if !is_done {
                    element = item
                  }
                  is_done
                }
                let assign_step = match default {
                  Some(expr) =>
                    if element is Undefined {
                      gen_eval_expr_cont(env, expr, fn(def_value) {
                        gen_apply_assign_target(assigner, def_value)
                      })
                    } else {
                      gen_apply_assign_target(assigner, element)
                    }
                  None => gen_apply_assign_target(assigner, element)
                }
                gen_bind(assign_step, fn(control) {
                  match control {
                    Normal(_) =>
                      gen_destructure_array_pattern_iter(
                        env,
                        pattern,
                        iterator,
                        next_method,
                        index + 1,
                        next_done,
                        assign,
                        k,
                      )
                    _ =>
                      if next_done {
                        Done(control)
                      } else {
                        gen_iterator_close(iterator, control)
                      }
                  }
                })
              }
              _ =>
                if done {
                  Done(control)
                } else {
                  gen_iterator_close(iterator, control)
                }
            }
          })
        }
        _ => {
          let mut element = Undefined
          let next_done = if done {
            true
          } else {
            let (is_done, item) = iterator_step_value(iterator, next_method) catch {
              ThrowSignal(value) => return Done(Throw(value))
              err => raise err
            }
            if !is_done {
              element = item
            }
            is_done
          }
          let assign_step = match default {
            Some(expr) =>
              if element is Undefined {
                match binding {
                  @engine.VarBinding::Name(name) =>
                    gen_eval_named_expr_cont(env, expr, name, None, fn(
                      def_value,
                    ) {
                      gen_assign_binding_value(
                        env,
                        binding,
                        def_value,
                        assign,
                        fn() { Done(Normal(Undefined)) },
                      )
                    })
                  _ =>
                    gen_eval_expr_cont(env, expr, fn(def_value) {
                      gen_assign_binding_value(
                        env,
                        binding,
                        def_value,
                        assign,
                        fn() { Done(Normal(Undefined)) },
                      )
                    })
                }
              } else {
                gen_assign_binding_value(env, binding, element, assign, fn() {
                  Done(Normal(Undefined))
                })
              }
            None =>
              gen_assign_binding_value(env, binding, element, assign, fn() {
                Done(Normal(Undefined))
              })
          }
          gen_bind(assign_step, fn(control) {
            match control {
              Normal(_) =>
                gen_destructure_array_pattern_iter(
                  env,
                  pattern,
                  iterator,
                  next_method,
                  index + 1,
                  next_done,
                  assign,
                  k,
                )
              _ =>
                if next_done {
                  Done(control)
                } else {
                  gen_iterator_close(iterator, control)
                }
            }
          })
        }
      }
    @engine.ArrayPatternElem::Rest(binding) =>
      match binding {
        @engine.VarBinding::Target(expr) => {
          let mut target_assign : ((Value) -> Unit raise)? = None
          let prep_step = gen_prepare_assignment_target(env, expr, fn(
            assigner,
          ) {
            target_assign = Some(assigner)
            Done(Normal(Undefined))
          })
          gen_bind(prep_step, fn(control) {
            match control {
              Normal(_) => {
                let assigner = match target_assign {
                  Some(value) => value
                  None => fn(_) { () }
                }
                let rest_values : Array[Value?] = []
                if !done {
                  for {
                    let (is_done, item) = iterator_step_value(
                      iterator, next_method,
                    ) catch {
                      ThrowSignal(value) => return Done(Throw(value))
                      err => raise err
                    }
                    if is_done {
                      break
                    }
                    rest_values.push(Some(item))
                  }
                }
                let rest_value = new_array_value(rest_values)
                gen_bind(gen_apply_assign_target(assigner, rest_value), fn(
                  control,
                ) {
                  match control {
                    Normal(_) => k()
                    _ => Done(control)
                  }
                })
              }
              _ =>
                if done {
                  Done(control)
                } else {
                  gen_iterator_close(iterator, control)
                }
            }
          })
        }
        _ => {
          let rest_values : Array[Value?] = []
          if !done {
            for {
              let (is_done, item) = iterator_step_value(iterator, next_method) catch {
                ThrowSignal(value) => return Done(Throw(value))
                err => raise err
              }
              if is_done {
                break
              }
              rest_values.push(Some(item))
            }
          }
          let rest_value = new_array_value(rest_values)
          gen_bind(
            gen_assign_binding_value(env, binding, rest_value, assign, fn() {
              Done(Normal(Undefined))
            }),
            fn(control) {
              match control {
                Normal(_) => k()
                _ => Done(control)
              }
            },
          )
        }
      }
  }
}

///|
fn gen_destructure_array_pattern(
  env : Env,
  pattern : Array[@engine.ArrayPatternElem],
  value : Value,
  assign : (String, Value) -> Unit raise,
  k : () -> GenStep raise,
) -> GenStep raise {
  gen_with_root_frame(fn() raise {
    let (iterator, next_method) = get_iterator_from_value(value)
    let _ = gen_root_push(iterator)
    let _ = gen_root_push(next_method)
    gen_destructure_array_pattern_iter(
      env, pattern, iterator, next_method, 0, false, assign, k,
    )
  })
}

///|
fn gen_destructure_object_pattern(
  env : Env,
  pattern : Array[@engine.ObjectPatternElem],
  obj : Value,
  bound : Array[String],
  index : Int,
  assign : (String, Value) -> Unit raise,
  k : () -> GenStep raise,
) -> GenStep raise {
  gen_with_root_frame(fn() raise {
    let _ = gen_root_push(obj)
    gen_destructure_object_pattern_inner(
      env, pattern, obj, bound, index, assign, k,
    )
  })
}

///|
fn gen_destructure_object_pattern_inner(
  env : Env,
  pattern : Array[@engine.ObjectPatternElem],
  obj : Value,
  bound : Array[String],
  index : Int,
  assign : (String, Value) -> Unit raise,
  k : () -> GenStep raise,
) -> GenStep raise {
  if index >= pattern.length() {
    return k()
  }
  match pattern[index] {
    @engine.ObjectPatternElem::Bind(key, binding, default) => {
      let handle_key = fn(key_name : String) -> GenStep raise {
        bound.push(key_name)
        match binding {
          @engine.VarBinding::Target(expr) => {
            let mut target_assign : ((Value) -> Unit raise)? = None
            let prep_step = gen_prepare_assignment_target(env, expr, fn(
              assigner,
            ) {
              target_assign = Some(assigner)
              Done(Normal(Undefined))
            })
            gen_bind(prep_step, fn(control) {
              match control {
                Normal(_) => {
                  let assigner = match target_assign {
                    Some(value) => value
                    None => fn(_) { () }
                  }
                  let prop_value = property_get(obj, key_name)
                  let assign_step = match default {
                    Some(expr) =>
                      if prop_value is Undefined {
                        gen_eval_expr_cont(env, expr, fn(def_value) {
                          gen_apply_assign_target(assigner, def_value)
                        })
                      } else {
                        gen_apply_assign_target(assigner, prop_value)
                      }
                    None => gen_apply_assign_target(assigner, prop_value)
                  }
                  gen_bind(assign_step, fn(control) {
                    match control {
                      Normal(_) =>
                        gen_destructure_object_pattern_inner(
                          env,
                          pattern,
                          obj,
                          bound,
                          index + 1,
                          assign,
                          k,
                        )
                      _ => Done(control)
                    }
                  })
                }
                _ => Done(control)
              }
            })
          }
          _ => {
            let prop_value = property_get(obj, key_name)
            match default {
              Some(expr) =>
                if prop_value is Undefined {
                  match binding {
                    @engine.VarBinding::Name(name) =>
                      gen_eval_named_expr_cont(env, expr, name, None, fn(
                        def_value,
                      ) {
                        gen_assign_binding_value(
                          env,
                          binding,
                          def_value,
                          assign,
                          fn() {
                            gen_destructure_object_pattern_inner(
                              env,
                              pattern,
                              obj,
                              bound,
                              index + 1,
                              assign,
                              k,
                            )
                          },
                        )
                      })
                    _ =>
                      gen_eval_expr_cont(env, expr, fn(def_value) {
                        gen_assign_binding_value(
                          env,
                          binding,
                          def_value,
                          assign,
                          fn() {
                            gen_destructure_object_pattern_inner(
                              env,
                              pattern,
                              obj,
                              bound,
                              index + 1,
                              assign,
                              k,
                            )
                          },
                        )
                      })
                  }
                } else {
                  gen_assign_binding_value(env, binding, prop_value, assign, fn() {
                    gen_destructure_object_pattern_inner(
                      env,
                      pattern,
                      obj,
                      bound,
                      index + 1,
                      assign,
                      k,
                    )
                  })
                }
              None =>
                gen_assign_binding_value(env, binding, prop_value, assign, fn() {
                  gen_destructure_object_pattern_inner(
                    env,
                    pattern,
                    obj,
                    bound,
                    index + 1,
                    assign,
                    k,
                  )
                })
            }
          }
        }
      }
      match key {
        @engine.ObjKey::Computed(expr) =>
          gen_eval_expr_cont(env, expr, fn(value) {
            handle_key(property_key_name(value))
          })
        _ => handle_key(obj_key_simple_name(key))
      }
    }
    @engine.ObjectPatternElem::Rest(binding) => {
      let rest_value = new_object_value()
      match rest_value {
        Object(rest_obj) => copy_data_properties_excluding(rest_obj, obj, bound)
        _ => ()
      }
      gen_assign_binding_value(env, binding, rest_value, assign, k)
    }
  }
}

///|
fn gen_eval_assign_op(
  env : Env,
  op : @engine.BinaryOp,
  lhs : @engine.Expr,
  rhs : @engine.Expr,
  k : (Value) -> GenStep raise,
) -> GenStep raise {
  gen_with_root_frame(fn() raise {
    match lhs {
      Ident(name, _) => {
        let assigner = prepare_identifier_assignment_target(
          env,
          name,
          env.strict,
        )
        let current = env_get(env, name)
        let _ = gen_root_push(current)
        match op {
          @engine.BinaryOp::LogicalAnd =>
            if is_truthy(current) {
              gen_eval_expr_cont(env, rhs, fn(value) raise {
                let _ = gen_root_push(value)
                if is_anonymous_function_definition(rhs) {
                  set_anonymous_function_name(value, name, None)
                }
                assigner(value)
                k(value)
              })
            } else {
              k(current)
            }
          @engine.BinaryOp::LogicalOr =>
            if is_truthy(current) {
              k(current)
            } else {
              gen_eval_expr_cont(env, rhs, fn(value) raise {
                let _ = gen_root_push(value)
                if is_anonymous_function_definition(rhs) {
                  set_anonymous_function_name(value, name, None)
                }
                assigner(value)
                k(value)
              })
            }
          @engine.BinaryOp::Coalesce =>
            if is_nullish(current) {
              gen_eval_expr_cont(env, rhs, fn(value) raise {
                let _ = gen_root_push(value)
                if is_anonymous_function_definition(rhs) {
                  set_anonymous_function_name(value, name, None)
                }
                assigner(value)
                k(value)
              })
            } else {
              k(current)
            }
          _ =>
            gen_eval_expr_cont(env, rhs, fn(value) raise {
              let _ = gen_root_push(value)
              let result = eval_binary(op, current, value)
              let _ = gen_root_push(result)
              assigner(result)
              k(result)
            })
        }
      }
      Member(obj_expr, key, _) =>
        match obj_expr {
          @engine.Expr::Super =>
            match key {
              @engine.MemberKey::Private(_) => {
                let _ = throw_syntax_error(
                  "private class field forbidden after super",
                )
                Done(Throw(Undefined))
              }
              @engine.MemberKey::Computed(expr) => {
                let (base, receiver) = super_base_and_receiver(env)
                let _ = gen_root_push(base)
                let _ = gen_root_push(receiver)
                gen_eval_expr_cont(env, expr, fn(key_value) raise {
                  let _ = gen_root_push(key_value)
                  let name = property_key_name(key_value)
                  let base_obj = to_object(base)
                  let _ = gen_root_push(base_obj)
                  let current = property_get_with_receiver(
                    base_obj, name, receiver,
                  )
                  let _ = gen_root_push(current)
                  match op {
                    @engine.BinaryOp::LogicalAnd =>
                      if is_truthy(current) {
                        gen_eval_expr_cont(env, rhs, fn(value) raise {
                          let _ = gen_root_push(value)
                          super_set_property(env, base, receiver, name, value)
                          k(value)
                        })
                      } else {
                        k(current)
                      }
                    @engine.BinaryOp::LogicalOr =>
                      if is_truthy(current) {
                        k(current)
                      } else {
                        gen_eval_expr_cont(env, rhs, fn(value) raise {
                          let _ = gen_root_push(value)
                          super_set_property(env, base, receiver, name, value)
                          k(value)
                        })
                      }
                    @engine.BinaryOp::Coalesce =>
                      if is_nullish(current) {
                        gen_eval_expr_cont(env, rhs, fn(value) raise {
                          let _ = gen_root_push(value)
                          super_set_property(env, base, receiver, name, value)
                          k(value)
                        })
                      } else {
                        k(current)
                      }
                    _ =>
                      gen_eval_expr_cont(env, rhs, fn(value) raise {
                        let _ = gen_root_push(value)
                        let result = eval_binary(op, current, value)
                        let _ = gen_root_push(result)
                        super_set_property(env, base, receiver, name, result)
                        k(result)
                      })
                  }
                })
              }
              _ => {
                let (base, receiver) = super_base_and_receiver(env)
                let name = member_key_name(env, key)
                let base_obj = to_object(base)
                let _ = gen_root_push(base)
                let _ = gen_root_push(receiver)
                let _ = gen_root_push(base_obj)
                let current = property_get_with_receiver(
                  base_obj, name, receiver,
                )
                let _ = gen_root_push(current)
                match op {
                  @engine.BinaryOp::LogicalAnd =>
                    if is_truthy(current) {
                      gen_eval_expr_cont(env, rhs, fn(value) raise {
                        let _ = gen_root_push(value)
                        super_set_property(env, base, receiver, name, value)
                        k(value)
                      })
                    } else {
                      k(current)
                    }
                  @engine.BinaryOp::LogicalOr =>
                    if is_truthy(current) {
                      k(current)
                    } else {
                      gen_eval_expr_cont(env, rhs, fn(value) raise {
                        let _ = gen_root_push(value)
                        super_set_property(env, base, receiver, name, value)
                        k(value)
                      })
                    }
                  @engine.BinaryOp::Coalesce =>
                    if is_nullish(current) {
                      gen_eval_expr_cont(env, rhs, fn(value) raise {
                        let _ = gen_root_push(value)
                        super_set_property(env, base, receiver, name, value)
                        k(value)
                      })
                    } else {
                      k(current)
                    }
                  _ =>
                    gen_eval_expr_cont(env, rhs, fn(value) raise {
                      let _ = gen_root_push(value)
                      let result = eval_binary(op, current, value)
                      let _ = gen_root_push(result)
                      super_set_property(env, base, receiver, name, result)
                      k(result)
                    })
                }
              }
            }
          _ =>
            match key {
              @engine.MemberKey::Private(name) =>
                gen_eval_expr_cont(env, obj_expr, fn(obj_value) raise {
                  let key_name = env_private_key_checked(env, name)
                  let _ = gen_root_push(obj_value)
                  let current = private_property_get(obj_value, key_name, name)
                  let _ = gen_root_push(current)
                  match op {
                    @engine.BinaryOp::LogicalAnd =>
                      if is_truthy(current) {
                        gen_eval_expr_cont(env, rhs, fn(value) raise {
                          let _ = gen_root_push(value)
                          private_property_set(obj_value, key_name, name, value)
                          k(value)
                        })
                      } else {
                        k(current)
                      }
                    @engine.BinaryOp::LogicalOr =>
                      if is_truthy(current) {
                        k(current)
                      } else {
                        gen_eval_expr_cont(env, rhs, fn(value) raise {
                          let _ = gen_root_push(value)
                          private_property_set(obj_value, key_name, name, value)
                          k(value)
                        })
                      }
                    @engine.BinaryOp::Coalesce =>
                      if is_nullish(current) {
                        gen_eval_expr_cont(env, rhs, fn(value) raise {
                          let _ = gen_root_push(value)
                          private_property_set(obj_value, key_name, name, value)
                          k(value)
                        })
                      } else {
                        k(current)
                      }
                    _ =>
                      gen_eval_expr_cont(env, rhs, fn(value) raise {
                        let _ = gen_root_push(value)
                        let result = eval_binary(op, current, value)
                        let _ = gen_root_push(result)
                        private_property_set(obj_value, key_name, name, result)
                        k(result)
                      })
                  }
                })
              _ =>
                gen_eval_expr_cont(env, obj_expr, fn(obj_value) raise {
                  let _ = gen_root_push(obj_value)
                  let target = to_object(obj_value)
                  let _ = gen_root_push(target)
                  gen_eval_member_key(env, key, fn(name) raise {
                    let current = property_get_with_receiver(
                      target, name, obj_value,
                    )
                    let _ = gen_root_push(current)
                    match op {
                      @engine.BinaryOp::LogicalAnd =>
                        if is_truthy(current) {
                          gen_eval_expr_cont(env, rhs, fn(value) raise {
                            let _ = gen_root_push(value)
                            let _ = property_set(
                              target,
                              name,
                              value,
                              env.strict,
                            )
                            k(value)
                          })
                        } else {
                          k(current)
                        }
                      @engine.BinaryOp::LogicalOr =>
                        if is_truthy(current) {
                          k(current)
                        } else {
                          gen_eval_expr_cont(env, rhs, fn(value) raise {
                            let _ = gen_root_push(value)
                            let _ = property_set(
                              target,
                              name,
                              value,
                              env.strict,
                            )
                            k(value)
                          })
                        }
                      @engine.BinaryOp::Coalesce =>
                        if is_nullish(current) {
                          gen_eval_expr_cont(env, rhs, fn(value) raise {
                            let _ = gen_root_push(value)
                            let _ = property_set(
                              target,
                              name,
                              value,
                              env.strict,
                            )
                            k(value)
                          })
                        } else {
                          k(current)
                        }
                      _ =>
                        gen_eval_expr_cont(env, rhs, fn(value) raise {
                          let _ = gen_root_push(value)
                          let result = eval_binary(op, current, value)
                          let _ = gen_root_push(result)
                          let _ = property_set(target, name, result, env.strict)
                          k(result)
                        })
                    }
                  })
                })
            }
        }
      _ =>
        match unwrap_paren(lhs) {
          @engine.Expr::Call(_, _, _) =>
            if !env.strict {
              gen_eval_expr_cont(env, lhs, fn(_) raise {
                let _ = throw_reference_error("invalid assignment target")
                Done(Throw(Undefined))
              })
            } else {
              let _ = throw_type_error("invalid assignment target")
              Done(Throw(Undefined))
            }
          _ => {
            let _ = throw_type_error("invalid assignment target")
            Done(Throw(Undefined))
          }
        }
    }
  })
}
