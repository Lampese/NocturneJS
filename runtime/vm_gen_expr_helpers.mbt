///|
fn gen_eval_expr_cont(
  env : Env,
  expr : @engine.Expr,
  k : (Value) -> GenStep raise,
) -> GenStep raise {
  if !expr_contains_yield(expr) {
    return try {
      let value = eval_expr(env, expr)
      k(value)
    } catch {
      ThrowSignal(value) => Done(Throw(value))
      err => raise err
    }
  }
  match expr {
    Yield(expr_opt, delegate) =>
      if delegate {
        match expr_opt {
          Some(inner) => gen_eval_yield_star(env, inner, k)
          None => {
            let _ = throw_syntax_error("missing yield* operand")
            Done(Throw(Undefined))
          }
        }
      } else {
        let is_async_gen = current_function_is_async_generator()
        let yield_with_value = fn(value : Value) {
          Yield(value, fn(resume_state) {
            match resume_state {
              GenResume::Next(arg) => k(arg)
              GenResume::Return(arg) =>
                if is_async_gen {
                  gen_async_return(arg)
                } else {
                  Done(Return(arg))
                }
              GenResume::Throw(arg) => Done(Throw(arg))
            }
          })
        }
        match expr_opt {
          Some(inner) =>
            gen_eval_expr_cont(env, inner, fn(value) {
              if is_async_gen {
                Await(value, fn(resume_state) {
                  match resume_state {
                    GenResume::Next(arg) => yield_with_value(arg)
                    GenResume::Return(arg) => gen_async_return(arg)
                    GenResume::Throw(arg) => Done(Throw(arg))
                  }
                })
              } else {
                yield_with_value(value)
              }
            })
          None =>
            if is_async_gen {
              Await(Undefined, fn(resume_state) {
                match resume_state {
                  GenResume::Next(arg) => yield_with_value(arg)
                  GenResume::Return(arg) => gen_async_return(arg)
                  GenResume::Throw(arg) => Done(Throw(arg))
                }
              })
            } else {
              yield_with_value(Undefined)
            }
        }
      }
    Binary(op, left, right, _) =>
      gen_with_root_frame(fn() raise {
        gen_eval_expr_cont(env, left, fn(l) raise {
          let _ = gen_root_push(l)
          match op {
            @engine.BinaryOp::LogicalAnd =>
              if is_truthy(l) {
                gen_eval_expr_cont(env, right, fn(r) raise { k(r) })
              } else {
                k(l)
              }
            @engine.BinaryOp::LogicalOr =>
              if is_truthy(l) {
                k(l)
              } else {
                gen_eval_expr_cont(env, right, fn(r) raise { k(r) })
              }
            @engine.BinaryOp::Coalesce =>
              if is_nullish(l) {
                gen_eval_expr_cont(env, right, fn(r) raise { k(r) })
              } else {
                k(l)
              }
            _ =>
              gen_eval_expr_cont(env, right, fn(r) raise {
                k(eval_binary(op, l, r))
              })
          }
        })
      })
    PrivateIn(name, right, _) =>
      gen_eval_expr_cont(env, right, fn(target) raise {
        k(Bool(eval_private_in(env, name, target)))
      })
    Conditional(test_expr, conseq, alt) =>
      gen_eval_expr_cont(env, test_expr, fn(test_value) raise {
        if is_truthy(test_value) {
          gen_eval_expr_cont(env, conseq, k)
        } else {
          gen_eval_expr_cont(env, alt, k)
        }
      })
    Sequence(exprs) => gen_eval_sequence(env, exprs, 0, k)
    Assign(name, rhs, _, is_cover) =>
      gen_with_root_frame(fn() raise {
        let assigner = prepare_identifier_assignment_target(
          env,
          name,
          env.strict,
        )
        gen_eval_expr_cont(env, rhs, fn(value) raise {
          let _ = gen_root_push(value)
          if !is_cover && is_anonymous_function_definition(rhs) {
            set_anonymous_function_name(value, name, None)
          }
          gen_bind(gen_apply_assign_target(assigner, value), fn(control) raise {
            match control {
              Normal(_) => k(value)
              Return(v) => Done(Return(v))
              Throw(v) => Done(Throw(v))
              Break(label, v) => Done(Break(label, v))
              Continue(label, v) => Done(Continue(label, v))
            }
          })
        })
      })
    AssignInvalid(lhs, _, _) =>
      gen_eval_expr_cont(env, lhs, fn(_) raise {
        let _ = throw_reference_error("invalid assignment target")
        Done(Throw(Undefined))
      })
    AssignArray(pattern, rhs) =>
      gen_with_root_frame(fn() raise {
        gen_eval_expr_cont(env, rhs, fn(value) raise {
          let _ = gen_root_push(value)
          gen_destructure_array_pattern(
            env,
            pattern,
            value,
            fn(name, v) raise { env_set(env, name, v) },
            fn() raise { k(value) },
          )
        })
      })
    AssignObject(pattern, rhs) =>
      gen_with_root_frame(fn() raise {
        gen_eval_expr_cont(env, rhs, fn(value) raise {
          let _ = gen_root_push(value)
          let obj = to_object(value)
          let _ = gen_root_push(obj)
          gen_destructure_object_pattern(
            env,
            pattern,
            obj,
            [],
            0,
            fn(name, v) raise { env_set(env, name, v) },
            fn() raise { k(value) },
          )
        })
      })
    AssignOp(op, lhs, rhs, _) => gen_eval_assign_op(env, op, lhs, rhs, k)
    AssignMember(obj_expr, key, rhs, _) =>
      gen_with_root_frame(fn() raise {
        gen_prepare_member_assignment_target(env, obj_expr, key, fn(
          assigner,
        ) raise {
          gen_eval_expr_cont(env, rhs, fn(value) raise {
            let _ = gen_root_push(value)
            gen_bind(gen_apply_assign_target(assigner, value), fn(
              control,
            ) raise {
              match control {
                Normal(_) => k(value)
                Return(v) => Done(Return(v))
                Throw(v) => Done(Throw(v))
                Break(label, v) => Done(Break(label, v))
                Continue(label, v) => Done(Continue(label, v))
              }
            })
          })
        })
      })
    Await(inner, _) =>
      gen_eval_expr_cont(env, inner, fn(value) raise {
        match current_function() {
          Some(func) =>
            if func.is_async {
              Await(value, fn(resume_state) raise {
                match resume_state {
                  GenResume::Next(arg) => k(arg)
                  GenResume::Return(arg) => Done(Return(arg))
                  GenResume::Throw(arg) => Done(Throw(arg))
                }
              })
            } else {
              k(value)
            }
          None => k(value)
        }
      })
    Unary(op, inner, _) =>
      gen_eval_expr_cont(env, inner, fn(value) raise {
        k(eval_unary(op, value))
      })
    Call(callee, args, _) => gen_eval_call(env, callee, args, k)
    Member(obj_expr, key, _) =>
      gen_with_root_frame(fn() raise {
        gen_eval_expr_cont(env, obj_expr, fn(obj_value) raise {
          let _ = gen_root_push(obj_value)
          match key {
            @engine.MemberKey::Private(name) => {
              let key_name = env_private_key_checked(env, name)
              k(private_property_get(obj_value, key_name, name))
            }
            _ => {
              let target = to_object(obj_value)
              gen_eval_member_key(env, key, fn(name) raise {
                k(property_get_with_receiver(target, name, obj_value))
              })
            }
          }
        })
      })
    OptionalChain(base, segments) =>
      gen_eval_optional_chain(env, base, segments, k)
    New(callee, args) =>
      gen_with_root_frame(fn() raise {
        gen_eval_expr_cont(env, callee, fn(callee_value) raise {
          let _ = gen_root_push(callee_value)
          gen_eval_args(env, args, 0, [], fn(argv) raise {
            k(call_constructor(callee_value, argv))
          })
        })
      })
    TemplateLiteral(parts, exprs) =>
      gen_eval_template_literal(env, parts, exprs, k)
    TaggedTemplate(callee, parts, exprs) =>
      gen_eval_tagged_template(env, callee, parts, exprs, k)
    ClassExpr(class_def) =>
      gen_bind(gen_eval_class(env, class_def), fn(control) raise {
        match control {
          Normal(value) => k(value)
          Return(value) => Done(Return(value))
          Throw(value) => Done(Throw(value))
          Break(label, value_opt) => Done(Break(label, value_opt))
          Continue(label, value_opt) => Done(Continue(label, value_opt))
        }
      })
    ArrayLiteral(elems) =>
      gen_with_root_frame(fn() raise {
        gen_eval_array_literal(env, elems, 0, [], k)
      })
    ObjectLiteral(props) =>
      gen_with_root_frame(fn() raise {
        let obj = new_object_value()
        match obj {
          Object(value) => {
            let _ = gen_root_push(obj)
            gen_eval_object_literal(env, props, 0, value, k)
          }
          _ => k(obj)
        }
      })
    Paren(inner) => gen_eval_expr_cont(env, inner, k)
    Spread(inner) => gen_eval_expr_cont(env, inner, k)
    _ => {
      let _ = throw_type_error("unsupported generator expression")
      Done(Throw(Undefined))
    }
  }
}

///|
fn gen_eval_named_expr_cont(
  env : Env,
  expr : @engine.Expr,
  name : String,
  key_value : Value?,
  k : (Value) -> GenStep raise,
) -> GenStep raise {
  let hint_name = function_name_from_key(name, key_value, None)
  match unwrap_paren(expr) {
    @engine.Expr::ClassExpr(class_def) =>
      if class_def.name is None {
        if expr_contains_yield(expr) {
          return gen_bind(
            gen_eval_class(env, class_def, name_hint=Some(hint_name)),
            fn(control) raise {
              match control {
                Normal(value) => k(value)
                Return(value) => Done(Return(value))
                Throw(value) => Done(Throw(value))
                Break(label, value_opt) => Done(Break(label, value_opt))
                Continue(label, value_opt) => Done(Continue(label, value_opt))
              }
            },
          )
        }
        let value = eval_class(env, class_def, name_hint=Some(hint_name))
        return k(value)
      }
    _ => ()
  }
  gen_eval_expr_cont(env, expr, fn(value) raise {
    if is_anonymous_function_definition(expr) {
      set_anonymous_function_name(value, name, key_value)
    }
    k(value)
  })
}

///|
fn gen_eval_array_literal(
  env : Env,
  elems : Array[@engine.Expr?],
  index : Int,
  values : Array[Value?],
  k : (Value) -> GenStep raise,
) -> GenStep raise {
  if index >= elems.length() {
    let array_proto_value = match value_from_object(array_proto_for_env(env)) {
      Some(value) => Some(value)
      None => value_from_object(array_proto())
    }
    return k(new_array_value_with_proto(values, proto=array_proto_value))
  }
  match elems[index] {
    Some(@engine.Expr::Spread(inner)) =>
      gen_eval_expr_cont(env, inner, fn(value) raise {
        let _ = gen_root_push(value)
        let list = build_spread_list(value)
        for item in list {
          values.push(Some(item))
          let _ = gen_root_push(item)

        }
        gen_eval_array_literal(env, elems, index + 1, values, k)
      })
    Some(expr) =>
      gen_eval_expr_cont(env, expr, fn(value) raise {
        values.push(Some(value))
        let _ = gen_root_push(value)
        gen_eval_array_literal(env, elems, index + 1, values, k)
      })
    None => {
      values.push(None)
      gen_eval_array_literal(env, elems, index + 1, values, k)
    }
  }
}

///|
fn gen_eval_template_literal(
  env : Env,
  parts : @engine.TemplateParts,
  exprs : Array[@engine.Expr],
  k : (Value) -> GenStep raise,
) -> GenStep raise {
  gen_with_root_frame(fn() raise {
    let cooked = parts.cooked
    let mut acc = ""
    if !cooked.is_empty() {
      match cooked[0] {
        Some(text) => acc = text
        None => {
          let _ = throw_syntax_error(
            "invalid escape sequence in template literal",
          )

        }
      }
    }
    gen_eval_template_parts(env, cooked, exprs, 0, acc, k)
  })
}

///|
fn gen_eval_template_parts(
  env : Env,
  parts : Array[String?],
  exprs : Array[@engine.Expr],
  index : Int,
  acc : String,
  k : (Value) -> GenStep raise,
) -> GenStep raise {
  if index >= exprs.length() {
    return k(String(acc))
  }
  gen_eval_expr_cont(env, exprs[index], fn(value) raise {
    let _ = gen_root_push(value)
    let mut next = acc + to_string_strict(value)
    if index + 1 < parts.length() {
      match parts[index + 1] {
        Some(text) => next = next + text
        None => {
          let _ = throw_syntax_error(
            "invalid escape sequence in template literal",
          )

        }
      }
    }
    gen_eval_template_parts(env, parts, exprs, index + 1, next, k)
  })
}

///|
fn gen_eval_template_values(
  env : Env,
  exprs : Array[@engine.Expr],
  index : Int,
  values : Array[Value],
  k : (Array[Value]) -> GenStep raise,
) -> GenStep raise {
  if index >= exprs.length() {
    return k(values)
  }
  gen_eval_expr_cont(env, exprs[index], fn(value) raise {
    values.push(value)
    let _ = gen_root_push(value)
    gen_eval_template_values(env, exprs, index + 1, values, k)
  })
}

///|
fn gen_eval_tagged_template(
  env : Env,
  callee : @engine.Expr,
  parts : @engine.TemplateParts,
  exprs : Array[@engine.Expr],
  k : (Value) -> GenStep raise,
) -> GenStep raise {
  gen_with_root_frame(fn() raise {
    match callee {
      @engine.Expr::Member(obj_expr, key, _) =>
        gen_eval_expr_cont(env, obj_expr, fn(target_value) raise {
          let _ = gen_root_push(target_value)
          let callee_value = match key {
            @engine.MemberKey::Private(name) => {
              let key_name = env_private_key_checked(env, name)
              private_property_get(target_value, key_name, name)
            }
            _ => {
              let name = member_key_name(env, key)
              let target_obj = to_object(target_value)
              property_get_with_receiver(target_obj, name, target_value)
            }
          }
          let _ = gen_root_push(callee_value)
          let template_obj = build_template_object(env, parts)
          let _ = gen_root_push(template_obj)
          gen_eval_template_values(env, exprs, 0, [], fn(values) raise {
            let argv : Array[Value] = [template_obj]
            for value in values {
              argv.push(value)
            }
            k(call_value_with_this(callee_value, argv, target_value))
          })
        })
      _ =>
        gen_eval_expr_cont(env, callee, fn(callee_value) raise {
          let _ = gen_root_push(callee_value)
          let template_obj = build_template_object(env, parts)
          let _ = gen_root_push(template_obj)
          gen_eval_template_values(env, exprs, 0, [], fn(values) raise {
            let argv : Array[Value] = [template_obj]
            for value in values {
              argv.push(value)
            }
            k(call_value(callee_value, argv))
          })
        })
    }
  })
}

///|
fn gen_eval_sequence(
  env : Env,
  exprs : Array[@engine.Expr],
  index : Int,
  k : (Value) -> GenStep raise,
) -> GenStep raise {
  if exprs.is_empty() {
    return k(Undefined)
  }
  if index >= exprs.length() - 1 {
    return gen_eval_expr_cont(env, exprs[index], k)
  }
  gen_eval_expr_cont(env, exprs[index], fn(_) raise {
    gen_eval_sequence(env, exprs, index + 1, k)
  })
}
