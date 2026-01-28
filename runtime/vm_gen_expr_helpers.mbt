///|
fn gen_eval_expr_cont(
  env : Env,
  expr : @engine.Expr,
  k : (Value) -> GenStep raise,
) -> GenStep raise {
  gen_bytecode_eval_expr_cont(env, expr, k)
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
