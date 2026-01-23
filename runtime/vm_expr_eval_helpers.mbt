///|
fn eval_template_literal(
  env : Env,
  parts : @engine.TemplateParts,
  exprs : Array[@engine.Expr],
) -> Value raise {
  let mut result = ""
  let cooked = parts.cooked
  if !cooked.is_empty() {
    match cooked[0] {
      Some(text) => result = text
      None => {
        let _ = throw_syntax_error(
          "invalid escape sequence in template literal",
        )

      }
    }
  }
  let mut i = 0
  while i < exprs.length() {
    let value = eval_expr(env, exprs[i])
    result = result + to_string_strict(value)
    if i + 1 < cooked.length() {
      match cooked[i + 1] {
        Some(text) => result = result + text
        None => {
          let _ = throw_syntax_error(
            "invalid escape sequence in template literal",
          )

        }
      }
    }
    i = i + 1
  }
  String(result)
}

///|
fn build_template_object(
  env : Env,
  parts : @engine.TemplateParts,
) -> Value raise {
  let registry = template_registry_for_env(env)
  match registry.get(parts.site_id) {
    Some(value) => value
    None => {
      let cooked : Array[Value?] = []
      let raw_parts : Array[Value?] = []
      for part in parts.cooked {
        match part {
          Some(text) => cooked.push(Some(String(text)))
          None => cooked.push(Some(Undefined))
        }
      }
      for part in parts.raw {
        raw_parts.push(Some(String(part)))
      }
      let cooked_value = new_array_value(cooked)
      let raw_value = new_array_value(raw_parts)
      match cooked_value {
        Array(arr) => props_set(arr.props, "raw", property_data_non_enum(raw_value))
        _ => ()
      }
      let _ = object_freeze(raw_value)
      let _ = object_freeze(cooked_value)
      template_registry_set(registry, parts.site_id, cooked_value)
      cooked_value
    }
  }
}

///|
fn eval_tagged_template(
  env : Env,
  callee : @engine.Expr,
  parts : @engine.TemplateParts,
  exprs : Array[@engine.Expr],
) -> Value raise {
  let mut this_value : Value? = None
  let callee_value = match callee {
    @engine.Expr::Member(obj_expr, key, _) => {
      let target = eval_expr(env, obj_expr)
      this_value = Some(target)
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
    }
    _ => eval_expr(env, callee)
  }
  let template_obj = build_template_object(env, parts)
  let argv : Array[Value] = [template_obj]
  for expr in exprs {
    argv.push(eval_expr(env, expr))
  }
  match this_value {
    Some(target) => call_value_with_this(callee_value, argv, target)
    None => call_value(callee_value, argv)
  }
}

///|
fn eval_typeof(env : Env, expr : @engine.Expr) -> Value raise {
  match expr {
    @engine.Expr::Ident(name, _) =>
      if env_has(env, name) {
        String(typeof_value(env_get(env, name)))
      } else {
        String("undefined")
      }
    @engine.Expr::Paren(inner) => eval_typeof(env, inner)
    _ => {
      let value = eval_expr(env, expr)
      String(typeof_value(value))
    }
  }
}

///|
fn eval_expr(env : Env, expr : @engine.Expr) -> Value raise {
  match expr {
    Number(value, _, _) => Number(value)
    Bool(value) => Bool(value)
    Null => Null
    String(value, _, _) => String(value)
    TemplateLiteral(parts, exprs) => eval_template_literal(env, parts, exprs)
    TaggedTemplate(callee, parts, exprs) =>
      eval_tagged_template(env, callee, parts, exprs)
    Regexp(pattern, flags, offset) => {
      let proto = match value_from_object(regexp_proto_for_env(env)) {
        Some(value) => Some(value)
        None => value_from_object(regexp_proto())
      }
      new_regexp_value(pattern, flags, proto, offset~)
    }
    BigInt(value) => BigInt(parse_bigint_literal(value))
    Ident(name, offset) =>
      with_expr_pos(offset, fn() raise { env_get(env, name) })
    This => env_get(env, "this")
    NewTarget(_) => env.new_target
    ImportMeta(_) => import_meta_value()
    Super => throw_reference_error("super is not defined")
    Yield(_, _) => throw_type_error("yield not supported")
    Await(inner, offset) =>
      with_expr_pos(offset, fn() raise {
        let value = eval_expr(env, inner)
        match current_function() {
          Some(func) => if func.is_async { await_value(value) } else { value }
          None => value
        }
      })
    Unary(Typeof, inner, offset) =>
      with_expr_pos(offset, fn() raise { eval_typeof(env, inner) })
    Unary(op, inner, offset) =>
      with_expr_pos(offset, fn() raise {
        match op {
          Delete => eval_delete(env, inner)
          _ => eval_unary(op, eval_expr(env, inner))
        }
      })
    Update(op, inner, kind, offset) =>
      with_expr_pos(offset, fn() raise { eval_update(env, op, inner, kind) })
    New(callee, args) => eval_new(env, callee, args)
    Binary(op, left, right, offset) =>
      with_expr_pos(offset, fn() raise {
        with_gc_frame(fn() raise {
          let l = eval_expr(env, left)
          let _ = gc_root_push(l)
          match op {
            LogicalAnd => if is_truthy(l) { eval_expr(env, right) } else { l }
            LogicalOr => if is_truthy(l) { l } else { eval_expr(env, right) }
            Coalesce => if is_nullish(l) { eval_expr(env, right) } else { l }
            _ => {
              let r = eval_expr(env, right)
              eval_binary(op, l, r)
            }
          }
        })
      })
    PrivateIn(name, right, offset) =>
      with_expr_pos(offset, fn() raise {
        let target = eval_expr(env, right)
        Bool(eval_private_in(env, name, target))
      })
    Conditional(test_expr, conseq, alt) =>
      if is_truthy(eval_expr(env, test_expr)) {
        eval_expr(env, conseq)
      } else {
        eval_expr(env, alt)
      }
    Sequence(exprs) => {
      let mut last = Undefined
      for inner in exprs {
        last = eval_expr(env, inner)
      }
      last
    }
    Assign(name, rhs, offset, is_cover) =>
      with_expr_pos(offset, fn() raise {
        with_gc_frame(fn() raise {
          let assigner = prepare_identifier_assignment_target(
            env,
            name,
            env.strict,
          )
          let value = eval_expr(env, rhs)
          let _ = gc_root_push(value)
          if !is_cover && is_anonymous_function_definition(rhs) {
            set_anonymous_function_name(value, name, None)
          }
          assigner(value)
          value
        })
      })
    AssignInvalid(lhs, _, offset) =>
      with_expr_pos(offset, fn() raise {
        let _ = eval_expr(env, lhs)
        throw_reference_error("invalid assignment target")
      })
    AssignArray(pattern, rhs) =>
      with_gc_frame(fn() raise {
        let value = eval_expr(env, rhs)
        let _ = gc_root_push(value)
        destructure_array_pattern(env, pattern, value, fn(name, v) raise {
          env_set(env, name, v)
        })
        value
      })
    AssignObject(pattern, rhs) =>
      with_gc_frame(fn() raise {
        let value = eval_expr(env, rhs)
        let _ = gc_root_push(value)
        destructure_object_pattern(env, pattern, value, fn(name, v) raise {
          env_set(env, name, v)
        })
        value
      })
    AssignOp(op, lhs, rhs, offset) =>
      with_expr_pos(offset, fn() raise { eval_assign_op(env, op, lhs, rhs) })
    AssignMember(obj, key, rhs, offset) =>
      with_expr_pos(offset, fn() raise {
        with_gc_frame(fn() raise {
          let (roots, assigner) = prepare_member_assignment_target_with_roots(
            env, obj, key,
          )
          for root in roots {
            let _ = gc_root_push(root)

          }
          let value = eval_expr(env, rhs)
          let _ = gc_root_push(value)
          assigner(value)
          value
        })
      })
    Call(callee, args, offset) =>
      with_expr_pos(offset, fn() raise {
        match callee {
          Super => eval_super_call(env, args)
          Member(Super, key, _) => eval_super_member_call(env, key, args)
          _ => eval_call(env, callee, args)
        }
      })
    Member(obj, key, offset) =>
      with_expr_pos(offset, fn() raise {
        match obj {
          Super => eval_super_member(env, key)
          _ => eval_member(env, obj, key)
        }
      })
    OptionalChain(base, segments) => eval_optional_chain(env, base, segments)
    FunctionExpr(func) =>
      match func.name {
        Some(name) => {
          let name_env = Env::new(Some(env))
          let func_value = to_function_value(name_env, func, false)
          env_define_readonly(
            name_env,
            name,
            Value::Function(func_value),
            false,
          )
          Value::Function(func_value)
        }
        None => Value::Function(to_function_value(env, func, false))
      }
    ClassExpr(class_def) => eval_class(env, class_def)
    ObjectLiteral(props) => eval_object_literal(env, props)
    ArrayLiteral(elems) => eval_array_literal(env, elems)
    Spread(expr) => eval_expr(env, expr)
    Paren(expr) => eval_expr(env, expr)
  }
}
