///|

///|
fn typeof_value(value : Value) -> String {
  if is_html_dda_value(value) {
    return "undefined"
  }
  match value {
    Undefined => "undefined"
    Null => "object"
    Bool(_) => "boolean"
    Number(_) => "number"
    String(_) => "string"
    BigInt(_) => "bigint"
    Symbol(_) => "symbol"
    Builtin(_) => "function"
    Function(_) => "function"
    BoundFunction(_) => "function"
    Object(obj) =>
      match obj.proxy_data {
        Some(_) => if is_callable(value) { "function" } else { "object" }
        None => "object"
      }
    Array(_) => "object"
    Arguments(_) => "object"
  }
}

///|
fn eval_unary_code(code : Int, value : Value) -> Value raise {
  match code {
    0 =>
      match value {
        BigInt(_) => throw_type_error("cannot convert BigInt to number")
        _ => Number(to_number(value))
      }
    1 =>
      match to_numeric(value) {
        BigInt(v) => BigInt(v.neg())
        Number(num) =>
          if num == 0.0 {
            if is_negative_zero(num) {
              Number(0.0)
            } else {
              Number(negative_zero())
            }
          } else {
            Number(0.0 - num)
          }
        _ => fail("unreachable")
      }
    2 =>
      match to_numeric(value) {
        BigInt(v) => BigInt(v.neg().sub(@bigint.BigInt::from_int(1)))
        Number(num) => {
          let v = to_int32(num)
          Number(Double::from_int(Int::lnot(v)))
        }
        _ => fail("unreachable")
      }
    3 => Bool(!is_truthy(value))
    4 => String(typeof_value(value))
    5 => Undefined
    6 => Bool(true)
    _ => throw_type_error("invalid bytecode unary op")
  }
}

fn eval_unary(op : @engine.UnaryOp, value : Value) -> Value raise {
  eval_unary_code(bytecode_unary_op_code(op), value)
}

///|
fn eval_binary_code(code : Int, left : Value, right : Value) -> Value raise {
  match code {
    0 => eval_add(left, right)
    1 => {
      let l = to_numeric(left)
      let r = to_numeric(right)
      match (l, r) {
        (BigInt(a), BigInt(b)) => BigInt(a.sub(b))
        (BigInt(_), _) => throw_type_error("cannot mix BigInt and other types")
        (_, BigInt(_)) => throw_type_error("cannot mix BigInt and other types")
        (Number(a), Number(b)) => Number(a - b)
        _ => Number(to_number(l) - to_number(r))
      }
    }
    2 => {
      let l = to_numeric(left)
      let r = to_numeric(right)
      match (l, r) {
        (BigInt(a), BigInt(b)) => BigInt(a.mul(b))
        (BigInt(_), _) => throw_type_error("cannot mix BigInt and other types")
        (_, BigInt(_)) => throw_type_error("cannot mix BigInt and other types")
        (Number(a), Number(b)) => Number(a * b)
        _ => Number(to_number(l) * to_number(r))
      }
    }
    3 => {
      let l = to_numeric(left)
      let r = to_numeric(right)
      match (l, r) {
        (BigInt(a), BigInt(b)) =>
          if b.is_zero() {
            throw_range_error("BigInt division by zero")
          } else {
            BigInt(a.div(b))
          }
        (BigInt(_), _) => throw_type_error("cannot mix BigInt and other types")
        (_, BigInt(_)) => throw_type_error("cannot mix BigInt and other types")
        (Number(a), Number(b)) => Number(a / b)
        _ => Number(to_number(l) / to_number(r))
      }
    }
    4 => {
      let l = to_numeric(left)
      let r = to_numeric(right)
      match (l, r) {
        (BigInt(a), BigInt(b)) =>
          if b.is_zero() {
            throw_range_error("BigInt division by zero")
          } else {
            BigInt(a.mod(b))
          }
        (BigInt(_), _) => throw_type_error("cannot mix BigInt and other types")
        (_, BigInt(_)) => throw_type_error("cannot mix BigInt and other types")
        (Number(a), Number(b)) => Number(Double::mod(a, b))
        _ => Number(Double::mod(to_number(l), to_number(r)))
      }
    }
    5 => {
      let l = to_numeric(left)
      let r = to_numeric(right)
      match (l, r) {
        (BigInt(a), BigInt(b)) =>
          if b.compare_int(0) < 0 {
            throw_range_error("BigInt negative exponent")
          } else {
            BigInt(@bigint.BigInt::pow(a, b))
          }
        (BigInt(_), _) => throw_type_error("cannot mix BigInt and other types")
        (_, BigInt(_)) => throw_type_error("cannot mix BigInt and other types")
        (Number(a), Number(b)) => Number(@math.pow(a, b))
        _ => Number(@math.pow(to_number(l), to_number(r)))
      }
    }
    6 => {
      let l = to_numeric(left)
      let r = to_numeric(right)
      match (l, r) {
        (BigInt(a), BigInt(b)) => {
          let shift = bigint_shift_count(b)
          if shift >= 0 {
            BigInt(a << shift)
          } else {
            BigInt(bigint_shift_right_floor(a, -shift))
          }
        }
        (BigInt(_), _) => throw_type_error("cannot mix BigInt and other types")
        (_, BigInt(_)) => throw_type_error("cannot mix BigInt and other types")
        _ => {
          let l = to_int32(to_number(l))
          let r = shift_count(r)
          Number(Double::from_int(l << r))
        }
      }
    }
    7 => {
      let l = to_numeric(left)
      let r = to_numeric(right)
      match (l, r) {
        (BigInt(a), BigInt(b)) => {
          let shift = bigint_shift_count(b)
          if shift >= 0 {
            BigInt(bigint_shift_right_floor(a, shift))
          } else {
            BigInt(a << -shift)
          }
        }
        (BigInt(_), _) => throw_type_error("cannot mix BigInt and other types")
        (_, BigInt(_)) => throw_type_error("cannot mix BigInt and other types")
        _ => {
          let l = to_int32(to_number(l))
          let r = shift_count(r)
          Number(Double::from_int(l >> r))
        }
      }
    }
    8 => {
      let l = to_numeric(left)
      let r = to_numeric(right)
      match (l, r) {
        (BigInt(_), _) => throw_type_error("cannot mix BigInt and other types")
        (_, BigInt(_)) => throw_type_error("cannot mix BigInt and other types")
        _ => {
          let l = to_uint32(to_number(l))
          let r = shift_count(r)
          let shifted = l >> r
          Number(UInt::to_double(shifted))
        }
      }
    }
    9 => Bool(compare_values(left, right, CmpLt))
    10 => Bool(compare_values(left, right, CmpLte))
    11 => Bool(compare_values(left, right, CmpGt))
    12 => Bool(compare_values(left, right, CmpGte))
    13 => Bool(eval_in(left, right))
    14 => Bool(eval_instanceof(left, right))
    15 => Bool(loose_eq(left, right))
    16 => Bool(!loose_eq(left, right))
    17 => Bool(strict_eq(left, right))
    18 => Bool(!strict_eq(left, right))
    19 => {
      let l = to_numeric(left)
      let r = to_numeric(right)
      match (l, r) {
        (BigInt(a), BigInt(b)) => BigInt(bigint_bitwise_op(a, b, 0))
        (BigInt(_), _) => throw_type_error("cannot mix BigInt and other types")
        (_, BigInt(_)) => throw_type_error("cannot mix BigInt and other types")
        _ => {
          let l = to_int32(to_number(l))
          let r = to_int32(to_number(r))
          Number(Double::from_int(Int::land(l, r)))
        }
      }
    }
    20 => {
      let l = to_numeric(left)
      let r = to_numeric(right)
      match (l, r) {
        (BigInt(a), BigInt(b)) => BigInt(bigint_bitwise_op(a, b, 2))
        (BigInt(_), _) => throw_type_error("cannot mix BigInt and other types")
        (_, BigInt(_)) => throw_type_error("cannot mix BigInt and other types")
        _ => {
          let l = to_int32(to_number(l))
          let r = to_int32(to_number(r))
          Number(Double::from_int(Int::lxor(l, r)))
        }
      }
    }
    21 => {
      let l = to_numeric(left)
      let r = to_numeric(right)
      match (l, r) {
        (BigInt(a), BigInt(b)) => BigInt(bigint_bitwise_op(a, b, 1))
        (BigInt(_), _) => throw_type_error("cannot mix BigInt and other types")
        (_, BigInt(_)) => throw_type_error("cannot mix BigInt and other types")
        _ => {
          let l = to_int32(to_number(l))
          let r = to_int32(to_number(r))
          Number(Double::from_int(Int::lor(l, r)))
        }
      }
    }
    22 => left
    23 => left
    24 => left
    _ => throw_type_error("invalid bytecode binary op")
  }
}

fn eval_binary(
  op : @engine.BinaryOp,
  left : Value,
  right : Value,
) -> Value raise {
  eval_binary_code(bytecode_binary_op_code(op), left, right)
}

///|
fn eval_add(left : Value, right : Value) -> Value raise {
  let mut l = left
  let mut r = right
  if is_object_like(l) {
    l = to_primitive_default(l)
  }
  if is_object_like(r) {
    r = to_primitive_default(r)
  }
  match (l, r) {
    (String(a), String(b)) => String(a + b)
    (String(a), b) =>
      if is_symbol_like(b) {
        throw_type_error("cannot convert symbol to string")
      } else {
        String(a + to_string_strict(b))
      }
    (a, String(b)) =>
      if is_symbol_like(a) {
        throw_type_error("cannot convert symbol to string")
      } else {
        String(to_string_strict(a) + b)
      }
    (BigInt(a), BigInt(b)) => BigInt(a.add(b))
    (BigInt(_), _) => throw_type_error("cannot mix BigInt and other types")
    (_, BigInt(_)) => throw_type_error("cannot mix BigInt and other types")
    (a, b) =>
      if is_symbol_like(a) || is_symbol_like(b) {
        throw_type_error("cannot convert symbol to number")
      } else {
        Number(to_number(a) + to_number(b))
      }
  }
}

///|
fn eval_in(left : Value, right : Value) -> Bool raise {
  if !is_object_like(right) {
    let _ = throw_type_error("invalid 'in' operand")
    return false
  }
  let name = property_key_name(left)
  has_property_value(right, name)
}

///|
fn eval_private_in(env : Env, name : String, target : Value) -> Bool raise {
  if !is_object_like(target) {
    let _ = throw_type_error("invalid 'in' operand")
    return false
  }
  let key_name = env_private_key_checked(env, name)
  match props_map_for_value(target) {
    Some(props) =>
      match props.get(key_name) {
        Some(_) => true
        None => false
      }
    None => false
  }
}

///|
fn eval_instanceof(left : Value, right : Value) -> Bool raise {
  if !is_object_like(right) {
    let _ = throw_type_error("invalid 'instanceof' right operand")
    return false
  }
  match symbol_has_instance_key() {
    Some(key) =>
      match property_get(right, key) {
        Undefined | Null => ()
        method_value => {
          if !is_callable(method_value) {
            let _ = throw_type_error("invalid 'instanceof' right operand")
            return false
          }
          let result = call_value_with_this(method_value, [left], right)
          return is_truthy(result)
        }
      }
    None => ()
  }
  if !is_callable(right) {
    let _ = throw_type_error("invalid 'instanceof' right operand")
    return false
  }
  ordinary_instanceof(left, right)
}

///|
fn update_values(op : @engine.UpdateOp, value : Value) -> (Value, Value) raise {
  let is_inc = match op {
    @engine.UpdateOp::Inc => true
    @engine.UpdateOp::Dec => false
  }
  update_values_with_flags(is_inc, value)
}

///|
fn update_result(
  kind : @engine.UpdateKind,
  old_value : Value,
  new_value : Value,
) -> Value {
  let is_prefix = match kind {
    @engine.UpdateKind::Prefix => true
    @engine.UpdateKind::Postfix => false
  }
  update_result_with_flags(is_prefix, old_value, new_value)
}

///|
fn update_values_with_flags(
  is_inc : Bool,
  value : Value,
) -> (Value, Value) raise {
  match value {
    BigInt(v) => {
      let one = @bigint.BigInt::from_int(1)
      let new_value = if is_inc { v.add(one) } else { v.sub(one) }
      (BigInt(v), BigInt(new_value))
    }
    _ => {
      let old_num = to_number(value)
      let new_num = if is_inc { old_num + 1.0 } else { old_num - 1.0 }
      (Number(old_num), Number(new_num))
    }
  }
}

///|
fn update_result_with_flags(
  is_prefix : Bool,
  old_value : Value,
  new_value : Value,
) -> Value {
  if is_prefix { new_value } else { old_value }
}

///|
fn eval_update(
  env : Env,
  op : @engine.UpdateOp,
  expr : @engine.Expr,
  kind : @engine.UpdateKind,
) -> Value raise {
  match unwrap_paren(expr) {
    Ident(name, _) => {
      let target = resolve_binding_target(env, name)
      let current = get_binding_value_from_target(env, name, target, env.strict)
      let (old_value, new_value) = update_values(op, current)
      assign_binding_value_to_target(env, name, target, new_value, env.strict)
      update_result(kind, old_value, new_value)
    }
    Member(obj_expr, key, _) =>
      match obj_expr {
        @engine.Expr::Super =>
          match key {
            @engine.MemberKey::Private(_) =>
              throw_syntax_error("private class field forbidden after super")
            @engine.MemberKey::Computed(expr) => {
              let (base, receiver) = super_base_and_receiver(env)
              let key_value = eval_expr(env, expr)
              let name = property_key_name(key_value)
              let base_obj = to_object(base)
              let current = property_get_with_receiver(base_obj, name, receiver)
              let (old_value, new_value) = update_values(op, current)
              super_set_property(env, base, receiver, name, new_value)
              update_result(kind, old_value, new_value)
            }
            _ => {
              let (base, receiver) = super_base_and_receiver(env)
              let name = member_key_name(env, key)
              let base_obj = to_object(base)
              let current = property_get_with_receiver(base_obj, name, receiver)
              let (old_value, new_value) = update_values(op, current)
              super_set_property(env, base, receiver, name, new_value)
              update_result(kind, old_value, new_value)
            }
          }
        _ =>
          match key {
            @engine.MemberKey::Private(name) => {
              let target = eval_expr(env, obj_expr)
              let key_name = env_private_key_checked(env, name)
              let current = private_property_get(target, key_name, name)
              let (old_value, new_value) = update_values(op, current)
              private_property_set(target, key_name, name, new_value)
              update_result(kind, old_value, new_value)
            }
            @engine.MemberKey::Computed(expr) => {
              let target_value = eval_expr(env, obj_expr)
              let key_value = eval_expr(env, expr)
              let target_obj = to_object(target_value)
              let name = property_key_name(key_value)
              let current = property_get_with_receiver(
                target_obj, name, target_value,
              )
              let (old_value, new_value) = update_values(op, current)
              let _ = set_property_for_value(
                target_value,
                name,
                new_value,
                env.strict,
              )
              update_result(kind, old_value, new_value)
            }
            _ => {
              let target_value = eval_expr(env, obj_expr)
              let name = member_key_name(env, key)
              let target_obj = to_object(target_value)
              let current = property_get_with_receiver(
                target_obj, name, target_value,
              )
              let (old_value, new_value) = update_values(op, current)
              let _ = set_property_for_value(
                target_value,
                name,
                new_value,
                env.strict,
              )
              update_result(kind, old_value, new_value)
            }
          }
      }
    Call(_, _, _) =>
      if !env.strict {
        let _ = eval_expr(env, expr)
        throw_reference_error("invalid update target")
      } else {
        fail("invalid update target")
      }
    _ => fail("invalid update target")
  }
}

///|
fn eval_delete(env : Env, expr : @engine.Expr) -> Value raise {
  match unwrap_paren(expr) {
    Ident(name, _) => delete_ident(env, name, env.strict)
    Member(_, @engine.MemberKey::Private(_), _) =>
      throw_syntax_error("cannot delete a private class field")
    Member(@engine.Expr::Super, key, _) =>
      match key {
        @engine.MemberKey::Private(_) =>
          throw_syntax_error("cannot delete a private class field")
        @engine.MemberKey::Computed(expr) => {
          let _ = super_base_and_receiver(env)
          let _ = eval_expr(env, expr)
          throw_reference_error("unsupported reference to 'super'")
        }
        _ => {
          let _ = super_base_and_receiver(env)
          throw_reference_error("unsupported reference to 'super'")
        }
      }
    Member(obj_expr, key, _) => {
      let target = eval_expr(env, obj_expr)
      let name = member_key_name(env, key)
      delete_property(env, target, name)
    }
    OptionalChain(base, segments) => {
      for segment in segments {
        match segment {
          @engine.ChainSegment::Member(@engine.MemberKey::Private(_), _, _) =>
            return throw_syntax_error("cannot delete a private class field")
          _ => ()
        }
      }
      eval_delete_optional_chain(env, base, segments)
    }
    _ => {
      let _ = eval_expr(env, expr)
      Bool(true)
    }
  }
}

///|
fn delete_ident(env : Env, name : String, strict : Bool) -> Value raise {
  match env.with_object {
    Some(obj) =>
      if has_property_value(obj, name) && !is_unscopable(obj, name) {
        return delete_property_with_strict(strict, obj, name)
      }
    None => ()
  }
  match env.bindings.get(name) {
    Some(_) =>
      if env.deletable_bindings.contains(name) {
        env_bindings_remove(env, name)
        env.readonly_bindings.remove(name)
        env.uninitialized_bindings.remove(name)
        env.deletable_bindings.remove(name)
        Bool(true)
      } else {
        Bool(false)
      }
    None =>
      match env.parent {
        Some(parent) => delete_ident(parent, name, strict)
        None =>
          match global_object(env) {
            Some(obj) =>
              delete_property_with_strict(strict, Value::Object(obj), name)
            None => Bool(true)
          }
      }
  }
}

///|
fn delete_property_with_strict(
  strict : Bool,
  target : Value,
  name : String,
) -> Value raise {
  let ok = delete_property_value(target, name)
  if !ok && strict {
    return throw_type_error("cannot delete property")
  }
  Bool(ok)
}

///|
fn delete_property(env : Env, target : Value, name : String) -> Value raise {
  let ok = delete_property_value(target, name)
  if !ok && env.strict {
    return throw_type_error("cannot delete property")
  }
  Bool(ok)
}

///|
fn delete_property_value(target : Value, name : String) -> Bool raise {
  match target {
    Null => {
      let _ = throw_type_error("cannot delete property")
      false
    }
    Undefined => {
      let _ = throw_type_error("cannot delete property")
      false
    }
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => proxy_delete_property(data, name)
        None => {
          match obj.string_data {
            Some(value) => {
              if name == "length" {
                return false
              }
              match parse_array_index(name) {
                Some(index) =>
                  if index >= 0 && index < value.length() {
                    return false
                  }
                None => ()
              }
            }
            None => ()
          }
          let ok = match obj.props.get(name) {
            Some(prop) =>
              if !prop.configurable {
                false
              } else {
                props_remove(obj.props, name)
                true
              }
            None => true
          }
          ok
        }
      }
    Function(func) => {
      if name == "prototype" && !func.props.contains(name) {
        ensure_function_prototype(func)
      }
      let ok = match func.props.get(name) {
        Some(prop) =>
          if !prop.configurable {
            false
          } else {
            props_remove(func.props, name)
            true
          }
        None => true
      }
      ok
    }
    BoundFunction(bound) => {
      let ok = match bound.props.get(name) {
        Some(prop) =>
          if !prop.configurable {
            false
          } else {
            props_remove(bound.props, name)
            true
          }
        None => true
      }
      ok
    }
    Builtin(builtin) => {
      let ok = match builtin.props.get(name) {
        Some(prop) =>
          if !prop.configurable {
            false
          } else {
            props_remove(builtin.props, name)
            true
          }
        None => true
      }
      ok
    }
    Array(arr) =>
      match arr.typed_array_data {
        Some(data) =>
          match canonical_numeric_index_string(name) {
            Some(index) => !typed_array_is_valid_integer_index(data, index)
            None => {
              let mut ok = true
              match arr.props.get(name) {
                Some(prop) =>
                  if !prop.configurable {
                    ok = false
                  } else {
                    props_remove(arr.props, name)
                  }
                None => ()
              }
              ok
            }
          }
        None => {
          let mut ok = true
          match parse_array_index(name) {
            Some(index) =>
              if index >= 0 {
                match arr.props.get(name) {
                  Some(prop) =>
                    if !prop.configurable {
                      ok = false
                    } else {
                      props_remove(arr.props, name)
                    }
                  None => ()
                }
                if ok && index < arr.elements.length() {
                  rc_replace_optional_value(arr.elements[index], None)
                  arr.elements[index] = None
                }
              }
            None =>
              match arr.props.get(name) {
                Some(prop) =>
                  if !prop.configurable {
                    ok = false
                  } else {
                    props_remove(arr.props, name)
                  }
                None => ()
              }
          }
          ok
        }
      }
    Arguments(args) => {
      let mut ok = true
      match parse_array_index(name) {
        Some(index) =>
          if index >= 0 {
            match args.props.get(name) {
              Some(prop) =>
                if !prop.configurable {
                  ok = false
                } else {
                  props_remove(args.props, name)
                }
              None => ()
            }
            if ok && index < args.elements.length() {
              rc_replace_optional_value(args.elements[index], None)
              args.elements[index] = None
              if index < args.mapped.length() {
                args.mapped[index] = false
              }
            }
          }
        None =>
          match args.props.get(name) {
            Some(prop) =>
              if !prop.configurable {
                ok = false
              } else {
                props_remove(args.props, name)
              }
            None => ()
          }
      }
      ok
    }
    String(_) | Number(_) | Bool(_) | BigInt(_) | Symbol(_) => {
      let obj = to_object(target)
      delete_property_value(obj, name)
    }
  }
}

///|
fn delete_property_or_throw(target : Value, name : String) -> Unit raise {
  let ok = delete_property_value(target, name)
  if !ok {
    let _ = throw_type_error("cannot delete property")

  }
}

///|
fn eval_delete_optional_chain(
  env : Env,
  base : @engine.Expr,
  segments : Array[@engine.ChainSegment],
) -> Value raise {
  if segments.is_empty() {
    return Bool(true)
  }
  let mut current = eval_expr(env, base)
  let mut this_value : Value? = None
  for index, segment in segments {
    let is_last = index == segments.length() - 1
    match segment {
      @engine.ChainSegment::Member(key, optional, offset) => {
        if optional && is_nullish(current) {
          return Bool(true)
        }
        if is_last {
          let name = member_key_name(env, key)
          return delete_property(env, current, name)
        }
        let next = with_expr_pos(offset, fn() raise {
          let name = member_key_name(env, key)
          let target = to_object(current)
          property_get(target, name)
        })
        this_value = Some(current)
        current = next
      }
      @engine.ChainSegment::Call(args, optional, offset) => {
        if optional && is_nullish(current) {
          return Bool(true)
        }
        let argv = eval_args(env, args)
        let result = with_expr_pos(offset, fn() raise {
          match this_value {
            Some(this_obj) => call_value_with_this(current, argv, this_obj)
            None => call_value(current, argv)
          }
        })
        current = result
        this_value = None
        if is_last {
          return Bool(true)
        }
      }
    }
  }
  Bool(true)
}
