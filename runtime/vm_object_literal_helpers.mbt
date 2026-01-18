///|
fn eval_object_literal(
  env : Env,
  props : Array[@engine.ObjectProp],
) -> Value raise {
  with_gc_frame(fn() raise {
    let obj = new_object_value()
    match obj {
      Object(value) => {
        let _ = gc_root_push(obj)
        for prop in props {
          let _ = with_gc_frame(fn() raise {
            match prop {
              @engine.ObjectProp::KeyValue(key, expr) =>
                if obj_key_is_proto(key) {
                  let proto_value = eval_expr(env, expr)
                  let _ = gc_root_push(proto_value)
                  match proto_value {
                    Null => value.proto = None
                    _ if is_object_like(proto_value) =>
                      value.proto = Some(proto_value)
                    _ => ()
                  }
                } else {
                  let (name, key_value) = obj_key_property_name(env, key)
                  match key_value {
                    Some(key_value_item) => {
                      let _ = gc_root_push(key_value_item)

                    }
                    None => ()
                  }
                  let prop_value = eval_named_expr(env, expr, name, key_value)
                  let _ = gc_root_push(prop_value)
                  value.props.set(name, property_data(prop_value))
                }
              @engine.ObjectProp::Method(key, func) => {
                let (name, key_value) = obj_key_property_name(env, key)
                match key_value {
                  Some(key_value_item) => {
                    let _ = gc_root_push(key_value_item)

                  }
                  None => ()
                }
                let func_value = to_function_value(env, func, false)
                let func = Value::Function(func_value)
                let _ = gc_root_push(func)
                func_value.is_constructor = false
                set_function_name(
                  func_value,
                  function_name_from_key(name, key_value, None),
                )
                func_value.home_object = Some(Value::Object(value))
                value.props.set(name, property_data(func))
              }
              @engine.ObjectProp::Shorthand(name) => {
                let prop_value = env_get(env, name)
                let _ = gc_root_push(prop_value)
                value.props.set(name, property_data(prop_value))
              }
              @engine.ObjectProp::Getter(key, func) => {
                let (name, key_value) = obj_key_property_name(env, key)
                match key_value {
                  Some(key_value_item) => {
                    let _ = gc_root_push(key_value_item)

                  }
                  None => ()
                }
                let func_value = to_function_value(env, func, false)
                let getter = Value::Function(func_value)
                let _ = gc_root_push(getter)
                func_value.is_constructor = false
                set_function_name(
                  func_value,
                  function_name_from_key(name, key_value, Some("get ")),
                )
                func_value.home_object = Some(Value::Object(value))
                set_accessor_property(value.props, name, Some(getter), None)
              }
              @engine.ObjectProp::Setter(key, func) => {
                let (name, key_value) = obj_key_property_name(env, key)
                match key_value {
                  Some(key_value_item) => {
                    let _ = gc_root_push(key_value_item)

                  }
                  None => ()
                }
                let func_value = to_function_value(env, func, false)
                let setter = Value::Function(func_value)
                let _ = gc_root_push(setter)
                func_value.is_constructor = false
                set_function_name(
                  func_value,
                  function_name_from_key(name, key_value, Some("set ")),
                )
                func_value.home_object = Some(Value::Object(value))
                set_accessor_property(value.props, name, None, Some(setter))
              }
              @engine.ObjectProp::Spread(expr) => {
                let spread_value = eval_expr(env, expr)
                let _ = gc_root_push(spread_value)
                copy_data_properties(value, spread_value)
              }
            }
            Undefined
          })

        }
        obj
      }
      _ => obj
    }
  })
}

///|
fn gen_eval_obj_key(
  env : Env,
  key : @engine.ObjKey,
  k : (String, Value?) -> GenStep raise,
) -> GenStep raise {
  match key {
    @engine.ObjKey::Computed(expr) =>
      gen_eval_expr_cont(env, expr, fn(value) raise {
        let _ = gen_root_push(value)
        k(property_key_name(value), Some(value))
      })
    _ => k(obj_key_simple_name(key), None)
  }
}

///|
fn gen_eval_object_literal(
  env : Env,
  props : Array[@engine.ObjectProp],
  index : Int,
  target : ObjectValue,
  k : (Value) -> GenStep raise,
) -> GenStep raise {
  if index >= props.length() {
    return k(Value::Object(target))
  }
  let prop = props[index]
  match prop {
    @engine.ObjectProp::KeyValue(key, expr) =>
      if obj_key_is_proto(key) {
        gen_eval_expr_cont(env, expr, fn(value) raise {
          let _ = gen_root_push(value)
          match value {
            Null => target.proto = None
            _ if is_object_like(value) => target.proto = Some(value)
            _ => ()
          }
          gen_eval_object_literal(env, props, index + 1, target, k)
        })
      } else {
        gen_eval_obj_key(env, key, fn(name, key_value) raise {
          match key_value {
            Some(key_value_item) => {
              let _ = gen_root_push(key_value_item)

            }
            None => ()
          }
          gen_eval_named_expr_cont(env, expr, name, key_value, fn(value) raise {
            let _ = gen_root_push(value)
            target.props.set(name, property_data(value))
            gen_eval_object_literal(env, props, index + 1, target, k)
          })
        })
      }
    @engine.ObjectProp::Method(key, func) =>
      gen_eval_obj_key(env, key, fn(name, key_value) raise {
        match key_value {
          Some(key_value_item) => {
            let _ = gen_root_push(key_value_item)

          }
          None => ()
        }
        let func_value = to_function_value(env, func, false)
        let func = Value::Function(func_value)
        let _ = gen_root_push(func)
        func_value.is_constructor = false
        set_function_name(
          func_value,
          function_name_from_key(name, key_value, None),
        )
        func_value.home_object = Some(Value::Object(target))
        target.props.set(name, property_data(func))
        gen_eval_object_literal(env, props, index + 1, target, k)
      })
    @engine.ObjectProp::Shorthand(name) => {
      let value = env_get(env, name)
      let _ = gen_root_push(value)
      target.props.set(name, property_data(value))
      gen_eval_object_literal(env, props, index + 1, target, k)
    }
    @engine.ObjectProp::Getter(key, func) =>
      gen_eval_obj_key(env, key, fn(name, key_value) raise {
        match key_value {
          Some(key_value_item) => {
            let _ = gen_root_push(key_value_item)

          }
          None => ()
        }
        let func_value = to_function_value(env, func, false)
        func_value.is_constructor = false
        set_function_name(
          func_value,
          function_name_from_key(name, key_value, Some("get ")),
        )
        func_value.home_object = Some(Value::Object(target))
        let getter = Value::Function(func_value)
        let _ = gen_root_push(getter)
        set_accessor_property(target.props, name, Some(getter), None)
        gen_eval_object_literal(env, props, index + 1, target, k)
      })
    @engine.ObjectProp::Setter(key, func) =>
      gen_eval_obj_key(env, key, fn(name, key_value) raise {
        match key_value {
          Some(key_value_item) => {
            let _ = gen_root_push(key_value_item)

          }
          None => ()
        }
        let func_value = to_function_value(env, func, false)
        func_value.is_constructor = false
        set_function_name(
          func_value,
          function_name_from_key(name, key_value, Some("set ")),
        )
        func_value.home_object = Some(Value::Object(target))
        let setter = Value::Function(func_value)
        let _ = gen_root_push(setter)
        set_accessor_property(target.props, name, None, Some(setter))
        gen_eval_object_literal(env, props, index + 1, target, k)
      })
    @engine.ObjectProp::Spread(expr) =>
      gen_eval_expr_cont(env, expr, fn(value) raise {
        let _ = gen_root_push(value)
        copy_data_properties(target, value)
        gen_eval_object_literal(env, props, index + 1, target, k)
      })
  }
}

///|
fn obj_key_simple_name(key : @engine.ObjKey) -> String {
  match key {
    @engine.ObjKey::Ident(id) => id
    @engine.ObjKey::Private(id) => "#" + id
    @engine.ObjKey::String(s) => s
    @engine.ObjKey::Number(value) => number_to_string_radix(value, 10)
    @engine.ObjKey::Computed(_) => ""
  }
}

///|
fn obj_key_is_proto(key : @engine.ObjKey) -> Bool {
  match key {
    @engine.ObjKey::Ident(name) => name == "__proto__"
    @engine.ObjKey::String(name) => name == "__proto__"
    _ => false
  }
}

///|
fn obj_key_property_name(
  env : Env,
  key : @engine.ObjKey,
) -> (String, Value?) raise {
  match key {
    @engine.ObjKey::Computed(expr) => {
      let value = eval_expr(env, expr)
      (property_key_name(value), Some(value))
    }
    _ => (obj_key_simple_name(key), None)
  }
}

///|
fn gen_obj_key_property_name(
  env : Env,
  key : @engine.ObjKey,
  k : (String, Value?) -> GenStep raise,
) -> GenStep raise {
  match key {
    @engine.ObjKey::Computed(expr) =>
      gen_eval_expr_cont(env, expr, fn(value) raise {
        let _ = gen_root_push(value)
        k(property_key_name(value), Some(value))
      })
    _ => k(obj_key_simple_name(key), None)
  }
}
