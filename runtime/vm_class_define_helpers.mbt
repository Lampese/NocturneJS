///|
fn define_class_method(
  target : Value,
  name : String,
  func : FunctionValue,
) -> Unit raise {
  let desc = property_descriptor_object(
    property_data_non_enum(Value::Function(func)),
  )
  let _ = define_property(target, prop_key_value_from_name(name), desc)

}

///|
fn define_class_accessor(
  target : Value,
  name : String,
  getter : Value?,
  setter : Value?,
) -> Unit raise {
  let mut final_getter = getter
  let mut final_setter = setter
  match props_map_for_value(target) {
    Some(props) =>
      match props.get(name) {
        Some(prop) =>
          if prop.getter is Some(_) || prop.setter is Some(_) {
            if final_getter is None {
              final_getter = prop.getter
            }
            if final_setter is None {
              final_setter = prop.setter
            }
          }
        None => ()
      }
    None => ()
  }
  let desc = property_descriptor_object(Property::{
    value: Undefined,
    writable: false,
    configurable: true,
    enumerable: false,
    getter: final_getter,
    setter: final_setter,
  })
  let _ = define_property(target, prop_key_value_from_name(name), desc)

}

///|
priv enum ClassStaticInit {
  Field(ClassFieldRecord)
  Block(Array[@engine.Stmt])
}

///|
fn static_super_constructor_for_env(
  env : Env,
  ctor_func : FunctionValue,
) -> Value? {
  match ctor_func.super_constructor {
    Some(value) => Some(value)
    None =>
      match function_proto_for_env(env) {
        Some(value) => Some(value)
        None => function_proto()
      }
  }
}

///|
fn with_static_super_context(
  ctor_func : FunctionValue,
  static_super_ctor : Value?,
  f : () -> Value raise,
) -> Value raise {
  let saved_home = ctor_func.home_object
  let saved_super = ctor_func.super_constructor
  ctor_func.home_object = Some(Value::Function(ctor_func))
  ctor_func.super_constructor = static_super_ctor
  try with_current_function(ctor_func, f) catch {
    err => {
      ctor_func.home_object = saved_home
      ctor_func.super_constructor = saved_super
      raise err
    }
  } noraise {
    value => {
      ctor_func.home_object = saved_home
      ctor_func.super_constructor = saved_super
      value
    }
  }
}

///|
fn with_static_super_context_step(
  ctor_func : FunctionValue,
  static_super_ctor : Value?,
  f : () -> GenStep raise,
) -> GenStep raise {
  let saved_home = ctor_func.home_object
  let saved_super = ctor_func.super_constructor
  ctor_func.home_object = Some(Value::Function(ctor_func))
  ctor_func.super_constructor = static_super_ctor
  let step = with_current_function_step(ctor_func, f) catch {
    err => {
      ctor_func.home_object = saved_home
      ctor_func.super_constructor = saved_super
      raise err
    }
  }
  gen_bind_noraise(step, fn(control) {
    ctor_func.home_object = saved_home
    ctor_func.super_constructor = saved_super
    Done(control)
  })
}

///|
fn with_class_field_init_context(f : () -> Value raise) -> Value raise {
  let mut current : FunctionValue? = None
  let mut saved_flag = false
  match current_function() {
    Some(func) => {
      current = Some(func)
      saved_flag = func.in_class_field_init
      func.in_class_field_init = true
    }
    None => ()
  }
  try f() catch {
    err => {
      match current {
        Some(func) => func.in_class_field_init = saved_flag
        None => ()
      }
      raise err
    }
  } noraise {
    value => {
      match current {
        Some(func) => func.in_class_field_init = saved_flag
        None => ()
      }
      value
    }
  }
}

///|
fn with_class_field_init_context_step(f : () -> GenStep raise) -> GenStep raise {
  let mut current : FunctionValue? = None
  let mut saved_flag = false
  match current_function() {
    Some(func) => {
      current = Some(func)
      saved_flag = func.in_class_field_init
      func.in_class_field_init = true
    }
    None => ()
  }
  let step = f() catch {
    err => {
      match current {
        Some(func) => func.in_class_field_init = saved_flag
        None => ()
      }
      raise err
    }
  }
  gen_bind_noraise(step, fn(control) {
    match current {
      Some(func) => func.in_class_field_init = saved_flag
      None => ()
    }
    Done(control)
  })
}

///|
fn eval_class_field_initializer(env : Env, expr : @engine.Expr) -> Value raise {
  let saved_new_target = env.new_target
  env.new_target = Undefined
  try with_class_field_init_context(fn() raise { eval_expr(env, expr) }) catch {
    err => {
      env.new_target = saved_new_target
      raise err
    }
  } noraise {
    value => {
      env.new_target = saved_new_target
      value
    }
  }
}

///|
fn gen_eval_class_field_initializer(
  env : Env,
  expr : @engine.Expr,
) -> GenStep raise {
  let saved_new_target = env.new_target
  env.new_target = Undefined
  let step = with_class_field_init_context_step(fn() {
    gen_eval_expr_cont(env, expr, fn(value) { Done(Normal(value)) })
  })
  gen_bind(step, fn(control) {
    env.new_target = saved_new_target
    Done(control)
  })
}
