///|
let root_env_ref : Ref[Env?] = Ref::new(None)

///|
let env_stack_ref : Ref[Array[Env]] = Ref::new([])

///|
let source_stack_ref : Ref[Array[String]] = Ref::new([])

///|
let expr_pos_stack_ref : Ref[Array[Int]] = Ref::new([])

///|
let source_path_stack_ref : Ref[Array[String]] = Ref::new([])

///|
let current_function_stack_ref : Ref[Array[FunctionValue]] = Ref::new([])

///|
let current_generator_data_ref : Ref[GeneratorData?] = Ref::new(None)

///|
let max_call_depth : Int = 1000

///|
let yield_star_raw_ref : Ref[Bool] = Ref::new(false)

///|
fn set_root_env(env : Env) -> Unit {
  root_env_ref.update(fn(_) { Some(env) })
}

///|
fn root_env() -> Env? {
  let mut value : Env? = None
  root_env_ref.update(fn(current) {
    value = current
    current
  })
  value
}

///|
fn with_root_env(env : Env, f : () -> Value raise) -> Value raise {
  let prev = root_env()
  set_root_env(env)
  try f() catch {
    err => {
      match prev {
        Some(prev_env) => set_root_env(prev_env)
        None => ()
      }
      raise err
    }
  } noraise {
    result => {
      match prev {
        Some(prev_env) => set_root_env(prev_env)
        None => ()
      }
      result
    }
  }
}

///|
fn clone_root_env() -> Env {
  match root_env() {
    Some(env) => {
      let cloned = Env::{
        id: alloc_id(),
        parent: None,
        bindings: env.bindings.copy(),
        readonly_bindings: env.readonly_bindings.copy(),
        deletable_bindings: env.deletable_bindings.copy(),
        private_bindings: env.private_bindings.copy(),
        strict: env.strict,
        with_object: None,
        var_object: None,
        new_target: Undefined,
        allow_new_target: false,
        allow_super: false,
        allow_super_call: false,
        this_uninitialized: false,
        is_param_env: false,
        is_catch_env: false,
        param_binding_names: @hashmap.new(),
        uninitialized_bindings: @hashmap.new(),
        var_env: None,
      }
      register_env(cloned)
      cloned
    }
    None => Env::new(None)
  }
}

///|
fn push_env(env : Env) -> Unit {
  env_stack_ref.update(fn(stack) {
    stack.push(env)
    stack
  })
}

///|
fn pop_env() -> Unit {
  env_stack_ref.update(fn(stack) {
    let _ = stack.pop()
    stack
  })
}

///|
fn env_stack() -> Array[Env] {
  let mut value : Array[Env] = []
  env_stack_ref.update(fn(current) {
    value = current
    current
  })
  value
}

///|
fn current_env() -> Env? {
  let stack = env_stack()
  if stack.is_empty() {
    root_env()
  } else {
    Some(stack[stack.length() - 1])
  }
}

///|
fn push_source(source : String) -> Unit {
  source_stack_ref.update(fn(current) {
    current.push(source)
    current
  })
}

///|
fn pop_source() -> Unit {
  source_stack_ref.update(fn(current) {
    if !current.is_empty() {
      let _ = current.pop()

    }
    current
  })
}

///|
fn current_source() -> String? {
  let mut value : String? = None
  source_stack_ref.update(fn(current) {
    if !current.is_empty() {
      value = Some(current[current.length() - 1])
    }
    current
  })
  value
}

///|
fn push_source_path(path : String) -> Unit {
  source_path_stack_ref.update(fn(current) {
    current.push(path)
    current
  })
}

///|
fn pop_source_path() -> Unit {
  source_path_stack_ref.update(fn(current) {
    if !current.is_empty() {
      let _ = current.pop()

    }
    current
  })
}

///|
fn current_source_path() -> String? {
  let mut value : String? = None
  source_path_stack_ref.update(fn(current) {
    if !current.is_empty() {
      value = Some(current[current.length() - 1])
    }
    current
  })
  value
}

///|
fn with_source_value(source : String, f : () -> Value raise) -> Value raise {
  push_source(source)
  try {
    let result = f()
    pop_source()
    result
  } catch {
    err => {
      pop_source()
      raise err
    }
  }
}

///|
fn with_source_value_step(
  source : String,
  f : () -> GenStep raise,
) -> GenStep raise {
  push_source(source)
  try {
    let result = f()
    pop_source()
    result
  } catch {
    err => {
      pop_source()
      raise err
    }
  }
}

///|
fn with_function_source(
  func : FunctionValue,
  f : () -> Value raise,
) -> Value raise {
  match func.source_root {
    Some(source) => with_source_value(source, f)
    None => f()
  }
}

///|
fn with_function_source_step(
  func : FunctionValue,
  f : () -> GenStep raise,
) -> GenStep raise {
  match func.source_root {
    Some(source) => with_source_value_step(source, f)
    None => f()
  }
}

///|
fn with_source_path(path : String, f : () -> Value raise) -> Value raise {
  push_source_path(path)
  try {
    let result = f()
    pop_source_path()
    result
  } catch {
    err => {
      pop_source_path()
      raise err
    }
  }
}

///|
fn with_source_path_string_option(
  path : String,
  f : () -> String? raise,
) -> String? raise {
  push_source_path(path)
  try {
    let result = f()
    pop_source_path()
    result
  } catch {
    err => {
      pop_source_path()
      raise err
    }
  }
}

///|
fn with_source_path_step(
  path : String,
  f : () -> GenStep raise,
) -> GenStep raise {
  push_source_path(path)
  try {
    let result = f()
    pop_source_path()
    result
  } catch {
    err => {
      pop_source_path()
      raise err
    }
  }
}

///|

///|
fn set_yield_star_raw(value : Bool) -> Unit {
  let _ = yield_star_raw_ref.update(fn(_) { value })

}

///|
fn take_yield_star_raw() -> Bool {
  let mut value = false
  let _ = yield_star_raw_ref.update(fn(current) {
    value = current
    false
  })
  value
}

///|

///|

///|
fn push_expr_pos(offset : Int) -> Unit {
  expr_pos_stack_ref.update(fn(current) {
    current.push(offset)
    current
  })
}

///|
fn pop_expr_pos() -> Unit {
  expr_pos_stack_ref.update(fn(current) {
    if !current.is_empty() {
      let _ = current.pop()

    }
    current
  })
}

///|
fn expr_pos_stack() -> Array[Int] {
  let mut value : Array[Int] = []
  expr_pos_stack_ref.update(fn(current) {
    value = current
    current
  })
  value
}

///|
fn with_expr_pos(offset : Int, f : () -> Value raise) -> Value raise {
  push_expr_pos(offset)
  try {
    let result = f()
    pop_expr_pos()
    result
  } catch {
    err => {
      pop_expr_pos()
      raise err
    }
  }
}

///|
fn with_expr_pos_call_ref(
  offset : Int,
  f : () -> (Value, Value) raise,
) -> (Value, Value) raise {
  push_expr_pos(offset)
  try {
    let result = f()
    pop_expr_pos()
    result
  } catch {
    err => {
      pop_expr_pos()
      raise err
    }
  }
}

///|
fn current_function() -> FunctionValue? {
  let mut value : FunctionValue? = None
  current_function_stack_ref.update(fn(current) {
    if !current.is_empty() {
      value = Some(current[current.length() - 1])
    }
    current
  })
  value
}

///|
fn current_function_stack() -> Array[FunctionValue] {
  let mut value : Array[FunctionValue] = []
  current_function_stack_ref.update(fn(current) {
    value = current
    current
  })
  value
}

///|
fn current_function_is_async_generator() -> Bool {
  match current_function() {
    Some(func) => func.is_async && func.is_generator
    None => false
  }
}

///|
fn current_function_is_generator() -> Bool {
  match current_function() {
    Some(func) => func.is_generator
    None => false
  }
}

///|
fn in_class_field_init_context() -> Bool {
  match current_function() {
    Some(func) => func.in_class_field_init
    None => false
  }
}

///|
fn current_super_function() -> FunctionValue? {
  let mut value : FunctionValue? = None
  current_function_stack_ref.update(fn(current) {
    let mut i = current.length()
    while i > 0 {
      i = i - 1
      let func = current[i]
      if !func.is_arrow ||
        func.home_object is Some(_) ||
        func.super_constructor is Some(_) {
        value = Some(func)
        break
      }
    }
    current
  })
  value
}

///|
fn with_current_function(
  func : FunctionValue,
  f : () -> Value raise,
) -> Value raise {
  let mut depth = 0
  current_function_stack_ref.update(fn(current) {
    depth = current.length()
    current
  })
  if depth >= max_call_depth {
    let _ = throw_range_error("stack overflow")

  }
  current_function_stack_ref.update(fn(current) {
    current.push(func)
    current
  })
  try {
    let result = f()
    current_function_stack_ref.update(fn(current) {
      let _ = current.pop()
      current
    })
    result
  } catch {
    err => {
      current_function_stack_ref.update(fn(current) {
        let _ = current.pop()
        current
      })
      raise err
    }
  }
}

///|
fn with_current_function_step(
  func : FunctionValue,
  f : () -> GenStep raise,
) -> GenStep raise {
  let mut depth = 0
  current_function_stack_ref.update(fn(current) {
    depth = current.length()
    current
  })
  if depth >= max_call_depth {
    let _ = throw_range_error("stack overflow")

  }
  current_function_stack_ref.update(fn(current) {
    current.push(func)
    current
  })
  try {
    let result = f()
    current_function_stack_ref.update(fn(current) {
      let _ = current.pop()
      current
    })
    result
  } catch {
    err => {
      current_function_stack_ref.update(fn(current) {
        let _ = current.pop()
        current
      })
      raise err
    }
  }
}

///|
fn with_current_generator_data_step(
  data : GeneratorData,
  f : () -> GenStep raise,
) -> GenStep raise {
  let mut saved : GeneratorData? = None
  current_generator_data_ref.update(fn(current) {
    saved = current
    Some(data)
  })
  try {
    let result = f()
    current_generator_data_ref.update(fn(_) { saved })
    result
  } catch {
    err => {
      current_generator_data_ref.update(fn(_) { saved })
      raise err
    }
  }
}

///|
fn gen_root_depth() -> Int {
  let mut depth = 0
  current_generator_data_ref.update(fn(current) {
    match current {
      Some(data) => depth = data.roots.length()
      None => depth = 0
    }
    current
  })
  depth
}

///|
fn gen_root_push(value : Value) -> Int {
  let mut depth = 0
  current_generator_data_ref.update(fn(current) {
    match current {
      Some(data) => {
        data.roots.push(value)
        depth = data.roots.length()
      }
      None => depth = 0
    }
    current
  })
  depth
}

///|
fn gen_root_pop_to(depth : Int) -> Unit {
  current_generator_data_ref.update(fn(current) {
    match current {
      Some(data) =>
        while data.roots.length() > depth {
          let _ = data.roots.pop()

        }
      None => ()
    }
    current
  })
}

///|
fn gen_wrap_step_with_root(base : Int, step : GenStep) -> GenStep {
  match step {
    Yield(value, next_step) =>
      Yield(value, fn(resume_state) raise {
        gen_wrap_step_with_root(base, next_step(resume_state))
      })
    Await(value, next_step) =>
      Await(value, fn(resume_state) raise {
        gen_wrap_step_with_root(base, next_step(resume_state))
      })
    Done(control) => {
      gen_root_pop_to(base)
      Done(control)
    }
  }
}

///|
fn gen_with_root_frame(f : () -> GenStep raise) -> GenStep raise {
  let base = gen_root_depth()
  try {
    let step = f()
    gen_wrap_step_with_root(base, step)
  } catch {
    err => {
      gen_root_pop_to(base)
      raise err
    }
  }
}

///|
fn with_env_value(env : Env, f : () -> Value raise) -> Value raise {
  push_env(env)
  try {
    let result = f()
    pop_env()
    result
  } catch {
    err => {
      pop_env()
      raise err
    }
  }
}

///|
fn with_env_step(env : Env, f : () -> GenStep raise) -> GenStep raise {
  push_env(env)
  try {
    let result = f()
    pop_env()
    result
  } catch {
    err => {
      pop_env()
      raise err
    }
  }
}
