///|
fn build_spread_list(value : Value) -> Array[Value] raise {
  let (iterator, next_method) = get_iterator_from_value(value)
  let args : Array[Value] = []
  while true {
    try {
      let (done, item) = iterator_step_value(iterator, next_method)
      if done {
        break
      }
      args.push(item)
    } catch {
      err => {
        iterator_close_on_error(iterator)
        raise err
      }
    }
  }
  args
}

///|
fn build_arg_list(value : Value) -> Array[Value] raise {
  let target = to_object(value)
  let len_value = property_get(target, "length")
  let len_num = to_number(len_value)
  let mut len = 0
  if !(Double::is_nan(len_num) || len_num < 0.0) {
    len = Int::max(0, Int64::to_int(Double::to_int64(Double::trunc(len_num))))
  }
  let args : Array[Value] = []
  for i = 0; i < len; i = i + 1 {
    args.push(property_get(target, Int::to_string(i)))
  }
  args
}

///|
fn call_bound_function(
  bound : BoundFunctionValue,
  args : Array[Value],
  new_target? : Value = Undefined,
) -> Value raise {
  let final_args : Array[Value] = []
  for value in bound.bound_args {
    final_args.push(value)
  }
  for value in args {
    final_args.push(value)
  }
  call_value_with_this(bound.target, final_args, bound.bound_this, new_target~)
}

///|
fn call_value(value : Value, args : Array[Value]) -> Value raise {
  match value {
    Function(func) => call_user_function(func, args)
    BoundFunction(bound) => call_bound_function(bound, args)
    Builtin(builtin) => call_builtin_with_this(builtin, args, Undefined)
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => proxy_apply_value(data, Undefined, args)
        None => throw_type_error("not a function")
      }
    _ => throw_type_error("not a function")
  }
}

///|
fn call_value_with_this(
  value : Value,
  args : Array[Value],
  this_value : Value,
  new_target? : Value = Undefined,
) -> Value raise {
  match value {
    Function(func) =>
      call_user_function_with_this(func, args, this_value, new_target)
    BoundFunction(bound) => call_bound_function(bound, args, new_target~)
    Builtin(builtin) => call_builtin_with_this(builtin, args, this_value)
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => proxy_apply_value(data, this_value, args)
        None => throw_type_error("not a function")
      }
    _ => throw_type_error("not a function")
  }
}

///|
fn call_builtin_with_this(
  builtin : BuiltinValue,
  args : Array[Value],
  this_value : Value,
) -> Value raise {
  let target = match builtin.this_value {
    Some(_) => builtin
    None =>
      BuiltinValue::{
        kind: builtin.kind,
        id: builtin.id,
        props: builtin.props,
        this_value: Some(this_value),
        proto: builtin.proto,
        extensible: builtin.extensible,
        is_html_dda: builtin.is_html_dda,
        realm_env: builtin.realm_env,
      }
  }
  match builtin.realm_env {
    Some(env) => with_env_value(env, fn() raise { call_builtin(target, args) })
    None => call_builtin(target, args)
  }
}

///|
fn call_user_function(func : FunctionValue, args : Array[Value]) -> Value raise {
  call_user_function_with_this(func, args, Undefined, Undefined)
}

///|
fn call_user_function_body_impl(
  func : FunctionValue,
  args : Array[Value],
  this_value : Value,
  new_target : Value,
) -> Value raise {
  let is_derived_constructor = func.is_class_constructor &&
    func.super_constructor is Some(_)
  let init_fields_before_params = func.is_class_constructor &&
    !is_derived_constructor
  let has_param_expr = has_param_expressions(func)
  let this_bound = if func.is_arrow {
    Undefined
  } else if is_derived_constructor {
    Undefined
  } else if func.is_strict {
    this_value
  } else {
    coerce_this(func.env, this_value)
  }
  let arg_len = args.length()
  let simple_params = has_simple_parameter_list(func)
  if has_param_expr {
    let func_env = Env::new(Some(func.env))
    func_env.strict = func.is_strict
    if !func.is_arrow {
      func_env.new_target = new_target
      func_env.allow_new_target = true
      func_env.allow_super = func.home_object is Some(_)
      func_env.allow_super_call = func.is_class_constructor &&
        func.super_constructor is Some(_)
    }
    if is_derived_constructor {
      func_env.this_uninitialized = true
    }
    if !func.is_arrow {
      env_set_local(func_env, "this", this_bound)
    }
    match func.name {
      Some(name) =>
        if func.local_name_binding {
          env_define_readonly(func_env, name, Value::Function(func), false)
        }
      None => ()
    }
    let arguments_binding = !func.is_arrow &&
      !params_contains_name(func.params, "arguments")
    if arguments_binding {
      let args_value = new_arguments_value(
        func,
        func_env,
        args,
        func.is_strict,
        false,
      )
      env_set_local(func_env, "arguments", args_value)
    }
    let arg_env = Env::new(Some(func_env))
    arg_env.var_env = Some(arg_env)
    let param_env = Env::new(Some(arg_env))
    param_env.strict = func.is_strict
    param_env.var_env = Some(arg_env)
    let var_env = Env::new(Some(param_env))
    var_env.var_env = Some(var_env)
    func_env.var_env = Some(var_env)
    init_param_binding_names(param_env, func, arguments_binding)
    declare_param_bindings_uninitialized(param_env, func)
    let _ = with_current_function(func, fn() raise {
      if init_fields_before_params {
        init_instance_fields(func, param_env, this_bound)
      }
      with_env_value(param_env, fn() raise {
        for i, param in func.params {
          if param.is_rest {
            let rest_values : Array[Value?] = []
            let mut j = i
            while j < arg_len {
              rest_values.push(Some(args[j]))
              j = j + 1
            }
            initialize_var_binding(
              param_env,
              param.binding,
              new_array_value(rest_values),
            )
            break
          } else {
            let mut value = if i < arg_len { args[i] } else { Undefined }
            match func.param_inits[i] {
              Some(init) =>
                if value is Undefined {
                  value = match param.binding {
                    @engine.VarBinding::Name(name) =>
                      eval_named_expr(param_env, init, name, None)
                    _ => eval_expr(param_env, init)
                  }
                }
              None => ()
            }
            initialize_var_binding(param_env, param.binding, value)
          }
        }
        Undefined
      })
    })
    let result = with_current_function(func, fn() raise {
      if !is_derived_constructor && !init_fields_before_params {
        init_instance_fields(func, func_env, this_bound)
      }
      let body_env = prepare_function_body_env(var_env, func)
      let top_level_func_decls = collect_top_level_function_decls(func.body)
      let func_var_names = compute_annex_b_var_func_decls(
        func.body,
        body_env.strict,
        annex_b_param_names_for_func(func),
        !func.is_arrow,
      )
      with_top_level_function_decls(top_level_func_decls, fn() raise {
          with_annex_b_var_func_decls(func_var_names, fn() raise {
            hoist_decls(body_env, func.body, false, false)
            merge_arg_env_bindings(arg_env, var_env)
            with_env_value(body_env, fn() raise {
              try {
                let bytecode = bytecode_compile_function_body(func)
                match bytecode {
                  Some(code) => bytecode_exec(body_env, code)
                  None => {
                    let _ = throw_type_error("bytecode compile failed")
                    Undefined
                  }
                }
              } catch {
                ReturnSignal(value) => value
                BreakSignal((_, _)) => fail("illegal break")
                ContinueSignal((_, _)) => fail("illegal continue")
                ThrowSignal(value) => raise ThrowSignal(value)
              err => raise err
            }
          })
        })
      })
    })
    class_constructor_result(func, result, func_env, this_bound, new_target)
  } else {
    let frame = Env::new(Some(func.env))
    frame.var_env = Some(frame)
    frame.strict = func.is_strict
    if !func.is_arrow {
      frame.new_target = new_target
      frame.allow_new_target = true
      frame.allow_super = func.home_object is Some(_)
      frame.allow_super_call = func.is_class_constructor &&
        func.super_constructor is Some(_)
    }
    if is_derived_constructor {
      frame.this_uninitialized = true
    }
    if !func.is_arrow {
      env_set_local(frame, "this", this_bound)
    }
    match func.name {
      Some(name) =>
        if func.local_name_binding {
          env_define_readonly(frame, name, Value::Function(func), false)
        }
      None => ()
    }
    if !func.is_arrow && !params_contains_name(func.params, "arguments") {
      env_set_local(
        frame,
        "arguments",
        new_arguments_value(func, frame, args, func.is_strict, simple_params),
      )
    }
    if init_fields_before_params {
      let _ = with_current_function(func, fn() raise {
        init_instance_fields(func, frame, this_bound)
        Undefined
      })

    }
    let _ = with_env_value(frame, fn() raise {
      for i, param in func.params {
        if param.is_rest {
          let rest_values : Array[Value?] = []
          let mut j = i
          while j < arg_len {
            rest_values.push(Some(args[j]))
            j = j + 1
          }
          assign_var_binding(
            frame,
            param.binding,
            new_array_value(rest_values),
            false,
          )
          break
        } else {
          let mut value = if i < arg_len { args[i] } else { Undefined }
          match func.param_inits[i] {
            Some(init) =>
              if value is Undefined {
                value = match param.binding {
                  @engine.VarBinding::Name(name) =>
                    eval_named_expr(frame, init, name, None)
                  _ => eval_expr(frame, init)
                }
              }
            None => ()
          }
          assign_var_binding(frame, param.binding, value, false)
        }
      }
      Undefined
    })
    let result = with_current_function(func, fn() raise {
      if !is_derived_constructor && !init_fields_before_params {
        init_instance_fields(func, frame, this_bound)
      }
      let body_env = prepare_function_body_env(frame, func)
      let top_level_func_decls = collect_top_level_function_decls(func.body)
      let func_var_names = compute_annex_b_var_func_decls(
        func.body,
        body_env.strict,
        annex_b_param_names_for_func(func),
        !func.is_arrow,
      )
      with_top_level_function_decls(top_level_func_decls, fn() raise {
      with_annex_b_var_func_decls(func_var_names, fn() raise {
        hoist_decls(body_env, func.body, false, false)
        with_env_value(body_env, fn() raise {
          try {
            let bytecode = bytecode_compile_function_body(func)
            match bytecode {
              Some(code) => bytecode_exec(body_env, code)
              None => {
                let _ = throw_type_error("bytecode compile failed")
                Undefined
              }
            }
          } catch {
            ReturnSignal(value) => value
            BreakSignal((_, _)) => fail("illegal break")
            ContinueSignal((_, _)) => fail("illegal continue")
            ThrowSignal(value) => raise ThrowSignal(value)
              err => raise err
            }
          })
        })
      })
    })
    class_constructor_result(func, result, frame, this_bound, new_target)
  }
}

///|
fn call_user_function_body(
  func : FunctionValue,
  args : Array[Value],
  this_value : Value,
  new_target : Value,
) -> Value raise {
  with_function_source(func, fn() raise {
    call_user_function_body_impl(func, args, this_value, new_target)
  })
}

///|
fn call_user_function_with_this(
  func : FunctionValue,
  args : Array[Value],
  this_value : Value,
  new_target : Value,
) -> Value raise {
  if func.is_class_constructor && new_target is Undefined {
    return with_env_value(func.env, fn() raise {
      throw_type_error("class constructor cannot be invoked without 'new'")
    })
  }
  if func.is_generator {
    if func.is_async {
      return new_async_generator_value(func, args, this_value, new_target)
    }
    return new_generator_value(func, args, this_value, new_target)
  }
  if func.is_async {
    return call_async_function(func, args, this_value, new_target)
  }
  call_user_function_body(func, args, this_value, new_target)
}

///|
fn call_async_function(
  func : FunctionValue,
  args : Array[Value],
  this_value : Value,
  new_target : Value,
) -> Value raise {
  let (promise, resolve, reject) = new_promise_capability(Undefined)
  try {
    let gen_value = new_generator_value(func, args, this_value, new_target)
    match gen_value {
      Object(obj) => {
        async_function_data_set(obj.id, AsyncFunctionData::{ resolve, reject })
        async_function_resume(obj, GenResume::Next(Undefined))
      }
      _ => {
        let _ = call_value_with_this(
          reject,
          [type_error_value("async function setup failed")],
          Undefined,
        )

      }
    }
  } catch {
    ThrowSignal(err) => {
      let _ = call_value_with_this(reject, [err], Undefined)

    }
    err => raise err
  }
  promise
}

///|
fn new_async_generator_value(
  func : FunctionValue,
  args : Array[Value],
  this_value : Value,
  new_target : Value,
) -> Value raise {
  let value = new_generator_value(func, args, this_value, new_target)
  match value {
    Object(obj) => {
      obj.async_generator_data = Some(AsyncGeneratorData::{
        state: AsyncGeneratorState::SuspendedStart,
        queue_index: 0,
        queue: [],
      })
      value
    }
    _ => value
  }
}

///|
fn new_generator_value(
  func : FunctionValue,
  args : Array[Value],
  this_value : Value,
  new_target : Value,
) -> Value raise {
  let has_param_expr = has_param_expressions(func)
  let simple_params = has_simple_parameter_list(func)
  let this_bound = if func.is_arrow {
    Undefined
  } else if func.is_strict {
    this_value
  } else {
    coerce_this(func.env, this_value)
  }
  let arg_len = args.length()
  let mut arg_env_opt : Env? = None
  let frame = if has_param_expr {
    let func_env = Env::new(Some(func.env))
    func_env.strict = func.is_strict
    if !func.is_arrow {
      func_env.new_target = new_target
      func_env.allow_new_target = true
      func_env.allow_super = func.home_object is Some(_)
      func_env.allow_super_call = func.is_class_constructor &&
        func.super_constructor is Some(_)
    }
    if !func.is_arrow {
      env_set_local(func_env, "this", this_bound)
    }
    match func.name {
      Some(name) =>
        if func.local_name_binding {
          env_define_readonly(func_env, name, Value::Function(func), false)
        }
      None => ()
    }
    let arguments_binding = !func.is_arrow &&
      !params_contains_name(func.params, "arguments")
    if arguments_binding {
      let args_value = new_arguments_value(
        func,
        func_env,
        args,
        func.is_strict,
        simple_params,
      )
      env_set_local(func_env, "arguments", args_value)
    }
    let arg_env = Env::new(Some(func_env))
    arg_env.var_env = Some(arg_env)
    let param_env = Env::new(Some(arg_env))
    param_env.strict = func.is_strict
    arg_env_opt = Some(arg_env)
    param_env.var_env = Some(arg_env)
    let var_env = Env::new(Some(param_env))
    var_env.var_env = Some(var_env)
    func_env.var_env = Some(var_env)
    init_param_binding_names(param_env, func, arguments_binding)
    declare_param_bindings_uninitialized(param_env, func)
    let _ = with_current_function(func, fn() raise {
      with_env_value(param_env, fn() raise {
        for i, param in func.params {
          if param.is_rest {
            let rest_values : Array[Value?] = []
            let mut j = i
            while j < arg_len {
              rest_values.push(Some(args[j]))
              j = j + 1
            }
            initialize_var_binding(
              param_env,
              param.binding,
              new_array_value(rest_values),
            )
            break
          } else {
            let mut value = if i < arg_len { args[i] } else { Undefined }
            match func.param_inits[i] {
              Some(init) =>
                if value is Undefined {
                  value = match param.binding {
                    @engine.VarBinding::Name(name) =>
                      eval_named_expr(param_env, init, name, None)
                    _ => eval_expr(param_env, init)
                  }
                }
              None => ()
            }
            initialize_var_binding(param_env, param.binding, value)
          }
        }
        Undefined
      })
    })
    var_env
  } else {
    let frame = Env::new(Some(func.env))
    frame.var_env = Some(frame)
    frame.strict = func.is_strict
    if !func.is_arrow {
      frame.new_target = new_target
      frame.allow_new_target = true
      frame.allow_super = func.home_object is Some(_)
      frame.allow_super_call = func.is_class_constructor &&
        func.super_constructor is Some(_)
    }
    if !func.is_arrow {
      env_set_local(frame, "this", this_bound)
    }
    match func.name {
      Some(name) =>
        if func.local_name_binding {
          env_define_readonly(frame, name, Value::Function(func), false)
        }
      None => ()
    }
    if !func.is_arrow && !params_contains_name(func.params, "arguments") {
      env_set_local(
        frame,
        "arguments",
        new_arguments_value(func, frame, args, func.is_strict, simple_params),
      )
    }
    let _ = with_current_function(func, fn() raise {
      with_env_value(frame, fn() raise {
        for i, param in func.params {
          if param.is_rest {
            let rest_values : Array[Value?] = []
            let mut j = i
            while j < arg_len {
              rest_values.push(Some(args[j]))
              j = j + 1
            }
            assign_var_binding(
              frame,
              param.binding,
              new_array_value(rest_values),
              false,
            )
            break
          } else {
            let mut value = if i < arg_len { args[i] } else { Undefined }
            match func.param_inits[i] {
              Some(init) =>
                if value is Undefined {
                  value = match param.binding {
                    @engine.VarBinding::Name(name) =>
                      eval_named_expr(frame, init, name, None)
                    _ => eval_expr(frame, init)
                  }
                }
              None => ()
            }
            assign_var_binding(frame, param.binding, value, false)
          }
        }
        Undefined
      })
    })
    frame
  }
  let func_var_names = compute_annex_b_var_func_decls(
    func.body,
    frame.strict,
    annex_b_param_names_for_func(func),
    !func.is_arrow,
  )
  let _ = with_annex_b_var_func_decls(func_var_names, fn() raise {
    hoist_decls(frame, func.body, false, false)
    match arg_env_opt {
      Some(arg_env) => merge_arg_env_bindings(arg_env, frame)
      None => ()
    }
    Undefined
  })
  let body_env = prepare_function_body_env(frame, func)
  let top_level_func_decls = collect_top_level_function_decls(func.body)
  let bytecode_opt = bytecode_compile_function_body(func)
  let next_step = match bytecode_opt {
    Some(bytecode) => {
      let state = bytecode_exec_state_new(body_env, bytecode)
      fn(resume_state : GenResume) raise {
        let step = with_top_level_function_decls_step(top_level_func_decls, fn() raise {
          bytecode_exec_step(state, resume_state)
        })
        top_level_function_decls_wrap_step(step, top_level_func_decls)
      }
    }
    None => {
      let _ = throw_type_error("bytecode compile failed")
      fn(_ : GenResume) raise { Done(Normal(Undefined)) }
    }
  }
  let data = GeneratorData::{
    state: GeneratorState::SuspendedStart,
    env: body_env,
    func,
    next_step,
    roots: [],
  }
  ensure_function_prototype(func)
  let fallback_proto = if func.is_async {
    match value_from_object(async_generator_proto_for_env(func.env)) {
      Some(value) => Some(value)
      None => value_from_object(async_generator_proto())
    }
  } else {
    match value_from_object(generator_proto_for_env(func.env)) {
      Some(value) => Some(value)
      None => value_from_object(generator_proto())
    }
  }
  let proto_value = match props_get(func.props, "prototype") {
    Some(prop) =>
      if is_object_like(prop.value) {
        Some(prop.value)
      } else {
        fallback_proto
      }
    None => fallback_proto
  }
  let obj = register_object_value(ObjectValue::{
    id: alloc_id(),
    props: props_new(),
    proto: proto_value,
    string_data: None,
    number_data: None,
    bool_data: None,
    bigint_data: None,
    symbol_data: None,
    regexp_data: None,
    generator_data: Some(data),
    async_generator_data: None,
    proxy_data: None,
    map_data: None,
    set_data: None,
    map_iter_data: None,
    set_iter_data: None,
    iterator_wrap_data: None,
    iterator_concat_data: None,
    iterator_helper_data: None,
    weakmap_data: None,
    weakset_data: None,
    weakref_data: None,
    finreg_data: None,
    date_data: None,
    array_buffer_data: None,
    dataview_data: None,
    is_html_dda: false,
    is_error: false,
    is_module_namespace: false,
    realm_env: None,
    immutable_proto: false,
    extensible: true,
  })
  rc_incref_optional_value(proto_value)
  rc_incref_env(body_env)
  rc_incref_value(Function(func))
  Object(obj)
}
