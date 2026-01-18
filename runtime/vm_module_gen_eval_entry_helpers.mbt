///|
fn gen_eval_module_body(
  env : Env,
  body : Array[@engine.Stmt],
  result_value : Value,
) -> GenStep raise {
  gen_bind(gen_eval_stmt_seq(env, body, 0), fn(control) {
    match control {
      Normal(_) => Done(Normal(result_value))
      Return(_) => Done(Throw(module_syntax_error_value(env, "illegal return")))
      Break(_, _) =>
        Done(Throw(module_syntax_error_value(env, "illegal break")))
      Continue(_, _) =>
        Done(Throw(module_syntax_error_value(env, "illegal continue")))
      Throw(value) => Done(Throw(value))
    }
  })
}

///|
fn module_eval_with_context_step(
  source_path : String,
  source_value : String,
  exports_obj : ObjectValue,
  env_id : Int,
  f : () -> GenStep raise,
) -> GenStep raise {
  with_source_path_step(source_path, fn() raise {
    with_source_value_step(source_value, fn() raise {
      with_module_exports_step(exports_obj, fn() raise {
        with_module_env_id_step(env_id, f)
      })
    })
  })
}

///|
fn module_eval_wrap_next_step(
  next_step : (GenResume) -> GenStep raise,
  source_path : String,
  source_value : String,
  exports_obj : ObjectValue,
  env_id : Int,
) -> (GenResume) -> GenStep raise {
  fn(resume_) raise {
    let step = module_eval_with_context_step(
      source_path,
      source_value,
      exports_obj,
      env_id,
      fn() raise { next_step(resume_) },
    )
    module_eval_wrap_step(step, source_path, source_value, exports_obj, env_id)
  }
}

///|
fn module_eval_wrap_step(
  step : GenStep,
  source_path : String,
  source_value : String,
  exports_obj : ObjectValue,
  env_id : Int,
) -> GenStep {
  match step {
    Yield(value, next_step) =>
      Yield(
        value,
        module_eval_wrap_next_step(
          next_step, source_path, source_value, exports_obj, env_id,
        ),
      )
    Await(value, next_step) =>
      Await(
        value,
        module_eval_wrap_next_step(
          next_step, source_path, source_value, exports_obj, env_id,
        ),
      )
    Done(control) => Done(control)
  }
}

///|
fn module_async_eval(
  env : Env,
  body : Array[@engine.Stmt],
  result_value : Value,
  source_path : String,
  source_value : String,
  exports_obj : ObjectValue,
) -> Value raise {
  let func_def = @engine.FunctionDef::new(
    None,
    ([] : Array[@engine.ParamBinding]),
    ([] : Array[@engine.Expr?]),
    body,
    true,
    false,
    true,
    false,
    0,
    0,
  )
  let func_value = to_function_value(env, func_def, false)
  let top_level_func_decls = collect_top_level_function_decls(body)
  let next_step = fn(_ : GenResume) raise {
    let step = module_eval_with_context_step(
      source_path,
      source_value,
      exports_obj,
      env.id,
      fn() raise {
        let inner = with_top_level_function_decls_step(top_level_func_decls, fn() raise {
          gen_eval_module_body(env, body, result_value)
        })
        top_level_function_decls_wrap_step(inner, top_level_func_decls)
      },
    )
    module_eval_wrap_step(step, source_path, source_value, exports_obj, env.id)
  }
  let gen_data = GeneratorData::{
    state: GeneratorState::SuspendedStart,
    env,
    func: func_value,
    next_step,
    roots: [],
  }
  ensure_function_prototype(func_value)
  let fallback_proto = match
    value_from_object(async_generator_proto_for_env(env)) {
    Some(value) => Some(value)
    None => value_from_object(async_generator_proto())
  }
  let proto_value = match func_value.props.get("prototype") {
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
    props: Map::new(),
    proto: proto_value,
    string_data: None,
    number_data: None,
    bool_data: None,
    bigint_data: None,
    symbol_data: None,
    regexp_data: None,
    generator_data: Some(gen_data),
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
  let (promise, resolve, reject) = new_promise_capability(Undefined)
  async_function_data_set(obj.id, AsyncFunctionData::{ resolve, reject })
  async_function_resume(obj, GenResume::Next(Undefined))
  promise
}
