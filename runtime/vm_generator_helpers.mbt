///|
fn generator_next_object(obj : ObjectValue, arg : Value) -> Value raise {
  let (value, done) = generator_step_result(obj, GenResume::Next(arg))
  let raw = take_yield_star_raw()
  if !done && raw {
    return value
  }
  generator_result_object(value, done)
}

///|
fn generator_return_object(obj : ObjectValue, arg : Value) -> Value raise {
  let (value, done) = generator_step_result(obj, GenResume::Return(arg))
  let raw = take_yield_star_raw()
  if !done && raw {
    return value
  }
  generator_result_object(value, done)
}

///|
fn generator_throw_object(obj : ObjectValue, arg : Value) -> Value raise {
  let (value, done) = generator_step_result(obj, GenResume::Throw(arg))
  let raw = take_yield_star_raw()
  if !done && raw {
    return value
  }
  generator_result_object(value, done)
}

///|
fn async_generator_request(
  this_value : Value?,
  resume_state : GenResume,
) -> Value raise {
  let (promise, resolve, reject) = new_promise_capability(Undefined)
  match this_value {
    Some(Object(obj)) =>
      match obj.async_generator_data {
        Some(data) =>
          async_generator_enqueue_request(
            obj, data, resume_state, promise, resolve, reject,
          )
        None => {
          let err = type_error_value("not an AsyncGenerator object")
          let _ = call_value_with_this(reject, [err], Undefined)

        }
      }
    _ => {
      let err = type_error_value("not an AsyncGenerator object")
      let _ = call_value_with_this(reject, [err], Undefined)

    }
  }
  promise
}

///|
fn async_generator_enqueue_request(
  obj : ObjectValue,
  data : AsyncGeneratorData,
  resume_state : GenResume,
  promise : Value,
  resolve : Value,
  reject : Value,
) -> Unit raise {
  data.queue.push(AsyncGeneratorRequest::{
    resume_state,
    promise,
    resolve,
    reject,
  })
  if !(data.state is AsyncGeneratorState::Executing) {
    async_generator_resume_next(obj, None)
  }
}

///|
fn async_generator_queue_empty(data : AsyncGeneratorData) -> Bool {
  data.queue_index >= data.queue.length()
}

///|
fn async_generator_queue_peek(
  data : AsyncGeneratorData,
) -> AsyncGeneratorRequest? {
  if async_generator_queue_empty(data) {
    None
  } else {
    Some(data.queue[data.queue_index])
  }
}

///|
fn async_generator_queue_pop(
  data : AsyncGeneratorData,
) -> AsyncGeneratorRequest? {
  if async_generator_queue_empty(data) {
    None
  } else {
    let request = data.queue[data.queue_index]
    data.queue_index = data.queue_index + 1
    Some(request)
  }
}

///|
fn async_generator_resolve_or_reject(
  data : AsyncGeneratorData,
  result : Value,
  is_reject : Bool,
) -> Unit raise {
  match async_generator_queue_pop(data) {
    Some(request) => {
      let handler = if is_reject { request.reject } else { request.resolve }
      let _ = call_value_with_this(handler, [result], Undefined)

    }
    None => ()
  }
}

///|
fn async_generator_resolve(
  data : AsyncGeneratorData,
  value : Value,
  done : Bool,
) -> Unit raise {
  let result = generator_result_object(value, done)
  async_generator_resolve_or_reject(data, result, false)
}

///|
fn async_generator_reject(
  data : AsyncGeneratorData,
  exception : Value,
) -> Unit raise {
  async_generator_resolve_or_reject(data, exception, true)
}

///|
fn async_generator_complete(
  obj : ObjectValue,
  data : AsyncGeneratorData,
) -> Unit {
  match data.state {
    AsyncGeneratorState::Completed => ()
    _ => data.state = AsyncGeneratorState::Completed
  }
  match obj.generator_data {
    Some(gen_data) => gen_data.state = GeneratorState::Completed
    None => ()
  }
}

///|
fn new_async_generator_resolve_function(
  generator : ObjectValue,
  resume_next : Bool,
  is_reject : Bool,
) -> Value {
  let func = new_builtin_value(BuiltinFunction::AsyncGeneratorResolveFunction)
  match func {
    Builtin(builtin) =>
      async_generator_resolve_func_data_set(builtin.id, AsyncGeneratorResolveFunctionData::{
        generator: Object(generator),
        resume_next,
        is_reject,
      })
    _ => ()
  }
  func
}

///|
fn new_async_generator_resolve_functions(
  generator : ObjectValue,
  resume_next : Bool,
) -> (Value, Value) {
  let resolve = new_async_generator_resolve_function(
    generator, resume_next, false,
  )
  let reject = new_async_generator_resolve_function(
    generator, resume_next, true,
  )
  (resolve, reject)
}

///|
fn async_generator_completed_return(
  obj : ObjectValue,
  data : AsyncGeneratorData,
  value : Value,
) -> Unit raise {
  let promise_ctor = promise_ctor_value_for_current_env()
  let mut promise = Undefined
  try {
    promise = promise_resolve_value(promise_ctor, value, false)
  } catch {
    ThrowSignal(err) => promise = promise_resolve_value(promise_ctor, err, true)
    err => raise err
  }
  match promise_data_from_value(promise) {
    Some(promise_data) => {
      let (resolve, reject) = new_async_generator_resolve_functions(obj, true)
      perform_promise_then(
        promise,
        promise_data,
        resolve,
        reject,
        Undefined,
        Undefined,
      )
    }
    None => {
      let _ = throw_type_error("not a Promise")

    }
  }
  let _ = data

}

///|
fn async_generator_await(obj : ObjectValue, value : Value) -> Unit raise {
  let promise_ctor = promise_ctor_value_for_current_env()
  let promise = promise_resolve_value(promise_ctor, value, false)
  match promise_data_from_value(promise) {
    Some(promise_data) => {
      let (resolve, reject) = new_async_generator_resolve_functions(obj, false)
      perform_promise_then(
        promise,
        promise_data,
        resolve,
        reject,
        Undefined,
        Undefined,
      )
    }
    None => {
      let _ = throw_type_error("not a Promise")

    }
  }
}

///|
fn async_generator_resume_next(
  obj : ObjectValue,
  resume_override : GenResume?,
) -> Unit raise {
  let data = match obj.async_generator_data {
    Some(data) => data
    None => return
  }
  let gen_data = match obj.generator_data {
    Some(gen_data) => gen_data
    None => return
  }
  let mut resume_from_await = resume_override
  while !async_generator_queue_empty(data) {
    match data.state {
      AsyncGeneratorState::Executing =>
        match resume_from_await {
          Some(resume_state) => {
            resume_from_await = None
            let step = generator_step_exec_raw(gen_data, resume_state) catch {
              ThrowSignal(value) => {
                data.state = AsyncGeneratorState::Completed
                async_generator_reject(data, value)
                continue
              }
              err => raise err
            }
            match step {
              Yield(value, _) => {
                data.state = AsyncGeneratorState::SuspendedYield
                async_generator_resolve(data, value, false)
              }
              Await(value, _) =>
                try {
                  async_generator_await(obj, value)
                  return
                } catch {
                  ThrowSignal(err) => {
                    resume_from_await = Some(GenResume::Throw(err))
                    continue
                  }
                  err => raise err
                }
              Done(control) => {
                data.state = AsyncGeneratorState::Completed
                match control {
                  Normal(value) => async_generator_resolve(data, value, true)
                  Return(value) => async_generator_resolve(data, value, true)
                  Throw(value) => async_generator_reject(data, value)
                  Break(_, _) =>
                    async_generator_reject(
                      data,
                      type_error_value("invalid break in generator"),
                    )
                  Continue(_, _) =>
                    async_generator_reject(
                      data,
                      type_error_value("invalid continue in generator"),
                    )
                }
              }
            }
            continue
          }
          None => return
        }
      AsyncGeneratorState::AwaitingReturn => return
      AsyncGeneratorState::SuspendedStart => {
        let request = match async_generator_queue_peek(data) {
          Some(request) => request
          None => return
        }
        match request.resume_state {
          GenResume::Next(_) => ()
          _ => {
            async_generator_complete(obj, data)
            continue
          }
        }
      }
      AsyncGeneratorState::Completed => {
        let request = match async_generator_queue_peek(data) {
          Some(request) => request
          None => return
        }
        match request.resume_state {
          GenResume::Next(_) => async_generator_resolve(data, Undefined, true)
          GenResume::Return(value) => {
            data.state = AsyncGeneratorState::AwaitingReturn
            async_generator_completed_return(obj, data, value) catch {
              ThrowSignal(err) => {
                data.state = AsyncGeneratorState::Completed
                async_generator_reject(data, err)
              }
              err => raise err
            }
          }
          GenResume::Throw(value) => async_generator_reject(data, value)
        }
        return
      }
      AsyncGeneratorState::SuspendedYield => ()
    }
    let request = match async_generator_queue_peek(data) {
      Some(request) => request
      None => return
    }
    data.state = AsyncGeneratorState::Executing
    let step = generator_step_exec_raw(gen_data, request.resume_state) catch {
      ThrowSignal(value) => {
        data.state = AsyncGeneratorState::Completed
        async_generator_reject(data, value)
        continue
      }
      err => raise err
    }
    match step {
      Yield(value, _) => {
        data.state = AsyncGeneratorState::SuspendedYield
        async_generator_resolve(data, value, false)
      }
      Await(value, _) =>
        try {
          async_generator_await(obj, value)
          return
        } catch {
          ThrowSignal(err) => {
            resume_from_await = Some(GenResume::Throw(err))
            continue
          }
          err => raise err
        }
      Done(control) => {
        data.state = AsyncGeneratorState::Completed
        match control {
          Normal(value) => async_generator_resolve(data, value, true)
          Return(value) => async_generator_resolve(data, value, true)
          Throw(value) => async_generator_reject(data, value)
          Break(_, _) =>
            async_generator_reject(
              data,
              type_error_value("invalid break in generator"),
            )
          Continue(_, _) =>
            async_generator_reject(
              data,
              type_error_value("invalid continue in generator"),
            )
        }
      }
    }
  }
}

///|
fn new_async_function_resolve_function(
  generator : ObjectValue,
  is_reject : Bool,
) -> Value {
  let func = new_builtin_value(BuiltinFunction::AsyncFunctionResolveFunction)
  match func {
    Builtin(builtin) =>
      async_function_resolve_func_data_set(builtin.id, AsyncFunctionResolveFunctionData::{
        generator: Object(generator),
        is_reject,
      })
    _ => ()
  }
  func
}

///|
fn new_async_function_resolve_functions(
  generator : ObjectValue,
) -> (Value, Value) {
  let resolve = new_async_function_resolve_function(generator, false)
  let reject = new_async_function_resolve_function(generator, true)
  (resolve, reject)
}

///|
fn async_function_await(obj : ObjectValue, value : Value) -> Unit raise {
  let promise_ctor = promise_ctor_value_for_current_env()
  let promise = promise_resolve_value(promise_ctor, value, false)
  match promise_data_from_value(promise) {
    Some(promise_data) => {
      let (resolve, reject) = new_async_function_resolve_functions(obj)
      perform_promise_then(
        promise,
        promise_data,
        resolve,
        reject,
        Undefined,
        Undefined,
      )
    }
    None => {
      let _ = throw_type_error("not a Promise")

    }
  }
}

///|
fn async_function_resume(
  obj : ObjectValue,
  resume_state : GenResume,
) -> Unit raise {
  let gen_data = match obj.generator_data {
    Some(data) => data
    None => return
  }
  let async_data = match async_function_data_get(obj.id) {
    Some(data) => data
    None => return
  }
  let step = generator_step_exec_raw(gen_data, resume_state) catch {
    ThrowSignal(err) => {
      async_function_data_clear(obj.id)
      let _ = call_value_with_this(async_data.reject, [err], Undefined)
      return
    }
    err => raise err
  }
  match step {
    Yield(_, _) => {
      async_function_data_clear(obj.id)
      let _ = call_value_with_this(
        async_data.reject,
        [type_error_value("yield in async function")],
        Undefined,
      )

    }
    Await(value, _) =>
      async_function_await(obj, value) catch {
        ThrowSignal(err) => {
          async_function_data_clear(obj.id)
          let _ = call_value_with_this(async_data.reject, [err], Undefined)

        }
        err => raise err
      }
    Done(control) => {
      async_function_data_clear(obj.id)
      match control {
        Normal(value) => {
          let _ = call_value_with_this(async_data.resolve, [value], Undefined)

        }
        Return(value) => {
          let _ = call_value_with_this(async_data.resolve, [value], Undefined)

        }
        Throw(value) => {
          let _ = call_value_with_this(async_data.reject, [value], Undefined)

        }
        Break(_, _) => fail("illegal break")
        Continue(_, _) => fail("illegal continue")
      }
    }
  }
}

///|
fn generator_step_result(
  obj : ObjectValue,
  resume_state : GenResume,
) -> (Value, Bool) raise {
  match obj.generator_data {
    Some(data) =>
      match data.state {
        GeneratorState::Executing => {
          let _ = throw_type_error("cannot invoke a running generator")
          (Undefined, true)
        }
        GeneratorState::Completed =>
          match resume_state {
            GenResume::Next(_) => (Undefined, true)
            GenResume::Return(value) => (value, true)
            GenResume::Throw(value) => raise ThrowSignal(value)
          }
        GeneratorState::SuspendedStart =>
          match resume_state {
            GenResume::Next(_) =>
              generator_step_exec(data, GenResume::Next(Undefined))
            GenResume::Return(value) => {
              data.state = GeneratorState::Completed
              (value, true)
            }
            GenResume::Throw(value) => {
              data.state = GeneratorState::Completed
              raise ThrowSignal(value)
            }
          }
        _ => generator_step_exec(data, resume_state)
      }
    None => {
      let _ = throw_type_error("not a generator")
      (Undefined, true)
    }
  }
}

///|
fn generator_step_exec_raw(
  data : GeneratorData,
  resume_state : GenResume,
) -> GenStep raise {
  data.state = GeneratorState::Executing
  try {
    let func_var_names = compute_annex_b_var_func_decls(
      data.func.body,
      data.func.is_strict,
      annex_b_param_names_for_func(data.func),
      !data.func.is_arrow,
    )
    let step = with_function_source_step(data.func, fn() raise {
      with_annex_b_var_func_decls(func_var_names, fn() raise {
        with_current_function_step(data.func, fn() raise {
          with_current_generator_data_step(data, fn() raise {
            with_env_step(data.env, fn() raise {
              (data.next_step)(resume_state)
            })
          })
        })
      })
    })
    match step {
      Yield(value, next_step) => {
        data.next_step = next_step
        data.state = GeneratorState::SuspendedYield
        Yield(value, next_step)
      }
      Await(value, next_step) => {
        data.next_step = next_step
        Await(value, next_step)
      }
      Done(control) => {
        data.state = GeneratorState::Completed
        Done(control)
      }
    }
  } catch {
    ThrowSignal(value) => {
      data.state = GeneratorState::Completed
      raise ThrowSignal(value)
    }
    err => raise err
  }
}

///|
fn generator_step_exec(
  data : GeneratorData,
  resume_state : GenResume,
) -> (Value, Bool) raise {
  let step = generator_step_exec_raw(data, resume_state)
  match step {
    Yield(value, _) => (value, false)
    Await(_, _) => {
      data.state = GeneratorState::Completed
      let _ = throw_type_error("await not supported")
      (Undefined, true)
    }
    Done(control) =>
      match control {
        Normal(value) => (value, true)
        Return(value) => (value, true)
        Throw(value) => raise ThrowSignal(value)
        Break(_, _) => {
          let _ = throw_type_error("invalid break in generator")
          (Undefined, true)
        }
        Continue(_, _) => {
          let _ = throw_type_error("invalid continue in generator")
          (Undefined, true)
        }
      }
  }
}

///|
fn generator_result_object(value : Value, done : Bool) -> Value {
  let result = new_object_value()
  match result {
    Object(obj) => {
      obj.props.set("value", property_data(value))
      obj.props.set("done", property_data(Bool(done)))
    }
    _ => ()
  }
  result
}
