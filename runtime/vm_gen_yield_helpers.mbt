///|
fn gen_eval_yield_star(
  env : Env,
  expr : @engine.Expr,
  k : (Value) -> GenStep raise,
) -> GenStep raise {
  gen_eval_expr_cont(env, expr, fn(value) raise {
    gen_with_root_frame(fn() raise {
      let is_async = match current_function() {
        Some(func) => func.is_async
        None => false
      }
      if is_async {
        let (iterator, next_method) = get_async_iterator_from_value(value)
        let _ = gen_root_push(iterator)
        let _ = gen_root_push(next_method)
        gen_async_yield_star_loop(
          iterator,
          next_method,
          GenResume::Next(Undefined),
          true,
          false,
          k,
        )
      } else {
        let (iterator, next_method) = get_iterator_from_value(value)
        let _ = gen_root_push(iterator)
        let _ = gen_root_push(next_method)
        gen_yield_star_loop(
          iterator,
          next_method,
          GenResume::Next(Undefined),
          false,
          k,
        )
      }
    })
  })
}

///|
fn gen_yield_star_loop(
  iterator : Value,
  next_method : Value,
  resume_state : GenResume,
  returning : Bool,
  k : (Value) -> GenStep raise,
) -> GenStep raise {
  try {
    let mut return_mode = returning
    let result = match resume_state {
      GenResume::Next(arg) => {
        let argv = [arg]
        call_value_with_this(next_method, argv, iterator)
      }
      GenResume::Return(arg) => {
        return_mode = true
        let return_method = property_get(iterator, "return")
        match return_method {
          Undefined | Null => return Done(Return(arg))
          _ =>
            if !is_callable(return_method) {
              let _ = throw_type_error("not a function")
              return Done(Throw(Undefined))
            } else {
              call_value_with_this(return_method, [arg], iterator)
            }
        }
      }
      GenResume::Throw(arg) => {
        let throw_method = property_get(iterator, "throw")
        match throw_method {
          Undefined | Null => {
            let return_method = property_get(iterator, "return")
            match return_method {
              Undefined | Null => ()
              _ => {
                let _ = call_value_with_this(return_method, [], iterator)

              }
            }
            let _ = throw_type_error("iterator does not have a throw method")
            return Done(Throw(Undefined))
          }
          _ =>
            if !is_callable(throw_method) {
              let _ = throw_type_error("not a function")
              return Done(Throw(Undefined))
            } else {
              call_value_with_this(throw_method, [arg], iterator)
            }
        }
      }
    }
    if !is_object_like(result) {
      let _ = throw_type_error("iterator must return an object")
      return Done(Throw(Undefined))
    }
    let done = is_truthy(property_get(result, "done"))
    if done {
      let value = property_get(result, "value")
      if return_mode {
        Done(Return(value))
      } else {
        k(value)
      }
    } else {
      set_yield_star_raw(true)
      Yield(result, fn(next_resume) raise {
        gen_yield_star_loop(iterator, next_method, next_resume, return_mode, k)
      })
    }
  } catch {
    ThrowSignal(err) => Done(Throw(err))
    err => raise err
  }
}

///|
fn gen_async_yield_star_loop(
  iterator : Value,
  next_method : Value,
  resume_state : GenResume,
  first : Bool,
  returning : Bool,
  k : (Value) -> GenStep raise,
) -> GenStep raise {
  if !first {
    return gen_unwrap_async_yield_resume(resume_state, fn(unwrapped) raise {
      gen_async_yield_star_loop(
        iterator, next_method, unwrapped, true, returning, k,
      )
    })
  }
  let mut return_mode = returning
  let result = match resume_state {
    GenResume::Next(arg) => {
      let argv = [arg]
      call_value_with_this(next_method, argv, iterator)
    }
    GenResume::Return(arg) => {
      return_mode = true
      let return_method = property_get(iterator, "return")
      match return_method {
        Undefined | Null => return gen_async_return(arg)
        _ =>
          if !is_callable(return_method) {
            let _ = throw_type_error("not a function")
            return Done(Throw(Undefined))
          } else {
            call_value_with_this(return_method, [arg], iterator)
          }
      }
    }
    GenResume::Throw(arg) => {
      let throw_method = property_get(iterator, "throw")
      match throw_method {
        Undefined | Null => {
          async_iterator_close_on_error(iterator)
          let _ = throw_type_error("iterator does not have a throw method")
          return Done(Throw(Undefined))
        }
        _ =>
          if !is_callable(throw_method) {
            let _ = throw_type_error("not a function")
            return Done(Throw(Undefined))
          } else {
            call_value_with_this(throw_method, [arg], iterator)
          }
      }
    }
  }
  Await(result, fn(result_resume) raise {
    match result_resume {
      GenResume::Next(awaited) => {
        if !is_object_like(awaited) {
          let _ = throw_type_error("iterator must return an object")
          return Done(Throw(Undefined))
        }
        let done = is_truthy(property_get(awaited, "done")) catch {
          ThrowSignal(err) => return Done(Throw(err))
          err => raise err
        }
        let value = property_get(awaited, "value") catch {
          ThrowSignal(err) => return Done(Throw(err))
          err => raise err
        }
        if done {
          if return_mode {
            Done(Return(value))
          } else {
            k(value)
          }
        } else {
          Yield(value, fn(next_resume) raise {
            gen_async_yield_star_loop(
              iterator, next_method, next_resume, false, return_mode, k,
            )
          })
        }
      }
      GenResume::Return(arg) => gen_async_return(arg)
      GenResume::Throw(arg) => Done(Throw(arg))
    }
  })
}
