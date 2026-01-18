///|
fn gen_bind(step : GenStep, f : (GenControl) -> GenStep raise) -> GenStep raise {
  match step {
    Yield(value, next_step) =>
      Yield(value, fn(resume_state) raise {
        gen_bind(next_step(resume_state), f)
      })
    Await(value, next_step) =>
      Await(value, fn(resume_state) raise {
        gen_bind(next_step(resume_state), f)
      })
    Done(control) => f(control)
  }
}

///|
fn gen_bind_noraise(step : GenStep, f : (GenControl) -> GenStep) -> GenStep {
  match step {
    Yield(value, next_step) =>
      Yield(value, fn(resume_state) raise {
        gen_bind_noraise(next_step(resume_state), f)
      })
    Await(value, next_step) =>
      Await(value, fn(resume_state) raise {
        gen_bind_noraise(next_step(resume_state), f)
      })
    Done(control) => f(control)
  }
}

///|
fn gen_await_value(value : Value, k : (Value) -> GenStep raise) -> GenStep {
  Await(value, fn(resume_state) raise {
    match resume_state {
      GenResume::Next(arg) => k(arg)
      GenResume::Return(arg) => Done(Return(arg))
      GenResume::Throw(arg) => Done(Throw(arg))
    }
  })
}

///|
fn gen_unwrap_async_yield_resume(
  resume_state : GenResume,
  k : (GenResume) -> GenStep raise,
) -> GenStep {
  match resume_state {
    GenResume::Next(arg) =>
      gen_await_value(arg, fn(awaited) raise { k(GenResume::Next(awaited)) })
    GenResume::Return(arg) =>
      gen_await_value(arg, fn(awaited) raise { k(GenResume::Return(awaited)) })
    GenResume::Throw(arg) =>
      gen_await_value(arg, fn(awaited) raise { k(GenResume::Throw(awaited)) })
  }
}

///|
fn gen_async_return(value : Value) -> GenStep {
  Await(value, fn(resume_state) {
    match resume_state {
      GenResume::Next(arg) => Done(Return(arg))
      GenResume::Return(arg) => Done(Return(arg))
      GenResume::Throw(arg) => Done(Throw(arg))
    }
  })
}
