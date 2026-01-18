///|
let annex_b_var_func_decls_ref : Ref[Array[@hashmap.HashMap[Int, Bool]]] = Ref::new([],
)

///|
let top_level_func_decls_ref : Ref[Array[@hashmap.HashMap[Int, Bool]]] = Ref::new([],
)

///|
fn annex_b_var_func_decl_contains(func : @engine.FunctionDef) -> Bool {
  let mut result = false
  annex_b_var_func_decls_ref.update(fn(stack) {
    if !stack.is_empty() {
      let current = stack[stack.length() - 1]
      result = current.contains(func.offset)
    }
    stack
  })
  result
}

///|
fn[T] with_annex_b_var_func_decls(
  decls : @hashmap.HashMap[Int, Bool],
  f : () -> T raise,
) -> T raise {
  annex_b_var_func_decls_ref.update(fn(stack) {
    stack.push(decls)
    stack
  })
  try {
    let result = f()
    annex_b_var_func_decls_ref.update(fn(stack) {
      ignore(stack.pop())
      stack
    })
    result
  } catch {
    err => {
      annex_b_var_func_decls_ref.update(fn(stack) {
        ignore(stack.pop())
        stack
      })
      raise err
    }
  }
}

///|
fn top_level_function_decl_contains(func : @engine.FunctionDef) -> Bool {
  let mut result = false
  top_level_func_decls_ref.update(fn(stack) {
    if !stack.is_empty() {
      let current = stack[stack.length() - 1]
      result = current.contains(func.offset)
    }
    stack
  })
  result
}

///|
fn[T] with_top_level_function_decls(
  decls : @hashmap.HashMap[Int, Bool],
  f : () -> T raise,
) -> T raise {
  top_level_func_decls_ref.update(fn(stack) {
    stack.push(decls)
    stack
  })
  try {
    let result = f()
    top_level_func_decls_ref.update(fn(stack) {
      ignore(stack.pop())
      stack
    })
    result
  } catch {
    err => {
      top_level_func_decls_ref.update(fn(stack) {
        ignore(stack.pop())
        stack
      })
      raise err
    }
  }
}

///|
fn with_top_level_function_decls_step(
  decls : @hashmap.HashMap[Int, Bool],
  f : () -> GenStep raise,
) -> GenStep raise {
  top_level_func_decls_ref.update(fn(stack) {
    stack.push(decls)
    stack
  })
  try {
    let result = f()
    top_level_func_decls_ref.update(fn(stack) {
      ignore(stack.pop())
      stack
    })
    result
  } catch {
    err => {
      top_level_func_decls_ref.update(fn(stack) {
        ignore(stack.pop())
        stack
      })
      raise err
    }
  }
}

///|
fn top_level_function_decls_wrap_next_step(
  next_step : (GenResume) -> GenStep raise,
  decls : @hashmap.HashMap[Int, Bool],
) -> (GenResume) -> GenStep raise {
  fn(resume_) raise {
    let step = with_top_level_function_decls_step(decls, fn() raise {
      next_step(resume_)
    })
    top_level_function_decls_wrap_step(step, decls)
  }
}

///|
fn top_level_function_decls_wrap_step(
  step : GenStep,
  decls : @hashmap.HashMap[Int, Bool],
) -> GenStep {
  match step {
    Yield(value, next_step) =>
      Yield(value, top_level_function_decls_wrap_next_step(next_step, decls))
    Await(value, next_step) =>
      Await(value, top_level_function_decls_wrap_next_step(next_step, decls))
    Done(control) => Done(control)
  }
}
