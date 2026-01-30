///|
fn run_gc_internal(remove_weak : Bool) -> Unit {
  gc_debug_dump_roots("begin")
  let state = new_gc_state()
  gc_prepare_meta(state)
  gc_count_roots(state)
  gc_debug_dump_assert_meta(state, "pre")
  if remove_weak {
    gc_remove_weak_objects(state)
  }
  gc_decref(state)
  gc_scan(state)
  gc_debug_dump_assert_meta(state, "post")
  gc_free_cycles(state)
}

///|
pub fn run_gc() -> Unit {
  run_gc_internal(true)
}

///|
fn run_gc_without_weak() -> Unit {
  run_gc_internal(false)
}

///|
fn gc_request() -> Unit {
  let mut threshold = 0
  let mut count = 0
  gc_threshold_ref().update(fn(current) {
    threshold = current
    current
  })
  if threshold < 0 {
    return ()
  }
  gc_alloc_count_ref().update(fn(current) {
    count = current
    current
  })
  if count > threshold {
    gc_pending_ref().update(fn(_) { true })
  }
}

///|
fn gc_is_pending() -> Bool {
  let mut pending = false
  gc_pending_ref().update(fn(current) {
    pending = current
    current
  })
  pending
}

///|
fn gc_root_depth() -> Int {
  let mut depth = 0
  gc_value_root_ref().update(fn(stack) {
    depth = stack.length()
    stack
  })
  depth
}

///|
fn gc_root_push(value : Value) -> Int {
  let mut depth = 0
  gc_value_root_ref().update(fn(stack) {
    stack.push(value)
    depth = stack.length()
    stack
  })
  depth
}

///|
fn gc_root_pop_to(depth : Int) -> Unit {
  gc_value_root_ref().update(fn(stack) {
    while stack.length() > depth {
      let _ = stack.pop()

    }
    stack
  })
}

///|
fn with_gc_frame(f : () -> Value raise) -> Value raise {
  let base = gc_root_depth()
  try f() catch {
    err => {
      gc_root_pop_to(base)
      raise err
    }
  } noraise {
    result => {
      gc_root_pop_to(base)
      result
    }
  }
}

///|
fn gc_maybe_collect() -> Unit {
  gc_request()
  let mut pending = false
  let mut count = 0
  let mut threshold = 0
  gc_pending_ref().update(fn(current) {
    pending = current
    current
  })
  if !pending {
    return ()
  }
  gc_threshold_ref().update(fn(current) {
    threshold = current
    current
  })
  if threshold < 0 {
    gc_pending_ref().update(fn(_) { false })
    return ()
  }
  run_gc()
  gc_pending_ref().update(fn(_) { false })
  gc_alloc_count_ref().update(fn(current) {
    count = current
    current
  })
  let next_threshold = count + count / 2
  gc_threshold_ref().update(fn(_) { next_threshold })
}

///|
fn gc_maybe_collect_with_root(value : Value) -> Unit {
  let base = gc_root_depth()
  match current_env() {
    Some(env) => {
      let _ = gc_root_push(global_object_value(env))

    }
    None => ()
  }
  let _ = gc_root_push(value)
  gc_maybe_collect()
  gc_root_pop_to(base)
}

///|
pub fn set_gc_threshold(threshold : Int) -> Unit {
  gc_threshold_ref().update(fn(_) { threshold })
}
