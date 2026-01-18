///|
pub fn set_gc_debug(enabled : Bool) -> Unit {
  gc_debug_ref().update(fn(_) { enabled })
}

///|
fn gc_debug_enabled() -> Bool {
  let mut enabled = false
  gc_debug_ref().update(fn(current) {
    enabled = current
    current
  })
  enabled
}

///|
fn gc_debug_value_has_prop(value : Value, name : String) -> Bool {
  match value {
    Object(obj) => obj.props.get(name) is Some(_)
    Function(func) => func.props.get(name) is Some(_)
    BoundFunction(bound) => bound.props.get(name) is Some(_)
    Builtin(builtin) => builtin.props.get(name) is Some(_)
    _ => false
  }
}

///|
fn gc_debug_prop_value(value : Value, name : String) -> Value? {
  match value {
    Object(obj) =>
      match obj.props.get(name) {
        Some(prop) => Some(prop.value)
        None => None
      }
    Function(func) =>
      match func.props.get(name) {
        Some(prop) => Some(prop.value)
        None => None
      }
    BoundFunction(bound) =>
      match bound.props.get(name) {
        Some(prop) => Some(prop.value)
        None => None
      }
    Builtin(builtin) =>
      match builtin.props.get(name) {
        Some(prop) => Some(prop.value)
        None => None
      }
    _ => None
  }
}

///|
fn gc_debug_dump_roots(phase : String) -> Unit {
  if !gc_debug_enabled() {
    return ()
  }
  let env_len = env_stack().length()
  let func_len = current_function_stack().length()
  let root_len = gc_root_depth()
  let mut root_env_present = false
  let mut current_env_present = false
  let mut global_present = false
  let mut assert_present = false
  let mut deep_equal_present = false
  let mut root_global_present = false
  let mut root_assert_present = false
  let mut root_deep_equal_present = false
  let mut root_binding_assert_present = false
  let mut root_binding_deep_equal_present = false
  let mut current_binding_assert_present = false
  let mut current_binding_deep_equal_present = false
  let mut root_deep_equal_compare_present = false
  let mut current_deep_equal_compare_present = false
  match root_env() {
    Some(env) => {
      root_env_present = true
      match global_object(env) {
        Some(obj) => {
          root_global_present = true
          match obj.props.get("assert") {
            Some(prop) => {
              root_assert_present = true
              if gc_debug_value_has_prop(prop.value, "deepEqual") {
                root_deep_equal_present = true
              }
              match gc_debug_prop_value(prop.value, "deepEqual") {
                Some(value) =>
                  if gc_debug_value_has_prop(value, "_compare") {
                    root_deep_equal_compare_present = true
                  }
                None => ()
              }
            }
            None => ()
          }
        }
        None => ()
      }
      match env.bindings.get("assert") {
        Some(value) => {
          root_binding_assert_present = true
          if gc_debug_value_has_prop(value, "deepEqual") {
            root_binding_deep_equal_present = true
          }
        }
        None => ()
      }
    }
    None => ()
  }
  match current_env() {
    Some(env) => {
      current_env_present = true
      match global_object(env) {
        Some(obj) => {
          global_present = true
          match obj.props.get("assert") {
            Some(prop) => {
              assert_present = true
              if gc_debug_value_has_prop(prop.value, "deepEqual") {
                deep_equal_present = true
              }
              match gc_debug_prop_value(prop.value, "deepEqual") {
                Some(value) =>
                  if gc_debug_value_has_prop(value, "_compare") {
                    current_deep_equal_compare_present = true
                  }
                None => ()
              }
            }
            None => ()
          }
        }
        None => ()
      }
      match env.bindings.get("assert") {
        Some(value) => {
          current_binding_assert_present = true
          if gc_debug_value_has_prop(value, "deepEqual") {
            current_binding_deep_equal_present = true
          }
        }
        None => ()
      }
    }
    None => ()
  }
  let mut alloc_count = 0
  gc_alloc_count_ref().update(fn(current) {
    alloc_count = current
    current
  })
  let mut threshold = 0
  gc_threshold_ref().update(fn(current) {
    threshold = current
    current
  })
  let mut job_len = 0
  job_queue_ref.update(fn(queue) {
    job_len = queue.length()
    queue
  })
  let mut timer_len = 0
  timer_queue_ref.update(fn(queue) {
    timer_len = queue.length()
    queue
  })
  let mut source_path = ""
  match current_source_path() {
    Some(path) => source_path = path
    None => ()
  }
  println(
    "gc_debug \{phase} alloc=\{alloc_count} threshold=\{threshold} envs=\{env_len} funcs=\{func_len} roots=\{root_len} root_env=\{root_env_present} root_global=\{root_global_present} root_assert=\{root_assert_present} root_deepEqual=\{root_deep_equal_present} root_deepCompare=\{root_deep_equal_compare_present} root_bind_assert=\{root_binding_assert_present} root_bind_deepEqual=\{root_binding_deep_equal_present} current_env=\{current_env_present} global=\{global_present} assert=\{assert_present} deepEqual=\{deep_equal_present} deepCompare=\{current_deep_equal_compare_present} bind_assert=\{current_binding_assert_present} bind_deepEqual=\{current_binding_deep_equal_present} jobs=\{job_len} timers=\{timer_len} src=\{source_path}",
  )
}

///|
fn gc_debug_dump_meta_for_value(
  state : GcState,
  label : String,
  value : Value,
) -> Unit {
  if !gc_debug_enabled() {
    return ()
  }
  match value_id(value) {
    Some(id) =>
      match state.meta.get(id) {
        Some(meta) =>
          println(
            "gc_debug_meta \{label} id=\{id} ref=\{meta.ref_count} mark=\{meta.mark} in_tmp=\{meta.in_tmp}",
          )
        None => println("gc_debug_meta \{label} id=\{id} ref=missing")
      }
    None => println("gc_debug_meta \{label} id=none")
  }
}

///|
fn gc_debug_dump_meta_for_env(
  state : GcState,
  label : String,
  env : Env,
) -> Unit {
  if !gc_debug_enabled() {
    return ()
  }
  match state.meta.get(env.id) {
    Some(meta) =>
      println(
        "gc_debug_meta \{label} id=\{env.id} ref=\{meta.ref_count} mark=\{meta.mark} in_tmp=\{meta.in_tmp}",
      )
    None => println("gc_debug_meta \{label} id=\{env.id} ref=missing")
  }
}

///|
fn gc_debug_dump_assert_meta(state : GcState, phase : String) -> Unit {
  if !gc_debug_enabled() {
    return ()
  }
  match root_env() {
    Some(env) => {
      gc_debug_dump_meta_for_env(state, "root_env@\{phase}", env)
      match global_object(env) {
        Some(obj) =>
          match obj.props.get("assert") {
            Some(prop) => {
              let assert_value = prop.value
              gc_debug_dump_meta_for_value(
                state,
                "assert@\{phase}",
                assert_value,
              )
              match gc_debug_prop_value(assert_value, "deepEqual") {
                Some(deep_value) => {
                  gc_debug_dump_meta_for_value(
                    state,
                    "assert.deepEqual@\{phase}",
                    deep_value,
                  )
                  match gc_debug_prop_value(deep_value, "_compare") {
                    Some(compare_value) =>
                      gc_debug_dump_meta_for_value(
                        state,
                        "assert.deepEqual._compare@\{phase}",
                        compare_value,
                      )
                    None =>
                      println(
                        "gc_debug_meta assert.deepEqual._compare missing@\{phase}",
                      )
                  }
                }
                None =>
                  println("gc_debug_meta assert.deepEqual missing@\{phase}")
              }
            }
            None => println("gc_debug_meta assert missing@\{phase}")
          }
        None => println("gc_debug_meta global missing@\{phase}")
      }
    }
    None => println("gc_debug_meta root_env missing@\{phase}")
  }
}
