///|
fn gc_count_internal_refs(state : GcState) -> Unit {
  gc_registry_ref().update(fn(registry) {
    registry.values.each(fn(_, value) {
      gc_visit_value_children(
        state,
        value,
        fn(child) { gc_incref_value(state, child) },
        fn(env) { gc_incref_env(state, env) },
        true,
      )
    })
    registry.envs.each(fn(_, env) {
      gc_visit_env_children(env, fn(child) { gc_incref_value(state, child) }, fn(
        child_env,
      ) {
        gc_incref_env(state, child_env)
      })
    })
    registry
  })
}

///|
fn gc_remove_weak_objects(state : GcState) -> Unit {
  let mut index = 0
  while index < state.weakmaps.length() {
    let obj = state.weakmaps[index]
    match obj.weakmap_data {
      Some(data) => {
        let mut i = 0
        while i < data.entries.length() {
          let (key, value) = data.entries[i]
          if is_live_value(state, key) {
            i = i + 1
          } else {
            gc_decref_value(state, value)
            let _ = data.entries.remove(i)

          }
        }
      }
      None => ()
    }
    index = index + 1
  }
  index = 0
  while index < state.weaksets.length() {
    let obj = state.weaksets[index]
    match obj.weakset_data {
      Some(data) => {
        let mut i = 0
        while i < data.entries.length() {
          let key = data.entries[i]
          if is_live_value(state, key) {
            i = i + 1
          } else {
            let _ = data.entries.remove(i)

          }
        }
      }
      None => ()
    }
    index = index + 1
  }
  for obj in state.weakrefs {
    match obj.weakref_data {
      Some(data) =>
        if !is_live_value(state, data.target) {
          obj.weakref_data = Some(WeakRefData::{ target: Undefined })
        }
      None => ()
    }
  }
  for obj in state.finregs {
    match obj.finreg_data {
      Some(data) => {
        let mut i = 0
        while i < data.entries.length() {
          let entry = data.entries[i]
          match entry.token {
            Undefined => ()
            _ =>
              if !is_live_value(state, entry.token) {
                entry.token = Undefined
              }
          }
          if !is_live_value(state, entry.target) {
            let realm_env = realm_env_from_env(data.realm_env)
            enqueue_job_with_env(Some(realm_env), data.callback, [
              entry.held_value,
            ])
            gc_incref_value(state, data.callback)
            gc_incref_value(state, entry.held_value)
            gc_incref_env(state, realm_env)
            let _ = data.entries.remove(i)
            gc_decref_value(state, entry.held_value)
          } else {
            i = i + 1
          }
        }
      }
      None => ()
    }
  }
}

///|
fn gc_decref(state : GcState) -> Unit {
  gc_registry_ref().update(fn(registry) {
    registry.values.each(fn(id, value) {
      gc_visit_value_children(
        state,
        value,
        fn(child) { gc_decref_value(state, child) },
        fn(env) { gc_decref_env(state, env) },
        false,
      )
      match state.meta.get(id) {
        Some(meta) => {
          meta.mark = true
          if meta.ref_count == 0 && !meta.in_tmp {
            meta.in_tmp = true
          }
        }
        None => ()
      }
    })
    registry.envs.each(fn(id, env) {
      gc_visit_env_children(env, fn(child) { gc_decref_value(state, child) }, fn(
        child_env,
      ) {
        gc_decref_env(state, child_env)
      })
      match state.meta.get(id) {
        Some(meta) => {
          meta.mark = true
          if meta.ref_count == 0 && !meta.in_tmp {
            meta.in_tmp = true
          }
        }
        None => ()
      }
    })
    registry
  })
}

///|
fn gc_scan_incref_value(state : GcState, value : Value) -> Bool {
  match value_id(value) {
    Some(id) =>
      match state.meta.get(id) {
        Some(meta) => {
          let was_zero = meta.ref_count == 0
          if was_zero && meta.in_tmp {
            meta.in_tmp = false
          }
          meta.ref_count = meta.ref_count + 1
          was_zero
        }
        None => false
      }
    None => false
  }
}

///|
fn gc_scan_incref_env(state : GcState, env : Env) -> Bool {
  match state.meta.get(env.id) {
    Some(meta) => {
      let was_zero = meta.ref_count == 0
      if was_zero && meta.in_tmp {
        meta.in_tmp = false
      }
      meta.ref_count = meta.ref_count + 1
      was_zero
    }
    None => false
  }
}

///|
fn gc_scan_incref_value_keep_tmp(state : GcState, value : Value) -> Unit {
  match value_id(value) {
    Some(id) =>
      match state.meta.get(id) {
        Some(meta) => meta.ref_count = meta.ref_count + 1
        None => ()
      }
    None => ()
  }
}

///|
fn gc_scan_incref_env_keep_tmp(state : GcState, env : Env) -> Unit {
  match state.meta.get(env.id) {
    Some(meta) => meta.ref_count = meta.ref_count + 1
    None => ()
  }
}

///|
fn gc_scan(state : GcState) -> Unit {
  let mut changed = true
  while changed {
    changed = false
    gc_registry_ref().update(fn(registry) {
      registry.envs.each(fn(id, env) {
        match state.meta.get(id) {
          Some(meta) =>
            if meta.ref_count > 0 {
              gc_visit_env_children(
                env,
                fn(child) {
                  if gc_scan_incref_value(state, child) {
                    changed = true
                  }
                },
                fn(child_env) {
                  if gc_scan_incref_env(state, child_env) {
                    changed = true
                  }
                },
              )
            }
          None => ()
        }
      })
      registry.values.each(fn(id, value) {
        match state.meta.get(id) {
          Some(meta) =>
            if meta.ref_count > 0 {
              gc_visit_value_children(
                state,
                value,
                fn(child) {
                  if gc_scan_incref_value(state, child) {
                    changed = true
                  }
                },
                fn(env) { if gc_scan_incref_env(state, env) { changed = true } },
                false,
              )
            }
          None => ()
        }
      })
      registry
    })
  }
  gc_registry_ref().update(fn(registry) {
    registry.values.each(fn(id, value) {
      match state.meta.get(id) {
        Some(meta) =>
          if meta.in_tmp {
            gc_visit_value_children(
              state,
              value,
              fn(child) { gc_scan_incref_value_keep_tmp(state, child) },
              fn(env) { gc_scan_incref_env_keep_tmp(state, env) },
              false,
            )
          }
        None => ()
      }
    })
    registry.envs.each(fn(id, env) {
      match state.meta.get(id) {
        Some(meta) =>
          if meta.in_tmp {
            gc_visit_env_children(
              env,
              fn(child) { gc_scan_incref_value_keep_tmp(state, child) },
              fn(child_env) { gc_scan_incref_env_keep_tmp(state, child_env) },
            )
          }
        None => ()
      }
    })
    registry
  })
}

///|
fn gc_free_cycles(state : GcState) -> Unit {
  gc_registry_ref().update(fn(registry) {
    let before = registry.values.length() + registry.envs.length()
    registry.values.retain(fn(id, value) {
      match state.meta.get(id) {
        Some(meta) =>
          if meta.in_tmp {
            gc_release_value(value)
            false
          } else {
            true
          }
        None => true
      }
    })
    registry.envs.retain(fn(id, env) {
      match state.meta.get(id) {
        Some(meta) =>
          if meta.in_tmp {
            gc_release_env(env)
            false
          } else {
            true
          }
        None => true
      }
    })
    let after = registry.values.length() + registry.envs.length()
    if after < before {
      let removed = before - after
      gc_alloc_count_ref().update(fn(count) {
        if count > removed {
          count - removed
        } else {
          0
        }
      })
    }
    registry
  })
}
