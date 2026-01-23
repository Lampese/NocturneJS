///|
priv struct TimerEntry {
  id : Int
  callback : Value
}

///|
priv struct JobEntry {
  func : Value
  args : Array[Value]
  env : Env?
}

///|
priv struct WorkerState {
  id : Int
  env : Env
  worker_obj : ObjectValue
  parent_obj : ObjectValue
  mut queue_to_main : Array[Value]
  mut queue_to_worker : Array[Value]
}

///|
let job_queue_ref : Ref[Array[JobEntry]] = Ref::new([])

///|
let timer_queue_ref : Ref[Array[TimerEntry]] = Ref::new([])

///|
let next_timer_id_ref : Ref[Int] = Ref::new(1)

///|
let worker_table_ref : Ref[Map[Int, WorkerState]] = Ref::new(Map::new())

///|
let next_worker_id_ref : Ref[Int] = Ref::new(1)

///|
fn rc_decref_job_entry(entry : JobEntry) -> Unit {
  rc_decref_value(entry.func)
  rc_decref_values(entry.args)
  rc_decref_optional_env(entry.env)
}

///|
fn rc_incref_worker_state(state : WorkerState) -> Unit {
  rc_incref_env(state.env)
  rc_incref_value(Object(state.worker_obj))
  rc_incref_value(Object(state.parent_obj))
  rc_incref_values(state.queue_to_main)
  rc_incref_values(state.queue_to_worker)
}

///|
fn rc_decref_worker_state(state : WorkerState) -> Unit {
  rc_decref_env(state.env)
  rc_decref_value(Object(state.worker_obj))
  rc_decref_value(Object(state.parent_obj))
  rc_decref_values(state.queue_to_main)
  rc_decref_values(state.queue_to_worker)
}

///|
fn enqueue_job(func : Value, args : Array[Value]) -> Unit {
  enqueue_job_with_env(current_env(), func, args)
}

///|
fn enqueue_job_with_env(env : Env?, func : Value, args : Array[Value]) -> Unit {
  let job_env = match env {
    Some(inner) => Some(realm_env_from_env(inner))
    None =>
      match root_env() {
        Some(root) => Some(root)
        None => None
      }
  }
  rc_incref_value(func)
  rc_incref_values(args)
  rc_incref_optional_env(job_env)
  job_queue_ref.update(fn(queue) {
    queue.push(JobEntry::{ func, args, env: job_env })
    queue
  })
}

///|
fn has_pending_jobs() -> Bool {
  let mut value = false
  job_queue_ref.update(fn(queue) {
    if !queue.is_empty() {
      value = true
    }
    queue
  })
  value
}

///|
fn run_pending_jobs() -> Unit raise {
  while true {
    let mut pending : Array[JobEntry] = []
    job_queue_ref.update(fn(queue) {
      if !queue.is_empty() {
        pending = queue
        []
      } else {
        queue
      }
    })
    if pending.is_empty() {
      break
    }
    let mut index = 0
    while index < pending.length() {
      let entry = pending[index]
      try {
        match entry.env {
          Some(env) => {
            let result = with_env_value(env, fn() raise {
              call_value_with_this(entry.func, entry.args, Undefined)
            })
            gc_maybe_collect_with_root(result)
          }
          None => {
            let result = call_value_with_this(entry.func, entry.args, Undefined)
            gc_maybe_collect_with_root(result)
          }
        }
        rc_decref_job_entry(entry)
      } catch {
        err => {
          rc_decref_job_entry(entry)
          index = index + 1
          while index < pending.length() {
            rc_decref_job_entry(pending[index])
            index = index + 1
          }
          raise err
        }
      }
      index = index + 1
    }
  }
}

///|
fn next_timer_id() -> Int {
  let mut value : Int = 0
  next_timer_id_ref.update(fn(current) {
    value = current
    current + 1
  })
  value
}

///|
fn enqueue_timer(callback : Value, _delay_ms : Int64) -> Int {
  let id = next_timer_id()
  rc_incref_value(callback)
  timer_queue_ref.update(fn(queue) {
    queue.push(TimerEntry::{ id, callback })
    queue
  })
  id
}

///|
fn clear_timer(id : Int) -> Unit {
  timer_queue_ref.update(fn(queue) {
    let keep : Array[TimerEntry] = []
    for entry in queue {
      if entry.id != id {
        keep.push(entry)
      } else {
        rc_decref_value(entry.callback)
      }
    }
    keep
  })
}

///|
fn next_worker_id() -> Int {
  let mut value : Int = 0
  next_worker_id_ref.update(fn(current) {
    value = current
    current + 1
  })
  value
}

///|
fn worker_table_snapshot() -> Array[WorkerState] {
  let items : Array[WorkerState] = []
  worker_table_ref.update(fn(table) {
    for _, state in table {
      items.push(state)
    }
    table
  })
  items
}

///|
fn worker_state_get(id : Int) -> WorkerState? {
  let mut value : WorkerState? = None
  worker_table_ref.update(fn(table) {
    value = table.get(id)
    table
  })
  value
}

///|
fn worker_state_set(state : WorkerState) -> Unit {
  worker_table_ref.update(fn(table) {
    match table.get(state.id) {
      Some(existing) => rc_decref_worker_state(existing)
      None => ()
    }
    rc_incref_worker_state(state)
    table.set(state.id, state)
    table
  })
}

///|
fn worker_enqueue(id : Int, to_main : Bool, message : Value) -> Unit {
  match worker_state_get(id) {
    Some(state) =>
      if to_main {
        rc_incref_value(message)
        state.queue_to_main.push(message)
      } else {
        rc_incref_value(message)
        state.queue_to_worker.push(message)
      }
    None => ()
  }
}

///|
fn run_pending_timers() -> Unit raise {
  let mut pending : Array[TimerEntry] = []
  timer_queue_ref.update(fn(queue) {
    pending = queue
    []
  })
  while !pending.is_empty() {
    let current = pending
    pending = []
    let mut index = 0
    while index < current.length() {
      let entry = current[index]
      try {
        let result = call_value(entry.callback, [])
        gc_maybe_collect_with_root(result)
        rc_decref_value(entry.callback)
      } catch {
        err => {
          rc_decref_value(entry.callback)
          index = index + 1
          while index < current.length() {
            rc_decref_value(current[index].callback)
            index = index + 1
          }
          raise err
        }
      }
      index = index + 1
    }
    timer_queue_ref.update(fn(queue) {
      if !queue.is_empty() {
        pending = queue
        []
      } else {
        queue
      }
    })
  }
}

///|
fn has_pending_timers() -> Bool {
  let mut value = false
  timer_queue_ref.update(fn(queue) {
    if !queue.is_empty() {
      value = true
    }
    queue
  })
  value
}

///|
fn run_pending_workers() -> Unit raise {
  let mut did_work = true
  while did_work {
    did_work = false
    let states = worker_table_snapshot()
    for state in states {
      if !state.queue_to_main.is_empty() {
        let pending = state.queue_to_main
        state.queue_to_main = []
        did_work = true
        let mut index = 0
        while index < pending.length() {
          let message = pending[index]
          try {
            deliver_worker_message_main(state, message)
            rc_decref_value(message)
          } catch {
            err => {
              rc_decref_value(message)
              index = index + 1
              while index < pending.length() {
                rc_decref_value(pending[index])
                index = index + 1
              }
              raise err
            }
          }
          index = index + 1
        }
      }
      if !state.queue_to_worker.is_empty() {
        let pending = state.queue_to_worker
        state.queue_to_worker = []
        did_work = true
        let mut index = 0
        while index < pending.length() {
          let message = pending[index]
          try {
            deliver_worker_message_worker(state, message)
            rc_decref_value(message)
          } catch {
            err => {
              rc_decref_value(message)
              index = index + 1
              while index < pending.length() {
                rc_decref_value(pending[index])
                index = index + 1
              }
              raise err
            }
          }
          index = index + 1
        }
      }
    }
  }
}

///|
fn job_queue_clear() -> Unit {
  job_queue_ref.update(fn(queue) {
    for entry in queue {
      rc_decref_job_entry(entry)
    }
    []
  })
}

///|
fn timer_queue_clear() -> Unit {
  timer_queue_ref.update(fn(queue) {
    for entry in queue {
      rc_decref_value(entry.callback)
    }
    []
  })
}

///|
fn worker_table_clear() -> Unit {
  worker_table_ref.update(fn(table) {
    for _, state in table {
      rc_decref_worker_state(state)
    }
    Map::new()
  })
}

///|
fn has_pending_workers() -> Bool {
  let states = worker_table_snapshot()
  for state in states {
    if !state.queue_to_main.is_empty() || !state.queue_to_worker.is_empty() {
      return true
    }
  }
  false
}
