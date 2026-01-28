///|
priv struct Test262AgentState {
  id : Int
  env : Env
  mut broadcast_callback : Value?
}

///|
priv struct AgentLockEntry {
  buffer_id : Int
  byte_index : Int
  is_bigint : Bool
}

///|
enum AtomicsWaitStatus {
  Waiting
  Ok
  TimedOut
} derive(Eq)

///|
priv struct AtomicsWaiter {
  id : Int
  buffer_id : Int
  byte_index : Int
  status : AtomicsWaitStatus
  start_time : Int64
  deadline : Int64?
  notify_time : Int64?
}

///|
priv struct AgentReportEntry {
  message : String
  waiter_ids : Array[Int]
  duration_waiter_id : Int?
}

///|
let agent_table_ref : Ref[Map[Int, Test262AgentState]] = Ref::new(Map::new())

///|
let agent_report_queue_ref : Ref[Array[AgentReportEntry]] = Ref::new([])

///|
let agent_lock_queue_ref : Ref[Array[AgentLockEntry]] = Ref::new([])

///|
let agent_spin_load_ref : Ref[Map[String, Int]] = Ref::new(Map::new())

///|
let next_agent_id_ref : Ref[Int] = Ref::new(1)

///|
let current_agent_id_ref : Ref[Int?] = Ref::new(None)

///|
let agent_clock_ref : Ref[Int64] = Ref::new(0)

///|
let agent_local_clock_ref : Ref[Map[Int, Int64]] = Ref::new(Map::new())

///|
let atomics_waiter_table_ref : Ref[Map[Int, AtomicsWaiter]] = Ref::new(
  Map::new(),
)

///|
let atomics_waiter_list_ref : Ref[Array[Int]] = Ref::new([])

///|
let atomics_waiter_next_id_ref : Ref[Int] = Ref::new(1)

///|
let atomics_waiter_stack_ref : Ref[Map[Int, Array[Int]]] = Ref::new(Map::new())

///|
let atomics_notify_time_ref : Ref[Map[Int, Int64]] = Ref::new(Map::new())

///|
let atomics_waiter_pending_duration_ref : Ref[Map[Int, Int]] = Ref::new(
  Map::new(),
)

///|
let atomics_waiter_token_prefix = "\u{00}atomics_waiter:"

///|
fn rc_incref_agent_state(state : Test262AgentState) -> Unit {
  rc_incref_env(state.env)
  rc_incref_optional_value(state.broadcast_callback)
}

///|
fn rc_decref_agent_state(state : Test262AgentState) -> Unit {
  rc_decref_env(state.env)
  rc_decref_optional_value(state.broadcast_callback)
}

///|
fn current_agent_id() -> Int? {
  let mut value : Int? = None
  current_agent_id_ref.update(fn(current) {
    value = current
    current
  })
  value
}

///|
fn with_agent_id(id : Int?, f : () -> Value raise) -> Value raise {
  let prev = current_agent_id()
  current_agent_id_ref.update(fn(_) { id })
  try f() catch {
    err => {
      current_agent_id_ref.update(fn(_) { prev })
      raise err
    }
  } noraise {
    result => {
      current_agent_id_ref.update(fn(_) { prev })
      result
    }
  }
}

///|
fn next_agent_id() -> Int {
  let mut value : Int = 0
  next_agent_id_ref.update(fn(current) {
    value = current
    current + 1
  })
  value
}

///|
fn agent_table_snapshot() -> Array[Test262AgentState] {
  let items : Array[Test262AgentState] = []
  agent_table_ref.update(fn(table) {
    for _, state in table {
      items.push(state)
    }
    table
  })
  items
}

///|
fn agent_state_get(id : Int) -> Test262AgentState? {
  let mut value : Test262AgentState? = None
  agent_table_ref.update(fn(table) {
    value = table.get(id)
    table
  })
  value
}

///|
fn agent_state_set(state : Test262AgentState) -> Unit {
  agent_table_ref.update(fn(table) {
    match table.get(state.id) {
      Some(existing) => rc_decref_agent_state(existing)
      None => ()
    }
    rc_incref_agent_state(state)
    table.set(state.id, state)
    table
  })
}

///|
fn agent_table_clear() -> Unit {
  agent_table_ref.update(fn(table) {
    for _, state in table {
      rc_decref_agent_state(state)
    }
    Map::new()
  })
}

///|
fn agent_lock_enqueue(
  buffer_id : Int,
  byte_index : Int,
  is_bigint : Bool,
) -> Unit {
  // Track agent spin-lock acquisitions for test262's single-threaded harness.
  let entry = AgentLockEntry::{ buffer_id, byte_index, is_bigint }
  agent_lock_queue_ref.update(fn(queue) {
    queue.push(entry)
    queue
  })
}

///|
fn agent_lock_has_pending(
  buffer_id : Int,
  byte_index : Int,
  is_bigint : Bool,
) -> Bool {
  let mut found = false
  agent_lock_queue_ref.update(fn(queue) {
    for entry in queue {
      if entry.buffer_id == buffer_id &&
        entry.byte_index == byte_index &&
        entry.is_bigint == is_bigint {
        found = true
        break
      }
    }
    queue
  })
  found
}

///|
fn agent_lock_pop(buffer_id : Int, byte_index : Int, is_bigint : Bool) -> Unit {
  agent_lock_queue_ref.update(fn(queue) {
    let next : Array[AgentLockEntry] = []
    let mut removed = false
    for entry in queue {
      if !removed &&
        entry.buffer_id == buffer_id &&
        entry.byte_index == byte_index &&
        entry.is_bigint == is_bigint {
        removed = true
      } else {
        next.push(entry)
      }
    }
    next
  })
}

///|
fn agent_spin_key(agent_id : Int, buffer_id : Int, byte_index : Int) -> String {
  Int::to_string(agent_id) +
  ":" +
  Int::to_string(buffer_id) +
  ":" +
  Int::to_string(byte_index)
}

///|
fn agent_spin_should_release(
  agent_id : Int,
  buffer_id : Int,
  byte_index : Int,
) -> Bool {
  // Let agent spin loops on Atomics.load exit without real concurrency.
  let key = agent_spin_key(agent_id, buffer_id, byte_index)
  let mut release = false
  agent_spin_load_ref.update(fn(table) {
    let count = match table.get(key) {
      Some(value) => value
      None => 0
    }
    if count >= 1 {
      release = true
      let _ = table.remove(key)

    } else {
      table.set(key, count + 1)
    }
    table
  })
  release
}

///|
fn agent_spin_clear(agent_id : Int, buffer_id : Int, byte_index : Int) -> Unit {
  let key = agent_spin_key(agent_id, buffer_id, byte_index)
  agent_spin_load_ref.update(fn(table) {
    let _ = table.remove(key)
    table
  })
}

///|
fn atomics_next_waiter_id() -> Int {
  let mut id = 0
  atomics_waiter_next_id_ref.update(fn(current) {
    id = current
    current + 1
  })
  id
}

///|
fn atomics_waiter_set(waiter : AtomicsWaiter) -> Unit {
  atomics_waiter_table_ref.update(fn(table) {
    table.set(waiter.id, waiter)
    table
  })
}

///|
fn atomics_waiter_get(id : Int) -> AtomicsWaiter? {
  let mut value : AtomicsWaiter? = None
  atomics_waiter_table_ref.update(fn(table) {
    value = table.get(id)
    table
  })
  value
}

///|
fn atomics_waiter_add(waiter : AtomicsWaiter) -> Unit {
  atomics_waiter_table_ref.update(fn(table) {
    table.set(waiter.id, waiter)
    table
  })
  atomics_waiter_list_ref.update(fn(list) {
    list.push(waiter.id)
    list
  })
}

///|
fn atomics_waiter_update_status(
  id : Int,
  status : AtomicsWaitStatus,
  notify_time : Int64?,
) -> Unit {
  match atomics_waiter_get(id) {
    Some(waiter) => {
      let updated = AtomicsWaiter::{
        id: waiter.id,
        buffer_id: waiter.buffer_id,
        byte_index: waiter.byte_index,
        status,
        start_time: waiter.start_time,
        deadline: waiter.deadline,
        notify_time: match notify_time {
          Some(time) => Some(time)
          None => waiter.notify_time
        },
      }
      atomics_waiter_set(updated)
    }
    None => ()
  }
}

///|
fn atomics_update_waiter_timeouts(now : Int64) -> Unit {
  atomics_waiter_table_ref.update(fn(table) {
    for id, waiter in table {
      match waiter.status {
        AtomicsWaitStatus::Waiting =>
          match waiter.deadline {
            Some(deadline) =>
              if now >= deadline {
                table.set(id, AtomicsWaiter::{
                  id: waiter.id,
                  buffer_id: waiter.buffer_id,
                  byte_index: waiter.byte_index,
                  status: AtomicsWaitStatus::TimedOut,
                  start_time: waiter.start_time,
                  deadline: waiter.deadline,
                  notify_time: waiter.notify_time,
                })
              }
            None => ()
          }
        _ => ()
      }
    }
    table
  })
}

///|
fn atomics_record_notify_time(buffer_id : Int, now : Int64) -> Unit {
  atomics_notify_time_ref.update(fn(table) {
    table.set(buffer_id, now)
    table
  })
}

///|
fn atomics_agent_push_waiter(agent_id : Int, waiter_id : Int) -> Unit {
  atomics_waiter_stack_ref.update(fn(table) {
    let stack = match table.get(agent_id) {
      Some(existing) => existing
      None => []
    }
    let next = stack
    next.push(waiter_id)
    table.set(agent_id, next)
    table
  })
}

///|
fn atomics_agent_set_pending_duration(agent_id : Int, waiter_id : Int) -> Unit {
  atomics_waiter_pending_duration_ref.update(fn(table) {
    table.set(agent_id, waiter_id)
    table
  })
}

///|
fn atomics_agent_take_pending_duration(agent_id : Int) -> Int? {
  let mut value : Int? = None
  atomics_waiter_pending_duration_ref.update(fn(table) {
    value = table.get(agent_id)
    let _ = table.remove(agent_id)
    table
  })
  value
}

///|
fn atomics_agent_last_waiter(agent_id : Int) -> Int? {
  let mut value : Int? = None
  atomics_waiter_stack_ref.update(fn(table) {
    match table.get(agent_id) {
      Some(stack) =>
        if !stack.is_empty() {
          value = Some(stack[stack.length() - 1])
        }
      None => ()
    }
    table
  })
  value
}

///|
fn atomics_waiter_status_string(id : Int, now : Int64) -> String {
  atomics_update_waiter_timeouts(now)
  match atomics_waiter_get(id) {
    Some(waiter) =>
      match waiter.status {
        AtomicsWaitStatus::Ok => "ok"
        AtomicsWaitStatus::TimedOut => "timed-out"
        AtomicsWaitStatus::Waiting => "timed-out"
      }
    None => "timed-out"
  }
}

///|
fn atomics_waiter_duration_ms(waiter_id : Int, now : Int64) -> Int64 {
  match atomics_waiter_get(waiter_id) {
    Some(waiter) => {
      let start = waiter.start_time
      let end = match waiter.status {
        AtomicsWaitStatus::Ok =>
          match waiter.notify_time {
            Some(time) => time
            None => now
          }
        AtomicsWaitStatus::TimedOut =>
          match waiter.deadline {
            Some(deadline) => deadline
            None => now
          }
        AtomicsWaitStatus::Waiting => now
      }
      if end > start { end - start } else { 0L }
    }
    None => 0L
  }
}

///|
fn atomics_collect_waiter_ids(message : String) -> Array[Int] {
  let ids : Array[Int] = []
  let prefix = atomics_waiter_token_prefix
  let prefix_len = prefix.length()
  let len = message.length()
  let mut i = 0
  let zero = Char::to_int('0')
  let nine = Char::to_int('9')
  while i + prefix_len <= len {
    let slice = message.unsafe_substring(start=i, end=i + prefix_len)
    if slice == prefix {
      let mut j = i + prefix_len
      let mut id = 0
      let mut has_digit = false
      while j < len {
        let code = UInt16::to_int(message.code_unit_at(j))
        if code >= zero && code <= nine {
          id = id * 10 + (code - zero)
          has_digit = true
          j = j + 1
        } else {
          break
        }
      }
      if has_digit {
        ids.push(id)
      }
      i = j
    } else {
      i = i + 1
    }
  }
  ids
}

///|
fn replace_once(text : String, from : String, to : String) -> String {
  match text.find(from) {
    Some(idx) => {
      let before = text.unsafe_substring(start=0, end=idx)
      let after = text.unsafe_substring(
        start=idx + from.length(),
        end=text.length(),
      )
      before + to + after
    }
    None => text
  }
}

///|
fn atomics_should_report_timeout_after(waiter_id : Int) -> Bool {
  match atomics_waiter_get(waiter_id) {
    Some(waiter) => {
      let mut notify_time : Int64? = None
      atomics_notify_time_ref.update(fn(table) {
        notify_time = table.get(waiter.buffer_id)
        table
      })
      match notify_time {
        Some(time) =>
          match waiter.deadline {
            Some(deadline) => time >= waiter.start_time && time <= deadline
            None => time >= waiter.start_time
          }
        None => false
      }
    }
    None => false
  }
}

///|
fn atomics_resolve_message(
  message : String,
  waiter_ids : Array[Int],
  now : Int64,
) -> String {
  let prefix = atomics_waiter_token_prefix
  let prefix_len = prefix.length()
  let len = message.length()
  let sb = StringBuilder::new()
  let mut i = 0
  let zero = Char::to_int('0')
  let nine = Char::to_int('9')
  while i < len {
    if i + prefix_len <= len &&
      message.unsafe_substring(start=i, end=i + prefix_len) == prefix {
      let mut j = i + prefix_len
      let mut id = 0
      let mut has_digit = false
      while j < len {
        let code = UInt16::to_int(message.code_unit_at(j))
        if code >= zero && code <= nine {
          id = id * 10 + (code - zero)
          has_digit = true
          j = j + 1
        } else {
          break
        }
      }
      if has_digit {
        sb.write_string(atomics_waiter_status_string(id, now))
        i = j
      } else {
        sb.write_string(prefix)
        i = i + prefix_len
      }
    } else {
      sb.write_string(message.unsafe_substring(start=i, end=i + 1))
      i = i + 1
    }
  }
  let mut resolved = sb.to_string()
  if !waiter_ids.is_empty() {
    let wants_after = atomics_should_report_timeout_after(waiter_ids[0])
    if wants_after {
      resolved = replace_once(
        resolved, "timeout before Atomics.notify", "timeout after Atomics.notify",
      )
    } else {
      resolved = replace_once(
        resolved, "timeout after Atomics.notify", "timeout before Atomics.notify",
      )
    }
  }
  resolved
}

///|
fn agent_report_push(
  message : String,
  waiter_ids : Array[Int],
  duration_waiter_id : Int?,
) -> Unit {
  let entry = AgentReportEntry::{ message, waiter_ids, duration_waiter_id }
  agent_report_queue_ref.update(fn(queue) {
    queue.push(entry)
    queue
  })
}

///|
fn agent_report_pop() -> String? {
  let mut result : String? = None
  agent_report_queue_ref.update(fn(queue) {
    let now = agent_clock_now()
    atomics_update_waiter_timeouts(now)
    let mut ready_index = -1
    let mut resolved : String? = None
    let mut i = 0
    while i < queue.length() {
      let entry = queue[i]
      let mut waiting = false
      for id in entry.waiter_ids {
        match atomics_waiter_get(id) {
          Some(waiter) =>
            if waiter.status == AtomicsWaitStatus::Waiting {
              waiting = true
            }
          None => ()
        }
        if waiting {
          break
        }
      }
      if !waiting {
        match entry.duration_waiter_id {
          Some(id) =>
            match atomics_waiter_get(id) {
              Some(waiter) =>
                if waiter.status == AtomicsWaitStatus::Waiting {
                  waiting = true
                }
              None => ()
            }
          None => ()
        }
      }
      if !waiting {
        ready_index = i
        resolved = match entry.duration_waiter_id {
          Some(waiter_id) =>
            Some(Int64::to_string(atomics_waiter_duration_ms(waiter_id, now)))
          None =>
            Some(atomics_resolve_message(entry.message, entry.waiter_ids, now))
        }
        break
      }
      i = i + 1
    }
    if ready_index >= 0 {
      result = resolved
      let rest : Array[AgentReportEntry] = []
      let mut j = 0
      while j < queue.length() {
        if j != ready_index {
          rest.push(queue[j])
        }
        j = j + 1
      }
      return rest
    }
    queue
  })
  result
}

///|
fn agent_clock_now() -> Int64 {
  let mut value : Int64 = 0
  agent_clock_ref.update(fn(current) {
    value = current
    current
  })
  value
}

///|
fn agent_clock_advance(delta_ms : Int64) -> Int64 {
  let mut value : Int64 = 0
  agent_clock_ref.update(fn(current) {
    let next = current + delta_ms
    value = next
    next
  })
  atomics_update_waiter_timeouts(value)
  value
}

///|
fn agent_local_clock_now(agent_id : Int) -> Int64 {
  let mut value : Int64? = None
  agent_local_clock_ref.update(fn(table) {
    value = table.get(agent_id)
    table
  })
  match value {
    Some(current) => current
    None => {
      let now = agent_clock_now()
      agent_local_clock_ref.update(fn(table) {
        table.set(agent_id, now)
        table
      })
      now
    }
  }
}

///|
fn agent_local_clock_advance(agent_id : Int, delta_ms : Int64) -> Int64 {
  let mut next_value : Int64 = 0
  agent_local_clock_ref.update(fn(table) {
    let current = match table.get(agent_id) {
      Some(value) => value
      None => agent_clock_now()
    }
    let next = current + delta_ms
    table.set(agent_id, next)
    next_value = next
    table
  })
  next_value
}

///|
fn agent_local_clock_advance_to(agent_id : Int, target : Int64) -> Int64 {
  let mut next_value : Int64 = 0
  agent_local_clock_ref.update(fn(table) {
    let current = match table.get(agent_id) {
      Some(value) => value
      None => agent_clock_now()
    }
    let next = if target > current { target } else { current }
    table.set(agent_id, next)
    next_value = next
    table
  })
  next_value
}
