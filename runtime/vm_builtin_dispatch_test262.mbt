///|
fn call_builtin_test262(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match builtin.kind {
    BuiltinFunction::Test262DetachArrayBuffer => {
      if !args.is_empty() {
        detach_array_buffer(args[0])
      }
      Undefined
    }
    BuiltinFunction::Test262EvalScript => {
      let env = realm_env_from_value(builtin.this_value)
      let source = if args.is_empty() {
        "undefined"
      } else {
        to_string_value(args[0])
      }
      let script = parse_script_with_pos(source)
      let result = with_root_env(env, fn() raise { eval_script(env, script) })
      run_pending_timers()
      run_pending_workers()
      result
    }
    BuiltinFunction::Test262CodePointRange => {
      let start = if args.is_empty() {
        UInt::default()
      } else {
        to_uint32(to_number(args[0]))
      }
      let end = if args.length() > 1 {
        to_uint32(to_number(args[1]))
      } else {
        UInt::default()
      }
      String(test262_code_point_range(start, end))
    }
    BuiltinFunction::Test262CreateRealm => {
      let base_env = realm_env_from_value(builtin.this_value)
      let new_env = new_realm_env(base_env)
      install_test262_helpers(new_env)
    }
    BuiltinFunction::Test262Gc => {
      run_gc()
      Undefined
    }
    BuiltinFunction::Test262IsHTMLDDA => Null
    BuiltinFunction::Test262AgentStart => {
      if current_agent_id() is Some(_) {
        return throw_type_error("cannot be called inside an agent")
      }
      let script = if args.is_empty() { "" } else { to_string_value(args[0]) }
      let base_env = match root_env() {
        Some(env) => env
        None => Env::new(None)
      }
      let agent_env = new_realm_env(base_env)
      let _ = install_test262_helpers(agent_env)
      let agent_id = next_agent_id()
      agent_state_set(Test262AgentState::{
        id: agent_id,
        env: agent_env,
        broadcast_callback: None,
      })
      let _ = with_root_env(agent_env, fn() raise {
        with_agent_id(Some(agent_id), fn() raise {
          eval_script(agent_env, parse_script_with_pos(script))
        })
      })
      run_pending_timers()
      run_pending_workers()
      Undefined
    }
    BuiltinFunction::Test262AgentGetReport => {
      if current_agent_id() is Some(_) {
        return throw_type_error("must be called outside of an agent")
      }
      match agent_report_pop() {
        Some(message) => String(message)
        None => Null
      }
    }
    BuiltinFunction::Test262AgentBroadcast => {
      if current_agent_id() is Some(_) {
        return throw_type_error("cannot be called inside an agent")
      }
      if args.is_empty() {
        return Undefined
      }
      let sab = args[0]
      let val = if args.length() > 1 { args[1] } else { Number(0.0) }
      for agent in agent_table_snapshot() {
        match agent.broadcast_callback {
          Some(callback) => {
            let _ = with_root_env(agent.env, fn() raise {
              with_agent_id(Some(agent.id), fn() raise {
                call_value_with_this(callback, [sab, val], Undefined)
              })
            })

          }
          None => ()
        }
      }
      Undefined
    }
    BuiltinFunction::Test262AgentReport => {
      let agent_id = match current_agent_id() {
        Some(id) => id
        None => return throw_type_error("must be called inside an agent")
      }
      let message = if args.is_empty() {
        "undefined"
      } else {
        to_string_value(args[0])
      }
      let waiter_ids = atomics_collect_waiter_ids(message)
      let pending_duration = atomics_agent_take_pending_duration(agent_id)
      let num = string_to_number(message)
      let message_is_number = !Double::is_nan(num) && !Double::is_inf(num)
      let mut duration_waiter_id : Int? = None
      match pending_duration {
        Some(waiter_id) =>
          if waiter_ids.is_empty() && message_is_number {
            duration_waiter_id = Some(waiter_id)
          }
        None => ()
      }
      if waiter_ids.is_empty() && duration_waiter_id is None {
        match atomics_agent_last_waiter(agent_id) {
          Some(waiter_id) => waiter_ids.push(waiter_id)
          None => ()
        }
      }
      agent_report_push(message, waiter_ids, duration_waiter_id)
      Undefined
    }
    BuiltinFunction::Test262AgentLeaving => {
      if current_agent_id() is None {
        return throw_type_error("must be called inside an agent")
      }
      Undefined
    }
    BuiltinFunction::Test262AgentReceiveBroadcast => {
      let id = match current_agent_id() {
        Some(value) => value
        None => return throw_type_error("must be called inside an agent")
      }
      if args.is_empty() || !is_callable(args[0]) {
        return throw_type_error("expecting function")
      }
      match agent_state_get(id) {
        Some(state) => {
          state.broadcast_callback = Some(args[0])
          agent_state_set(state)
        }
        None => ()
      }
      Undefined
    }
    BuiltinFunction::Test262AgentSleep => {
      let duration = if args.is_empty() {
        0
      } else {
        to_int32(to_number(args[0]))
      }
      let delta = Int64::from_int(duration)
      let _ = agent_clock_advance(delta)
      match current_agent_id() {
        Some(agent_id) => {
          let _ = agent_local_clock_advance(agent_id, delta)
        }
        None => ()
      }
      Undefined
    }
    BuiltinFunction::Test262AgentMonotonicNow =>
      match current_agent_id() {
        Some(agent_id) =>
          Number(agent_local_clock_now(agent_id).to_double())
        None => Number(agent_clock_now().to_double())
      }
    _ => throw_type_error("invalid builtin")
  }
}
