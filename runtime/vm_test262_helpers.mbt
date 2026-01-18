///|
fn new_test262_agent_object() -> Value {
  let obj_value = new_object_value_with_proto(value_from_object(object_proto()))
  match obj_value {
    Object(obj) => {
      obj.props.set(
        "start",
        property_data(new_builtin_value(BuiltinFunction::Test262AgentStart)),
      )
      obj.props.set(
        "getReport",
        property_data(new_builtin_value(BuiltinFunction::Test262AgentGetReport)),
      )
      obj.props.set(
        "broadcast",
        property_data(new_builtin_value(BuiltinFunction::Test262AgentBroadcast)),
      )
      obj.props.set(
        "report",
        property_data(new_builtin_value(BuiltinFunction::Test262AgentReport)),
      )
      obj.props.set(
        "leaving",
        property_data(new_builtin_value(BuiltinFunction::Test262AgentLeaving)),
      )
      obj.props.set(
        "receiveBroadcast",
        property_data(
          new_builtin_value(BuiltinFunction::Test262AgentReceiveBroadcast),
        ),
      )
      obj.props.set(
        "sleep",
        property_data(new_builtin_value(BuiltinFunction::Test262AgentSleep)),
      )
      obj.props.set(
        "monotonicNow",
        property_data(
          new_builtin_value(BuiltinFunction::Test262AgentMonotonicNow),
        ),
      )
    }
    _ => ()
  }
  obj_value
}

///|
fn new_test262_object(env : Env) -> Value {
  let obj_value = new_object_value_with_proto(value_from_object(object_proto()))
  match obj_value {
    Object(obj) => {
      obj.realm_env = Some(env)
      obj.props.set(
        "detachArrayBuffer",
        property_data(
          new_builtin_value(BuiltinFunction::Test262DetachArrayBuffer),
        ),
      )
      obj.props.set(
        "evalScript",
        property_data(new_builtin_value(BuiltinFunction::Test262EvalScript)),
      )
      obj.props.set(
        "codePointRange",
        property_data(new_builtin_value(BuiltinFunction::Test262CodePointRange)),
      )
      obj.props.set("global", property_data(global_object_value(env)))
      obj.props.set(
        "createRealm",
        property_data(new_builtin_value(BuiltinFunction::Test262CreateRealm)),
      )
      obj.props.set(
        "IsHTMLDDA",
        property_data(new_html_dda_builtin(BuiltinFunction::Test262IsHTMLDDA)),
      )
      obj.props.set(
        "gc",
        property_data(new_builtin_value(BuiltinFunction::Test262Gc)),
      )
      obj.props.set("agent", property_data(new_test262_agent_object()))
    }
    _ => ()
  }
  obj_value
}

///|
fn install_test262_helpers(env : Env) -> Value {
  let global_value = global_object_value(env)
  let obj262 = new_test262_object(env)
  match global_value {
    Object(obj) => {
      obj.props.set(
        "print",
        property_data(new_builtin_value(BuiltinFunction::Print)),
      )
      obj.props.set("$262", property_data(obj262))
    }
    _ => ()
  }
  obj262
}
