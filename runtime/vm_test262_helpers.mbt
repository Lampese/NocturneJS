///|
fn new_test262_agent_object() -> Value {
  let obj_value = new_object_value_with_proto(value_from_object(object_proto()))
  match obj_value {
    Object(obj) => {
      props_set(obj.props, 
        "start",
        property_data(new_builtin_value(BuiltinFunction::Test262AgentStart)),
      )
      props_set(obj.props, 
        "getReport",
        property_data(new_builtin_value(BuiltinFunction::Test262AgentGetReport)),
      )
      props_set(obj.props, 
        "broadcast",
        property_data(new_builtin_value(BuiltinFunction::Test262AgentBroadcast)),
      )
      props_set(obj.props, 
        "report",
        property_data(new_builtin_value(BuiltinFunction::Test262AgentReport)),
      )
      props_set(obj.props, 
        "leaving",
        property_data(new_builtin_value(BuiltinFunction::Test262AgentLeaving)),
      )
      props_set(obj.props, 
        "receiveBroadcast",
        property_data(
          new_builtin_value(BuiltinFunction::Test262AgentReceiveBroadcast),
        ),
      )
      props_set(obj.props, 
        "sleep",
        property_data(new_builtin_value(BuiltinFunction::Test262AgentSleep)),
      )
      props_set(obj.props, 
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
      rc_replace_optional_env(obj.realm_env, Some(env))
      obj.realm_env = Some(env)
      props_set(obj.props, 
        "detachArrayBuffer",
        property_data(
          new_builtin_value(BuiltinFunction::Test262DetachArrayBuffer),
        ),
      )
      props_set(obj.props, 
        "evalScript",
        property_data(new_builtin_value(BuiltinFunction::Test262EvalScript)),
      )
      props_set(obj.props, 
        "codePointRange",
        property_data(new_builtin_value(BuiltinFunction::Test262CodePointRange)),
      )
      props_set(obj.props, "global", property_data(global_object_value(env)))
      props_set(obj.props, 
        "createRealm",
        property_data(new_builtin_value(BuiltinFunction::Test262CreateRealm)),
      )
      props_set(obj.props, 
        "IsHTMLDDA",
        property_data(new_html_dda_builtin(BuiltinFunction::Test262IsHTMLDDA)),
      )
      props_set(obj.props, 
        "gc",
        property_data(new_builtin_value(BuiltinFunction::Test262Gc)),
      )
      props_set(obj.props, "agent", property_data(new_test262_agent_object()))
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
      props_set(obj.props, 
        "print",
        property_data(new_builtin_value(BuiltinFunction::Print)),
      )
      props_set(obj.props, "$262", property_data(obj262))
    }
    _ => ()
  }
  obj262
}
