///|
let error_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let aggregate_error_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let eval_error_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let syntax_error_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let range_error_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let type_error_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let reference_error_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let uri_error_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
fn error_proto() -> ObjectValue? {
  get_proto_ref(error_proto_ref)
}

///|
fn aggregate_error_proto() -> ObjectValue? {
  get_proto_ref(aggregate_error_proto_ref)
}

///|
fn eval_error_proto() -> ObjectValue? {
  get_proto_ref(eval_error_proto_ref)
}

///|
fn syntax_error_proto() -> ObjectValue? {
  get_proto_ref(syntax_error_proto_ref)
}

///|
fn range_error_proto() -> ObjectValue? {
  get_proto_ref(range_error_proto_ref)
}

///|
fn type_error_proto() -> ObjectValue? {
  get_proto_ref(type_error_proto_ref)
}

///|
fn reference_error_proto() -> ObjectValue? {
  get_proto_ref(reference_error_proto_ref)
}

///|
fn uri_error_proto() -> ObjectValue? {
  get_proto_ref(uri_error_proto_ref)
}

///|
fn error_proto_for_env_name(env : Env, name : String) -> ObjectValue? {
  match global_object(env) {
    Some(global_obj) =>
      match global_obj.props.get(name) {
        Some(prop) =>
          match prop.value {
            Builtin(builtin) =>
              match builtin.props.get("prototype") {
                Some(proto_prop) =>
                  match proto_prop.value {
                    Object(proto_obj) => Some(proto_obj)
                    _ => None
                  }
                None => None
              }
            Function(func) =>
              match func.props.get("prototype") {
                Some(proto_prop) =>
                  match proto_prop.value {
                    Object(proto_obj) => Some(proto_obj)
                    _ => None
                  }
                None => None
              }
            _ => None
          }
        None => None
      }
    None => None
  }
}

///|
fn type_error_proto_for_env(env : Env) -> ObjectValue? {
  error_proto_for_env_name(env, "TypeError")
}

///|
fn reference_error_proto_for_env(env : Env) -> ObjectValue? {
  error_proto_for_env_name(env, "ReferenceError")
}

///|
fn syntax_error_proto_for_env(env : Env) -> ObjectValue? {
  error_proto_for_env_name(env, "SyntaxError")
}

///|
fn range_error_proto_for_env(env : Env) -> ObjectValue? {
  error_proto_for_env_name(env, "RangeError")
}

///|
fn uri_error_proto_for_env(env : Env) -> ObjectValue? {
  error_proto_for_env_name(env, "URIError")
}
