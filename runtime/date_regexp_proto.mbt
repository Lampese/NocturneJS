///|
let regexp_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let date_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
fn regexp_proto() -> ObjectValue? {
  get_proto_ref(regexp_proto_ref)
}

///|
fn date_proto() -> ObjectValue? {
  get_proto_ref(date_proto_ref)
}

///|
fn date_proto_for_env(env : Env) -> ObjectValue? {
  match global_object(env) {
    Some(global_obj) =>
      match global_obj.props.get("Date") {
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
fn regexp_proto_for_env(env : Env) -> ObjectValue? {
  match global_object(env) {
    Some(global_obj) =>
      match global_obj.props.get("RegExp") {
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
            _ => None
          }
        None => None
      }
    None => None
  }
}
