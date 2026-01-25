///|
let object_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let function_proto_ref : Ref[Value?] = Ref::new(None)

///|
fn object_proto() -> ObjectValue? {
  get_proto_ref(object_proto_ref)
}

///|
fn function_proto() -> Value? {
  get_value_ref(function_proto_ref)
}

///|
fn object_proto_for_env(env : Env) -> ObjectValue? {
  match global_object(env) {
    Some(global_obj) =>
      match props_get(global_obj.props, "Object") {
        Some(prop) =>
          match prop.value {
            Builtin(builtin) =>
              match props_get(builtin.props, "prototype") {
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
fn function_proto_for_env(env : Env) -> Value? {
  match global_object(env) {
    Some(global_obj) =>
      match props_get(global_obj.props, "Function") {
        Some(prop) =>
          match prop.value {
            Builtin(builtin) =>
              match props_get(builtin.props, "prototype") {
                Some(proto_prop) => Some(proto_prop.value)
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
fn function_ctor_for_env(env : Env) -> Value? {
  match global_object(env) {
    Some(global_obj) =>
      match props_get(global_obj.props, "Function") {
        Some(prop) => Some(prop.value)
        None => None
      }
    None => None
  }
}
