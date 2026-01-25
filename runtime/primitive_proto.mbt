///|
let string_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let bool_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let number_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let bigint_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let symbol_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
fn string_proto() -> ObjectValue? {
  get_proto_ref(string_proto_ref)
}

///|
fn bool_proto() -> ObjectValue? {
  get_proto_ref(bool_proto_ref)
}

///|
fn number_proto() -> ObjectValue? {
  get_proto_ref(number_proto_ref)
}

///|
fn bigint_proto() -> ObjectValue? {
  get_proto_ref(bigint_proto_ref)
}

///|
fn symbol_proto() -> ObjectValue? {
  get_proto_ref(symbol_proto_ref)
}

///|
fn string_proto_for_env(env : Env) -> ObjectValue? {
  match global_object(env) {
    Some(global_obj) =>
      match props_get(global_obj.props, "String") {
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
            Function(func) =>
              match props_get(func.props, "prototype") {
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
fn number_proto_for_env(env : Env) -> ObjectValue? {
  match global_object(env) {
    Some(global_obj) =>
      match props_get(global_obj.props, "Number") {
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
            Function(func) =>
              match props_get(func.props, "prototype") {
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
fn bool_proto_for_env(env : Env) -> ObjectValue? {
  match global_object(env) {
    Some(global_obj) =>
      match props_get(global_obj.props, "Boolean") {
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
            Function(func) =>
              match props_get(func.props, "prototype") {
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
fn bigint_proto_for_env(env : Env) -> ObjectValue? {
  match global_object(env) {
    Some(global_obj) =>
      match props_get(global_obj.props, "BigInt") {
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
            Function(func) =>
              match props_get(func.props, "prototype") {
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
fn symbol_proto_for_env(env : Env) -> ObjectValue? {
  match global_object(env) {
    Some(global_obj) =>
      match props_get(global_obj.props, "Symbol") {
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
            Function(func) =>
              match props_get(func.props, "prototype") {
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
