///|
let map_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let set_proto_ref_cell : Ref[ObjectValue?] = Ref::new(None)

///|
let weakmap_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let weakset_proto_ref_cell : Ref[ObjectValue?] = Ref::new(None)

///|
let weakref_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let finreg_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
fn map_proto() -> ObjectValue? {
  get_proto_ref(map_proto_ref)
}

///|
fn set_proto() -> ObjectValue? {
  get_proto_ref(set_proto_ref_cell)
}

///|
fn weakmap_proto() -> ObjectValue? {
  get_proto_ref(weakmap_proto_ref)
}

///|
fn weakset_proto() -> ObjectValue? {
  get_proto_ref(weakset_proto_ref_cell)
}

///|
fn weakref_proto() -> ObjectValue? {
  get_proto_ref(weakref_proto_ref)
}

///|
fn finreg_proto() -> ObjectValue? {
  get_proto_ref(finreg_proto_ref)
}

///|
fn map_proto_for_env(env : Env) -> ObjectValue? {
  match global_object(env) {
    Some(global_obj) =>
      match props_get(global_obj.props, "Map") {
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
fn set_proto_for_env(env : Env) -> ObjectValue? {
  match global_object(env) {
    Some(global_obj) =>
      match props_get(global_obj.props, "Set") {
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
fn weakmap_proto_for_env(env : Env) -> ObjectValue? {
  match global_object(env) {
    Some(global_obj) =>
      match props_get(global_obj.props, "WeakMap") {
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
fn weakset_proto_for_env(env : Env) -> ObjectValue? {
  match global_object(env) {
    Some(global_obj) =>
      match props_get(global_obj.props, "WeakSet") {
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
fn weakref_proto_for_env(env : Env) -> ObjectValue? {
  match global_object(env) {
    Some(global_obj) =>
      match props_get(global_obj.props, "WeakRef") {
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
fn finreg_proto_for_env(env : Env) -> ObjectValue? {
  match global_object(env) {
    Some(global_obj) =>
      match props_get(global_obj.props, "FinalizationRegistry") {
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
