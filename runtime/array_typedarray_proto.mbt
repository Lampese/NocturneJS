///|
let array_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let array_buffer_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let shared_array_buffer_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let dataview_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let typed_array_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let uint8_array_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let int8_array_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let uint8_clamped_array_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let uint16_array_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let int16_array_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let uint32_array_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let int32_array_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let bigint64_array_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let biguint64_array_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let float16_array_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let float32_array_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let float64_array_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
fn array_proto() -> ObjectValue? {
  get_proto_ref(array_proto_ref)
}

///|
fn array_buffer_proto() -> ObjectValue? {
  get_proto_ref(array_buffer_proto_ref)
}

///|
fn shared_array_buffer_proto() -> ObjectValue? {
  get_proto_ref(shared_array_buffer_proto_ref)
}

///|
fn dataview_proto() -> ObjectValue? {
  get_proto_ref(dataview_proto_ref)
}

///|
fn typed_array_proto_for_kind(kind : TypedArrayKind) -> ObjectValue? {
  match kind {
    TypedArrayKind::Uint8 => get_proto_ref(uint8_array_proto_ref)
    TypedArrayKind::Int8 => get_proto_ref(int8_array_proto_ref)
    TypedArrayKind::Uint8Clamped => get_proto_ref(uint8_clamped_array_proto_ref)
    TypedArrayKind::Uint16 => get_proto_ref(uint16_array_proto_ref)
    TypedArrayKind::Int16 => get_proto_ref(int16_array_proto_ref)
    TypedArrayKind::Uint32 => get_proto_ref(uint32_array_proto_ref)
    TypedArrayKind::Int32 => get_proto_ref(int32_array_proto_ref)
    TypedArrayKind::BigInt64 => get_proto_ref(bigint64_array_proto_ref)
    TypedArrayKind::BigUint64 => get_proto_ref(biguint64_array_proto_ref)
    TypedArrayKind::Float16 => get_proto_ref(float16_array_proto_ref)
    TypedArrayKind::Float32 => get_proto_ref(float32_array_proto_ref)
    TypedArrayKind::Float64 => get_proto_ref(float64_array_proto_ref)
  }
}

///|
fn array_proto_for_env(env : Env) -> ObjectValue? {
  match global_object(env) {
    Some(global_obj) =>
      match global_obj.props.get("Array") {
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
fn array_buffer_proto_for_env(env : Env) -> ObjectValue? {
  match global_object(env) {
    Some(global_obj) =>
      match global_obj.props.get("ArrayBuffer") {
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
fn shared_array_buffer_proto_for_env(env : Env) -> ObjectValue? {
  match global_object(env) {
    Some(global_obj) =>
      match global_obj.props.get("SharedArrayBuffer") {
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
fn dataview_proto_for_env(env : Env) -> ObjectValue? {
  match global_object(env) {
    Some(global_obj) =>
      match global_obj.props.get("DataView") {
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
fn typed_array_ctor_for_env(env : Env) -> Value? {
  match global_object(env) {
    Some(global_obj) =>
      match global_obj.props.get("TypedArray") {
        Some(prop) => Some(prop.value)
        None => None
      }
    None => None
  }
}

///|
fn typed_array_proto_for_kind_env(
  env : Env,
  kind : TypedArrayKind,
) -> ObjectValue? {
  let name = typed_array_constructor_name(kind)
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
fn typed_array_ctor_for_kind_env(env : Env, kind : TypedArrayKind) -> Value? {
  let name = typed_array_constructor_name(kind)
  match global_object(env) {
    Some(global_obj) =>
      match global_obj.props.get(name) {
        Some(prop) => Some(prop.value)
        None => None
      }
    None => None
  }
}
