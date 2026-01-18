///|
let iterator_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let async_iterator_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let async_from_sync_iterator_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let async_iterator_proto_env_ref : Ref[Map[Int, ObjectValue]] = Ref::new(
  Map::new(),
)

///|
let iterator_concat_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let iterator_helper_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let iterator_wrap_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let array_iterator_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let string_iterator_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let regexp_string_iterator_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let map_iterator_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
let set_iterator_proto_ref : Ref[ObjectValue?] = Ref::new(None)

///|
fn iterator_proto() -> ObjectValue? {
  get_proto_ref(iterator_proto_ref)
}

///|
fn async_iterator_proto() -> ObjectValue? {
  get_proto_ref(async_iterator_proto_ref)
}

///|
fn async_from_sync_iterator_proto() -> ObjectValue? {
  get_proto_ref(async_from_sync_iterator_proto_ref)
}

///|
fn iterator_concat_proto() -> ObjectValue? {
  get_proto_ref(iterator_concat_proto_ref)
}

///|
fn iterator_helper_proto() -> ObjectValue? {
  get_proto_ref(iterator_helper_proto_ref)
}

///|
fn iterator_wrap_proto() -> ObjectValue? {
  get_proto_ref(iterator_wrap_proto_ref)
}

///|
fn array_iterator_proto() -> ObjectValue? {
  get_proto_ref(array_iterator_proto_ref)
}

///|
fn string_iterator_proto() -> ObjectValue? {
  get_proto_ref(string_iterator_proto_ref)
}

///|
fn regexp_string_iterator_proto() -> ObjectValue? {
  get_proto_ref(regexp_string_iterator_proto_ref)
}

///|
fn map_iterator_proto() -> ObjectValue? {
  get_proto_ref(map_iterator_proto_ref)
}

///|
fn set_iterator_proto() -> ObjectValue? {
  get_proto_ref(set_iterator_proto_ref)
}

///|
fn iterator_proto_for_env(env : Env) -> ObjectValue? {
  match global_object(env) {
    Some(global_obj) =>
      match global_obj.props.get("Iterator") {
        Some(prop) =>
          match prop.value {
            Builtin(builtin) =>
              match builtin.props.get("prototype") {
                Some(proto_prop) =>
                  match proto_prop.value {
                    Object(proto_obj) => {
                      match object_proto_for_env(env) {
                        Some(obj_proto) =>
                          proto_obj.proto = Some(Object(obj_proto))
                        None => ()
                      }
                      Some(proto_obj)
                    }
                    _ => None
                  }
                None => None
              }
            Function(func) =>
              match func.props.get("prototype") {
                Some(proto_prop) =>
                  match proto_prop.value {
                    Object(proto_obj) => {
                      match object_proto_for_env(env) {
                        Some(obj_proto) =>
                          proto_obj.proto = Some(Object(obj_proto))
                        None => ()
                      }
                      Some(proto_obj)
                    }
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
