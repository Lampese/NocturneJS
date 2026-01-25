///|
fn new_builtin_value(kind : BuiltinFunction) -> Value {
  let props = props_new()
  props_set(props, 
    "length",
    property_data_readonly(Number(Double::from_int(builtin_length(kind)))),
  )
  props_set(props, "name", property_data_readonly(String(builtin_name(kind))))
  let realm_env = match current_env() {
    Some(env) => Some(env)
    None => root_env()
  }
  let proto = match realm_env {
    Some(env) =>
      match function_proto_for_env(env) {
        Some(value) => Some(value)
        None => function_proto()
      }
    None => function_proto()
  }
  let builtin = register_builtin_value(BuiltinValue::{
    kind,
    id: alloc_id(),
    props,
    this_value: None,
    proto,
    extensible: true,
    is_html_dda: false,
    realm_env,
  })
  rc_incref_optional_value(proto)
  rc_incref_optional_env(realm_env)
  Builtin(builtin)
}

///|
fn new_builtin_value_with_this(
  kind : BuiltinFunction,
  this_value : Value,
) -> Value {
  match new_builtin_value(kind) {
    Builtin(builtin) => {
      rc_incref_value(this_value)
      let with_this = register_builtin_value(BuiltinValue::{
        kind: builtin.kind,
        id: builtin.id,
        props: builtin.props,
        this_value: Some(this_value),
        proto: builtin.proto,
        extensible: builtin.extensible,
        is_html_dda: builtin.is_html_dda,
        realm_env: builtin.realm_env,
      })
      Builtin(with_this)
    }
    value => value
  }
}

///|
fn new_html_dda_builtin(kind : BuiltinFunction) -> Value {
  match new_builtin_value(kind) {
    Builtin(builtin) => {
      builtin.is_html_dda = true
      Builtin(builtin)
    }
    value => value
  }
}

///|
