///|
fn init_promise_proxy_ctors(
  env : Env,
  promise_proto_obj : ObjectValue,
  promise_proto_value : Value,
  species_key : String,
) -> (Value, Value) {
  let promise_ctor = new_builtin_value(BuiltinFunction::PromiseConstructor)
  match promise_ctor {
    Builtin(builtin) => {
      props_set(builtin.props, "prototype", property_data_const(promise_proto_value))
      props_set(builtin.props, 
        "resolve",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::PromiseResolve),
        ),
      )
      props_set(builtin.props, 
        "reject",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::PromiseReject),
        ),
      )
      props_set(builtin.props, 
        "all",
        property_data_non_enum(new_builtin_value(BuiltinFunction::PromiseAll)),
      )
      props_set(builtin.props, 
        "allSettled",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::PromiseAllSettled),
        ),
      )
      props_set(builtin.props, 
        "any",
        property_data_non_enum(new_builtin_value(BuiltinFunction::PromiseAny)),
      )
      props_set(builtin.props, 
        "race",
        property_data_non_enum(new_builtin_value(BuiltinFunction::PromiseRace)),
      )
      props_set(builtin.props, 
        "try",
        property_data_non_enum(new_builtin_value(BuiltinFunction::PromiseTry)),
      )
      props_set(builtin.props, 
        "withResolvers",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::PromiseWithResolvers),
        ),
      )
      set_accessor_property_with_enum(
        builtin.props,
        species_key,
        Some(new_builtin_value(BuiltinFunction::SpeciesGetter)),
        None,
        false,
      )
    }
    _ => ()
  }
  props_set(promise_proto_obj.props, 
    "constructor",
    property_data_non_enum(promise_ctor),
  )
  match promise_ctor {
    Builtin(builtin) => register_promise_ctor_for_env(env, builtin)
    _ => ()
  }
  let proxy_ctor = new_builtin_value(BuiltinFunction::ProxyConstructor)
  match proxy_ctor {
    Builtin(builtin) =>
      props_set(builtin.props, 
        "revocable",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::ProxyRevocable),
        ),
      )
    _ => ()
  }
  (promise_ctor, proxy_ctor)
}
