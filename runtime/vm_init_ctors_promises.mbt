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
      builtin.props.set("prototype", property_data_const(promise_proto_value))
      builtin.props.set(
        "resolve",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::PromiseResolve),
        ),
      )
      builtin.props.set(
        "reject",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::PromiseReject),
        ),
      )
      builtin.props.set(
        "all",
        property_data_non_enum(new_builtin_value(BuiltinFunction::PromiseAll)),
      )
      builtin.props.set(
        "allSettled",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::PromiseAllSettled),
        ),
      )
      builtin.props.set(
        "any",
        property_data_non_enum(new_builtin_value(BuiltinFunction::PromiseAny)),
      )
      builtin.props.set(
        "race",
        property_data_non_enum(new_builtin_value(BuiltinFunction::PromiseRace)),
      )
      builtin.props.set(
        "try",
        property_data_non_enum(new_builtin_value(BuiltinFunction::PromiseTry)),
      )
      builtin.props.set(
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
  promise_proto_obj.props.set(
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
      builtin.props.set(
        "revocable",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::ProxyRevocable),
        ),
      )
    _ => ()
  }
  (promise_ctor, proxy_ctor)
}
