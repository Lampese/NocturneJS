///|
fn call_builtin_async_function(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match builtin.kind {
    BuiltinFunction::AsyncFunctionResolveFunction =>
      match async_function_resolve_func_data_get(builtin.id) {
        Some(data) => {
          let arg = if args.is_empty() { Undefined } else { args[0] }
          match data.generator {
            Object(obj) => {
              let resume_state = if data.is_reject {
                GenResume::Throw(arg)
              } else {
                GenResume::Next(arg)
              }
              async_function_resume(obj, resume_state)
            }
            _ => ()
          }
          Undefined
        }
        None => Undefined
      }
    BuiltinFunction::AsyncFunctionConstructor =>
      match function_realm_env(Value::Builtin(builtin)) {
        Some(env) => function_from_args_with_env_kind(args, env, false, true)
        None => async_function_from_args(args)
      }
    _ => throw_type_error("invalid builtin")
  }
}
