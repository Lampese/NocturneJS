///|
fn call_builtin_generator(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match builtin.kind {
    BuiltinFunction::GeneratorNext => {
      let obj = require_generator(builtin.this_value)
      let arg = if args.is_empty() { Undefined } else { args[0] }
      generator_next_object(obj, arg)
    }
    BuiltinFunction::GeneratorReturn => {
      let obj = require_generator(builtin.this_value)
      let arg = if args.is_empty() { Undefined } else { args[0] }
      generator_return_object(obj, arg)
    }
    BuiltinFunction::GeneratorThrow => {
      let obj = require_generator(builtin.this_value)
      let arg = if args.is_empty() { Undefined } else { args[0] }
      generator_throw_object(obj, arg)
    }
    BuiltinFunction::AsyncGeneratorNext => {
      let arg = if args.is_empty() { Undefined } else { args[0] }
      async_generator_request(builtin.this_value, GenResume::Next(arg))
    }
    BuiltinFunction::AsyncGeneratorReturn => {
      let arg = if args.is_empty() { Undefined } else { args[0] }
      async_generator_request(builtin.this_value, GenResume::Return(arg))
    }
    BuiltinFunction::AsyncGeneratorThrow => {
      let arg = if args.is_empty() { Undefined } else { args[0] }
      async_generator_request(builtin.this_value, GenResume::Throw(arg))
    }
    BuiltinFunction::AsyncGeneratorResolveFunction =>
      match async_generator_resolve_func_data_get(builtin.id) {
        Some(data) => {
          let arg = if args.is_empty() { Undefined } else { args[0] }
          match data.generator {
            Object(obj) =>
              match obj.async_generator_data {
                Some(gen_data) =>
                  if data.resume_next {
                    gen_data.state = AsyncGeneratorState::Completed
                    if data.is_reject {
                      async_generator_reject(gen_data, arg)
                    } else {
                      async_generator_resolve(gen_data, arg, true)
                    }
                  } else {
                    let resume_state = if data.is_reject {
                      GenResume::Throw(arg)
                    } else {
                      GenResume::Next(arg)
                    }
                    async_generator_resume_next(obj, Some(resume_state))
                  }
                None => ()
              }
            _ => ()
          }
          Undefined
        }
        None => Undefined
      }
    BuiltinFunction::GeneratorFunctionConstructor =>
      match function_realm_env(Value::Builtin(builtin)) {
        Some(env) => function_from_args_with_env_kind(args, env, true, false)
        None => generator_function_from_args(args)
      }
    BuiltinFunction::AsyncGeneratorFunctionConstructor =>
      match function_realm_env(Value::Builtin(builtin)) {
        Some(env) => function_from_args_with_env_kind(args, env, true, true)
        None => async_generator_function_from_args(args)
      }
    _ => throw_type_error("invalid builtin")
  }
}
