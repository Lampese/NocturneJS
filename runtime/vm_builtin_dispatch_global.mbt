///|
fn call_builtin_global(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match builtin.kind {
    BuiltinFunction::Eval => {
      let eval_env = match builtin.realm_env {
        Some(env) => env
        None =>
          match root_env() {
            Some(env) => env
            None => Env::new(None)
          }
      }
      eval_with_env_mode(eval_env, args, false)
    }
    BuiltinFunction::IsNaN => {
      let value = if args.is_empty() { Undefined } else { args[0] }
      Bool(Double::is_nan(to_number(value)))
    }
    BuiltinFunction::IsFinite => {
      let value = if args.is_empty() { Undefined } else { args[0] }
      let num = to_number(value)
      Bool(!Double::is_nan(num) && !Double::is_inf(num))
    }
    BuiltinFunction::ParseInt => parse_int_value(args)
    BuiltinFunction::ParseFloat => parse_float_value(args)
    BuiltinFunction::DecodeURI => {
      let text = if args.is_empty() {
        "undefined"
      } else {
        to_string_strict(args[0])
      }
      String(uri_decode(text, false))
    }
    BuiltinFunction::DecodeURIComponent => {
      let text = if args.is_empty() {
        "undefined"
      } else {
        to_string_strict(args[0])
      }
      String(uri_decode(text, true))
    }
    BuiltinFunction::EncodeURI => {
      let text = if args.is_empty() {
        "undefined"
      } else {
        to_string_strict(args[0])
      }
      String(uri_encode(text, false))
    }
    BuiltinFunction::EncodeURIComponent => {
      let text = if args.is_empty() {
        "undefined"
      } else {
        to_string_strict(args[0])
      }
      String(uri_encode(text, true))
    }
    _ => throw_type_error("invalid builtin")
  }
}
