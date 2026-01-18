///|
fn call_builtin_boolean(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match builtin.kind {
    BuiltinFunction::BooleanConstructor => {
      if args.is_empty() {
        return Bool(false)
      }
      Bool(is_truthy(args[0]))
    }
    BuiltinFunction::BooleanToString => {
      let value = this_to_bool(builtin.this_value)
      if value {
        String("true")
      } else {
        String("false")
      }
    }
    BuiltinFunction::BooleanValueOf => Bool(this_to_bool(builtin.this_value))
    _ => throw_type_error("invalid builtin")
  }
}
