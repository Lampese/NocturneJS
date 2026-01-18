///|
fn call_builtin_json(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match builtin.kind {
    BuiltinFunction::JsonStringify => {
      let value = if args.is_empty() { Undefined } else { args[0] }
      let replacer = if args.length() > 1 { args[1] } else { Undefined }
      let space = if args.length() > 2 { args[2] } else { Undefined }
      json_stringify_with_replacer(value, replacer, space)
    }
    BuiltinFunction::JsonParse => {
      let text_value = if args.is_empty() { Undefined } else { args[0] }
      let reviver = if args.length() > 1 { args[1] } else { Undefined }
      json_parse_with_reviver(text_value, reviver)
    }
    BuiltinFunction::BjsonRead => bjson_read_value(args)
    BuiltinFunction::BjsonWrite => bjson_write_value(args)
    _ => throw_type_error("invalid builtin")
  }
}
