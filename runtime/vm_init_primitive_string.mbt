///|
fn init_string_proto(object_proto_obj : ObjectValue) -> (ObjectValue, Value) {
  let string_proto_value = {
    let obj = register_object_value(ObjectValue::{
      id: alloc_id(),
      props: Map::new(),
      proto: Some(Object(object_proto_obj)),
      string_data: Some(""),
      number_data: None,
      bool_data: None,
      bigint_data: None,
      symbol_data: None,
      regexp_data: None,
      generator_data: None,
      async_generator_data: None,
      proxy_data: None,
      map_data: None,
      set_data: None,
      map_iter_data: None,
      set_iter_data: None,
      iterator_wrap_data: None,
      iterator_concat_data: None,
      iterator_helper_data: None,
      weakmap_data: None,
      weakset_data: None,
      weakref_data: None,
      finreg_data: None,
      date_data: None,
      array_buffer_data: None,
      dataview_data: None,
      is_html_dda: false,
      is_error: false,
      is_module_namespace: false,
      realm_env: None,
      immutable_proto: false,
      extensible: true,
    })
    Object(obj)
  }
  let string_proto_obj = match string_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(object_proto_obj)))
  }
  set_proto_ref(string_proto_ref, Some(string_proto_obj))
  string_proto_obj.props.set(
    "charCodeAt",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringCharCodeAt)),
  )
  string_proto_obj.props.set(
    "charAt",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringCharAt)),
  )
  string_proto_obj.props.set(
    "codePointAt",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::StringCodePointAt),
    ),
  )
  string_proto_obj.props.set(
    "at",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringAt)),
  )
  string_proto_obj.props.set(
    "concat",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringConcat)),
  )
  string_proto_obj.props.set(
    "substring",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringSubstring)),
  )
  string_proto_obj.props.set(
    "substr",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringSubstr)),
  )
  string_proto_obj.props.set(
    "slice",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringSlice)),
  )
  string_proto_obj.props.set(
    "indexOf",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringIndexOf)),
  )
  string_proto_obj.props.set(
    "lastIndexOf",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::StringLastIndexOf),
    ),
  )
  string_proto_obj.props.set(
    "includes",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringIncludes)),
  )
  string_proto_obj.props.set(
    "startsWith",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringStartsWith)),
  )
  string_proto_obj.props.set(
    "endsWith",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringEndsWith)),
  )
  string_proto_obj.props.set(
    "match",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringMatch)),
  )
  string_proto_obj.props.set(
    "matchAll",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringMatchAll)),
  )
  string_proto_obj.props.set(
    "search",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringSearch)),
  )
  string_proto_obj.props.set(
    "split",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringSplit)),
  )
  string_proto_obj.props.set(
    "padStart",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringPadStart)),
  )
  string_proto_obj.props.set(
    "padEnd",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringPadEnd)),
  )
  string_proto_obj.props.set(
    "repeat",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringRepeat)),
  )
  string_proto_obj.props.set(
    "replace",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringReplace)),
  )
  string_proto_obj.props.set(
    "replaceAll",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringReplaceAll)),
  )
  string_proto_obj.props.set(
    "trim",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringTrim)),
  )
  let trim_start = new_builtin_value(BuiltinFunction::StringTrimStart)
  let trim_end = new_builtin_value(BuiltinFunction::StringTrimEnd)
  string_proto_obj.props.set("trimStart", property_data_non_enum(trim_start))
  string_proto_obj.props.set("trimEnd", property_data_non_enum(trim_end))
  string_proto_obj.props.set("trimLeft", property_data_non_enum(trim_start))
  string_proto_obj.props.set("trimRight", property_data_non_enum(trim_end))
  string_proto_obj.props.set(
    "toLowerCase",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::StringToLowerCase),
    ),
  )
  string_proto_obj.props.set(
    "toUpperCase",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::StringToUpperCase),
    ),
  )
  string_proto_obj.props.set(
    "toLocaleLowerCase",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::StringToLocaleLowerCase),
    ),
  )
  string_proto_obj.props.set(
    "toLocaleUpperCase",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::StringToLocaleUpperCase),
    ),
  )
  string_proto_obj.props.set(
    "localeCompare",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::StringLocaleCompare),
    ),
  )
  string_proto_obj.props.set(
    "normalize",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringNormalize)),
  )
  string_proto_obj.props.set(
    "isWellFormed",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::StringIsWellFormed),
    ),
  )
  string_proto_obj.props.set(
    "toWellFormed",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::StringToWellFormed),
    ),
  )
  string_proto_obj.props.set(
    "toString",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringToString)),
  )
  string_proto_obj.props.set(
    "valueOf",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringValueOf)),
  )
  string_proto_obj.props.set(
    "anchor",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringAnchor)),
  )
  string_proto_obj.props.set(
    "big",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringBig)),
  )
  string_proto_obj.props.set(
    "blink",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringBlink)),
  )
  string_proto_obj.props.set(
    "bold",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringBold)),
  )
  string_proto_obj.props.set(
    "fixed",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringFixed)),
  )
  string_proto_obj.props.set(
    "fontcolor",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringFontcolor)),
  )
  string_proto_obj.props.set(
    "fontsize",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringFontsize)),
  )
  string_proto_obj.props.set(
    "italics",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringItalics)),
  )
  string_proto_obj.props.set(
    "link",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringLink)),
  )
  string_proto_obj.props.set(
    "small",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringSmall)),
  )
  string_proto_obj.props.set(
    "strike",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringStrike)),
  )
  string_proto_obj.props.set(
    "sub",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringSub)),
  )
  string_proto_obj.props.set(
    "sup",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringSup)),
  )
  match symbol_iterator_key() {
    Some(key) =>
      string_proto_obj.props.set(
        key,
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::StringIterator),
        ),
      )
    None => ()
  }
  (string_proto_obj, string_proto_value)
}
