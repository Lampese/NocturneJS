///|
fn init_string_proto(object_proto_obj : ObjectValue) -> (ObjectValue, Value) {
  let string_proto_value = {
    let obj = register_object_value(ObjectValue::{
      id: alloc_id(),
      props: props_new(),
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
    rc_incref_optional_value(obj.proto)
    Object(obj)
  }
  let string_proto_obj = match string_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(object_proto_obj)))
  }
  set_proto_ref(string_proto_ref, Some(string_proto_obj))
  props_set(string_proto_obj.props, 
    "charCodeAt",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringCharCodeAt)),
  )
  props_set(string_proto_obj.props, 
    "charAt",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringCharAt)),
  )
  props_set(string_proto_obj.props, 
    "codePointAt",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::StringCodePointAt),
    ),
  )
  props_set(string_proto_obj.props, 
    "at",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringAt)),
  )
  props_set(string_proto_obj.props, 
    "concat",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringConcat)),
  )
  props_set(string_proto_obj.props, 
    "substring",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringSubstring)),
  )
  props_set(string_proto_obj.props, 
    "substr",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringSubstr)),
  )
  props_set(string_proto_obj.props, 
    "slice",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringSlice)),
  )
  props_set(string_proto_obj.props, 
    "indexOf",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringIndexOf)),
  )
  props_set(string_proto_obj.props, 
    "lastIndexOf",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::StringLastIndexOf),
    ),
  )
  props_set(string_proto_obj.props, 
    "includes",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringIncludes)),
  )
  props_set(string_proto_obj.props, 
    "startsWith",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringStartsWith)),
  )
  props_set(string_proto_obj.props, 
    "endsWith",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringEndsWith)),
  )
  props_set(string_proto_obj.props, 
    "match",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringMatch)),
  )
  props_set(string_proto_obj.props, 
    "matchAll",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringMatchAll)),
  )
  props_set(string_proto_obj.props, 
    "search",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringSearch)),
  )
  props_set(string_proto_obj.props, 
    "split",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringSplit)),
  )
  props_set(string_proto_obj.props, 
    "padStart",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringPadStart)),
  )
  props_set(string_proto_obj.props, 
    "padEnd",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringPadEnd)),
  )
  props_set(string_proto_obj.props, 
    "repeat",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringRepeat)),
  )
  props_set(string_proto_obj.props, 
    "replace",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringReplace)),
  )
  props_set(string_proto_obj.props, 
    "replaceAll",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringReplaceAll)),
  )
  props_set(string_proto_obj.props, 
    "trim",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringTrim)),
  )
  let trim_start = new_builtin_value(BuiltinFunction::StringTrimStart)
  let trim_end = new_builtin_value(BuiltinFunction::StringTrimEnd)
  props_set(string_proto_obj.props, "trimStart", property_data_non_enum(trim_start))
  props_set(string_proto_obj.props, "trimEnd", property_data_non_enum(trim_end))
  props_set(string_proto_obj.props, "trimLeft", property_data_non_enum(trim_start))
  props_set(string_proto_obj.props, "trimRight", property_data_non_enum(trim_end))
  props_set(string_proto_obj.props, 
    "toLowerCase",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::StringToLowerCase),
    ),
  )
  props_set(string_proto_obj.props, 
    "toUpperCase",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::StringToUpperCase),
    ),
  )
  props_set(string_proto_obj.props, 
    "toLocaleLowerCase",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::StringToLocaleLowerCase),
    ),
  )
  props_set(string_proto_obj.props, 
    "toLocaleUpperCase",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::StringToLocaleUpperCase),
    ),
  )
  props_set(string_proto_obj.props, 
    "localeCompare",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::StringLocaleCompare),
    ),
  )
  props_set(string_proto_obj.props, 
    "normalize",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringNormalize)),
  )
  props_set(string_proto_obj.props, 
    "isWellFormed",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::StringIsWellFormed),
    ),
  )
  props_set(string_proto_obj.props, 
    "toWellFormed",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::StringToWellFormed),
    ),
  )
  props_set(string_proto_obj.props, 
    "toString",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringToString)),
  )
  props_set(string_proto_obj.props, 
    "valueOf",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringValueOf)),
  )
  props_set(string_proto_obj.props, 
    "anchor",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringAnchor)),
  )
  props_set(string_proto_obj.props, 
    "big",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringBig)),
  )
  props_set(string_proto_obj.props, 
    "blink",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringBlink)),
  )
  props_set(string_proto_obj.props, 
    "bold",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringBold)),
  )
  props_set(string_proto_obj.props, 
    "fixed",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringFixed)),
  )
  props_set(string_proto_obj.props, 
    "fontcolor",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringFontcolor)),
  )
  props_set(string_proto_obj.props, 
    "fontsize",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringFontsize)),
  )
  props_set(string_proto_obj.props, 
    "italics",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringItalics)),
  )
  props_set(string_proto_obj.props, 
    "link",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringLink)),
  )
  props_set(string_proto_obj.props, 
    "small",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringSmall)),
  )
  props_set(string_proto_obj.props, 
    "strike",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringStrike)),
  )
  props_set(string_proto_obj.props, 
    "sub",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringSub)),
  )
  props_set(string_proto_obj.props, 
    "sup",
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringSup)),
  )
  match symbol_iterator_key() {
    Some(key) =>
      props_set(string_proto_obj.props, 
        key,
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::StringIterator),
        ),
      )
    None => ()
  }
  (string_proto_obj, string_proto_value)
}
