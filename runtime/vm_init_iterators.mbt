///|
fn init_generator_and_iterator_protos(
  object_proto_obj : ObjectValue,
  generator_function_proto_obj : ObjectValue,
  async_generator_function_proto_obj : ObjectValue,
) -> ObjectValue {
  let generator_proto_value = new_object_value_with_proto(
    Some(Object(object_proto_obj)),
  )
  let generator_proto_obj = match generator_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(object_proto_obj)))
  }
  set_proto_ref(generator_proto_ref, Some(generator_proto_obj))
  props_set(generator_function_proto_obj.props, 
    "prototype",
    property_data_readonly(Object(generator_proto_obj)),
  )
  props_set(generator_proto_obj.props, 
    "constructor",
    property_data_readonly(Object(generator_function_proto_obj)),
  )
  match symbol_to_string_tag_key() {
    Some(key) =>
      props_set(generator_proto_obj.props, 
        key,
        property_data_readonly(String("Generator")),
      )
    None => ()
  }
  props_set(generator_proto_obj.props, 
    "next",
    property_data_non_enum(new_builtin_value(BuiltinFunction::GeneratorNext)),
  )
  props_set(generator_proto_obj.props, 
    "return",
    property_data_non_enum(new_builtin_value(BuiltinFunction::GeneratorReturn)),
  )
  props_set(generator_proto_obj.props, 
    "throw",
    property_data_non_enum(new_builtin_value(BuiltinFunction::GeneratorThrow)),
  )
  let async_generator_proto_value = new_object_value_with_proto(
    Some(Object(object_proto_obj)),
  )
  let async_generator_proto_obj = match async_generator_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(object_proto_obj)))
  }
  set_proto_ref(async_generator_proto_ref, Some(async_generator_proto_obj))
  props_set(async_generator_function_proto_obj.props, 
    "prototype",
    property_data_readonly(Object(async_generator_proto_obj)),
  )
  props_set(async_generator_proto_obj.props, 
    "constructor",
    property_data_readonly(Object(async_generator_function_proto_obj)),
  )
  match symbol_to_string_tag_key() {
    Some(key) =>
      props_set(async_generator_proto_obj.props, 
        key,
        property_data_readonly(String("AsyncGenerator")),
      )
    None => ()
  }
  props_set(async_generator_proto_obj.props, 
    "next",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::AsyncGeneratorNext),
    ),
  )
  props_set(async_generator_proto_obj.props, 
    "return",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::AsyncGeneratorReturn),
    ),
  )
  props_set(async_generator_proto_obj.props, 
    "throw",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::AsyncGeneratorThrow),
    ),
  )
  match symbol_async_iterator_key() {
    Some(key) =>
      props_set(async_generator_proto_obj.props, 
        key,
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::AsyncIteratorReturnSelf),
        ),
      )
    None => ()
  }
  let iterator_proto_value = new_object_value_with_proto(
    Some(Object(object_proto_obj)),
  )
  let iterator_proto_obj = match iterator_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(object_proto_obj)))
  }
  rc_replace_optional_value(
    generator_proto_obj.proto,
    Some(Object(iterator_proto_obj)),
  )
  generator_proto_obj.proto = Some(Object(iterator_proto_obj))
  set_proto_ref(iterator_proto_ref, Some(iterator_proto_obj))
  let async_iterator_proto_value = new_object_value_with_proto(
    Some(Object(object_proto_obj)),
  )
  let async_iterator_proto_obj = match async_iterator_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(object_proto_obj)))
  }
  set_proto_ref(async_iterator_proto_ref, Some(async_iterator_proto_obj))
  let async_iterator_proto_value = value_from_object(async_iterator_proto())
  rc_replace_optional_value(
    async_generator_proto_obj.proto,
    async_iterator_proto_value,
  )
  async_generator_proto_obj.proto = async_iterator_proto_value
  let async_from_sync_iterator_proto_value = new_object_value_with_proto(
    async_iterator_proto_value,
  )
  let async_from_sync_iterator_proto_obj = match
    async_from_sync_iterator_proto_value {
    Object(obj) => obj
    _ => new_object_struct(async_iterator_proto_value)
  }
  set_proto_ref(
    async_from_sync_iterator_proto_ref,
    Some(async_from_sync_iterator_proto_obj),
  )
  props_set(async_from_sync_iterator_proto_obj.props, 
    "next",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::AsyncFromSyncIteratorNext),
    ),
  )
  props_set(async_from_sync_iterator_proto_obj.props, 
    "return",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::AsyncFromSyncIteratorReturn),
    ),
  )
  props_set(async_from_sync_iterator_proto_obj.props, 
    "throw",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::AsyncFromSyncIteratorThrow),
    ),
  )
  props_set(iterator_proto_obj.props, 
    "drop",
    property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorDrop)),
  )
  props_set(iterator_proto_obj.props, 
    "filter",
    property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorFilter)),
  )
  props_set(iterator_proto_obj.props, 
    "flatMap",
    property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorFlatMap)),
  )
  props_set(iterator_proto_obj.props, 
    "map",
    property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorMap)),
  )
  props_set(iterator_proto_obj.props, 
    "take",
    property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorTake)),
  )
  props_set(iterator_proto_obj.props, 
    "every",
    property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorEvery)),
  )
  props_set(iterator_proto_obj.props, 
    "find",
    property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorFind)),
  )
  props_set(iterator_proto_obj.props, 
    "forEach",
    property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorForEach)),
  )
  props_set(iterator_proto_obj.props, 
    "some",
    property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorSome)),
  )
  props_set(iterator_proto_obj.props, 
    "reduce",
    property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorReduce)),
  )
  props_set(iterator_proto_obj.props, 
    "toArray",
    property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorToArray)),
  )
  props_set(iterator_proto_obj.props, "constructor", Property::{
    value: Undefined,
    writable: false,
    configurable: true,
    enumerable: false,
    getter: Some(new_builtin_value(BuiltinFunction::IteratorConstructorGetter)),
    setter: Some(new_builtin_value(BuiltinFunction::IteratorConstructorSetter)),
  })
  match generator_proto() {
    Some(obj) => {
      let proto_value = value_from_object(iterator_proto())
      rc_replace_optional_value(obj.proto, proto_value)
      obj.proto = proto_value
    }
    None => ()
  }
  let iterator_helper_proto_value = new_object_value_with_proto(
    Some(Object(iterator_proto_obj)),
  )
  let iterator_helper_proto_obj = match iterator_helper_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(iterator_proto_obj)))
  }
  set_proto_ref(iterator_helper_proto_ref, Some(iterator_helper_proto_obj))
  props_set(iterator_helper_proto_obj.props, 
    "next",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::IteratorHelperNext),
    ),
  )
  props_set(iterator_helper_proto_obj.props, 
    "return",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::IteratorHelperReturn),
    ),
  )
  let iterator_concat_proto_value = new_object_value_with_proto(
    Some(Object(iterator_helper_proto_obj)),
  )
  let iterator_concat_proto_obj = match iterator_concat_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(iterator_helper_proto_obj)))
  }
  set_proto_ref(iterator_concat_proto_ref, Some(iterator_concat_proto_obj))
  props_set(iterator_concat_proto_obj.props, 
    "next",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::IteratorConcatNext),
    ),
  )
  props_set(iterator_concat_proto_obj.props, 
    "return",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::IteratorConcatReturn),
    ),
  )
  let iterator_wrap_proto_value = new_object_value_with_proto(
    Some(Object(iterator_proto_obj)),
  )
  let iterator_wrap_proto_obj = match iterator_wrap_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(iterator_proto_obj)))
  }
  set_proto_ref(iterator_wrap_proto_ref, Some(iterator_wrap_proto_obj))
  props_set(iterator_wrap_proto_obj.props, 
    "next",
    property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorWrapNext)),
  )
  props_set(iterator_wrap_proto_obj.props, 
    "return",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::IteratorWrapReturn),
    ),
  )
  let array_iterator_proto_value = new_object_value_with_proto(
    Some(Object(iterator_proto_obj)),
  )
  let array_iterator_proto_obj = match array_iterator_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(iterator_proto_obj)))
  }
  set_proto_ref(array_iterator_proto_ref, Some(array_iterator_proto_obj))
  props_set(array_iterator_proto_obj.props, 
    "next",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::ArrayIteratorNext),
    ),
  )
  let string_iterator_proto_value = new_object_value_with_proto(
    Some(Object(iterator_proto_obj)),
  )
  let string_iterator_proto_obj = match string_iterator_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(iterator_proto_obj)))
  }
  set_proto_ref(string_iterator_proto_ref, Some(string_iterator_proto_obj))
  props_set(string_iterator_proto_obj.props, 
    "next",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::StringIteratorNext),
    ),
  )
  let regexp_string_iterator_proto_value = new_object_value_with_proto(
    Some(Object(iterator_proto_obj)),
  )
  let regexp_string_iterator_proto_obj = match
    regexp_string_iterator_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(iterator_proto_obj)))
  }
  set_proto_ref(
    regexp_string_iterator_proto_ref,
    Some(regexp_string_iterator_proto_obj),
  )
  props_set(regexp_string_iterator_proto_obj.props, 
    "next",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::RegExpStringIteratorNext),
    ),
  )
  let map_iterator_proto_value = new_object_value_with_proto(
    Some(Object(iterator_proto_obj)),
  )
  let map_iterator_proto_obj = match map_iterator_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(iterator_proto_obj)))
  }
  set_proto_ref(map_iterator_proto_ref, Some(map_iterator_proto_obj))
  props_set(map_iterator_proto_obj.props, 
    "next",
    property_data_non_enum(new_builtin_value(BuiltinFunction::MapIteratorNext)),
  )
  let set_iterator_proto_value = new_object_value_with_proto(
    Some(Object(iterator_proto_obj)),
  )
  let set_iterator_proto_obj = match set_iterator_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(iterator_proto_obj)))
  }
  set_proto_ref(set_iterator_proto_ref, Some(set_iterator_proto_obj))
  props_set(set_iterator_proto_obj.props, 
    "next",
    property_data_non_enum(new_builtin_value(BuiltinFunction::SetIteratorNext)),
  )
  iterator_proto_obj
}
