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
  generator_function_proto_obj.props.set(
    "prototype",
    property_data_readonly(Object(generator_proto_obj)),
  )
  generator_proto_obj.props.set(
    "constructor",
    property_data_readonly(Object(generator_function_proto_obj)),
  )
  match symbol_to_string_tag_key() {
    Some(key) =>
      generator_proto_obj.props.set(
        key,
        property_data_readonly(String("Generator")),
      )
    None => ()
  }
  generator_proto_obj.props.set(
    "next",
    property_data_non_enum(new_builtin_value(BuiltinFunction::GeneratorNext)),
  )
  generator_proto_obj.props.set(
    "return",
    property_data_non_enum(new_builtin_value(BuiltinFunction::GeneratorReturn)),
  )
  generator_proto_obj.props.set(
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
  async_generator_function_proto_obj.props.set(
    "prototype",
    property_data_readonly(Object(async_generator_proto_obj)),
  )
  async_generator_proto_obj.props.set(
    "constructor",
    property_data_readonly(Object(async_generator_function_proto_obj)),
  )
  match symbol_to_string_tag_key() {
    Some(key) =>
      async_generator_proto_obj.props.set(
        key,
        property_data_readonly(String("AsyncGenerator")),
      )
    None => ()
  }
  async_generator_proto_obj.props.set(
    "next",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::AsyncGeneratorNext),
    ),
  )
  async_generator_proto_obj.props.set(
    "return",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::AsyncGeneratorReturn),
    ),
  )
  async_generator_proto_obj.props.set(
    "throw",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::AsyncGeneratorThrow),
    ),
  )
  match symbol_async_iterator_key() {
    Some(key) =>
      async_generator_proto_obj.props.set(
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
  async_generator_proto_obj.proto = value_from_object(async_iterator_proto())
  let async_from_sync_iterator_proto_value = new_object_value_with_proto(
    value_from_object(async_iterator_proto()),
  )
  let async_from_sync_iterator_proto_obj = match
    async_from_sync_iterator_proto_value {
    Object(obj) => obj
    _ => new_object_struct(value_from_object(async_iterator_proto()))
  }
  set_proto_ref(
    async_from_sync_iterator_proto_ref,
    Some(async_from_sync_iterator_proto_obj),
  )
  async_from_sync_iterator_proto_obj.props.set(
    "next",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::AsyncFromSyncIteratorNext),
    ),
  )
  async_from_sync_iterator_proto_obj.props.set(
    "return",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::AsyncFromSyncIteratorReturn),
    ),
  )
  async_from_sync_iterator_proto_obj.props.set(
    "throw",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::AsyncFromSyncIteratorThrow),
    ),
  )
  iterator_proto_obj.props.set(
    "drop",
    property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorDrop)),
  )
  iterator_proto_obj.props.set(
    "filter",
    property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorFilter)),
  )
  iterator_proto_obj.props.set(
    "flatMap",
    property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorFlatMap)),
  )
  iterator_proto_obj.props.set(
    "map",
    property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorMap)),
  )
  iterator_proto_obj.props.set(
    "take",
    property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorTake)),
  )
  iterator_proto_obj.props.set(
    "every",
    property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorEvery)),
  )
  iterator_proto_obj.props.set(
    "find",
    property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorFind)),
  )
  iterator_proto_obj.props.set(
    "forEach",
    property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorForEach)),
  )
  iterator_proto_obj.props.set(
    "some",
    property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorSome)),
  )
  iterator_proto_obj.props.set(
    "reduce",
    property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorReduce)),
  )
  iterator_proto_obj.props.set(
    "toArray",
    property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorToArray)),
  )
  iterator_proto_obj.props.set("constructor", Property::{
    value: Undefined,
    writable: false,
    configurable: true,
    enumerable: false,
    getter: Some(new_builtin_value(BuiltinFunction::IteratorConstructorGetter)),
    setter: Some(new_builtin_value(BuiltinFunction::IteratorConstructorSetter)),
  })
  match generator_proto() {
    Some(obj) => obj.proto = value_from_object(iterator_proto())
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
  iterator_helper_proto_obj.props.set(
    "next",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::IteratorHelperNext),
    ),
  )
  iterator_helper_proto_obj.props.set(
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
  iterator_concat_proto_obj.props.set(
    "next",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::IteratorConcatNext),
    ),
  )
  iterator_concat_proto_obj.props.set(
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
  iterator_wrap_proto_obj.props.set(
    "next",
    property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorWrapNext)),
  )
  iterator_wrap_proto_obj.props.set(
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
  array_iterator_proto_obj.props.set(
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
  string_iterator_proto_obj.props.set(
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
  regexp_string_iterator_proto_obj.props.set(
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
  map_iterator_proto_obj.props.set(
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
  set_iterator_proto_obj.props.set(
    "next",
    property_data_non_enum(new_builtin_value(BuiltinFunction::SetIteratorNext)),
  )
  iterator_proto_obj
}
