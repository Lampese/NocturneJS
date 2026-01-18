///|
fn init_generator_function_protos() -> (ObjectValue, ObjectValue, ObjectValue) {
  let generator_function_proto_value = new_object_value_with_proto(
    function_proto(),
  )
  let generator_function_proto_obj = match generator_function_proto_value {
    Object(obj) => obj
    _ => new_object_struct(function_proto())
  }
  set_proto_ref(
    generator_function_proto_ref,
    Some(generator_function_proto_obj),
  )
  let generator_function_ctor_value = new_object_value_with_proto(
    function_proto(),
  )
  let generator_function_ctor_obj = match generator_function_ctor_value {
    Object(obj) => obj
    _ => new_object_struct(function_proto())
  }
  set_proto_ref(generator_function_ctor_ref, Some(generator_function_ctor_obj))
  generator_function_ctor_obj.props.set(
    "prototype",
    property_data_const(Object(generator_function_proto_obj)),
  )
  generator_function_proto_obj.props.set(
    "constructor",
    property_data_non_enum(Object(generator_function_ctor_obj)),
  )
  let async_generator_function_proto_value = new_object_value_with_proto(
    function_proto(),
  )
  let async_generator_function_proto_obj = match
    async_generator_function_proto_value {
    Object(obj) => obj
    _ => new_object_struct(function_proto())
  }
  set_proto_ref(
    async_generator_function_proto_ref,
    Some(async_generator_function_proto_obj),
  )
  let async_generator_function_ctor_value = new_object_value_with_proto(
    function_proto(),
  )
  let async_generator_function_ctor_obj = match
    async_generator_function_ctor_value {
    Object(obj) => obj
    _ => new_object_struct(function_proto())
  }
  set_proto_ref(
    async_generator_function_ctor_ref,
    Some(async_generator_function_ctor_obj),
  )
  async_generator_function_ctor_obj.props.set(
    "prototype",
    property_data_const(Object(async_generator_function_proto_obj)),
  )
  async_generator_function_proto_obj.props.set(
    "constructor",
    property_data_non_enum(Object(async_generator_function_ctor_obj)),
  )
  let async_function_proto_value = new_object_value_with_proto(function_proto())
  let async_function_proto_obj = match async_function_proto_value {
    Object(obj) => obj
    _ => new_object_struct(function_proto())
  }
  set_proto_ref(async_function_proto_ref, Some(async_function_proto_obj))
  (
    generator_function_proto_obj, async_generator_function_proto_obj, async_function_proto_obj,
  )
}
