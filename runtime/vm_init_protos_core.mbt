///|
fn init_core_protos(
  env : Env,
) -> (
  ObjectValue,
  Value,
  Value,
  ObjectValue,
  ObjectValue,
  ObjectValue,
  ObjectValue,
  Value,
  Value,
  ObjectValue,
  Value,
  ObjectValue,
  Value,
  ObjectValue,
  Value,
) {
  let (object_proto_obj, object_proto_value, function_proto_value) = init_object_and_function_proto(
    env,
  )
  let (
    generator_function_proto_obj,
    async_generator_function_proto_obj,
    async_function_proto_obj,
  ) = init_generator_function_protos()
  let (array_proto_obj, array_proto_value, array_values_func) = init_array_proto(
    object_proto_obj,
  )
  let (array_buffer_proto_obj, array_buffer_proto_value) = init_array_buffer_proto(
    object_proto_obj,
  )
  let (shared_array_buffer_proto_obj, shared_array_buffer_proto_value) = init_shared_array_buffer_proto(
    object_proto_obj,
  )
  let (dataview_proto_obj, dataview_proto_value) = init_dataview_proto(
    object_proto_obj,
  )
  (
    object_proto_obj, object_proto_value, function_proto_value, generator_function_proto_obj,
    async_generator_function_proto_obj, async_function_proto_obj, array_proto_obj,
    array_proto_value, array_values_func, array_buffer_proto_obj, array_buffer_proto_value,
    shared_array_buffer_proto_obj, shared_array_buffer_proto_value, dataview_proto_obj,
    dataview_proto_value,
  )
}
