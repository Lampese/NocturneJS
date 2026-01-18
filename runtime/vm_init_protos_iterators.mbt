///|
fn init_iterator_and_collection_protos(
  object_proto_obj : ObjectValue,
  generator_function_proto_obj : ObjectValue,
  async_generator_function_proto_obj : ObjectValue,
) -> (
  ObjectValue,
  ObjectValue,
  Value,
  Value,
  ObjectValue,
  Value,
  Value,
  ObjectValue,
  Value,
  ObjectValue,
  Value,
  ObjectValue,
  Value,
  ObjectValue,
  Value,
) {
  let iterator_proto_obj = init_generator_and_iterator_protos(
    object_proto_obj, generator_function_proto_obj, async_generator_function_proto_obj,
  )
  let (
    map_proto_obj,
    map_proto_value,
    map_entries_func,
    set_proto_obj,
    set_proto_value,
    set_values_func,
    weakmap_proto_obj,
    weakmap_proto_value,
    weakset_proto_obj,
    weakset_proto_value,
    weakref_proto_obj,
    weakref_proto_value,
    finreg_proto_obj,
    finreg_proto_value,
  ) = init_collection_protos(object_proto_obj)
  (
    iterator_proto_obj, map_proto_obj, map_proto_value, map_entries_func, set_proto_obj,
    set_proto_value, set_values_func, weakmap_proto_obj, weakmap_proto_value, weakset_proto_obj,
    weakset_proto_value, weakref_proto_obj, weakref_proto_value, finreg_proto_obj,
    finreg_proto_value,
  )
}
