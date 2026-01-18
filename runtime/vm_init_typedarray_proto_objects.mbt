///|
fn init_typed_array_proto_object(
  proto_ref : Ref[ObjectValue?],
  typed_array_proto_obj : ObjectValue,
) -> ObjectValue {
  let proto_value = new_object_value_with_proto(
    Some(Object(typed_array_proto_obj)),
  )
  let proto_obj = match proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(typed_array_proto_obj)))
  }
  set_proto_ref(proto_ref, Some(proto_obj))
  proto_obj
}

///|
fn init_typed_array_proto_objects(
  typed_array_proto_obj : ObjectValue,
) -> (
  ObjectValue,
  ObjectValue,
  ObjectValue,
  ObjectValue,
  ObjectValue,
  ObjectValue,
  ObjectValue,
  ObjectValue,
  ObjectValue,
  ObjectValue,
  ObjectValue,
  ObjectValue,
) {
  let uint8_array_proto_obj = init_typed_array_proto_object(
    uint8_array_proto_ref, typed_array_proto_obj,
  )
  let int8_array_proto_obj = init_typed_array_proto_object(
    int8_array_proto_ref, typed_array_proto_obj,
  )
  let uint8_clamped_array_proto_obj = init_typed_array_proto_object(
    uint8_clamped_array_proto_ref, typed_array_proto_obj,
  )
  let uint16_array_proto_obj = init_typed_array_proto_object(
    uint16_array_proto_ref, typed_array_proto_obj,
  )
  let int16_array_proto_obj = init_typed_array_proto_object(
    int16_array_proto_ref, typed_array_proto_obj,
  )
  let uint32_array_proto_obj = init_typed_array_proto_object(
    uint32_array_proto_ref, typed_array_proto_obj,
  )
  let int32_array_proto_obj = init_typed_array_proto_object(
    int32_array_proto_ref, typed_array_proto_obj,
  )
  let bigint64_array_proto_obj = init_typed_array_proto_object(
    bigint64_array_proto_ref, typed_array_proto_obj,
  )
  let biguint64_array_proto_obj = init_typed_array_proto_object(
    biguint64_array_proto_ref, typed_array_proto_obj,
  )
  let float16_array_proto_obj = init_typed_array_proto_object(
    float16_array_proto_ref, typed_array_proto_obj,
  )
  let float32_array_proto_obj = init_typed_array_proto_object(
    float32_array_proto_ref, typed_array_proto_obj,
  )
  let float64_array_proto_obj = init_typed_array_proto_object(
    float64_array_proto_ref, typed_array_proto_obj,
  )
  (
    uint8_array_proto_obj, int8_array_proto_obj, uint8_clamped_array_proto_obj, uint16_array_proto_obj,
    int16_array_proto_obj, uint32_array_proto_obj, int32_array_proto_obj, bigint64_array_proto_obj,
    biguint64_array_proto_obj, float16_array_proto_obj, float32_array_proto_obj,
    float64_array_proto_obj,
  )
}
