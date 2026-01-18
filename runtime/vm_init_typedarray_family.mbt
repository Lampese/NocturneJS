///|
fn init_typed_array_family(
  object_proto_obj : ObjectValue,
  array_proto_obj : ObjectValue,
  species_key : String,
) -> (
  Value,
  Value,
  Value,
  Value,
  Value,
  Value,
  Value,
  Value,
  Value,
  Value,
  Value,
  Value,
  Value,
) {
  let typed_array_proto_obj = init_typed_array_proto_root(object_proto_obj)
  let typed_array_ctor = init_typed_array_proto(
    typed_array_proto_obj, array_proto_obj, species_key,
  )
  let (
    uint8_array_proto_obj,
    int8_array_proto_obj,
    uint8_clamped_array_proto_obj,
    uint16_array_proto_obj,
    int16_array_proto_obj,
    uint32_array_proto_obj,
    int32_array_proto_obj,
    bigint64_array_proto_obj,
    biguint64_array_proto_obj,
    float16_array_proto_obj,
    float32_array_proto_obj,
    float64_array_proto_obj,
  ) = init_typed_array_proto_objects(typed_array_proto_obj)
  let (
    uint8_array_ctor,
    int8_array_ctor,
    uint8_clamped_array_ctor,
    uint16_array_ctor,
    int16_array_ctor,
    uint32_array_ctor,
    int32_array_ctor,
    bigint64_array_ctor,
    biguint64_array_ctor,
    float16_array_ctor,
    float32_array_ctor,
    float64_array_ctor,
  ) = init_typed_array_ctors(
    typed_array_ctor, uint8_array_proto_obj, int8_array_proto_obj, uint8_clamped_array_proto_obj,
    uint16_array_proto_obj, int16_array_proto_obj, uint32_array_proto_obj, int32_array_proto_obj,
    bigint64_array_proto_obj, biguint64_array_proto_obj, float16_array_proto_obj,
    float32_array_proto_obj, float64_array_proto_obj, species_key,
  )
  (
    typed_array_ctor, uint8_array_ctor, int8_array_ctor, uint8_clamped_array_ctor,
    uint16_array_ctor, int16_array_ctor, uint32_array_ctor, int32_array_ctor, bigint64_array_ctor,
    biguint64_array_ctor, float16_array_ctor, float32_array_ctor, float64_array_ctor,
  )
}
