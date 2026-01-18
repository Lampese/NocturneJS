///|
fn init_typed_array_ctor(
  typed_array_ctor : Value,
  proto_obj : ObjectValue,
  ctor_kind : BuiltinFunction,
  bytes_per_element : Int,
  species_key : String,
) -> Value {
  let ctor = new_builtin_value(ctor_kind)
  match ctor {
    Builtin(builtin) => {
      builtin.proto = Some(typed_array_ctor)
      builtin.props.set("prototype", property_data_const(Object(proto_obj)))
      builtin.props.set(
        "BYTES_PER_ELEMENT",
        property_data_const(Number(Double::from_int(bytes_per_element))),
      )
      set_accessor_property(
        builtin.props,
        species_key,
        Some(new_builtin_value(BuiltinFunction::SpeciesGetter)),
        None,
      )
    }
    _ => ()
  }
  proto_obj.props.set("constructor", property_data_non_enum(ctor))
  proto_obj.props.set(
    "BYTES_PER_ELEMENT",
    property_data_const(Number(Double::from_int(bytes_per_element))),
  )
  ctor
}

///|
fn init_typed_array_ctors(
  typed_array_ctor : Value,
  uint8_array_proto_obj : ObjectValue,
  int8_array_proto_obj : ObjectValue,
  uint8_clamped_array_proto_obj : ObjectValue,
  uint16_array_proto_obj : ObjectValue,
  int16_array_proto_obj : ObjectValue,
  uint32_array_proto_obj : ObjectValue,
  int32_array_proto_obj : ObjectValue,
  bigint64_array_proto_obj : ObjectValue,
  biguint64_array_proto_obj : ObjectValue,
  float16_array_proto_obj : ObjectValue,
  float32_array_proto_obj : ObjectValue,
  float64_array_proto_obj : ObjectValue,
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
) {
  let uint8_array_ctor = init_typed_array_ctor(
    typed_array_ctor,
    uint8_array_proto_obj,
    BuiltinFunction::Uint8ArrayConstructor,
    1,
    species_key,
  )
  let int8_array_ctor = init_typed_array_ctor(
    typed_array_ctor,
    int8_array_proto_obj,
    BuiltinFunction::Int8ArrayConstructor,
    1,
    species_key,
  )
  let uint8_clamped_array_ctor = init_typed_array_ctor(
    typed_array_ctor,
    uint8_clamped_array_proto_obj,
    BuiltinFunction::Uint8ClampedArrayConstructor,
    1,
    species_key,
  )
  let uint16_array_ctor = init_typed_array_ctor(
    typed_array_ctor,
    uint16_array_proto_obj,
    BuiltinFunction::Uint16ArrayConstructor,
    2,
    species_key,
  )
  let int16_array_ctor = init_typed_array_ctor(
    typed_array_ctor,
    int16_array_proto_obj,
    BuiltinFunction::Int16ArrayConstructor,
    2,
    species_key,
  )
  let uint32_array_ctor = init_typed_array_ctor(
    typed_array_ctor,
    uint32_array_proto_obj,
    BuiltinFunction::Uint32ArrayConstructor,
    4,
    species_key,
  )
  let int32_array_ctor = init_typed_array_ctor(
    typed_array_ctor,
    int32_array_proto_obj,
    BuiltinFunction::Int32ArrayConstructor,
    4,
    species_key,
  )
  let bigint64_array_ctor = init_typed_array_ctor(
    typed_array_ctor,
    bigint64_array_proto_obj,
    BuiltinFunction::BigInt64ArrayConstructor,
    8,
    species_key,
  )
  let biguint64_array_ctor = init_typed_array_ctor(
    typed_array_ctor,
    biguint64_array_proto_obj,
    BuiltinFunction::BigUint64ArrayConstructor,
    8,
    species_key,
  )
  let float16_array_ctor = init_typed_array_ctor(
    typed_array_ctor,
    float16_array_proto_obj,
    BuiltinFunction::Float16ArrayConstructor,
    2,
    species_key,
  )
  let float32_array_ctor = init_typed_array_ctor(
    typed_array_ctor,
    float32_array_proto_obj,
    BuiltinFunction::Float32ArrayConstructor,
    4,
    species_key,
  )
  let float64_array_ctor = init_typed_array_ctor(
    typed_array_ctor,
    float64_array_proto_obj,
    BuiltinFunction::Float64ArrayConstructor,
    8,
    species_key,
  )
  (
    uint8_array_ctor, int8_array_ctor, uint8_clamped_array_ctor, uint16_array_ctor,
    int16_array_ctor, uint32_array_ctor, int32_array_ctor, bigint64_array_ctor, biguint64_array_ctor,
    float16_array_ctor, float32_array_ctor, float64_array_ctor,
  )
}
