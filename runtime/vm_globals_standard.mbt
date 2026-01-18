///|
fn init_standard_objects(
  object_proto_obj : ObjectValue,
) -> (Value, Value, Value, Value) {
  let json_obj = init_json_object(object_proto_obj)
  let reflect_obj = init_reflect_object(object_proto_obj)
  let atomics_obj = init_atomics_object(object_proto_obj)
  let math_obj = init_math_object(object_proto_obj)
  (json_obj, reflect_obj, atomics_obj, math_obj)
}
