///|
fn init_host_objects(object_proto_obj : ObjectValue) -> (Value, Value, Value) {
  let std_obj = init_std_object(object_proto_obj)
  let os_obj = init_os_object(object_proto_obj)
  let bjson_obj = init_bjson_object(object_proto_obj)
  (std_obj, os_obj, bjson_obj)
}
