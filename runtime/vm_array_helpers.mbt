///|
fn new_array_value_with_proto(
  elements : Array[Value?],
  proto? : Value? = None,
) -> Value {
  let props = Map::new()
  props_set(props, 
    "length",
    property_data_non_enum_non_config(
      Number(Double::from_int(elements.length())),
    ),
  )
  let proto_value = match proto {
    Some(value) => Some(value)
    None => value_from_object(array_proto())
  }
  let arr = register_array_value(ArrayValue::{
    id: alloc_id(),
    elements,
    proto: proto_value,
    props,
    typed_array_data: None,
    extensible: true,
  })
  rc_incref_optional_values(elements)
  rc_incref_optional_value(proto_value)
  Array(arr)
}

///|
fn new_array_value(elements : Array[Value?]) -> Value {
  new_array_value_with_proto(elements)
}

///|
fn array_from_args_with_proto(
  args : Array[Value],
  proto : Value?,
) -> Value raise {
  if args.length() == 1 {
    match args[0] {
      Number(n) => {
        let new_len_uint = to_uint32(n)
        let new_len_num = UInt::to_double(new_len_uint)
        if Double::is_nan(n) || new_len_num != n {
          return throw_range_error("invalid array length")
        }
        let len64 = Double::to_int64(new_len_num)
        let result = new_array_value_with_proto([], proto~)
        match result {
          Array(arr) =>
            set_length_prop(arr.props, Number(Int64::to_double(len64)))
          _ => ()
        }
        return result
      }
      _ => ()
    }
  }
  let elements : Array[Value?] = []
  for value in args {
    elements.push(Some(value))
  }
  new_array_value_with_proto(elements, proto~)
}
