///|
fn init_core_ctors(
  object_proto_obj : ObjectValue,
  object_proto_value : Value,
  function_proto_value : Value,
  generator_function_proto_obj : ObjectValue,
  async_generator_function_proto_obj : ObjectValue,
  async_function_proto_obj : ObjectValue,
  string_proto_obj : ObjectValue,
  string_proto_value : Value,
  number_proto_obj : ObjectValue,
  number_proto_value : Value,
  bool_proto_obj : ObjectValue,
  bool_proto_value : Value,
  bigint_proto_obj : ObjectValue,
  bigint_proto_value : Value,
  symbol_proto_obj : ObjectValue,
  symbol_proto_value : Value,
  iterator_proto_obj : ObjectValue,
) -> (Value, Value, Value, Value, Value, Value, Value, Value, Value, Value) {
  let object_ctor = init_object_ctor(object_proto_obj, object_proto_value)
  let function_ctor = init_function_ctor(function_proto_value)
  init_generator_family_ctors(
    function_ctor, generator_function_proto_obj, async_generator_function_proto_obj,
    async_function_proto_obj,
  )
  let string_ctor = init_string_ctor(string_proto_obj, string_proto_value)
  let (number_ctor, parse_int_func, parse_float_func) = init_number_ctor(
    number_proto_obj, number_proto_value,
  )
  let bool_ctor = init_bool_ctor(bool_proto_obj, bool_proto_value)
  let bigint_ctor = init_bigint_ctor(bigint_proto_obj, bigint_proto_value)
  let symbol_ctor = init_symbol_ctor(symbol_proto_obj, symbol_proto_value)
  let iterator_ctor = init_iterator_ctor(iterator_proto_obj)
  (
    object_ctor, function_ctor, string_ctor, number_ctor, bool_ctor, bigint_ctor,
    symbol_ctor, iterator_ctor, parse_int_func, parse_float_func,
  )
}
