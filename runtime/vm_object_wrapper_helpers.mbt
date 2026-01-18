///|
fn new_string_object(
  value : String,
  proto? : Value? = value_from_object(string_proto()),
) -> Value {
  let obj = register_object_value(ObjectValue::{
    id: alloc_id(),
    props: Map::new(),
    proto,
    string_data: Some(value),
    number_data: None,
    bool_data: None,
    bigint_data: None,
    symbol_data: None,
    regexp_data: None,
    generator_data: None,
    async_generator_data: None,
    proxy_data: None,
    map_data: None,
    set_data: None,
    map_iter_data: None,
    set_iter_data: None,
    iterator_wrap_data: None,
    iterator_concat_data: None,
    iterator_helper_data: None,
    weakmap_data: None,
    weakset_data: None,
    weakref_data: None,
    finreg_data: None,
    date_data: None,
    array_buffer_data: None,
    dataview_data: None,
    is_html_dda: false,
    is_error: false,
    is_module_namespace: false,
    realm_env: None,
    immutable_proto: false,
    extensible: true,
  })
  Object(obj)
}

///|
fn new_number_object(
  value : Double,
  proto? : Value? = value_from_object(number_proto()),
) -> Value {
  let obj = register_object_value(ObjectValue::{
    id: alloc_id(),
    props: Map::new(),
    proto,
    string_data: None,
    number_data: Some(value),
    bool_data: None,
    bigint_data: None,
    symbol_data: None,
    regexp_data: None,
    generator_data: None,
    async_generator_data: None,
    proxy_data: None,
    map_data: None,
    set_data: None,
    map_iter_data: None,
    set_iter_data: None,
    iterator_wrap_data: None,
    iterator_concat_data: None,
    iterator_helper_data: None,
    weakmap_data: None,
    weakset_data: None,
    weakref_data: None,
    finreg_data: None,
    date_data: None,
    array_buffer_data: None,
    dataview_data: None,
    is_html_dda: false,
    is_error: false,
    is_module_namespace: false,
    realm_env: None,
    immutable_proto: false,
    extensible: true,
  })
  Object(obj)
}

///|
fn new_date_value(
  value : Double,
  proto? : Value? = value_from_object(date_proto()),
) -> Value {
  let obj = new_object_value_with_proto(proto)
  match obj {
    Object(value_obj) => value_obj.date_data = Some(value)
    _ => ()
  }
  obj
}

///|
fn new_bool_object(
  value : Bool,
  proto? : Value? = value_from_object(bool_proto()),
) -> Value {
  let obj = register_object_value(ObjectValue::{
    id: alloc_id(),
    props: Map::new(),
    proto,
    string_data: None,
    number_data: None,
    bool_data: Some(value),
    bigint_data: None,
    symbol_data: None,
    regexp_data: None,
    generator_data: None,
    async_generator_data: None,
    proxy_data: None,
    map_data: None,
    set_data: None,
    map_iter_data: None,
    set_iter_data: None,
    iterator_wrap_data: None,
    iterator_concat_data: None,
    iterator_helper_data: None,
    weakmap_data: None,
    weakset_data: None,
    weakref_data: None,
    finreg_data: None,
    date_data: None,
    array_buffer_data: None,
    dataview_data: None,
    is_html_dda: false,
    is_error: false,
    is_module_namespace: false,
    realm_env: None,
    immutable_proto: false,
    extensible: true,
  })
  Object(obj)
}

///|
fn new_bigint_object(
  value : @bigint.BigInt,
  proto? : Value? = value_from_object(bigint_proto()),
) -> Value {
  let obj = register_object_value(ObjectValue::{
    id: alloc_id(),
    props: Map::new(),
    proto,
    string_data: None,
    number_data: None,
    bool_data: None,
    bigint_data: Some(value),
    symbol_data: None,
    regexp_data: None,
    generator_data: None,
    async_generator_data: None,
    proxy_data: None,
    map_data: None,
    set_data: None,
    map_iter_data: None,
    set_iter_data: None,
    iterator_wrap_data: None,
    iterator_concat_data: None,
    iterator_helper_data: None,
    weakmap_data: None,
    weakset_data: None,
    weakref_data: None,
    finreg_data: None,
    date_data: None,
    array_buffer_data: None,
    dataview_data: None,
    is_html_dda: false,
    is_error: false,
    is_module_namespace: false,
    realm_env: None,
    immutable_proto: false,
    extensible: true,
  })
  Object(obj)
}

///|
fn new_symbol_object(
  value : SymbolValue,
  proto? : Value? = value_from_object(symbol_proto()),
) -> Value {
  let obj = register_object_value(ObjectValue::{
    id: alloc_id(),
    props: Map::new(),
    proto,
    string_data: None,
    number_data: None,
    bool_data: None,
    bigint_data: None,
    symbol_data: Some(value),
    regexp_data: None,
    generator_data: None,
    async_generator_data: None,
    proxy_data: None,
    map_data: None,
    set_data: None,
    map_iter_data: None,
    set_iter_data: None,
    iterator_wrap_data: None,
    iterator_concat_data: None,
    iterator_helper_data: None,
    weakmap_data: None,
    weakset_data: None,
    weakref_data: None,
    finreg_data: None,
    date_data: None,
    array_buffer_data: None,
    dataview_data: None,
    is_html_dda: false,
    is_error: false,
    is_module_namespace: false,
    realm_env: None,
    immutable_proto: false,
    extensible: true,
  })
  Object(obj)
}
