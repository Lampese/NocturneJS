///|
fn new_map_value_with_proto(proto : Value?) -> Value {
  let final_proto = match proto {
    Some(value) => Some(value)
    None => value_from_object(map_proto())
  }
  let obj = register_object_value(ObjectValue::{
    id: alloc_id(),
    props: Map::new(),
    proto: final_proto,
    string_data: None,
    number_data: None,
    bool_data: None,
    bigint_data: None,
    symbol_data: None,
    regexp_data: None,
    generator_data: None,
    async_generator_data: None,
    proxy_data: None,
    map_data: Some(MapData::{ entries: [] }),
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
fn new_set_value_with_proto(proto : Value?) -> Value {
  let final_proto = match proto {
    Some(value) => Some(value)
    None => value_from_object(set_proto())
  }
  let obj = register_object_value(ObjectValue::{
    id: alloc_id(),
    props: Map::new(),
    proto: final_proto,
    string_data: None,
    number_data: None,
    bool_data: None,
    bigint_data: None,
    symbol_data: None,
    regexp_data: None,
    generator_data: None,
    async_generator_data: None,
    proxy_data: None,
    map_data: None,
    set_data: Some(SetData::{ entries: [] }),
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
fn new_set_value() -> Value {
  new_set_value_with_proto(None)
}

///|
fn new_weakmap_value_with_proto(proto : Value?) -> Value {
  let final_proto = match proto {
    Some(value) => Some(value)
    None => value_from_object(weakmap_proto())
  }
  let obj = register_object_value(ObjectValue::{
    id: alloc_id(),
    props: Map::new(),
    proto: final_proto,
    string_data: None,
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
    weakmap_data: Some(WeakMapData::{ entries: [] }),
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
fn new_weakset_value_with_proto(proto : Value?) -> Value {
  let final_proto = match proto {
    Some(value) => Some(value)
    None => value_from_object(weakset_proto())
  }
  let obj = register_object_value(ObjectValue::{
    id: alloc_id(),
    props: Map::new(),
    proto: final_proto,
    string_data: None,
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
    weakset_data: Some(WeakSetData::{ entries: [] }),
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
fn new_weakref_value(target : Value, proto : Value?) -> Value {
  let proto_value = match proto {
    Some(value) => Some(value)
    None => value_from_object(weakref_proto())
  }
  let obj = register_object_value(ObjectValue::{
    id: alloc_id(),
    props: Map::new(),
    proto: proto_value,
    string_data: None,
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
    weakref_data: Some(WeakRefData::{ target, }),
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
fn new_finreg_value(callback : Value, realm_env : Env, proto : Value?) -> Value {
  let proto_value = match proto {
    Some(value) => Some(value)
    None => value_from_object(finreg_proto())
  }
  let realm_root = realm_env_from_env(realm_env)
  let obj = register_object_value(ObjectValue::{
    id: alloc_id(),
    props: Map::new(),
    proto: proto_value,
    string_data: None,
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
    finreg_data: Some(FinalizationRegistryData::{
      callback,
      entries: [],
      realm_env: realm_root,
    }),
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
