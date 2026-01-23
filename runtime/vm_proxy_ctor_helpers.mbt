///|
fn new_proxy_value(target : Value, handler : Value) -> Value raise {
  if !is_object_like(target) {
    return throw_type_error("proxy target must be an object")
  }
  if !is_object_like(handler) {
    return throw_type_error("proxy handler must be an object")
  }
  let callable = is_callable(target)
  let constructable = is_constructor_value(target)
  let obj = register_object_value(ObjectValue::{
    id: alloc_id(),
    props: Map::new(),
    proto: None,
    string_data: None,
    number_data: None,
    bool_data: None,
    bigint_data: None,
    symbol_data: None,
    regexp_data: None,
    generator_data: None,
    async_generator_data: None,
    proxy_data: Some(ProxyData::{ target, handler, callable, constructable }),
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
  rc_incref_value(target)
  rc_incref_value(handler)
  Object(obj)
}

///|
fn new_proxy_revoke_function(proxy : Value) -> Value {
  match new_builtin_value_with_this(BuiltinFunction::ProxyRevoke, proxy) {
    Builtin(builtin) => {
      props_set(builtin.props, "name", property_data_readonly(String("")))
      Builtin(builtin)
    }
    value => value
  }
}
