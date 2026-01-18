///|
fn new_regexp_value(
  pattern : String,
  flags : String,
  proto : Value?,
  offset? : Int = -1,
) -> Value raise {
  let data = try regexp_data_from_pattern(pattern, flags) catch {
    ThrowSignal(value) => {
      if offset >= 0 {
        match current_source() {
          Some(source) => {
            let (line, column) = offset_to_line_column(source, offset)
            raise ThrowSignal(
              new_error_value_with_pos(
                syntax_error_proto(),
                "invalid regular expression",
                line,
                column,
              ),
            )
          }
          None => raise ThrowSignal(value)
        }
      } else {
        raise ThrowSignal(value)
      }
      fail("unreachable")
    }
    err => raise err
  } noraise {
    value => value
  }
  let props = Map::new()
  props.set("lastIndex", property_data_non_enum_non_config(Number(0.0)))
  let obj = register_object_value(ObjectValue::{
    id: alloc_id(),
    props,
    proto,
    string_data: None,
    number_data: None,
    bool_data: None,
    bigint_data: None,
    symbol_data: None,
    regexp_data: Some(data),
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
