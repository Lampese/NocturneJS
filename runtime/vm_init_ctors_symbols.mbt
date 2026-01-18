///|
fn init_symbol_bindings(
  symbol_ctor : Value,
  symbol_proto_obj : ObjectValue,
  function_proto_value : Value,
  array_proto_obj : ObjectValue,
  array_values_func : Value,
  array_buffer_proto_obj : ObjectValue,
  shared_array_buffer_proto_obj : ObjectValue,
  regexp_proto_obj : ObjectValue,
  map_proto_obj : ObjectValue,
  map_entries_func : Value,
  set_proto_obj : ObjectValue,
  set_values_func : Value,
  string_proto_obj : ObjectValue,
  weakmap_proto_obj : ObjectValue,
  weakset_proto_obj : ObjectValue,
) -> (String, String) {
  let symbol_iterator = new_symbol("Symbol.iterator", symbol_iterator_ref)
  let iterator_key = symbol_prop_key(symbol_iterator)
  let symbol_to_string_tag = new_symbol(
    "Symbol.toStringTag", symbol_to_string_tag_ref,
  )
  let to_string_tag_key = symbol_prop_key(symbol_to_string_tag)
  init_symbol_tags(iterator_key, to_string_tag_key)
  let symbol_async_iterator = new_symbol(
    "Symbol.asyncIterator", symbol_async_iterator_ref,
  )
  let symbol_has_instance = new_symbol(
    "Symbol.hasInstance", symbol_has_instance_ref,
  )
  let symbol_is_concat_spreadable = new_symbol(
    "Symbol.isConcatSpreadable", symbol_is_concat_spreadable_ref,
  )
  let symbol_match = new_symbol("Symbol.match", symbol_match_ref)
  let symbol_match_all = new_symbol("Symbol.matchAll", symbol_match_all_ref)
  let symbol_replace = new_symbol("Symbol.replace", symbol_replace_ref)
  let symbol_search = new_symbol("Symbol.search", symbol_search_ref)
  let symbol_species = new_symbol("Symbol.species", symbol_species_ref)
  let symbol_split = new_symbol("Symbol.split", symbol_split_ref)
  let symbol_to_primitive = new_symbol(
    "Symbol.toPrimitive", symbol_to_primitive_ref,
  )
  let symbol_unscopables = new_symbol(
    "Symbol.unscopables", symbol_unscopables_ref,
  )
  let async_iterator_key = symbol_prop_key(symbol_async_iterator)
  let has_instance_key = symbol_prop_key(symbol_has_instance)
  let match_key = symbol_prop_key(symbol_match)
  let match_all_key = symbol_prop_key(symbol_match_all)
  let replace_key = symbol_prop_key(symbol_replace)
  let search_key = symbol_prop_key(symbol_search)
  let species_key = symbol_prop_key(symbol_species)
  let split_key = symbol_prop_key(symbol_split)
  let unscopables_key = symbol_prop_key(symbol_unscopables)
  init_async_iterator_props(async_iterator_key)
  apply_symbol_ctor_props(
    symbol_ctor, symbol_iterator, symbol_async_iterator, symbol_has_instance, symbol_is_concat_spreadable,
    symbol_match, symbol_match_all, symbol_replace, symbol_search, symbol_species,
    symbol_split, symbol_to_primitive, symbol_to_string_tag, symbol_unscopables,
  )
  let to_primitive_key = symbol_prop_key(symbol_to_primitive)
  apply_symbol_proto_props(
    symbol_proto_obj, to_primitive_key, to_string_tag_key,
  )
  apply_function_has_instance(function_proto_value, has_instance_key)
  apply_symbol_object_props(
    array_proto_obj, array_values_func, array_buffer_proto_obj, shared_array_buffer_proto_obj,
    regexp_proto_obj, map_proto_obj, map_entries_func, set_proto_obj, set_values_func,
    string_proto_obj, weakmap_proto_obj, weakset_proto_obj, iterator_key, to_string_tag_key,
    match_key, match_all_key, replace_key, search_key, split_key, unscopables_key,
  )
  (species_key, to_primitive_key)
}
