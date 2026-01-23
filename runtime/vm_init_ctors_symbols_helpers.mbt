///|
fn new_symbol(
  description : String,
  ref_cell : Ref[SymbolValue?],
) -> SymbolValue {
  let symbol_value = SymbolValue::{
    id: alloc_symbol_id(),
    description: Some(description),
    key: None,
  }
  register_symbol(symbol_value)
  set_symbol_ref(ref_cell, Some(symbol_value))
  symbol_value
}

///|
fn init_symbol_tags(iterator_key : String, to_string_tag_key : String) -> Unit {
  match iterator_proto() {
    Some(obj) => {
      props_set(obj.props, to_string_tag_key, Property::{
        value: Undefined,
        writable: false,
        configurable: true,
        enumerable: false,
        getter: Some(
          new_builtin_value(BuiltinFunction::IteratorToStringTagGetter),
        ),
        setter: Some(
          new_builtin_value(BuiltinFunction::IteratorToStringTagSetter),
        ),
      })
      props_set(obj.props, 
        iterator_key,
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::IteratorReturnSelf),
        ),
      )
    }
    None => ()
  }
  match iterator_helper_proto() {
    Some(obj) =>
      props_set(obj.props, 
        to_string_tag_key,
        property_data_readonly(String("Iterator Helper")),
      )
    None => ()
  }
  match array_iterator_proto() {
    Some(obj) =>
      props_set(obj.props, 
        to_string_tag_key,
        property_data_readonly(String("Array Iterator")),
      )
    None => ()
  }
  match string_iterator_proto() {
    Some(obj) =>
      props_set(obj.props, 
        to_string_tag_key,
        property_data_readonly(String("String Iterator")),
      )
    None => ()
  }
  match regexp_string_iterator_proto() {
    Some(obj) =>
      props_set(obj.props, 
        to_string_tag_key,
        property_data_readonly(String("RegExp String Iterator")),
      )
    None => ()
  }
  match map_iterator_proto() {
    Some(obj) =>
      props_set(obj.props, 
        to_string_tag_key,
        property_data_readonly(String("Map Iterator")),
      )
    None => ()
  }
  match set_iterator_proto() {
    Some(obj) =>
      props_set(obj.props, 
        to_string_tag_key,
        property_data_readonly(String("Set Iterator")),
      )
    None => ()
  }
  match generator_function_proto() {
    Some(obj) =>
      props_set(obj.props, 
        to_string_tag_key,
        property_data_readonly(String("GeneratorFunction")),
      )
    None => ()
  }
  match async_generator_function_proto() {
    Some(obj) =>
      props_set(obj.props, 
        to_string_tag_key,
        property_data_readonly(String("AsyncGeneratorFunction")),
      )
    None => ()
  }
  match async_function_proto() {
    Some(obj) =>
      props_set(obj.props, 
        to_string_tag_key,
        property_data_readonly(String("AsyncFunction")),
      )
    None => ()
  }
  match dataview_proto() {
    Some(obj) =>
      props_set(obj.props, 
        to_string_tag_key,
        property_data_readonly(String("DataView")),
      )
    None => ()
  }
  match generator_proto() {
    Some(obj) =>
      props_set(obj.props, 
        to_string_tag_key,
        property_data_readonly(String("Generator")),
      )
    None => ()
  }
  match async_generator_proto() {
    Some(obj) => {
      props_set(obj.props, 
        to_string_tag_key,
        property_data_readonly(String("AsyncGenerator")),
      )
      match symbol_async_iterator_key() {
        Some(key) =>
          props_set(obj.props, 
            key,
            property_data_non_enum(
              new_builtin_value(BuiltinFunction::AsyncIteratorReturnSelf),
            ),
          )
        None => ()
      }
    }
    None => ()
  }
  match bigint_proto() {
    Some(obj) =>
      props_set(obj.props, to_string_tag_key, property_data_readonly(String("BigInt")))
    None => ()
  }
  match promise_proto() {
    Some(obj) =>
      props_set(obj.props, 
        to_string_tag_key,
        property_data_readonly(String("Promise")),
      )
    None => ()
  }
  match weakref_proto() {
    Some(obj) =>
      props_set(obj.props, 
        to_string_tag_key,
        property_data_readonly(String("WeakRef")),
      )
    None => ()
  }
  match finreg_proto() {
    Some(obj) =>
      props_set(obj.props, 
        to_string_tag_key,
        property_data_readonly(String("FinalizationRegistry")),
      )
    None => ()
  }
}

///|
fn init_async_iterator_props(async_iterator_key : String) -> Unit {
  match async_iterator_proto() {
    Some(obj) =>
      props_set(obj.props, 
        async_iterator_key,
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::AsyncIteratorReturnSelf),
        ),
      )
    None => ()
  }
  match async_generator_proto() {
    Some(obj) =>
      props_set(obj.props, 
        async_iterator_key,
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::AsyncIteratorReturnSelf),
        ),
      )
    None => ()
  }
}

///|
fn apply_symbol_ctor_props(
  symbol_ctor : Value,
  symbol_iterator : SymbolValue,
  symbol_async_iterator : SymbolValue,
  symbol_has_instance : SymbolValue,
  symbol_is_concat_spreadable : SymbolValue,
  symbol_match : SymbolValue,
  symbol_match_all : SymbolValue,
  symbol_replace : SymbolValue,
  symbol_search : SymbolValue,
  symbol_species : SymbolValue,
  symbol_split : SymbolValue,
  symbol_to_primitive : SymbolValue,
  symbol_to_string_tag : SymbolValue,
  symbol_unscopables : SymbolValue,
) -> Unit {
  match symbol_ctor {
    Builtin(builtin) => {
      props_set(builtin.props, 
        "iterator",
        property_data_const(Symbol(symbol_iterator)),
      )
      props_set(builtin.props, 
        "asyncIterator",
        property_data_const(Symbol(symbol_async_iterator)),
      )
      props_set(builtin.props, 
        "hasInstance",
        property_data_const(Symbol(symbol_has_instance)),
      )
      props_set(builtin.props, 
        "isConcatSpreadable",
        property_data_const(Symbol(symbol_is_concat_spreadable)),
      )
      props_set(builtin.props, "match", property_data_const(Symbol(symbol_match)))
      props_set(builtin.props, 
        "matchAll",
        property_data_const(Symbol(symbol_match_all)),
      )
      props_set(builtin.props, "replace", property_data_const(Symbol(symbol_replace)))
      props_set(builtin.props, "search", property_data_const(Symbol(symbol_search)))
      props_set(builtin.props, "species", property_data_const(Symbol(symbol_species)))
      props_set(builtin.props, "split", property_data_const(Symbol(symbol_split)))
      props_set(builtin.props, 
        "toPrimitive",
        property_data_const(Symbol(symbol_to_primitive)),
      )
      props_set(builtin.props, 
        "toStringTag",
        property_data_const(Symbol(symbol_to_string_tag)),
      )
      props_set(builtin.props, 
        "unscopables",
        property_data_const(Symbol(symbol_unscopables)),
      )
    }
    _ => ()
  }
}

///|
fn apply_symbol_proto_props(
  symbol_proto_obj : ObjectValue,
  to_primitive_key : String,
  to_string_tag_key : String,
) -> Unit {
  props_set(symbol_proto_obj.props, 
    to_primitive_key,
    property_data_readonly(
      new_builtin_value(BuiltinFunction::SymbolToPrimitive),
    ),
  )
  props_set(symbol_proto_obj.props, 
    to_string_tag_key,
    property_data_readonly(String("Symbol")),
  )
  set_accessor_property_with_enum(
    symbol_proto_obj.props,
    "description",
    Some(new_builtin_value(BuiltinFunction::SymbolDescription)),
    None,
    false,
  )
}

///|
fn apply_function_has_instance(
  function_proto_value : Value,
  has_instance_key : String,
) -> Unit {
  match function_proto_value {
    Function(func) =>
      props_set(func.props, 
        has_instance_key,
        property_data_const(
          new_builtin_value(BuiltinFunction::FunctionSymbolHasInstance),
        ),
      )
    Builtin(builtin) =>
      props_set(builtin.props, 
        has_instance_key,
        property_data_const(
          new_builtin_value(BuiltinFunction::FunctionSymbolHasInstance),
        ),
      )
    _ => ()
  }
}

///|
fn make_array_unscopables_value() -> Value {
  let array_unscopables_value = new_object_value_with_proto(None)
  match array_unscopables_value {
    Object(obj) => {
      props_set(obj.props, "at", property_data(Bool(true)))
      props_set(obj.props, "copyWithin", property_data(Bool(true)))
      props_set(obj.props, "entries", property_data(Bool(true)))
      props_set(obj.props, "fill", property_data(Bool(true)))
      props_set(obj.props, "find", property_data(Bool(true)))
      props_set(obj.props, "findIndex", property_data(Bool(true)))
      props_set(obj.props, "findLast", property_data(Bool(true)))
      props_set(obj.props, "findLastIndex", property_data(Bool(true)))
      props_set(obj.props, "flat", property_data(Bool(true)))
      props_set(obj.props, "flatMap", property_data(Bool(true)))
      props_set(obj.props, "includes", property_data(Bool(true)))
      props_set(obj.props, "keys", property_data(Bool(true)))
      props_set(obj.props, "toReversed", property_data(Bool(true)))
      props_set(obj.props, "toSorted", property_data(Bool(true)))
      props_set(obj.props, "toSpliced", property_data(Bool(true)))
      props_set(obj.props, "values", property_data(Bool(true)))
    }
    _ => ()
  }
  array_unscopables_value
}

///|
fn apply_symbol_object_props(
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
  iterator_key : String,
  to_string_tag_key : String,
  match_key : String,
  match_all_key : String,
  replace_key : String,
  search_key : String,
  split_key : String,
  unscopables_key : String,
) -> Unit {
  props_set(array_proto_obj.props, 
    iterator_key,
    property_data_non_enum(array_values_func),
  )
  props_set(array_buffer_proto_obj.props, 
    to_string_tag_key,
    property_data_readonly(String("ArrayBuffer")),
  )
  props_set(shared_array_buffer_proto_obj.props, 
    to_string_tag_key,
    property_data_readonly(String("SharedArrayBuffer")),
  )
  props_set(regexp_proto_obj.props, 
    replace_key,
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::RegExpSymbolReplace),
    ),
  )
  props_set(regexp_proto_obj.props, 
    match_key,
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::RegExpSymbolMatch),
    ),
  )
  props_set(regexp_proto_obj.props, 
    match_all_key,
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::RegExpSymbolMatchAll),
    ),
  )
  props_set(regexp_proto_obj.props, 
    search_key,
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::RegExpSymbolSearch),
    ),
  )
  props_set(regexp_proto_obj.props, 
    split_key,
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::RegExpSymbolSplit),
    ),
  )
  let array_unscopables_value = make_array_unscopables_value()
  props_set(array_proto_obj.props, 
    unscopables_key,
    property_data_readonly(array_unscopables_value),
  )
  props_set(map_proto_obj.props, 
    iterator_key,
    property_data_non_enum(map_entries_func),
  )
  props_set(set_proto_obj.props, iterator_key, property_data_non_enum(set_values_func))
  props_set(string_proto_obj.props, 
    iterator_key,
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringIterator)),
  )
  props_set(map_proto_obj.props, 
    to_string_tag_key,
    property_data_readonly(String("Map")),
  )
  props_set(set_proto_obj.props, 
    to_string_tag_key,
    property_data_readonly(String("Set")),
  )
  props_set(weakmap_proto_obj.props, 
    to_string_tag_key,
    property_data_readonly(String("WeakMap")),
  )
  props_set(weakset_proto_obj.props, 
    to_string_tag_key,
    property_data_readonly(String("WeakSet")),
  )
}
