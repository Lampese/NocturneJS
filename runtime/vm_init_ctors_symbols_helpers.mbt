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
  ref_cell.update(fn(_) { Some(symbol_value) })
  symbol_value
}

///|
fn init_symbol_tags(iterator_key : String, to_string_tag_key : String) -> Unit {
  match iterator_proto() {
    Some(obj) => {
      obj.props.set(to_string_tag_key, Property::{
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
      obj.props.set(
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
      obj.props.set(
        to_string_tag_key,
        property_data_readonly(String("Iterator Helper")),
      )
    None => ()
  }
  match array_iterator_proto() {
    Some(obj) =>
      obj.props.set(
        to_string_tag_key,
        property_data_readonly(String("Array Iterator")),
      )
    None => ()
  }
  match string_iterator_proto() {
    Some(obj) =>
      obj.props.set(
        to_string_tag_key,
        property_data_readonly(String("String Iterator")),
      )
    None => ()
  }
  match regexp_string_iterator_proto() {
    Some(obj) =>
      obj.props.set(
        to_string_tag_key,
        property_data_readonly(String("RegExp String Iterator")),
      )
    None => ()
  }
  match map_iterator_proto() {
    Some(obj) =>
      obj.props.set(
        to_string_tag_key,
        property_data_readonly(String("Map Iterator")),
      )
    None => ()
  }
  match set_iterator_proto() {
    Some(obj) =>
      obj.props.set(
        to_string_tag_key,
        property_data_readonly(String("Set Iterator")),
      )
    None => ()
  }
  match generator_function_proto() {
    Some(obj) =>
      obj.props.set(
        to_string_tag_key,
        property_data_readonly(String("GeneratorFunction")),
      )
    None => ()
  }
  match async_generator_function_proto() {
    Some(obj) =>
      obj.props.set(
        to_string_tag_key,
        property_data_readonly(String("AsyncGeneratorFunction")),
      )
    None => ()
  }
  match async_function_proto() {
    Some(obj) =>
      obj.props.set(
        to_string_tag_key,
        property_data_readonly(String("AsyncFunction")),
      )
    None => ()
  }
  match dataview_proto() {
    Some(obj) =>
      obj.props.set(
        to_string_tag_key,
        property_data_readonly(String("DataView")),
      )
    None => ()
  }
  match generator_proto() {
    Some(obj) =>
      obj.props.set(
        to_string_tag_key,
        property_data_readonly(String("Generator")),
      )
    None => ()
  }
  match async_generator_proto() {
    Some(obj) => {
      obj.props.set(
        to_string_tag_key,
        property_data_readonly(String("AsyncGenerator")),
      )
      match symbol_async_iterator_key() {
        Some(key) =>
          obj.props.set(
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
      obj.props.set(to_string_tag_key, property_data_readonly(String("BigInt")))
    None => ()
  }
  match promise_proto() {
    Some(obj) =>
      obj.props.set(
        to_string_tag_key,
        property_data_readonly(String("Promise")),
      )
    None => ()
  }
  match weakref_proto() {
    Some(obj) =>
      obj.props.set(
        to_string_tag_key,
        property_data_readonly(String("WeakRef")),
      )
    None => ()
  }
  match finreg_proto() {
    Some(obj) =>
      obj.props.set(
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
      obj.props.set(
        async_iterator_key,
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::AsyncIteratorReturnSelf),
        ),
      )
    None => ()
  }
  match async_generator_proto() {
    Some(obj) =>
      obj.props.set(
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
      builtin.props.set(
        "iterator",
        property_data_const(Symbol(symbol_iterator)),
      )
      builtin.props.set(
        "asyncIterator",
        property_data_const(Symbol(symbol_async_iterator)),
      )
      builtin.props.set(
        "hasInstance",
        property_data_const(Symbol(symbol_has_instance)),
      )
      builtin.props.set(
        "isConcatSpreadable",
        property_data_const(Symbol(symbol_is_concat_spreadable)),
      )
      builtin.props.set("match", property_data_const(Symbol(symbol_match)))
      builtin.props.set(
        "matchAll",
        property_data_const(Symbol(symbol_match_all)),
      )
      builtin.props.set("replace", property_data_const(Symbol(symbol_replace)))
      builtin.props.set("search", property_data_const(Symbol(symbol_search)))
      builtin.props.set("species", property_data_const(Symbol(symbol_species)))
      builtin.props.set("split", property_data_const(Symbol(symbol_split)))
      builtin.props.set(
        "toPrimitive",
        property_data_const(Symbol(symbol_to_primitive)),
      )
      builtin.props.set(
        "toStringTag",
        property_data_const(Symbol(symbol_to_string_tag)),
      )
      builtin.props.set(
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
  symbol_proto_obj.props.set(
    to_primitive_key,
    property_data_readonly(
      new_builtin_value(BuiltinFunction::SymbolToPrimitive),
    ),
  )
  symbol_proto_obj.props.set(
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
      func.props.set(
        has_instance_key,
        property_data_const(
          new_builtin_value(BuiltinFunction::FunctionSymbolHasInstance),
        ),
      )
    Builtin(builtin) =>
      builtin.props.set(
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
      obj.props.set("at", property_data(Bool(true)))
      obj.props.set("copyWithin", property_data(Bool(true)))
      obj.props.set("entries", property_data(Bool(true)))
      obj.props.set("fill", property_data(Bool(true)))
      obj.props.set("find", property_data(Bool(true)))
      obj.props.set("findIndex", property_data(Bool(true)))
      obj.props.set("findLast", property_data(Bool(true)))
      obj.props.set("findLastIndex", property_data(Bool(true)))
      obj.props.set("flat", property_data(Bool(true)))
      obj.props.set("flatMap", property_data(Bool(true)))
      obj.props.set("includes", property_data(Bool(true)))
      obj.props.set("keys", property_data(Bool(true)))
      obj.props.set("toReversed", property_data(Bool(true)))
      obj.props.set("toSorted", property_data(Bool(true)))
      obj.props.set("toSpliced", property_data(Bool(true)))
      obj.props.set("values", property_data(Bool(true)))
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
  array_proto_obj.props.set(
    iterator_key,
    property_data_non_enum(array_values_func),
  )
  array_buffer_proto_obj.props.set(
    to_string_tag_key,
    property_data_readonly(String("ArrayBuffer")),
  )
  shared_array_buffer_proto_obj.props.set(
    to_string_tag_key,
    property_data_readonly(String("SharedArrayBuffer")),
  )
  regexp_proto_obj.props.set(
    replace_key,
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::RegExpSymbolReplace),
    ),
  )
  regexp_proto_obj.props.set(
    match_key,
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::RegExpSymbolMatch),
    ),
  )
  regexp_proto_obj.props.set(
    match_all_key,
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::RegExpSymbolMatchAll),
    ),
  )
  regexp_proto_obj.props.set(
    search_key,
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::RegExpSymbolSearch),
    ),
  )
  regexp_proto_obj.props.set(
    split_key,
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::RegExpSymbolSplit),
    ),
  )
  let array_unscopables_value = make_array_unscopables_value()
  array_proto_obj.props.set(
    unscopables_key,
    property_data_readonly(array_unscopables_value),
  )
  map_proto_obj.props.set(
    iterator_key,
    property_data_non_enum(map_entries_func),
  )
  set_proto_obj.props.set(iterator_key, property_data_non_enum(set_values_func))
  string_proto_obj.props.set(
    iterator_key,
    property_data_non_enum(new_builtin_value(BuiltinFunction::StringIterator)),
  )
  map_proto_obj.props.set(
    to_string_tag_key,
    property_data_readonly(String("Map")),
  )
  set_proto_obj.props.set(
    to_string_tag_key,
    property_data_readonly(String("Set")),
  )
  weakmap_proto_obj.props.set(
    to_string_tag_key,
    property_data_readonly(String("WeakMap")),
  )
  weakset_proto_obj.props.set(
    to_string_tag_key,
    property_data_readonly(String("WeakSet")),
  )
}
