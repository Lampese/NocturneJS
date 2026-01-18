///|
fn declare_global_ctors(
  env : Env,
  object_ctor : Value,
  function_ctor : Value,
  array_ctor : Value,
  array_buffer_ctor : Value,
  shared_array_buffer_ctor : Value,
  dataview_ctor : Value,
  typed_array_ctor : Value,
  uint8_array_ctor : Value,
  int8_array_ctor : Value,
  uint8_clamped_array_ctor : Value,
  uint16_array_ctor : Value,
  int16_array_ctor : Value,
  uint32_array_ctor : Value,
  int32_array_ctor : Value,
  bigint64_array_ctor : Value,
  biguint64_array_ctor : Value,
  float16_array_ctor : Value,
  float32_array_ctor : Value,
  float64_array_ctor : Value,
  string_ctor : Value,
  number_ctor : Value,
  bool_ctor : Value,
  bigint_ctor : Value,
  symbol_ctor : Value,
  iterator_ctor : Value,
  regexp_ctor : Value,
  date_ctor : Value,
  map_ctor : Value,
  set_ctor : Value,
  weakmap_ctor : Value,
  weakset_ctor : Value,
  weakref_ctor : Value,
  finreg_ctor : Value,
  promise_ctor : Value,
  proxy_ctor : Value,
  error_ctor : Value,
  aggregate_error_ctor : Value,
  eval_error_ctor : Value,
  syntax_error_ctor : Value,
  range_error_ctor : Value,
  type_error_ctor : Value,
  reference_error_ctor : Value,
  uri_error_ctor : Value,
) -> Unit {
  env_declare(env, "Object", object_ctor)
  env_declare(env, "Function", function_ctor)
  env_declare(env, "Array", array_ctor)
  env_declare(env, "ArrayBuffer", array_buffer_ctor)
  env_declare(env, "SharedArrayBuffer", shared_array_buffer_ctor)
  env_declare(env, "DataView", dataview_ctor)
  env_declare(env, "TypedArray", typed_array_ctor)
  env_declare(env, "Uint8Array", uint8_array_ctor)
  env_declare(env, "Int8Array", int8_array_ctor)
  env_declare(env, "Uint8ClampedArray", uint8_clamped_array_ctor)
  env_declare(env, "Uint16Array", uint16_array_ctor)
  env_declare(env, "Int16Array", int16_array_ctor)
  env_declare(env, "Uint32Array", uint32_array_ctor)
  env_declare(env, "Int32Array", int32_array_ctor)
  env_declare(env, "BigInt64Array", bigint64_array_ctor)
  env_declare(env, "BigUint64Array", biguint64_array_ctor)
  env_declare(env, "Float16Array", float16_array_ctor)
  env_declare(env, "Float32Array", float32_array_ctor)
  env_declare(env, "Float64Array", float64_array_ctor)
  env_declare(env, "String", string_ctor)
  env_declare(env, "Number", number_ctor)
  env_declare(env, "Boolean", bool_ctor)
  env_declare(env, "BigInt", bigint_ctor)
  env_declare(env, "Symbol", symbol_ctor)
  env_declare(env, "Iterator", iterator_ctor)
  env_declare(env, "RegExp", regexp_ctor)
  env_declare(env, "Date", date_ctor)
  env_declare(env, "Map", map_ctor)
  env_declare(env, "Set", set_ctor)
  env_declare(env, "WeakMap", weakmap_ctor)
  env_declare(env, "WeakSet", weakset_ctor)
  env_declare(env, "WeakRef", weakref_ctor)
  env_declare(env, "FinalizationRegistry", finreg_ctor)
  env_declare(env, "Promise", promise_ctor)
  env_declare(env, "Proxy", proxy_ctor)
  env_declare(env, "Error", error_ctor)
  env_declare(env, "AggregateError", aggregate_error_ctor)
  env_declare(env, "EvalError", eval_error_ctor)
  env_declare(env, "SyntaxError", syntax_error_ctor)
  env_declare(env, "RangeError", range_error_ctor)
  env_declare(env, "TypeError", type_error_ctor)
  env_declare(env, "ReferenceError", reference_error_ctor)
  env_declare(env, "URIError", uri_error_ctor)
}

///|
fn declare_standard_objects(
  env : Env,
  json_obj : Value,
  reflect_obj : Value,
  atomics_obj : Value,
  math_obj : Value,
  parse_int_func : Value,
  parse_float_func : Value,
) -> Unit {
  env_declare(env, "JSON", json_obj)
  env_declare(env, "Reflect", reflect_obj)
  env_declare(env, "Atomics", atomics_obj)
  env_declare(env, "Math", math_obj)
  env_declare(env, "parseInt", parse_int_func)
  env_declare(env, "parseFloat", parse_float_func)
}

///|
fn declare_module_helpers(env : Env) -> Unit {
  env_declare(env, "__import", new_builtin_value(BuiltinFunction::ModuleImport))
  env_declare(
    env,
    "__import_dynamic",
    new_builtin_value(BuiltinFunction::ModuleImportDynamic),
  )
  env_declare(
    env,
    "__import_binding",
    new_builtin_value(BuiltinFunction::ModuleImportBinding),
  )
  env_declare(
    env,
    "__binding",
    new_builtin_value(BuiltinFunction::ModuleBinding),
  )
  env_declare(env, "__export", new_builtin_value(BuiltinFunction::ModuleExport))
  env_declare(
    env,
    "__export_star",
    new_builtin_value(BuiltinFunction::ModuleExportStar),
  )
}

///|
fn declare_global_functions(env : Env) -> Unit {
  env_declare(env, "eval", new_builtin_value(BuiltinFunction::Eval))
  env_declare(env, "isNaN", new_builtin_value(BuiltinFunction::IsNaN))
  env_declare(env, "isFinite", new_builtin_value(BuiltinFunction::IsFinite))
  env_declare(env, "decodeURI", new_builtin_value(BuiltinFunction::DecodeURI))
  env_declare(
    env,
    "decodeURIComponent",
    new_builtin_value(BuiltinFunction::DecodeURIComponent),
  )
  env_declare(env, "encodeURI", new_builtin_value(BuiltinFunction::EncodeURI))
  env_declare(
    env,
    "encodeURIComponent",
    new_builtin_value(BuiltinFunction::EncodeURIComponent),
  )
  env_declare(env, "escape", new_builtin_value(BuiltinFunction::Escape))
  env_declare(env, "unescape", new_builtin_value(BuiltinFunction::Unescape))
}

///|
fn declare_host_objects(
  env : Env,
  std_obj : Value,
  os_obj : Value,
  bjson_obj : Value,
) -> Unit {
  env_declare(env, "std", std_obj)
  env_declare(env, "os", os_obj)
  env_declare(env, "bjson", bjson_obj)
}

///|
fn set_global_intrinsics(global : Value) -> Unit {
  match global {
    Object(obj) => {
      obj.props.set("NaN", property_data_const(Number(nan())))
      obj.props.set("Infinity", property_data_const(Number(inf())))
      obj.props.set("undefined", property_data_const(Undefined))
    }
    _ => ()
  }
}
