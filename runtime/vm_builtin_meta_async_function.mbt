///|
fn builtin_name_async_function(func : BuiltinFunction) -> String {
  match func {
    BuiltinFunction::AsyncFunctionConstructor => "AsyncFunction"
    BuiltinFunction::AsyncFunctionResolveFunction => ""
    _ => ""
  }
}

///|
fn builtin_length_async_function(func : BuiltinFunction) -> Int {
  match func {
    BuiltinFunction::AsyncFunctionConstructor => 1
    BuiltinFunction::AsyncFunctionResolveFunction => 1
    _ => 0
  }
}
