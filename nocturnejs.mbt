///|
pub type Value = @value.Value

///|
fn value_from_runtime(value : @runtime.Value) -> Value {
  match value {
    @runtime.Value::Undefined => @value.value_undefined()
    @runtime.Value::Null => @value.value_null()
    @runtime.Value::Bool(value) => @value.value_bool(value)
    @runtime.Value::Number(value) => @value.value_number(value)
    @runtime.Value::String(value) => @value.value_string(value)
    @runtime.Value::BigInt(value) => @value.value_bigint(value)
    @runtime.Value::Symbol(value) => @value.value_symbol(value)
    @runtime.Value::Builtin(value) => @value.value_builtin(value)
    @runtime.Value::Function(value) => @value.value_function(value)
    @runtime.Value::BoundFunction(value) => @value.value_bound_function(value)
    @runtime.Value::Object(value) => @value.value_object(value)
    @runtime.Value::Array(value) => @value.value_array(value)
    @runtime.Value::Arguments(value) => @value.value_arguments(value)
  }
}

///|
fn wrap_runtime_result(
  result : Result[@runtime.Value, @runtime.Value],
) -> Result[Value, Value] {
  match result {
    Ok(value) => Ok(value_from_runtime(value))
    Err(value) => Err(value_from_runtime(value))
  }
}

///|
pub struct Engine {
  runtime : @runtime.Vm
} derive(Show)

///|
pub fn Engine::new() -> Engine {
  Engine::{ runtime: @runtime.Vm::new() }
}

///|
pub fn Engine::run_gc(self : Engine) -> Unit {
  self.runtime.run_gc()
}

///|
pub fn Engine::set_gc_threshold(self : Engine, threshold : Int) -> Unit {
  self.runtime.set_gc_threshold(threshold)
}

///|
pub fn Engine::set_gc_debug(self : Engine, enabled : Bool) -> Unit {
  self.runtime.set_gc_debug(enabled)
}

///|
pub fn Engine::set_can_block(self : Engine, enabled : Bool) -> Unit {
  self.runtime.set_can_block(enabled)
}

///|
pub fn Engine::reset_runtime_state(self : Engine) -> Unit {
  self.runtime.reset_runtime_state()
}

///|
pub fn Engine::async_done_count(self : Engine) -> Int {
  self.runtime.async_done_count()
}

///|
pub fn Engine::take_print_log(self : Engine) -> Array[String] {
  self.runtime.take_print_log()
}

///|
pub fn Engine::format_error(self : Engine, value : Value) -> String {
  let _ = self
  @value.format_error(value)
}

///|
pub fn Engine::value_to_string(self : Engine, value : Value) -> String {
  let _ = self
  @value.value_to_string(value)
}

///|
pub fn Engine::error_name(self : Engine, value : Value) -> String? {
  let _ = self
  @value.error_name(value)
}

///|
pub fn Engine::eval(self : Engine, source : String) -> Value raise {
  value_from_runtime(self.runtime.eval(source))
}

///|
pub fn Engine::eval_with_path(
  self : Engine,
  source : String,
  path : String,
) -> Value raise {
  value_from_runtime(self.runtime.eval_with_path(source, path))
}

///|
pub fn Engine::eval_result(
  self : Engine,
  source : String,
) -> Result[Value, Value] raise {
  wrap_runtime_result(self.runtime.eval_result(source))
}

///|
pub fn Engine::eval_result_with_path(
  self : Engine,
  source : String,
  path : String,
) -> Result[Value, Value] raise {
  wrap_runtime_result(self.runtime.eval_result_with_path(source, path))
}

///|
pub fn Engine::eval_with_path_mode(
  self : Engine,
  source : String,
  path : String,
  force_module : Bool,
  force_strict : Bool,
) -> Value raise {
  value_from_runtime(
    self.runtime.eval_with_path_mode(source, path, force_module, force_strict),
  )
}

///|
pub fn Engine::eval_result_with_path_mode(
  self : Engine,
  source : String,
  path : String,
  force_module : Bool,
  force_strict : Bool,
) -> Result[Value, Value] raise {
  wrap_runtime_result(
    self.runtime.eval_result_with_path_mode(
      source, path, force_module, force_strict,
    ),
  )
}
