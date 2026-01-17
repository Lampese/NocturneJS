///|
pub type SymbolValue = @runtime.SymbolValue

///|
pub type BuiltinValue = @runtime.BuiltinValue

///|
pub type FunctionValue = @runtime.FunctionValue

///|
pub type BoundFunctionValue = @runtime.BoundFunctionValue

///|
pub type ObjectValue = @runtime.ObjectValue

///|
pub type ArrayValue = @runtime.ArrayValue

///|
pub type ArgumentsValue = @runtime.ArgumentsValue

///|
pub enum Value {
  Undefined
  Null
  Bool(Bool)
  Number(Double)
  String(String)
  BigInt(@bigint.BigInt)
  Symbol(SymbolValue)
  Builtin(BuiltinValue)
  Function(FunctionValue)
  BoundFunction(BoundFunctionValue)
  Object(ObjectValue)
  Array(ArrayValue)
  Arguments(ArgumentsValue)
} derive(Show, ToJson)

///|
pub fn value_undefined() -> Value {
  Value::Undefined
}

///|
pub fn value_null() -> Value {
  Value::Null
}

///|
pub fn value_bool(value : Bool) -> Value {
  Value::Bool(value)
}

///|
pub fn value_number(value : Double) -> Value {
  Value::Number(value)
}

///|
pub fn value_string(value : String) -> Value {
  Value::String(value)
}

///|
pub fn value_bigint(value : @bigint.BigInt) -> Value {
  Value::BigInt(value)
}

///|
pub fn value_symbol(value : SymbolValue) -> Value {
  Value::Symbol(value)
}

///|
pub fn value_builtin(value : BuiltinValue) -> Value {
  Value::Builtin(value)
}

///|
pub fn value_function(value : FunctionValue) -> Value {
  Value::Function(value)
}

///|
pub fn value_bound_function(value : BoundFunctionValue) -> Value {
  Value::BoundFunction(value)
}

///|
pub fn value_object(value : ObjectValue) -> Value {
  Value::Object(value)
}

///|
pub fn value_array(value : ArrayValue) -> Value {
  Value::Array(value)
}

///|
pub fn value_arguments(value : ArgumentsValue) -> Value {
  Value::Arguments(value)
}

///|
fn to_runtime(value : Value) -> @runtime.Value {
  match value {
    Value::Undefined => @runtime.value_undefined()
    Value::Null => @runtime.value_null()
    Value::Bool(value) => @runtime.value_bool(value)
    Value::Number(value) => @runtime.value_number(value)
    Value::String(value) => @runtime.value_string(value)
    Value::BigInt(value) => @runtime.value_bigint(value)
    Value::Symbol(value) => @runtime.value_symbol(value)
    Value::Builtin(value) => @runtime.value_builtin(value)
    Value::Function(value) => @runtime.value_function(value)
    Value::BoundFunction(value) => @runtime.value_bound_function(value)
    Value::Object(value) => @runtime.value_object(value)
    Value::Array(value) => @runtime.value_array(value)
    Value::Arguments(value) => @runtime.value_arguments(value)
  }
}

///|
pub fn value_to_string(value : Value) -> String {
  @runtime.value_to_string(to_runtime(value))
}

///|
pub fn format_error(value : Value) -> String {
  @runtime.format_error(to_runtime(value))
}

///|
pub fn error_name(value : Value) -> String? {
  @runtime.error_name(to_runtime(value))
}
