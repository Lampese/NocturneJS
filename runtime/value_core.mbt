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
pub fn Value::undefined() -> Value {
  Undefined
}
