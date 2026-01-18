///|
fn is_callable(value : Value) -> Bool {
  match value {
    Function(_) => true
    BoundFunction(_) => true
    Builtin(_) => true
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => data.callable
        None => false
      }
    _ => false
  }
}

///|
fn is_constructor_builtin(kind : BuiltinFunction) -> Bool {
  match kind {
    BuiltinFunction::ObjectConstructor => true
    BuiltinFunction::ProxyConstructor => true
    BuiltinFunction::ArrayConstructor => true
    BuiltinFunction::FunctionConstructor => true
    BuiltinFunction::GeneratorFunctionConstructor => true
    BuiltinFunction::AsyncFunctionConstructor => true
    BuiltinFunction::AsyncGeneratorFunctionConstructor => true
    BuiltinFunction::StringConstructor => true
    BuiltinFunction::NumberConstructor => true
    BuiltinFunction::BooleanConstructor => true
    BuiltinFunction::BigIntConstructor => true
    BuiltinFunction::SymbolConstructor => true
    BuiltinFunction::IteratorConstructor => true
    BuiltinFunction::RegExpConstructor => true
    BuiltinFunction::DateConstructor => true
    BuiltinFunction::ArrayBufferConstructor => true
    BuiltinFunction::SharedArrayBufferConstructor => true
    BuiltinFunction::DataViewConstructor => true
    BuiltinFunction::TypedArrayConstructor => true
    BuiltinFunction::Uint8ArrayConstructor => true
    BuiltinFunction::Int8ArrayConstructor => true
    BuiltinFunction::Uint8ClampedArrayConstructor => true
    BuiltinFunction::Uint16ArrayConstructor => true
    BuiltinFunction::Int16ArrayConstructor => true
    BuiltinFunction::Uint32ArrayConstructor => true
    BuiltinFunction::Int32ArrayConstructor => true
    BuiltinFunction::BigInt64ArrayConstructor => true
    BuiltinFunction::BigUint64ArrayConstructor => true
    BuiltinFunction::Float16ArrayConstructor => true
    BuiltinFunction::Float32ArrayConstructor => true
    BuiltinFunction::Float64ArrayConstructor => true
    BuiltinFunction::MapConstructor => true
    BuiltinFunction::SetConstructor => true
    BuiltinFunction::WeakMapConstructor => true
    BuiltinFunction::WeakSetConstructor => true
    BuiltinFunction::WeakRefConstructor => true
    BuiltinFunction::FinalizationRegistryConstructor => true
    BuiltinFunction::PromiseConstructor => true
    BuiltinFunction::WorkerConstructor => true
    BuiltinFunction::ErrorConstructor => true
    BuiltinFunction::AggregateErrorConstructor => true
    BuiltinFunction::EvalErrorConstructor => true
    BuiltinFunction::SyntaxErrorConstructor => true
    BuiltinFunction::RangeErrorConstructor => true
    BuiltinFunction::TypeErrorConstructor => true
    BuiltinFunction::ReferenceErrorConstructor => true
    BuiltinFunction::UriErrorConstructor => true
    _ => false
  }
}

///|
fn is_constructor_value(value : Value) -> Bool {
  match value {
    Function(func) => func.is_constructor
    BoundFunction(bound) => bound.is_constructor
    Builtin(builtin) => is_constructor_builtin(builtin.kind)
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => data.constructable
        None => false
      }
    _ => false
  }
}

///|
fn function_realm_env(value : Value) -> Env? {
  match value {
    Function(func) => Some(func.env)
    BoundFunction(bound) => function_realm_env(bound.target)
    Builtin(builtin) =>
      match builtin.realm_env {
        Some(env) => Some(env)
        None => root_env()
      }
    Object(obj) =>
      match obj.proxy_data {
        Some(data) =>
          if data.handler is Null {
            None
          } else {
            function_realm_env(data.target)
          }
        None => None
      }
    _ => None
  }
}

///|
fn function_realm_env_checked(value : Value) -> Env raise {
  match value {
    Function(func) => func.env
    BoundFunction(bound) => function_realm_env_checked(bound.target)
    Builtin(builtin) =>
      match builtin.realm_env {
        Some(env) => env
        None =>
          match root_env() {
            Some(env) => env
            None => Env::new(None)
          }
      }
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => {
          proxy_check_revoked(data)
          function_realm_env_checked(data.target)
        }
        None =>
          match root_env() {
            Some(env) => env
            None => Env::new(None)
          }
      }
    _ =>
      match root_env() {
        Some(env) => env
        None => Env::new(None)
      }
  }
}

///|
fn constructor_name(value : Value) -> String? {
  match value {
    Function(func) => func.name
    BoundFunction(bound) =>
      match bound.props.get("name") {
        Some(prop) =>
          match prop.value {
            String(name) => Some(name)
            _ => None
          }
        None => None
      }
    Builtin(builtin) => Some(builtin_name(builtin.kind))
    _ => None
  }
}

///|
fn throw_not_constructor(value : Value) -> Value raise {
  match constructor_name(value) {
    Some(name) => throw_type_error("\{name} is not a constructor")
    None => throw_type_error("not a constructor")
  }
}

///|
fn is_object_like(value : Value) -> Bool {
  match value {
    Object(_) => true
    Function(_) => true
    BoundFunction(_) => true
    Builtin(_) => true
    Array(_) => true
    Arguments(_) => true
    _ => false
  }
}

///|
fn is_array_proto_object(obj : ObjectValue) -> Bool {
  match array_proto() {
    Some(proto) => proto.id == obj.id
    None => false
  }
}

///|
fn proxy_check_revoked(data : ProxyData) -> Unit raise {
  if data.handler is Null {
    let _ = throw_type_error("revoked proxy")

  }
}

///|
fn is_array_value(value : Value) -> Bool raise {
  match value {
    Array(arr) => arr.typed_array_data is None
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => {
          proxy_check_revoked(data)
          is_array_value(data.target)
        }
        None => is_array_proto_object(obj)
      }
    _ => false
  }
}
