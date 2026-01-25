///|
fn eval_new(
  env : Env,
  callee : @engine.Expr,
  args : Array[@engine.Expr],
) -> Value raise {
  let ctor = eval_expr(env, callee)
  let argv = eval_args(env, args)
  call_constructor(ctor, argv)
}

///|
fn constructor_proto_from_target(
  target : Value,
  fallback : Value?,
) -> Value? raise {
  let proto_value = property_get(target, "prototype")
  if is_object_like(proto_value) {
    Some(proto_value)
  } else {
    let _ = function_realm_env_checked(target)
    fallback
  }
}

///|
fn call_constructor(
  callee : Value,
  args : Array[Value],
  new_target? : Value = Undefined,
) -> Value raise {
  let target_value = if new_target is Undefined { callee } else { new_target }
  match callee {
    Function(func) => {
      if !func.is_constructor {
        let _ = throw_not_constructor(Value::Function(func))

      }
      if func.is_default_constructor {
        match func.super_constructor {
          Some(_) => {
            let super_ctor = match get_proto_of_value(Value::Function(func)) {
              Some(value) => value
              None => Null
            }
            if !is_constructor_value(super_ctor) {
              return throw_not_constructor(super_ctor)
            }
            let result = call_constructor(
              super_ctor,
              args,
              new_target=target_value,
            )
            if is_object_like(result) &&
              (
                !props_is_empty(func.private_instance_props) ||
                !func.instance_fields.is_empty()
              ) {
              let frame = Env::new(Some(func.env))
              frame.var_env = Some(frame)
              frame.strict = func.is_strict
              frame.new_target = target_value
              env_set_local(frame, "this", result)
              let _ = with_current_function(func, fn() raise {
                init_instance_fields(func, frame, result)
                result
              })

            }
            return result
          }
          None => ()
        }
      }
      if func.is_class_constructor && func.super_constructor is Some(_) {
        return call_user_function_with_this(func, args, Undefined, target_value)
      }
      let realm_env = function_realm_env_checked(target_value)
      let default_proto = match
        value_from_object(object_proto_for_env(realm_env)) {
        Some(value) => Some(value)
        None => value_from_object(object_proto())
      }
      let proto = constructor_proto_from_target(target_value, default_proto)
      let instance = new_object_value_with_proto(proto)
      let result = call_user_function_with_this(
        func, args, instance, target_value,
      )
      if func.is_class_constructor {
        result
      } else if is_object_like(result) {
        result
      } else {
        instance
      }
    }
    BoundFunction(bound) => {
      if !bound.is_constructor {
        let _ = throw_not_constructor(Value::BoundFunction(bound))
        return Undefined
      }
      let final_args : Array[Value] = []
      for value in bound.bound_args {
        final_args.push(value)
      }
      for value in args {
        final_args.push(value)
      }
      let bound_self = Value::BoundFunction(bound)
      let bound_target = if new_target is Undefined {
        bound_self
      } else {
        new_target
      }
      let final_new_target = if same_value(bound_target, bound_self) {
        bound.target
      } else {
        bound_target
      }
      call_constructor(bound.target, final_args, new_target=final_new_target)
    }
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => proxy_construct_value(data, args, target_value)
        None => {
          let _ = throw_not_constructor(Value::Object(obj))
          Undefined
        }
      }
    Builtin(builtin) =>
      if !is_constructor_builtin(builtin.kind) {
        let _ = throw_not_constructor(Value::Builtin(builtin))
        Undefined
      } else {
        match builtin.kind {
          BuiltinFunction::ObjectConstructor =>
            if !(new_target is Undefined) &&
              !same_value(Value::Builtin(builtin), new_target) {
              let realm_env = function_realm_env_checked(new_target)
              let default_proto = match
                value_from_object(object_proto_for_env(realm_env)) {
                Some(value) => Some(value)
                None => value_from_object(object_proto())
              }
              let proto = constructor_proto_from_target(
                new_target, default_proto,
              )
              new_object_value_with_proto(proto)
            } else if args.is_empty() {
              new_object_value()
            } else {
              match args[0] {
                Undefined => new_object_value()
                Null => new_object_value()
                _ => to_object(args[0])
              }
            }
          BuiltinFunction::ProxyConstructor =>
            if args.length() < 2 {
              throw_type_error("invalid Proxy arguments")
            } else {
              new_proxy_value(args[0], args[1])
            }
          BuiltinFunction::ArrayConstructor => {
            let realm_env = function_realm_env_checked(target_value)
            let default_proto = match
              value_from_object(array_proto_for_env(realm_env)) {
              Some(value) => Some(value)
              None => value_from_object(array_proto())
            }
            let proto = constructor_proto_from_target(
              target_value, default_proto,
            )
            array_from_args_with_proto(args, proto)
          }
          BuiltinFunction::IteratorConstructor => {
            if same_value(Value::Builtin(builtin), target_value) {
              return throw_type_error("abstract class not constructable")
            }
            let realm_env = function_realm_env_checked(target_value)
            let default_proto = match
              value_from_object(iterator_proto_for_env(realm_env)) {
              Some(value) => Some(value)
              None => value_from_object(iterator_proto())
            }
            let proto = constructor_proto_from_target(
              target_value, default_proto,
            )
            new_object_value_with_proto(proto)
          }
          BuiltinFunction::FunctionConstructor => {
            let ctor_env = match function_realm_env(Value::Builtin(builtin)) {
              Some(env) => env
              None =>
                match root_env() {
                  Some(env) => env
                  None => Env::new(None)
                }
            }
            let func_value = function_from_args_with_env(args, ctor_env)
            if !(new_target is Undefined) {
              let fallback_env = function_realm_env_checked(new_target)
              let fallback_proto = match function_proto_for_env(fallback_env) {
                Some(value) => Some(value)
                None => function_proto()
              }
              let proto = constructor_proto_from_target(
                new_target, fallback_proto,
              )
              match func_value {
                Function(func) => {
                  rc_replace_optional_value(func.proto, proto)
                  func.proto = proto
                }
                _ => ()
              }
            }
            func_value
          }
          BuiltinFunction::GeneratorFunctionConstructor => {
            let ctor_env = match function_realm_env(Value::Builtin(builtin)) {
              Some(env) => env
              None =>
                match root_env() {
                  Some(env) => env
                  None => Env::new(None)
                }
            }
            let func_value = function_from_args_with_env_kind(
              args, ctor_env, true, false,
            )
            if !(new_target is Undefined) {
              let fallback_proto = match
                value_from_object(
                  generator_function_proto_for_env(
                    function_realm_env_checked(new_target),
                  ),
                ) {
                Some(value) => Some(value)
                None => value_from_object(generator_function_proto())
              }
              let proto = constructor_proto_from_target(
                new_target, fallback_proto,
              )
              match func_value {
                Function(func) => {
                  rc_replace_optional_value(func.proto, proto)
                  func.proto = proto
                }
                _ => ()
              }
            }
            func_value
          }
          BuiltinFunction::AsyncGeneratorFunctionConstructor => {
            let ctor_env = match function_realm_env(Value::Builtin(builtin)) {
              Some(env) => env
              None =>
                match root_env() {
                  Some(env) => env
                  None => Env::new(None)
                }
            }
            let func_value = function_from_args_with_env_kind(
              args, ctor_env, true, true,
            )
            if !(new_target is Undefined) {
              let fallback_proto = match
                value_from_object(
                  async_generator_function_proto_for_env(
                    function_realm_env_checked(new_target),
                  ),
                ) {
                Some(value) => Some(value)
                None => value_from_object(async_generator_function_proto())
              }
              let proto = constructor_proto_from_target(
                new_target, fallback_proto,
              )
              match func_value {
                Function(func) => {
                  rc_replace_optional_value(func.proto, proto)
                  func.proto = proto
                }
                _ => ()
              }
            }
            func_value
          }
          BuiltinFunction::AsyncFunctionConstructor => {
            let ctor_env = match function_realm_env(Value::Builtin(builtin)) {
              Some(env) => env
              None =>
                match root_env() {
                  Some(env) => env
                  None => Env::new(None)
                }
            }
            let func_value = function_from_args_with_env_kind(
              args, ctor_env, false, true,
            )
            if !(new_target is Undefined) {
              let fallback_proto = match
                value_from_object(
                  async_function_proto_for_env(
                    function_realm_env_checked(new_target),
                  ),
                ) {
                Some(value) => Some(value)
                None => value_from_object(async_function_proto())
              }
              let proto = constructor_proto_from_target(
                new_target, fallback_proto,
              )
              match func_value {
                Function(func) => {
                  rc_replace_optional_value(func.proto, proto)
                  func.proto = proto
                }
                _ => ()
              }
            }
            func_value
          }
          BuiltinFunction::StringConstructor => {
            let proto_value = property_get(target_value, "prototype")
            let default_proto = {
              let realm_env = function_realm_env_checked(target_value)
              match value_from_object(string_proto_for_env(realm_env)) {
                Some(value) => Some(value)
                None => value_from_object(string_proto())
              }
            }
            let proto = if is_object_like(proto_value) {
              Some(proto_value)
            } else {
              default_proto
            }
            if args.is_empty() {
              new_string_object("", proto~)
            } else if is_symbol_like(args[0]) {
              throw_type_error("cannot convert symbol to string")
            } else {
              new_string_object(to_string_strict(args[0]), proto~)
            }
          }
          BuiltinFunction::NumberConstructor => {
            let proto_value = property_get(target_value, "prototype")
            let default_proto = {
              let realm_env = function_realm_env_checked(target_value)
              match value_from_object(number_proto_for_env(realm_env)) {
                Some(value) => Some(value)
                None => value_from_object(number_proto())
              }
            }
            let proto = if is_object_like(proto_value) {
              Some(proto_value)
            } else {
              default_proto
            }
            if args.is_empty() {
              new_number_object(0.0, proto~)
            } else {
              let prim = if is_object_like(args[0]) {
                to_primitive(args[0], false)
              } else {
                args[0]
              }
              match prim {
                Symbol(_) => throw_type_error("cannot convert symbol to number")
                BigInt(b) => new_number_object(bigint_to_number(b), proto~)
                _ => new_number_object(to_number(prim), proto~)
              }
            }
          }
          BuiltinFunction::BooleanConstructor => {
            let proto_value = property_get(target_value, "prototype")
            let default_proto = {
              let realm_env = function_realm_env_checked(target_value)
              match value_from_object(bool_proto_for_env(realm_env)) {
                Some(value) => Some(value)
                None => value_from_object(bool_proto())
              }
            }
            let proto = if is_object_like(proto_value) {
              Some(proto_value)
            } else {
              default_proto
            }
            if args.is_empty() {
              new_bool_object(false, proto~)
            } else {
              new_bool_object(is_truthy(args[0]), proto~)
            }
          }
          BuiltinFunction::BigIntConstructor =>
            throw_type_error("BigInt is not a constructor")
          BuiltinFunction::SymbolConstructor =>
            throw_type_error("Symbol is not a constructor")
          BuiltinFunction::RegExpConstructor =>
            regexp_from_args(args, true, new_target=target_value)
          BuiltinFunction::DateConstructor => {
            let proto_value = property_get(target_value, "prototype")
            let default_proto = {
              let realm_env = function_realm_env_checked(target_value)
              match value_from_object(date_proto_for_env(realm_env)) {
                Some(value) => Some(value)
                None => value_from_object(date_proto())
              }
            }
            let proto = if is_object_like(proto_value) {
              Some(proto_value)
            } else {
              default_proto
            }
            new_date_value(date_from_args(args), proto~)
          }
          BuiltinFunction::ArrayBufferConstructor => {
            let len64 = if args.is_empty() {
              0L
            } else {
              to_index_int64(args[0])
            }
            let mut max_len64 = len64
            let mut resizable = false
            if args.length() > 1 {
              match args[1] {
                Undefined => ()
                Null => ()
                _ => {
                  let opts = to_object(args[1])
                  let max_value = property_get(opts, "maxByteLength")
                  if max_value is Undefined {
                    ()
                  } else {
                    let next_max_len64 = to_index_int64(max_value)
                    if len64 > next_max_len64 {
                      return throw_range_error(
                        "invalid array buffer max length",
                      )
                    }
                    max_len64 = next_max_len64
                    resizable = true
                  }
                }
              }
            }
            let default_proto = {
              let realm_env = function_realm_env_checked(target_value)
              match value_from_object(array_buffer_proto_for_env(realm_env)) {
                Some(value) => Some(value)
                None => value_from_object(array_buffer_proto())
              }
            }
            let proto = constructor_proto_from_target(
              target_value, default_proto,
            )
            let max_int = 2147483647
            if len64 > Int64::from_int(max_int) {
              return throw_range_error("invalid array buffer length")
            }
            if max_len64 > Int64::from_int(max_int) {
              return throw_range_error("invalid max array buffer length")
            }
            let len = Int64::to_int(len64)
            let max_len = Int64::to_int(max_len64)
            new_array_buffer_value_with_options(len, max_len, resizable, proto~)
          }
          BuiltinFunction::SharedArrayBufferConstructor => {
            let len64 = if args.is_empty() {
              0L
            } else {
              to_index_int64(args[0])
            }
            let mut max_len64 = len64
            let mut resizable = false
            if args.length() > 1 {
              match args[1] {
                Undefined => ()
                Null => ()
                _ => {
                  let opts = to_object(args[1])
                  let max_value = property_get(opts, "maxByteLength")
                  if max_value is Undefined {
                    ()
                  } else {
                    let next_max_len64 = to_index_int64(max_value)
                    if len64 > next_max_len64 {
                      return throw_range_error(
                        "invalid array buffer max length",
                      )
                    }
                    max_len64 = next_max_len64
                    resizable = true
                  }
                }
              }
            }
            let default_proto = {
              let realm_env = function_realm_env_checked(target_value)
              match
                value_from_object(shared_array_buffer_proto_for_env(realm_env)) {
                Some(value) => Some(value)
                None => value_from_object(shared_array_buffer_proto())
              }
            }
            let proto = constructor_proto_from_target(
              target_value, default_proto,
            )
            let max_int = 2147483647
            if len64 > Int64::from_int(max_int) {
              return throw_range_error("invalid array buffer length")
            }
            if max_len64 > Int64::from_int(max_int) {
              return throw_range_error("invalid max array buffer length")
            }
            let len = Int64::to_int(len64)
            let max_len = Int64::to_int(max_len64)
            new_shared_array_buffer_value_with_options(
              len, max_len, resizable, proto,
            )
          }
          BuiltinFunction::DataViewConstructor =>
            new_dataview_from_args(args, target_value)
          BuiltinFunction::TypedArrayConstructor =>
            throw_type_error("cannot be called")
          BuiltinFunction::Uint8ArrayConstructor =>
            new_typed_array_from_args(TypedArrayKind::Uint8, args, target_value)
          BuiltinFunction::Int8ArrayConstructor =>
            new_typed_array_from_args(TypedArrayKind::Int8, args, target_value)
          BuiltinFunction::Uint8ClampedArrayConstructor =>
            new_typed_array_from_args(
              TypedArrayKind::Uint8Clamped,
              args,
              target_value,
            )
          BuiltinFunction::Uint16ArrayConstructor =>
            new_typed_array_from_args(
              TypedArrayKind::Uint16,
              args,
              target_value,
            )
          BuiltinFunction::Int16ArrayConstructor =>
            new_typed_array_from_args(TypedArrayKind::Int16, args, target_value)
          BuiltinFunction::Uint32ArrayConstructor =>
            new_typed_array_from_args(
              TypedArrayKind::Uint32,
              args,
              target_value,
            )
          BuiltinFunction::Int32ArrayConstructor =>
            new_typed_array_from_args(TypedArrayKind::Int32, args, target_value)
          BuiltinFunction::BigInt64ArrayConstructor =>
            new_typed_array_from_args(
              TypedArrayKind::BigInt64,
              args,
              target_value,
            )
          BuiltinFunction::BigUint64ArrayConstructor =>
            new_typed_array_from_args(
              TypedArrayKind::BigUint64,
              args,
              target_value,
            )
          BuiltinFunction::Float16ArrayConstructor =>
            new_typed_array_from_args(
              TypedArrayKind::Float16,
              args,
              target_value,
            )
          BuiltinFunction::Float32ArrayConstructor =>
            new_typed_array_from_args(
              TypedArrayKind::Float32,
              args,
              target_value,
            )
          BuiltinFunction::Float64ArrayConstructor =>
            new_typed_array_from_args(
              TypedArrayKind::Float64,
              args,
              target_value,
            )
          BuiltinFunction::MapConstructor => {
            let target_env = function_realm_env_checked(target_value)
            let fallback_proto = match
              value_from_object(map_proto_for_env(target_env)) {
              Some(value) => Some(value)
              None => value_from_object(map_proto())
            }
            let proto = constructor_proto_from_target(
              target_value, fallback_proto,
            )
            let obj = new_map_value_with_proto(proto)
            if !args.is_empty() {
              init_map_or_set_from_iterable(obj, args[0], false)
            }
            obj
          }
          BuiltinFunction::SetConstructor => {
            let target_env = function_realm_env_checked(target_value)
            let fallback_proto = match
              value_from_object(set_proto_for_env(target_env)) {
              Some(value) => Some(value)
              None => value_from_object(set_proto())
            }
            let proto = constructor_proto_from_target(
              target_value, fallback_proto,
            )
            let obj = new_set_value_with_proto(proto)
            if !args.is_empty() {
              init_map_or_set_from_iterable(obj, args[0], true)
            }
            obj
          }
          BuiltinFunction::WeakMapConstructor => {
            let target_env = function_realm_env_checked(target_value)
            let fallback_proto = match
              value_from_object(weakmap_proto_for_env(target_env)) {
              Some(value) => Some(value)
              None => value_from_object(weakmap_proto())
            }
            let proto = constructor_proto_from_target(
              target_value, fallback_proto,
            )
            let obj = new_weakmap_value_with_proto(proto)
            if !args.is_empty() {
              init_map_or_set_from_iterable(obj, args[0], false)
            }
            obj
          }
          BuiltinFunction::WeakSetConstructor => {
            let target_env = function_realm_env_checked(target_value)
            let fallback_proto = match
              value_from_object(weakset_proto_for_env(target_env)) {
              Some(value) => Some(value)
              None => value_from_object(weakset_proto())
            }
            let proto = constructor_proto_from_target(
              target_value, fallback_proto,
            )
            let obj = new_weakset_value_with_proto(proto)
            if !args.is_empty() {
              init_map_or_set_from_iterable(obj, args[0], true)
            }
            obj
          }
          BuiltinFunction::WeakRefConstructor => {
            if args.is_empty() || !is_weakref_target(args[0]) {
              return throw_type_error("invalid target")
            }
            let target_env = function_realm_env_checked(target_value)
            let fallback_proto = match
              value_from_object(weakref_proto_for_env(target_env)) {
              Some(value) => Some(value)
              None => value_from_object(weakref_proto())
            }
            let proto = constructor_proto_from_target(
              target_value, fallback_proto,
            )
            new_weakref_value(args[0], proto)
          }
          BuiltinFunction::FinalizationRegistryConstructor => {
            if args.is_empty() || !is_callable(args[0]) {
              return throw_type_error("argument must be a function")
            }
            let target_env = function_realm_env_checked(target_value)
            let fallback_proto = match
              value_from_object(finreg_proto_for_env(target_env)) {
              Some(value) => Some(value)
              None => value_from_object(finreg_proto())
            }
            let proto = constructor_proto_from_target(
              target_value, fallback_proto,
            )
            new_finreg_value(args[0], target_env, proto)
          }
          BuiltinFunction::PromiseConstructor => {
            if args.is_empty() || !is_callable(args[0]) {
              return throw_type_error("argument must be a function")
            }
            let target_env = function_realm_env_checked(target_value)
            let fallback_proto = match
              value_from_object(promise_proto_for_env(target_env)) {
              Some(value) => Some(value)
              None => value_from_object(promise_proto())
            }
            let proto = constructor_proto_from_target(
              target_value, fallback_proto,
            )
            let obj = new_promise_value_with_proto(proto)
            let (resolve, reject) = create_promise_resolving_functions(obj)
            try {
              let _ = call_value_with_this(
                args[0],
                [resolve, reject],
                Undefined,
              )

            } catch {
              ThrowSignal(value) => {
                let _ = call_value_with_this(reject, [value], Undefined)

              }
              err => raise err
            }
            obj
          }
          BuiltinFunction::WorkerConstructor => create_worker_from_args(args)
          BuiltinFunction::ErrorConstructor => {
            let target_env = function_realm_env_checked(target_value)
            let fallback = match error_proto_for_env_name(target_env, "Error") {
              Some(proto) => Some(proto)
              None => error_proto()
            }
            let proto = error_proto_from_target(target_value, fallback)
            let message_info = error_message_from_args(args)
            let obj = new_error_value(
              proto,
              message_info.message,
              set_message=message_info.has_message,
            )
            if args.length() > 1 {
              match obj {
                Object(value) => install_error_cause(value, args[1])
                _ => ()
              }
            }
            obj
          }
          BuiltinFunction::AggregateErrorConstructor => {
            let target_env = function_realm_env_checked(target_value)
            let fallback = match
              error_proto_for_env_name(target_env, "AggregateError") {
              Some(proto) => Some(proto)
              None => aggregate_error_proto()
            }
            let proto = error_proto_from_target(target_value, fallback)
            let errors_proto = match
              value_from_object(array_proto_for_env(target_env)) {
              Some(value) => Some(value)
              None => value_from_object(array_proto())
            }
            let obj = new_aggregate_error_value_with_proto(
              args, proto, errors_proto,
            )
            if args.length() > 2 {
              match obj {
                Object(value) => install_error_cause(value, args[2])
                _ => ()
              }
            }
            obj
          }
          BuiltinFunction::EvalErrorConstructor => {
            let target_env = function_realm_env_checked(target_value)
            let fallback = match
              error_proto_for_env_name(target_env, "EvalError") {
              Some(proto) => Some(proto)
              None => eval_error_proto()
            }
            let proto = error_proto_from_target(target_value, fallback)
            let message_info = error_message_from_args(args)
            let obj = new_error_value(
              proto,
              message_info.message,
              set_message=message_info.has_message,
            )
            if args.length() > 1 {
              match obj {
                Object(value) => install_error_cause(value, args[1])
                _ => ()
              }
            }
            obj
          }
          BuiltinFunction::SyntaxErrorConstructor => {
            let target_env = function_realm_env_checked(target_value)
            let fallback = match syntax_error_proto_for_env(target_env) {
              Some(proto) => Some(proto)
              None => syntax_error_proto()
            }
            let proto = error_proto_from_target(target_value, fallback)
            let message_info = error_message_from_args(args)
            let obj = new_error_value(
              proto,
              message_info.message,
              set_message=message_info.has_message,
            )
            if args.length() > 1 {
              match obj {
                Object(value) => install_error_cause(value, args[1])
                _ => ()
              }
            }
            obj
          }
          BuiltinFunction::RangeErrorConstructor => {
            let target_env = function_realm_env_checked(target_value)
            let fallback = match range_error_proto_for_env(target_env) {
              Some(proto) => Some(proto)
              None => range_error_proto()
            }
            let proto = error_proto_from_target(target_value, fallback)
            let message_info = error_message_from_args(args)
            let obj = new_error_value(
              proto,
              message_info.message,
              set_message=message_info.has_message,
            )
            if args.length() > 1 {
              match obj {
                Object(value) => install_error_cause(value, args[1])
                _ => ()
              }
            }
            obj
          }
          BuiltinFunction::TypeErrorConstructor => {
            let target_env = function_realm_env_checked(target_value)
            let fallback = match type_error_proto_for_env(target_env) {
              Some(proto) => Some(proto)
              None => type_error_proto()
            }
            let proto = error_proto_from_target(target_value, fallback)
            let message_info = error_message_from_args(args)
            let obj = new_error_value(
              proto,
              message_info.message,
              set_message=message_info.has_message,
            )
            if args.length() > 1 {
              match obj {
                Object(value) => install_error_cause(value, args[1])
                _ => ()
              }
            }
            obj
          }
          BuiltinFunction::ReferenceErrorConstructor => {
            let target_env = function_realm_env_checked(target_value)
            let fallback = match reference_error_proto_for_env(target_env) {
              Some(proto) => Some(proto)
              None => reference_error_proto()
            }
            let proto = error_proto_from_target(target_value, fallback)
            let message_info = error_message_from_args(args)
            let obj = new_error_value(
              proto,
              message_info.message,
              set_message=message_info.has_message,
            )
            if args.length() > 1 {
              match obj {
                Object(value) => install_error_cause(value, args[1])
                _ => ()
              }
            }
            obj
          }
          BuiltinFunction::UriErrorConstructor => {
            let target_env = function_realm_env_checked(target_value)
            let fallback = match uri_error_proto_for_env(target_env) {
              Some(proto) => Some(proto)
              None => uri_error_proto()
            }
            let proto = error_proto_from_target(target_value, fallback)
            let message_info = error_message_from_args(args)
            let obj = new_error_value(
              proto,
              message_info.message,
              set_message=message_info.has_message,
            )
            if args.length() > 1 {
              match obj {
                Object(value) => install_error_cause(value, args[1])
                _ => ()
              }
            }
            obj
          }
          _ => {
            let instance = new_object_value()
            let result = call_builtin_with_this(builtin, args, instance)
            if is_object_like(result) {
              result
            } else {
              instance
            }
          }
        }
      }
    _ => {
      let _ = throw_not_constructor(callee)
      Undefined
    }
  }
}
