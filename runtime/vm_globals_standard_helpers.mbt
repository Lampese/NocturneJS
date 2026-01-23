///|
fn init_json_object(object_proto_obj : ObjectValue) -> Value {
  let json_obj = new_object_value_with_proto(Some(Object(object_proto_obj)))
  match json_obj {
    Object(obj) => {
      props_set(obj.props, 
        "stringify",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::JsonStringify),
        ),
      )
      props_set(obj.props, 
        "parse",
        property_data_non_enum(new_builtin_value(BuiltinFunction::JsonParse)),
      )
      match symbol_to_string_tag_key() {
        Some(key) => props_set(obj.props, key, property_data_readonly(String("JSON")))
        None => ()
      }
    }
    _ => ()
  }
  json_obj
}

///|
fn init_reflect_object(object_proto_obj : ObjectValue) -> Value {
  let reflect_obj = new_object_value_with_proto(Some(Object(object_proto_obj)))
  match reflect_obj {
    Object(obj) => {
      props_set(obj.props, 
        "apply",
        property_data_non_enum(new_builtin_value(BuiltinFunction::ReflectApply)),
      )
      props_set(obj.props, 
        "construct",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::ReflectConstruct),
        ),
      )
      props_set(obj.props, 
        "get",
        property_data_non_enum(new_builtin_value(BuiltinFunction::ReflectGet)),
      )
      props_set(obj.props, 
        "getOwnPropertyDescriptor",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::ReflectGetOwnPropertyDescriptor),
        ),
      )
      props_set(obj.props, 
        "getPrototypeOf",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::ReflectGetPrototypeOf),
        ),
      )
      props_set(obj.props, 
        "defineProperty",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::ReflectDefineProperty),
        ),
      )
      props_set(obj.props, 
        "has",
        property_data_non_enum(new_builtin_value(BuiltinFunction::ReflectHas)),
      )
      props_set(obj.props, 
        "isExtensible",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::ReflectIsExtensible),
        ),
      )
      props_set(obj.props, 
        "set",
        property_data_non_enum(new_builtin_value(BuiltinFunction::ReflectSet)),
      )
      props_set(obj.props, 
        "setPrototypeOf",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::ReflectSetPrototypeOf),
        ),
      )
      props_set(obj.props, 
        "deleteProperty",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::ReflectDeleteProperty),
        ),
      )
      props_set(obj.props, 
        "preventExtensions",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::ReflectPreventExtensions),
        ),
      )
      props_set(obj.props, 
        "ownKeys",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::ReflectOwnKeys),
        ),
      )
      match symbol_to_string_tag_key() {
        Some(key) =>
          props_set(obj.props, key, property_data_readonly(String("Reflect")))
        None => ()
      }
    }
    _ => ()
  }
  reflect_obj
}

///|
fn init_atomics_object(object_proto_obj : ObjectValue) -> Value {
  let atomics_obj = new_object_value_with_proto(Some(Object(object_proto_obj)))
  match atomics_obj {
    Object(obj) => {
      props_set(obj.props, 
        "add",
        property_data_non_enum(new_builtin_value(BuiltinFunction::AtomicsAdd)),
      )
      props_set(obj.props, 
        "and",
        property_data_non_enum(new_builtin_value(BuiltinFunction::AtomicsAnd)),
      )
      props_set(obj.props, 
        "or",
        property_data_non_enum(new_builtin_value(BuiltinFunction::AtomicsOr)),
      )
      props_set(obj.props, 
        "sub",
        property_data_non_enum(new_builtin_value(BuiltinFunction::AtomicsSub)),
      )
      props_set(obj.props, 
        "xor",
        property_data_non_enum(new_builtin_value(BuiltinFunction::AtomicsXor)),
      )
      props_set(obj.props, 
        "exchange",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::AtomicsExchange),
        ),
      )
      props_set(obj.props, 
        "compareExchange",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::AtomicsCompareExchange),
        ),
      )
      props_set(obj.props, 
        "load",
        property_data_non_enum(new_builtin_value(BuiltinFunction::AtomicsLoad)),
      )
      props_set(obj.props, 
        "store",
        property_data_non_enum(new_builtin_value(BuiltinFunction::AtomicsStore)),
      )
      props_set(obj.props, 
        "isLockFree",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::AtomicsIsLockFree),
        ),
      )
      props_set(obj.props, 
        "pause",
        property_data_non_enum(new_builtin_value(BuiltinFunction::AtomicsPause)),
      )
      props_set(obj.props, 
        "wait",
        property_data_non_enum(new_builtin_value(BuiltinFunction::AtomicsWait)),
      )
      props_set(obj.props, 
        "notify",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::AtomicsNotify),
        ),
      )
      match symbol_to_string_tag_key() {
        Some(key) =>
          props_set(obj.props, key, property_data_readonly(String("Atomics")))
        None => ()
      }
    }
    _ => ()
  }
  atomics_obj
}

///|
fn init_math_object(object_proto_obj : ObjectValue) -> Value {
  let math_obj = new_object_value_with_proto(Some(Object(object_proto_obj)))
  match math_obj {
    Object(obj) => {
      props_set(obj.props, "E", property_data_const(Number(2.718281828459045)))
      props_set(obj.props, "LN2", property_data_const(Number(0.6931471805599453)))
      props_set(obj.props, "LN10", property_data_const(Number(2.302585092994046)))
      props_set(obj.props, "LOG2E", property_data_const(Number(1.4426950408889634)))
      props_set(obj.props, "LOG10E", property_data_const(Number(0.4342944819032518)))
      props_set(obj.props, "PI", property_data_const(Number(@math.PI)))
      props_set(obj.props, "SQRT1_2", property_data_const(Number(0.7071067811865476)))
      props_set(obj.props, "SQRT2", property_data_const(Number(1.4142135623730951)))
      props_set(obj.props, 
        "abs",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathAbs)),
      )
      props_set(obj.props, 
        "acos",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathAcos)),
      )
      props_set(obj.props, 
        "acosh",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathAcosh)),
      )
      props_set(obj.props, 
        "asin",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathAsin)),
      )
      props_set(obj.props, 
        "asinh",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathAsinh)),
      )
      props_set(obj.props, 
        "atan",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathAtan)),
      )
      props_set(obj.props, 
        "atan2",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathAtan2)),
      )
      props_set(obj.props, 
        "atanh",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathAtanh)),
      )
      props_set(obj.props, 
        "cbrt",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathCbrt)),
      )
      props_set(obj.props, 
        "ceil",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathCeil)),
      )
      props_set(obj.props, 
        "clz32",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathClz32)),
      )
      props_set(obj.props, 
        "cos",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathCos)),
      )
      props_set(obj.props, 
        "cosh",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathCosh)),
      )
      props_set(obj.props, 
        "exp",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathExp)),
      )
      props_set(obj.props, 
        "expm1",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathExpm1)),
      )
      props_set(obj.props, 
        "floor",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathFloor)),
      )
      props_set(obj.props, 
        "fround",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathFround)),
      )
      props_set(obj.props, 
        "f16round",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathF16Round)),
      )
      props_set(obj.props, 
        "hypot",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathHypot)),
      )
      props_set(obj.props, 
        "imul",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathImul)),
      )
      props_set(obj.props, 
        "log",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathLog)),
      )
      props_set(obj.props, 
        "log1p",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathLog1p)),
      )
      props_set(obj.props, 
        "log2",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathLog2)),
      )
      props_set(obj.props, 
        "log10",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathLog10)),
      )
      props_set(obj.props, 
        "max",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathMax)),
      )
      props_set(obj.props, 
        "min",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathMin)),
      )
      props_set(obj.props, 
        "pow",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathPow)),
      )
      props_set(obj.props, 
        "random",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathRandom)),
      )
      props_set(obj.props, 
        "round",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathRound)),
      )
      props_set(obj.props, 
        "sign",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathSign)),
      )
      props_set(obj.props, 
        "sin",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathSin)),
      )
      props_set(obj.props, 
        "sinh",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathSinh)),
      )
      props_set(obj.props, 
        "sqrt",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathSqrt)),
      )
      props_set(obj.props, 
        "tan",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathTan)),
      )
      props_set(obj.props, 
        "tanh",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathTanh)),
      )
      props_set(obj.props, 
        "trunc",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathTrunc)),
      )
      props_set(obj.props, 
        "sumPrecise",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::MathSumPrecise),
        ),
      )
      match symbol_to_string_tag_key() {
        Some(key) => props_set(obj.props, key, property_data_readonly(String("Math")))
        None => ()
      }
    }
    _ => ()
  }
  math_obj
}
