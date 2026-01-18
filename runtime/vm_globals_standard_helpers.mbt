///|
fn init_json_object(object_proto_obj : ObjectValue) -> Value {
  let json_obj = new_object_value_with_proto(Some(Object(object_proto_obj)))
  match json_obj {
    Object(obj) => {
      obj.props.set(
        "stringify",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::JsonStringify),
        ),
      )
      obj.props.set(
        "parse",
        property_data_non_enum(new_builtin_value(BuiltinFunction::JsonParse)),
      )
      match symbol_to_string_tag_key() {
        Some(key) => obj.props.set(key, property_data_readonly(String("JSON")))
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
      obj.props.set(
        "apply",
        property_data_non_enum(new_builtin_value(BuiltinFunction::ReflectApply)),
      )
      obj.props.set(
        "construct",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::ReflectConstruct),
        ),
      )
      obj.props.set(
        "get",
        property_data_non_enum(new_builtin_value(BuiltinFunction::ReflectGet)),
      )
      obj.props.set(
        "getOwnPropertyDescriptor",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::ReflectGetOwnPropertyDescriptor),
        ),
      )
      obj.props.set(
        "getPrototypeOf",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::ReflectGetPrototypeOf),
        ),
      )
      obj.props.set(
        "defineProperty",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::ReflectDefineProperty),
        ),
      )
      obj.props.set(
        "has",
        property_data_non_enum(new_builtin_value(BuiltinFunction::ReflectHas)),
      )
      obj.props.set(
        "isExtensible",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::ReflectIsExtensible),
        ),
      )
      obj.props.set(
        "set",
        property_data_non_enum(new_builtin_value(BuiltinFunction::ReflectSet)),
      )
      obj.props.set(
        "setPrototypeOf",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::ReflectSetPrototypeOf),
        ),
      )
      obj.props.set(
        "deleteProperty",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::ReflectDeleteProperty),
        ),
      )
      obj.props.set(
        "preventExtensions",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::ReflectPreventExtensions),
        ),
      )
      obj.props.set(
        "ownKeys",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::ReflectOwnKeys),
        ),
      )
      match symbol_to_string_tag_key() {
        Some(key) =>
          obj.props.set(key, property_data_readonly(String("Reflect")))
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
      obj.props.set(
        "add",
        property_data_non_enum(new_builtin_value(BuiltinFunction::AtomicsAdd)),
      )
      obj.props.set(
        "and",
        property_data_non_enum(new_builtin_value(BuiltinFunction::AtomicsAnd)),
      )
      obj.props.set(
        "or",
        property_data_non_enum(new_builtin_value(BuiltinFunction::AtomicsOr)),
      )
      obj.props.set(
        "sub",
        property_data_non_enum(new_builtin_value(BuiltinFunction::AtomicsSub)),
      )
      obj.props.set(
        "xor",
        property_data_non_enum(new_builtin_value(BuiltinFunction::AtomicsXor)),
      )
      obj.props.set(
        "exchange",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::AtomicsExchange),
        ),
      )
      obj.props.set(
        "compareExchange",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::AtomicsCompareExchange),
        ),
      )
      obj.props.set(
        "load",
        property_data_non_enum(new_builtin_value(BuiltinFunction::AtomicsLoad)),
      )
      obj.props.set(
        "store",
        property_data_non_enum(new_builtin_value(BuiltinFunction::AtomicsStore)),
      )
      obj.props.set(
        "isLockFree",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::AtomicsIsLockFree),
        ),
      )
      obj.props.set(
        "pause",
        property_data_non_enum(new_builtin_value(BuiltinFunction::AtomicsPause)),
      )
      obj.props.set(
        "wait",
        property_data_non_enum(new_builtin_value(BuiltinFunction::AtomicsWait)),
      )
      obj.props.set(
        "notify",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::AtomicsNotify),
        ),
      )
      match symbol_to_string_tag_key() {
        Some(key) =>
          obj.props.set(key, property_data_readonly(String("Atomics")))
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
      obj.props.set("E", property_data_const(Number(2.718281828459045)))
      obj.props.set("LN2", property_data_const(Number(0.6931471805599453)))
      obj.props.set("LN10", property_data_const(Number(2.302585092994046)))
      obj.props.set("LOG2E", property_data_const(Number(1.4426950408889634)))
      obj.props.set("LOG10E", property_data_const(Number(0.4342944819032518)))
      obj.props.set("PI", property_data_const(Number(@math.PI)))
      obj.props.set("SQRT1_2", property_data_const(Number(0.7071067811865476)))
      obj.props.set("SQRT2", property_data_const(Number(1.4142135623730951)))
      obj.props.set(
        "abs",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathAbs)),
      )
      obj.props.set(
        "acos",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathAcos)),
      )
      obj.props.set(
        "acosh",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathAcosh)),
      )
      obj.props.set(
        "asin",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathAsin)),
      )
      obj.props.set(
        "asinh",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathAsinh)),
      )
      obj.props.set(
        "atan",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathAtan)),
      )
      obj.props.set(
        "atan2",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathAtan2)),
      )
      obj.props.set(
        "atanh",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathAtanh)),
      )
      obj.props.set(
        "cbrt",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathCbrt)),
      )
      obj.props.set(
        "ceil",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathCeil)),
      )
      obj.props.set(
        "clz32",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathClz32)),
      )
      obj.props.set(
        "cos",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathCos)),
      )
      obj.props.set(
        "cosh",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathCosh)),
      )
      obj.props.set(
        "exp",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathExp)),
      )
      obj.props.set(
        "expm1",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathExpm1)),
      )
      obj.props.set(
        "floor",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathFloor)),
      )
      obj.props.set(
        "fround",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathFround)),
      )
      obj.props.set(
        "f16round",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathF16Round)),
      )
      obj.props.set(
        "hypot",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathHypot)),
      )
      obj.props.set(
        "imul",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathImul)),
      )
      obj.props.set(
        "log",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathLog)),
      )
      obj.props.set(
        "log1p",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathLog1p)),
      )
      obj.props.set(
        "log2",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathLog2)),
      )
      obj.props.set(
        "log10",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathLog10)),
      )
      obj.props.set(
        "max",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathMax)),
      )
      obj.props.set(
        "min",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathMin)),
      )
      obj.props.set(
        "pow",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathPow)),
      )
      obj.props.set(
        "random",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathRandom)),
      )
      obj.props.set(
        "round",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathRound)),
      )
      obj.props.set(
        "sign",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathSign)),
      )
      obj.props.set(
        "sin",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathSin)),
      )
      obj.props.set(
        "sinh",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathSinh)),
      )
      obj.props.set(
        "sqrt",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathSqrt)),
      )
      obj.props.set(
        "tan",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathTan)),
      )
      obj.props.set(
        "tanh",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathTanh)),
      )
      obj.props.set(
        "trunc",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MathTrunc)),
      )
      obj.props.set(
        "sumPrecise",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::MathSumPrecise),
        ),
      )
      match symbol_to_string_tag_key() {
        Some(key) => obj.props.set(key, property_data_readonly(String("Math")))
        None => ()
      }
    }
    _ => ()
  }
  math_obj
}
