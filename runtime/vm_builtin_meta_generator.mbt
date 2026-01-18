///|
fn builtin_name_generator(func : BuiltinFunction) -> String {
  match func {
    BuiltinFunction::GeneratorFunctionConstructor => "GeneratorFunction"
    BuiltinFunction::AsyncGeneratorFunctionConstructor =>
      "AsyncGeneratorFunction"
    BuiltinFunction::GeneratorNext => "next"
    BuiltinFunction::GeneratorReturn => "return"
    BuiltinFunction::GeneratorThrow => "throw"
    BuiltinFunction::AsyncGeneratorNext => "next"
    BuiltinFunction::AsyncGeneratorReturn => "return"
    BuiltinFunction::AsyncGeneratorThrow => "throw"
    BuiltinFunction::AsyncGeneratorResolveFunction => ""
    _ => ""
  }
}

///|
fn builtin_length_generator(func : BuiltinFunction) -> Int {
  match func {
    BuiltinFunction::GeneratorFunctionConstructor => 1
    BuiltinFunction::AsyncGeneratorFunctionConstructor => 1
    BuiltinFunction::GeneratorNext => 1
    BuiltinFunction::GeneratorReturn => 1
    BuiltinFunction::GeneratorThrow => 1
    BuiltinFunction::AsyncGeneratorNext => 1
    BuiltinFunction::AsyncGeneratorReturn => 1
    BuiltinFunction::AsyncGeneratorThrow => 1
    BuiltinFunction::AsyncGeneratorResolveFunction => 1
    _ => 0
  }
}
