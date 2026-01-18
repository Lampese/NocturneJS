///|
fn builtin_name_iterator(func : BuiltinFunction) -> String {
  match func {
    BuiltinFunction::IteratorReturnSelf => "[Symbol.iterator]"
    BuiltinFunction::AsyncIteratorReturnSelf => "[Symbol.asyncIterator]"
    BuiltinFunction::AsyncFromSyncIteratorNext => "next"
    BuiltinFunction::AsyncFromSyncIteratorReturn => "return"
    BuiltinFunction::AsyncFromSyncIteratorThrow => "throw"
    BuiltinFunction::AsyncFromSyncIteratorUnwrap => ""
    BuiltinFunction::AsyncFromSyncIteratorCloseWrap => ""
    BuiltinFunction::IteratorConstructor => "Iterator"
    BuiltinFunction::IteratorFrom => "from"
    BuiltinFunction::IteratorConcat => "concat"
    BuiltinFunction::IteratorConcatNext => "next"
    BuiltinFunction::IteratorConcatReturn => "return"
    BuiltinFunction::IteratorWrapNext => "next"
    BuiltinFunction::IteratorWrapReturn => "return"
    BuiltinFunction::IteratorHelperNext => "next"
    BuiltinFunction::IteratorHelperReturn => "return"
    BuiltinFunction::IteratorDrop => "drop"
    BuiltinFunction::IteratorFilter => "filter"
    BuiltinFunction::IteratorFlatMap => "flatMap"
    BuiltinFunction::IteratorMap => "map"
    BuiltinFunction::IteratorTake => "take"
    BuiltinFunction::IteratorEvery => "every"
    BuiltinFunction::IteratorFind => "find"
    BuiltinFunction::IteratorForEach => "forEach"
    BuiltinFunction::IteratorSome => "some"
    BuiltinFunction::IteratorReduce => "reduce"
    BuiltinFunction::IteratorToArray => "toArray"
    BuiltinFunction::IteratorConstructorGetter => "get constructor"
    BuiltinFunction::IteratorConstructorSetter => "set constructor"
    BuiltinFunction::IteratorToStringTagGetter => "get [Symbol.toStringTag]"
    BuiltinFunction::IteratorToStringTagSetter => "set [Symbol.toStringTag]"
    _ => ""
  }
}

///|
fn builtin_length_iterator(func : BuiltinFunction) -> Int {
  match func {
    BuiltinFunction::IteratorReturnSelf => 0
    BuiltinFunction::AsyncIteratorReturnSelf => 0
    BuiltinFunction::AsyncFromSyncIteratorNext => 1
    BuiltinFunction::AsyncFromSyncIteratorReturn => 1
    BuiltinFunction::AsyncFromSyncIteratorThrow => 1
    BuiltinFunction::AsyncFromSyncIteratorUnwrap => 1
    BuiltinFunction::AsyncFromSyncIteratorCloseWrap => 1
    BuiltinFunction::IteratorConstructor => 0
    BuiltinFunction::IteratorFrom => 1
    BuiltinFunction::IteratorConcat => 0
    BuiltinFunction::IteratorConcatNext => 0
    BuiltinFunction::IteratorConcatReturn => 0
    BuiltinFunction::IteratorWrapNext => 0
    BuiltinFunction::IteratorWrapReturn => 0
    BuiltinFunction::IteratorHelperNext => 0
    BuiltinFunction::IteratorHelperReturn => 0
    BuiltinFunction::IteratorDrop => 1
    BuiltinFunction::IteratorFilter => 1
    BuiltinFunction::IteratorFlatMap => 1
    BuiltinFunction::IteratorMap => 1
    BuiltinFunction::IteratorTake => 1
    BuiltinFunction::IteratorEvery => 1
    BuiltinFunction::IteratorFind => 1
    BuiltinFunction::IteratorForEach => 1
    BuiltinFunction::IteratorSome => 1
    BuiltinFunction::IteratorReduce => 1
    BuiltinFunction::IteratorToArray => 0
    BuiltinFunction::IteratorConstructorGetter => 0
    BuiltinFunction::IteratorConstructorSetter => 1
    BuiltinFunction::IteratorToStringTagGetter => 0
    BuiltinFunction::IteratorToStringTagSetter => 1
    _ => 0
  }
}
