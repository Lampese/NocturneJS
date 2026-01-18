///|
fn builtin_name_typedarray(func : BuiltinFunction) -> String {
  match func {
    BuiltinFunction::TypedArrayConstructor => "TypedArray"
    BuiltinFunction::TypedArrayFrom => "from"
    BuiltinFunction::TypedArrayOf => "of"
    BuiltinFunction::TypedArrayGetLength => "get length"
    BuiltinFunction::TypedArrayGetBuffer => "get buffer"
    BuiltinFunction::TypedArrayGetByteLength => "get byteLength"
    BuiltinFunction::TypedArrayGetByteOffset => "get byteOffset"
    BuiltinFunction::TypedArrayGetToStringTag => "get [Symbol.toStringTag]"
    BuiltinFunction::TypedArrayAt => "at"
    BuiltinFunction::TypedArrayWith => "with"
    BuiltinFunction::TypedArrayValues => "values"
    BuiltinFunction::TypedArrayKeys => "keys"
    BuiltinFunction::TypedArrayEntries => "entries"
    BuiltinFunction::TypedArrayCopyWithin => "copyWithin"
    BuiltinFunction::TypedArrayEvery => "every"
    BuiltinFunction::TypedArraySome => "some"
    BuiltinFunction::TypedArrayForEach => "forEach"
    BuiltinFunction::TypedArrayMap => "map"
    BuiltinFunction::TypedArrayFilter => "filter"
    BuiltinFunction::TypedArrayReduce => "reduce"
    BuiltinFunction::TypedArrayReduceRight => "reduceRight"
    BuiltinFunction::TypedArrayFind => "find"
    BuiltinFunction::TypedArrayFindIndex => "findIndex"
    BuiltinFunction::TypedArrayFindLast => "findLast"
    BuiltinFunction::TypedArrayFindLastIndex => "findLastIndex"
    BuiltinFunction::TypedArrayReverse => "reverse"
    BuiltinFunction::TypedArrayToReversed => "toReversed"
    BuiltinFunction::TypedArraySlice => "slice"
    BuiltinFunction::TypedArraySubarray => "subarray"
    BuiltinFunction::TypedArraySort => "sort"
    BuiltinFunction::TypedArrayToSorted => "toSorted"
    BuiltinFunction::TypedArrayJoin => "join"
    BuiltinFunction::TypedArrayToLocaleString => "toLocaleString"
    BuiltinFunction::TypedArrayIndexOf => "indexOf"
    BuiltinFunction::TypedArrayLastIndexOf => "lastIndexOf"
    BuiltinFunction::TypedArrayIncludes => "includes"
    BuiltinFunction::Uint8ArrayConstructor => "Uint8Array"
    BuiltinFunction::Int8ArrayConstructor => "Int8Array"
    BuiltinFunction::Uint8ClampedArrayConstructor => "Uint8ClampedArray"
    BuiltinFunction::Uint16ArrayConstructor => "Uint16Array"
    BuiltinFunction::Int16ArrayConstructor => "Int16Array"
    BuiltinFunction::Uint32ArrayConstructor => "Uint32Array"
    BuiltinFunction::Int32ArrayConstructor => "Int32Array"
    BuiltinFunction::BigInt64ArrayConstructor => "BigInt64Array"
    BuiltinFunction::BigUint64ArrayConstructor => "BigUint64Array"
    BuiltinFunction::Float16ArrayConstructor => "Float16Array"
    BuiltinFunction::Float32ArrayConstructor => "Float32Array"
    BuiltinFunction::Float64ArrayConstructor => "Float64Array"
    BuiltinFunction::TypedArraySet => "set"
    BuiltinFunction::TypedArrayFill => "fill"
    _ => ""
  }
}

///|
fn builtin_length_typedarray(func : BuiltinFunction) -> Int {
  match func {
    BuiltinFunction::TypedArrayConstructor => 0
    BuiltinFunction::TypedArrayFrom => 1
    BuiltinFunction::TypedArrayOf => 0
    BuiltinFunction::TypedArrayGetLength => 0
    BuiltinFunction::TypedArrayGetBuffer => 0
    BuiltinFunction::TypedArrayGetByteLength => 0
    BuiltinFunction::TypedArrayGetByteOffset => 0
    BuiltinFunction::TypedArrayGetToStringTag => 0
    BuiltinFunction::TypedArrayAt => 1
    BuiltinFunction::TypedArrayWith => 2
    BuiltinFunction::TypedArrayValues => 0
    BuiltinFunction::TypedArrayKeys => 0
    BuiltinFunction::TypedArrayEntries => 0
    BuiltinFunction::TypedArrayCopyWithin => 2
    BuiltinFunction::TypedArrayEvery => 1
    BuiltinFunction::TypedArraySome => 1
    BuiltinFunction::TypedArrayForEach => 1
    BuiltinFunction::TypedArrayMap => 1
    BuiltinFunction::TypedArrayFilter => 1
    BuiltinFunction::TypedArrayReduce => 1
    BuiltinFunction::TypedArrayReduceRight => 1
    BuiltinFunction::TypedArrayFind => 1
    BuiltinFunction::TypedArrayFindIndex => 1
    BuiltinFunction::TypedArrayFindLast => 1
    BuiltinFunction::TypedArrayFindLastIndex => 1
    BuiltinFunction::TypedArrayReverse => 0
    BuiltinFunction::TypedArrayToReversed => 0
    BuiltinFunction::TypedArraySlice => 2
    BuiltinFunction::TypedArraySubarray => 2
    BuiltinFunction::TypedArraySort => 1
    BuiltinFunction::TypedArrayToSorted => 1
    BuiltinFunction::TypedArrayJoin => 1
    BuiltinFunction::TypedArrayToLocaleString => 0
    BuiltinFunction::TypedArrayIndexOf => 1
    BuiltinFunction::TypedArrayLastIndexOf => 1
    BuiltinFunction::TypedArrayIncludes => 1
    BuiltinFunction::Uint8ArrayConstructor => 3
    BuiltinFunction::Int8ArrayConstructor => 3
    BuiltinFunction::Uint8ClampedArrayConstructor => 3
    BuiltinFunction::Uint16ArrayConstructor => 3
    BuiltinFunction::Int16ArrayConstructor => 3
    BuiltinFunction::Uint32ArrayConstructor => 3
    BuiltinFunction::Int32ArrayConstructor => 3
    BuiltinFunction::BigInt64ArrayConstructor => 3
    BuiltinFunction::BigUint64ArrayConstructor => 3
    BuiltinFunction::Float16ArrayConstructor => 3
    BuiltinFunction::Float32ArrayConstructor => 3
    BuiltinFunction::Float64ArrayConstructor => 3
    BuiltinFunction::TypedArraySet => 1
    BuiltinFunction::TypedArrayFill => 1
    _ => 0
  }
}
