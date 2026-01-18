///|
fn builtin_name_collection(func : BuiltinFunction) -> String {
  match func {
    BuiltinFunction::MapConstructor => "Map"
    BuiltinFunction::MapGroupBy => "groupBy"
    BuiltinFunction::MapGet => "get"
    BuiltinFunction::MapGetOrInsert => "getOrInsert"
    BuiltinFunction::MapGetOrInsertComputed => "getOrInsertComputed"
    BuiltinFunction::MapSet => "set"
    BuiltinFunction::MapHas => "has"
    BuiltinFunction::MapDelete => "delete"
    BuiltinFunction::MapClear => "clear"
    BuiltinFunction::MapForEach => "forEach"
    BuiltinFunction::MapSize => "get size"
    BuiltinFunction::MapKeys => "keys"
    BuiltinFunction::MapValues => "values"
    BuiltinFunction::MapEntries => "entries"
    BuiltinFunction::MapIteratorNext => "next"
    BuiltinFunction::SetConstructor => "Set"
    BuiltinFunction::SetAdd => "add"
    BuiltinFunction::SetHas => "has"
    BuiltinFunction::SetDelete => "delete"
    BuiltinFunction::SetClear => "clear"
    BuiltinFunction::SetForEach => "forEach"
    BuiltinFunction::SetSize => "get size"
    BuiltinFunction::SetValues => "values"
    BuiltinFunction::SetEntries => "entries"
    BuiltinFunction::SetIsDisjointFrom => "isDisjointFrom"
    BuiltinFunction::SetIsSubsetOf => "isSubsetOf"
    BuiltinFunction::SetIsSupersetOf => "isSupersetOf"
    BuiltinFunction::SetIntersection => "intersection"
    BuiltinFunction::SetDifference => "difference"
    BuiltinFunction::SetSymmetricDifference => "symmetricDifference"
    BuiltinFunction::SetUnion => "union"
    BuiltinFunction::SetIteratorNext => "next"
    BuiltinFunction::WeakMapConstructor => "WeakMap"
    BuiltinFunction::WeakMapGet => "get"
    BuiltinFunction::WeakMapGetOrInsert => "getOrInsert"
    BuiltinFunction::WeakMapGetOrInsertComputed => "getOrInsertComputed"
    BuiltinFunction::WeakMapSet => "set"
    BuiltinFunction::WeakMapHas => "has"
    BuiltinFunction::WeakMapDelete => "delete"
    BuiltinFunction::WeakSetConstructor => "WeakSet"
    BuiltinFunction::WeakSetAdd => "add"
    BuiltinFunction::WeakSetHas => "has"
    BuiltinFunction::WeakSetDelete => "delete"
    BuiltinFunction::WeakRefConstructor => "WeakRef"
    BuiltinFunction::WeakRefDeref => "deref"
    BuiltinFunction::FinalizationRegistryConstructor => "FinalizationRegistry"
    BuiltinFunction::FinalizationRegistryRegister => "register"
    BuiltinFunction::FinalizationRegistryUnregister => "unregister"
    _ => ""
  }
}

///|
fn builtin_length_collection(func : BuiltinFunction) -> Int {
  match func {
    BuiltinFunction::MapConstructor => 0
    BuiltinFunction::MapGroupBy => 2
    BuiltinFunction::MapGet => 1
    BuiltinFunction::MapGetOrInsert => 2
    BuiltinFunction::MapGetOrInsertComputed => 2
    BuiltinFunction::MapSet => 2
    BuiltinFunction::MapHas => 1
    BuiltinFunction::MapDelete => 1
    BuiltinFunction::MapClear => 0
    BuiltinFunction::MapForEach => 1
    BuiltinFunction::MapSize => 0
    BuiltinFunction::MapKeys => 0
    BuiltinFunction::MapValues => 0
    BuiltinFunction::MapEntries => 0
    BuiltinFunction::MapIteratorNext => 0
    BuiltinFunction::SetConstructor => 0
    BuiltinFunction::SetAdd => 1
    BuiltinFunction::SetHas => 1
    BuiltinFunction::SetDelete => 1
    BuiltinFunction::SetClear => 0
    BuiltinFunction::SetForEach => 1
    BuiltinFunction::SetSize => 0
    BuiltinFunction::SetValues => 0
    BuiltinFunction::SetEntries => 0
    BuiltinFunction::SetIsDisjointFrom => 1
    BuiltinFunction::SetIsSubsetOf => 1
    BuiltinFunction::SetIsSupersetOf => 1
    BuiltinFunction::SetIntersection => 1
    BuiltinFunction::SetDifference => 1
    BuiltinFunction::SetSymmetricDifference => 1
    BuiltinFunction::SetUnion => 1
    BuiltinFunction::SetIteratorNext => 0
    BuiltinFunction::WeakMapConstructor => 0
    BuiltinFunction::WeakMapGet => 1
    BuiltinFunction::WeakMapGetOrInsert => 2
    BuiltinFunction::WeakMapGetOrInsertComputed => 2
    BuiltinFunction::WeakMapSet => 2
    BuiltinFunction::WeakMapHas => 1
    BuiltinFunction::WeakMapDelete => 1
    BuiltinFunction::WeakSetConstructor => 0
    BuiltinFunction::WeakSetAdd => 1
    BuiltinFunction::WeakSetHas => 1
    BuiltinFunction::WeakSetDelete => 1
    BuiltinFunction::WeakRefConstructor => 1
    BuiltinFunction::WeakRefDeref => 0
    BuiltinFunction::FinalizationRegistryConstructor => 1
    BuiltinFunction::FinalizationRegistryRegister => 2
    BuiltinFunction::FinalizationRegistryUnregister => 1
    _ => 0
  }
}
