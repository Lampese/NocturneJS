///|
fn builtin_name_object(func : BuiltinFunction) -> String {
  match func {
    BuiltinFunction::ObjectConstructor => "Object"
    BuiltinFunction::ProxyConstructor => "Proxy"
    BuiltinFunction::ProxyRevocable => "revocable"
    BuiltinFunction::ProxyRevoke => "revoke"
    BuiltinFunction::ObjectDefineProperty => "defineProperty"
    BuiltinFunction::ObjectDefineProperties => "defineProperties"
    BuiltinFunction::ObjectGetOwnPropertyDescriptor =>
      "getOwnPropertyDescriptor"
    BuiltinFunction::ObjectGetOwnPropertyNames => "getOwnPropertyNames"
    BuiltinFunction::ObjectGetOwnPropertySymbols => "getOwnPropertySymbols"
    BuiltinFunction::ObjectGetPrototypeOf => "getPrototypeOf"
    BuiltinFunction::ObjectSetPrototypeOf => "setPrototypeOf"
    BuiltinFunction::ObjectCreate => "create"
    BuiltinFunction::ObjectKeys => "keys"
    BuiltinFunction::ObjectValues => "values"
    BuiltinFunction::ObjectEntries => "entries"
    BuiltinFunction::ObjectGroupBy => "groupBy"
    BuiltinFunction::ObjectIs => "is"
    BuiltinFunction::ObjectIsExtensible => "isExtensible"
    BuiltinFunction::ObjectPreventExtensions => "preventExtensions"
    BuiltinFunction::ObjectSeal => "seal"
    BuiltinFunction::ObjectIsSealed => "isSealed"
    BuiltinFunction::ObjectIsFrozen => "isFrozen"
    BuiltinFunction::ObjectFreeze => "freeze"
    BuiltinFunction::ObjectGetOwnPropertyDescriptors =>
      "getOwnPropertyDescriptors"
    BuiltinFunction::ObjectFromEntries => "fromEntries"
    BuiltinFunction::ObjectAssign => "assign"
    BuiltinFunction::ObjectHasOwn => "hasOwn"
    BuiltinFunction::ObjectToString => "toString"
    BuiltinFunction::ObjectToLocaleString => "toLocaleString"
    BuiltinFunction::ObjectValueOf => "valueOf"
    BuiltinFunction::ObjectHasOwnProperty => "hasOwnProperty"
    BuiltinFunction::ObjectPropertyIsEnumerable => "propertyIsEnumerable"
    BuiltinFunction::ObjectIsPrototypeOf => "isPrototypeOf"
    BuiltinFunction::ObjectProtoGet => "get __proto__"
    BuiltinFunction::ObjectProtoSet => "set __proto__"
    BuiltinFunction::ObjectDefineGetter => "__defineGetter__"
    BuiltinFunction::ObjectDefineSetter => "__defineSetter__"
    BuiltinFunction::ObjectLookupGetter => "__lookupGetter__"
    BuiltinFunction::ObjectLookupSetter => "__lookupSetter__"
    BuiltinFunction::ReflectApply => "apply"
    BuiltinFunction::ReflectConstruct => "construct"
    BuiltinFunction::ReflectGet => "get"
    BuiltinFunction::ReflectGetOwnPropertyDescriptor =>
      "getOwnPropertyDescriptor"
    BuiltinFunction::ReflectGetPrototypeOf => "getPrototypeOf"
    BuiltinFunction::ReflectDefineProperty => "defineProperty"
    BuiltinFunction::ReflectHas => "has"
    BuiltinFunction::ReflectIsExtensible => "isExtensible"
    BuiltinFunction::ReflectSet => "set"
    BuiltinFunction::ReflectSetPrototypeOf => "setPrototypeOf"
    BuiltinFunction::ReflectDeleteProperty => "deleteProperty"
    BuiltinFunction::ReflectPreventExtensions => "preventExtensions"
    BuiltinFunction::ReflectOwnKeys => "ownKeys"
    _ => ""
  }
}

///|
fn builtin_length_object(func : BuiltinFunction) -> Int {
  match func {
    BuiltinFunction::ObjectConstructor => 1
    BuiltinFunction::ProxyConstructor => 2
    BuiltinFunction::ProxyRevocable => 2
    BuiltinFunction::ProxyRevoke => 0
    BuiltinFunction::ObjectDefineProperty => 3
    BuiltinFunction::ObjectDefineProperties => 2
    BuiltinFunction::ObjectGetOwnPropertyDescriptor => 2
    BuiltinFunction::ObjectGetOwnPropertyNames => 1
    BuiltinFunction::ObjectGetOwnPropertySymbols => 1
    BuiltinFunction::ObjectGetPrototypeOf => 1
    BuiltinFunction::ObjectSetPrototypeOf => 2
    BuiltinFunction::ObjectCreate => 2
    BuiltinFunction::ObjectKeys => 1
    BuiltinFunction::ObjectValues => 1
    BuiltinFunction::ObjectEntries => 1
    BuiltinFunction::ObjectGroupBy => 2
    BuiltinFunction::ObjectIs => 2
    BuiltinFunction::ObjectIsExtensible => 1
    BuiltinFunction::ObjectPreventExtensions => 1
    BuiltinFunction::ObjectSeal => 1
    BuiltinFunction::ObjectIsSealed => 1
    BuiltinFunction::ObjectIsFrozen => 1
    BuiltinFunction::ObjectFreeze => 1
    BuiltinFunction::ObjectGetOwnPropertyDescriptors => 1
    BuiltinFunction::ObjectFromEntries => 1
    BuiltinFunction::ObjectAssign => 2
    BuiltinFunction::ObjectHasOwn => 2
    BuiltinFunction::ObjectToString => 0
    BuiltinFunction::ObjectToLocaleString => 0
    BuiltinFunction::ObjectValueOf => 0
    BuiltinFunction::ObjectHasOwnProperty => 1
    BuiltinFunction::ObjectPropertyIsEnumerable => 1
    BuiltinFunction::ObjectIsPrototypeOf => 1
    BuiltinFunction::ObjectProtoGet => 0
    BuiltinFunction::ObjectProtoSet => 1
    BuiltinFunction::ObjectDefineGetter => 2
    BuiltinFunction::ObjectDefineSetter => 2
    BuiltinFunction::ObjectLookupGetter => 1
    BuiltinFunction::ObjectLookupSetter => 1
    BuiltinFunction::ReflectApply => 3
    BuiltinFunction::ReflectConstruct => 2
    BuiltinFunction::ReflectGet => 2
    BuiltinFunction::ReflectGetOwnPropertyDescriptor => 2
    BuiltinFunction::ReflectGetPrototypeOf => 1
    BuiltinFunction::ReflectDefineProperty => 3
    BuiltinFunction::ReflectHas => 2
    BuiltinFunction::ReflectIsExtensible => 1
    BuiltinFunction::ReflectSet => 3
    BuiltinFunction::ReflectSetPrototypeOf => 2
    BuiltinFunction::ReflectDeleteProperty => 2
    BuiltinFunction::ReflectPreventExtensions => 1
    BuiltinFunction::ReflectOwnKeys => 1
    _ => 0
  }
}
