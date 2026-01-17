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
pub struct ClassFieldRecord {
  is_private : Bool
  name : String
  display_name : String
  init : @engine.Expr?
}

///|
pub struct FunctionValue {
  name : String?
  mut source : String?
  source_root : String?
  source_path : String?
  line_number : Int?
  column_number : Int?
  local_name_binding : Bool
  params : Array[@engine.ParamBinding]
  param_inits : Array[@engine.Expr?]
  body : Array[@engine.Stmt]
  env : Env
  id : Int
  props : Map[String, Property]
  mut proto : Value?
  mut home_object : Value?
  mut super_constructor : Value?
  mut instance_fields : Array[ClassFieldRecord]
  private_instance_fields : Array[(String, @engine.Expr?)]
  mut private_instance_props : Map[String, Property]
  mut is_constructor : Bool
  mut is_class_constructor : Bool
  mut is_default_constructor : Bool
  mut extensible : Bool
  is_strict : Bool
  is_generator : Bool
  is_async : Bool
  is_arrow : Bool
  mut in_class_field_init : Bool
}

///|
pub impl Show for FunctionValue with output(self, logger) {
  let label = match self.name {
    Some(name) => "function \{name}"
    None => "function"
  }
  logger.write_string(label)
}

///|
pub impl ToJson for FunctionValue with to_json(self) {
  let label = match self.name {
    Some(name) => "function \{name}"
    None => "function"
  }
  Json::string(label)
}

///|
enum TypedArrayKind {
  Uint8
  Int8
  Uint16
  Int16
  Uint32
  Int32
  BigInt64
  BigUint64
  Float16
  Float32
  Float64
  Uint8Clamped
} derive(Eq, Show, ToJson)

///|
struct ArrayBufferData {
  mut bytes : Array[Byte]
  mut detached : Bool
  shared : Bool
  max_byte_length : Int
  resizable : Bool
}

///|
struct DataViewData {
  buffer : ObjectValue
  byte_offset : Int
  byte_length : Int
  length_tracking : Bool
}

///|
struct ProxyData {
  mut target : Value
  mut handler : Value
  callable : Bool
  constructable : Bool
}

///|
pub struct ObjectValue {
  id : Int
  props : Map[String, Property]
  mut proto : Value?
  string_data : String?
  number_data : Double?
  bool_data : Bool?
  bigint_data : @bigint.BigInt?
  symbol_data : SymbolValue?
  mut regexp_data : RegexpData?
  mut generator_data : GeneratorData?
  mut async_generator_data : AsyncGeneratorData?
  mut proxy_data : ProxyData?
  mut map_data : MapData?
  mut set_data : SetData?
  mut map_iter_data : MapIteratorData?
  mut set_iter_data : SetIteratorData?
  mut iterator_wrap_data : IteratorWrapData?
  mut iterator_concat_data : IteratorConcatData?
  mut iterator_helper_data : IteratorHelperData?
  mut weakmap_data : WeakMapData?
  mut weakset_data : WeakSetData?
  mut weakref_data : WeakRefData?
  mut finreg_data : FinalizationRegistryData?
  mut date_data : Double?
  mut array_buffer_data : ArrayBufferData?
  mut dataview_data : DataViewData?
  is_html_dda : Bool
  mut is_error : Bool
  mut is_module_namespace : Bool
  mut realm_env : Env?
  mut immutable_proto : Bool
  mut extensible : Bool
}

///|
pub struct SymbolValue {
  id : Int
  description : String?
  key : String?
} derive(Eq, Show, ToJson)

///|
struct TypedArrayData {
  kind : TypedArrayKind
  buffer : ObjectValue
  byte_offset : Int
  length : Int
  length_tracking : Bool
  bytes_per_element : Int
}

///|
priv enum GenControl {
  Normal(Value)
  Return(Value)
  Break(String?, Value?)
  Continue(String?, Value?)
  Throw(Value)
}

///|
priv enum GenResume {
  Next(Value)
  Return(Value)
  Throw(Value)
}

///|
priv enum GenStep {
  Yield(Value, (GenResume) -> GenStep raise)
  Await(Value, (GenResume) -> GenStep raise)
  Done(GenControl)
}

///|
priv enum GeneratorState {
  SuspendedStart
  SuspendedYield
  Executing
  Completed
}

///|
priv enum AsyncGeneratorState {
  SuspendedStart
  SuspendedYield
  Executing
  AwaitingReturn
  Completed
}

///|
priv struct AsyncGeneratorRequest {
  resume_state : GenResume
  promise : Value
  resolve : Value
  reject : Value
}

///|
struct AsyncGeneratorData {
  mut state : AsyncGeneratorState
  mut queue_index : Int
  queue : Array[AsyncGeneratorRequest]
}

///|
struct GeneratorData {
  mut state : GeneratorState
  env : Env
  func : FunctionValue
  mut next_step : (GenResume) -> GenStep raise
  roots : Array[Value]
}

///|
pub struct MapData {
  entries : Array[(Value, Value)?]
}

///|
pub struct WeakMapData {
  entries : Array[(Value, Value)]
}

///|
pub struct MapIteratorData {
  entries : Array[(Value, Value)?]
  mut index : Int
  kind : Int
  mut done : Bool
}

///|
pub struct SetData {
  entries : Array[Value?]
}

///|
pub struct SetIteratorData {
  entries : Array[Value?]
  mut index : Int
  kind : Int
  mut done : Bool
}

///|
pub struct IteratorWrapData {
  wrapped_iter : Value
  wrapped_next : Value
}

///|
pub struct IteratorConcatData {
  mut index : Int
  count : Int
  mut running : Bool
  mut iter : Value
  mut next : Value
  values : Array[(Value, Value)]
}

///|
pub struct IteratorHelperData {
  obj : Value
  next : Value
  func : Value
  mut inner : Value
  mut inner_next : Value
  mut count : Int64
  kind : Int
  mut executing : Bool
  mut done : Bool
}

///|
pub struct WeakSetData {
  entries : Array[Value]
}

///|
pub struct WeakRefData {
  target : Value
}

///|
pub struct FinalizationEntry {
  target : Value
  held_value : Value
  mut token : Value
}

///|
pub struct FinalizationRegistryData {
  callback : Value
  entries : Array[FinalizationEntry]
  realm_env : Env
}

///|
pub struct RegexpData {
  source : String
  flags : String
  regex : RegexpProgram
  global : Bool
  sticky : Bool
  unicode : Bool
  dot_all : Bool
  ignore_case : Bool
  multiline : Bool
  has_indices : Bool
  unicode_sets : Bool
}

///|
pub struct BoundFunctionValue {
  id : Int
  mut target : Value
  mut bound_this : Value
  bound_args : Array[Value]
  props : Map[String, Property]
  mut proto : Value?
  is_constructor : Bool
  mut extensible : Bool
}

///|
pub impl Show for BoundFunctionValue with output(self, logger) {
  let _ = self
  logger.write_string("function")
}

///|
pub impl ToJson for BoundFunctionValue with to_json(self) {
  let _ = self
  Json::string("function")
}

///|
pub impl Show for ObjectValue with output(self, logger) {
  let _ = self
  logger.write_string("[object Object]")
}

///|
pub impl ToJson for ObjectValue with to_json(self) {
  let _ = self
  Json::string("[object Object]")
}

///|
pub struct BuiltinValue {
  kind : BuiltinFunction
  id : Int
  props : Map[String, Property]
  mut this_value : Value?
  mut proto : Value?
  mut extensible : Bool
  mut is_html_dda : Bool
  mut realm_env : Env?
}

///|
pub impl Show for BuiltinValue with output(self, logger) {
  let _ = self
  logger.write_string("function")
}

///|
pub impl ToJson for BuiltinValue with to_json(self) {
  let _ = self
  Json::string("function")
}

///|
pub enum BuiltinFunction {
  JsonStringify
  JsonParse
  BjsonRead
  BjsonWrite
  ObjectConstructor
  ObjectDefineProperty
  ObjectDefineProperties
  ObjectGetOwnPropertyDescriptor
  ObjectGetOwnPropertyNames
  ObjectGetOwnPropertySymbols
  ObjectGetPrototypeOf
  ObjectSetPrototypeOf
  ObjectCreate
  ObjectKeys
  ObjectValues
  ObjectEntries
  ObjectIs
  ObjectIsExtensible
  ObjectPreventExtensions
  ObjectSeal
  ObjectIsSealed
  ObjectIsFrozen
  ObjectFreeze
  ObjectGetOwnPropertyDescriptors
  ObjectFromEntries
  ObjectGroupBy
  ObjectAssign
  ObjectHasOwn
  ObjectToString
  ObjectToLocaleString
  ObjectValueOf
  ObjectHasOwnProperty
  ObjectPropertyIsEnumerable
  ObjectIsPrototypeOf
  ObjectProtoGet
  ObjectProtoSet
  ObjectDefineGetter
  ObjectDefineSetter
  ObjectLookupGetter
  ObjectLookupSetter
  ProxyConstructor
  ProxyRevocable
  ProxyRevoke
  ArrayToString
  ArrayToLocaleString
  ArrayConstructor
  ArrayIsArray
  ArrayFrom
  ArrayOf
  ArrayAt
  ArrayPush
  ArrayPop
  ArrayShift
  ArrayUnshift
  ArraySplice
  ArraySlice
  ArrayReverse
  ArrayConcat
  ArrayJoin
  ArrayIndexOf
  ArrayLastIndexOf
  ArrayIncludes
  ArrayCopyWithin
  ArrayFill
  ArrayFind
  ArrayFindIndex
  ArrayFindLast
  ArrayFindLastIndex
  ArrayFlat
  ArrayFlatMap
  ArrayToReversed
  ArrayToSorted
  ArrayToSpliced
  ArrayWith
  ArrayMap
  ArrayForEach
  ArrayFilter
  ArrayEvery
  ArraySome
  ArrayReduce
  ArrayReduceRight
  ArraySort
  ArrayValues
  ArrayKeys
  ArrayEntries
  ArrayIteratorNext
  IteratorReturnSelf
  AsyncIteratorReturnSelf
  AsyncFromSyncIteratorNext
  AsyncFromSyncIteratorReturn
  AsyncFromSyncIteratorThrow
  AsyncFromSyncIteratorUnwrap
  AsyncFromSyncIteratorCloseWrap
  IteratorConstructor
  IteratorFrom
  IteratorConcat
  IteratorConcatNext
  IteratorConcatReturn
  IteratorWrapNext
  IteratorWrapReturn
  IteratorHelperNext
  IteratorHelperReturn
  IteratorDrop
  IteratorFilter
  IteratorFlatMap
  IteratorMap
  IteratorTake
  IteratorEvery
  IteratorFind
  IteratorForEach
  IteratorSome
  IteratorReduce
  IteratorToArray
  IteratorConstructorGetter
  IteratorConstructorSetter
  IteratorToStringTagGetter
  IteratorToStringTagSetter
  FunctionCall
  FunctionApply
  FunctionBind
  FunctionToString
  FunctionProtoLineNumber
  FunctionProtoColumnNumber
  FunctionSymbolHasInstance
  FunctionConstructor
  GeneratorFunctionConstructor
  AsyncFunctionConstructor
  AsyncGeneratorFunctionConstructor
  BooleanConstructor
  BooleanToString
  BooleanValueOf
  StringConstructor
  StringFromCharCode
  StringFromCodePoint
  StringRaw
  StringCharCodeAt
  StringCharAt
  StringCodePointAt
  StringConcat
  StringSubstring
  StringSubstr
  StringSlice
  StringIndexOf
  StringLastIndexOf
  StringSplit
  StringPadStart
  StringReplace
  StringReplaceAll
  StringToString
  StringValueOf
  StringAt
  StringIncludes
  StringEndsWith
  StringStartsWith
  StringRepeat
  StringPadEnd
  StringTrim
  StringTrimStart
  StringTrimEnd
  StringToLowerCase
  StringToUpperCase
  StringToLocaleLowerCase
  StringToLocaleUpperCase
  StringLocaleCompare
  StringNormalize
  StringIsWellFormed
  StringToWellFormed
  StringMatch
  StringMatchAll
  StringSearch
  StringIterator
  StringIteratorNext
  StringAnchor
  StringBig
  StringBlink
  StringBold
  StringFixed
  StringFontcolor
  StringFontsize
  StringItalics
  StringLink
  StringSmall
  StringStrike
  StringSub
  StringSup
  NumberConstructor
  NumberIsNaN
  NumberIsFinite
  NumberIsInteger
  NumberIsSafeInteger
  NumberToString
  NumberToLocaleString
  NumberToFixed
  NumberToExponential
  NumberToPrecision
  NumberValueOf
  BigIntConstructor
  BigIntToString
  BigIntValueOf
  BigIntAsIntN
  BigIntAsUintN
  SymbolConstructor
  SymbolFor
  SymbolKeyFor
  SymbolToString
  SymbolValueOf
  SymbolToPrimitive
  SymbolDescription
  ParseInt
  ParseFloat
  DecodeURI
  DecodeURIComponent
  EncodeURI
  EncodeURIComponent
  Escape
  Unescape
  MathAbs
  MathAcos
  MathAcosh
  MathAsin
  MathAsinh
  MathAtan
  MathAtan2
  MathAtanh
  MathCbrt
  MathCeil
  MathClz32
  MathCos
  MathCosh
  MathExp
  MathExpm1
  MathFloor
  MathFround
  MathF16Round
  MathHypot
  MathImul
  MathLog
  MathLog1p
  MathLog2
  MathLog10
  MathMax
  MathMin
  MathPow
  MathRandom
  MathRound
  MathSign
  MathSin
  MathSinh
  MathSqrt
  MathTan
  MathTanh
  MathTrunc
  MathSumPrecise
  RegExpConstructor
  RegExpExec
  RegExpTest
  RegExpToString
  RegExpCompile
  RegExpEscape
  RegExpGetFlags
  RegExpGetSource
  RegExpGetGlobal
  RegExpGetIgnoreCase
  RegExpGetMultiline
  RegExpGetDotAll
  RegExpGetUnicode
  RegExpGetUnicodeSets
  RegExpGetSticky
  RegExpGetHasIndices
  RegExpSymbolMatch
  RegExpSymbolMatchAll
  RegExpSymbolReplace
  RegExpSymbolSearch
  RegExpSymbolSplit
  RegExpStringIteratorNext
  SpeciesGetter
  DateConstructor
  DateParse
  DateUTC
  DateNow
  DateToISOString
  DateToString
  DateSymbolToPrimitive
  DateValueOf
  DateToUTCString
  DateToDateString
  DateToTimeString
  DateToLocaleString
  DateToLocaleDateString
  DateToLocaleTimeString
  DateToJSON
  DateGetTimezoneOffset
  DateGetTime
  DateGetYear
  DateGetFullYear
  DateGetUTCFullYear
  DateGetMonth
  DateGetUTCMonth
  DateGetDate
  DateGetUTCDate
  DateGetDay
  DateGetUTCDay
  DateGetHours
  DateGetUTCHours
  DateGetMinutes
  DateGetUTCMinutes
  DateGetSeconds
  DateGetUTCSeconds
  DateGetMilliseconds
  DateGetUTCMilliseconds
  DateSetTime
  DateSetMilliseconds
  DateSetUTCMilliseconds
  DateSetSeconds
  DateSetUTCSeconds
  DateSetMinutes
  DateSetUTCMinutes
  DateSetHours
  DateSetUTCHours
  DateSetDate
  DateSetUTCDate
  DateSetMonth
  DateSetUTCMonth
  DateSetYear
  DateSetFullYear
  DateSetUTCFullYear
  PromiseConstructor
  PromiseResolve
  PromiseReject
  PromiseThen
  PromiseCatch
  PromiseFinally
  PromiseAll
  PromiseAllSettled
  PromiseAny
  PromiseRace
  PromiseTry
  PromiseWithResolvers
  PromiseReactionJob
  PromiseResolveFunction
  PromiseRejectFunction
  PromiseResolveThenableJob
  PromiseAllResolveElement
  PromiseThenFinally
  PromiseFinallyValueThunk
  PromiseFinallyThrower
  PromiseCapabilityExecutor
  ReflectApply
  ReflectConstruct
  ReflectGet
  ReflectGetOwnPropertyDescriptor
  ReflectGetPrototypeOf
  ReflectDefineProperty
  ReflectHas
  ReflectIsExtensible
  ReflectSet
  ReflectSetPrototypeOf
  ReflectDeleteProperty
  ReflectPreventExtensions
  ReflectOwnKeys
  Eval
  IsNaN
  IsFinite
  UriErrorConstructor
  ModuleImport
  ModuleImportDynamic
  ModuleImportDynamicJob
  ModuleImportBinding
  ModuleBinding
  ModuleExport
  ModuleExportStar
  ModuleAsyncFulfilled
  ModuleAsyncRejected
  GeneratorNext
  GeneratorReturn
  GeneratorThrow
  AsyncGeneratorNext
  AsyncGeneratorReturn
  AsyncGeneratorThrow
  AsyncFunctionResolveFunction
  AsyncGeneratorResolveFunction
  MapConstructor
  MapGroupBy
  MapGet
  MapGetOrInsert
  MapGetOrInsertComputed
  MapSet
  MapHas
  MapDelete
  MapClear
  MapForEach
  MapSize
  MapKeys
  MapValues
  MapEntries
  MapIteratorNext
  SetConstructor
  SetAdd
  SetHas
  SetDelete
  SetClear
  SetForEach
  SetSize
  SetValues
  SetEntries
  SetIsDisjointFrom
  SetIsSubsetOf
  SetIsSupersetOf
  SetIntersection
  SetDifference
  SetSymmetricDifference
  SetUnion
  SetIteratorNext
  WeakMapConstructor
  WeakMapGet
  WeakMapGetOrInsert
  WeakMapGetOrInsertComputed
  WeakMapSet
  WeakMapHas
  WeakMapDelete
  WeakSetConstructor
  WeakSetAdd
  WeakSetHas
  WeakSetDelete
  WeakRefConstructor
  WeakRefDeref
  FinalizationRegistryConstructor
  FinalizationRegistryRegister
  FinalizationRegistryUnregister
  ArrayBufferIsView
  ArrayBufferByteLength
  ArrayBufferMaxByteLength
  ArrayBufferResizable
  ArrayBufferDetached
  ArrayBufferConstructor
  SharedArrayBufferConstructor
  ArrayBufferSlice
  ArrayBufferTransfer
  ArrayBufferTransferToFixedLength
  ArrayBufferResize
  SharedArrayBufferByteLength
  SharedArrayBufferMaxByteLength
  SharedArrayBufferGrowable
  SharedArrayBufferGrow
  SharedArrayBufferSlice
  AtomicsAdd
  AtomicsAnd
  AtomicsOr
  AtomicsSub
  AtomicsXor
  AtomicsExchange
  AtomicsCompareExchange
  AtomicsLoad
  AtomicsStore
  AtomicsIsLockFree
  AtomicsPause
  AtomicsWait
  AtomicsNotify
  DataViewConstructor
  DataViewGetBuffer
  DataViewGetByteLength
  DataViewGetByteOffset
  DataViewGetInt8
  DataViewGetUint8
  DataViewGetInt16
  DataViewGetUint16
  DataViewGetInt32
  DataViewGetUint32
  DataViewGetBigInt64
  DataViewGetBigUint64
  DataViewGetFloat16
  DataViewGetFloat32
  DataViewGetFloat64
  DataViewSetInt8
  DataViewSetUint8
  DataViewSetInt16
  DataViewSetUint16
  DataViewSetInt32
  DataViewSetUint32
  DataViewSetBigInt64
  DataViewSetBigUint64
  DataViewSetFloat16
  DataViewSetFloat32
  DataViewSetFloat64
  TypedArrayConstructor
  TypedArrayFrom
  TypedArrayOf
  TypedArrayGetLength
  TypedArrayGetBuffer
  TypedArrayGetByteLength
  TypedArrayGetByteOffset
  TypedArrayGetToStringTag
  TypedArrayAt
  TypedArrayWith
  TypedArrayValues
  TypedArrayKeys
  TypedArrayEntries
  TypedArrayCopyWithin
  TypedArrayEvery
  TypedArraySome
  TypedArrayForEach
  TypedArrayMap
  TypedArrayFilter
  TypedArrayReduce
  TypedArrayReduceRight
  TypedArrayFind
  TypedArrayFindIndex
  TypedArrayFindLast
  TypedArrayFindLastIndex
  TypedArrayReverse
  TypedArrayToReversed
  TypedArraySlice
  TypedArraySubarray
  TypedArraySort
  TypedArrayToSorted
  TypedArrayJoin
  TypedArrayToLocaleString
  TypedArrayIndexOf
  TypedArrayLastIndexOf
  TypedArrayIncludes
  Uint8ArrayConstructor
  Int8ArrayConstructor
  Uint8ClampedArrayConstructor
  Uint16ArrayConstructor
  Int16ArrayConstructor
  Uint32ArrayConstructor
  Int32ArrayConstructor
  BigInt64ArrayConstructor
  BigUint64ArrayConstructor
  Float16ArrayConstructor
  Float32ArrayConstructor
  Float64ArrayConstructor
  WorkerConstructor
  WorkerPostMessage
  TypedArraySet
  TypedArrayFill
  StdSprintf
  StdLoadScript
  StdTmpfile
  StdOpen
  StdLoadFile
  StdPopen
  StdParseExtJSON
  StdFdopen
  FilePuts
  FilePutByte
  FileSeek
  FileTell
  FileRead
  FileReadAsString
  FileGetByte
  FileGetline
  FileEof
  FileClose
  StdGc
  Print
  Test262DetachArrayBuffer
  Test262EvalScript
  Test262CodePointRange
  Test262CreateRealm
  Test262Gc
  Test262IsHTMLDDA
  Test262AgentStart
  Test262AgentGetReport
  Test262AgentBroadcast
  Test262AgentReport
  Test262AgentLeaving
  Test262AgentReceiveBroadcast
  Test262AgentSleep
  Test262AgentMonotonicNow
  OsSetTimeout
  OsClearTimeout
  OsOpen
  OsClose
  OsRead
  OsWrite
  OsSeek
  OsReaddir
  OsStat
  OsLstat
  OsReadlink
  OsSymlink
  OsMkdir
  OsRemove
  OsUtimes
  OsGetcwd
  OsRealpath
  OsIsatty
  OsExec
  OsPipe
  OsWaitpid
  OsKill
  ErrorToString
  ErrorIsError
  ErrorConstructor
  AggregateErrorConstructor
  EvalErrorConstructor
  SyntaxErrorConstructor
  RangeErrorConstructor
  TypeErrorConstructor
  ReferenceErrorConstructor
  ThrowTypeError
} derive(Show, Eq, ToJson)

///|
pub struct Property {
  value : Value
  writable : Bool
  configurable : Bool
  enumerable : Bool
  getter : Value?
  setter : Value?
}

///|
pub struct ArrayValue {
  id : Int
  elements : Array[Value?]
  mut proto : Value?
  props : Map[String, Property]
  mut typed_array_data : TypedArrayData?
  mut extensible : Bool
}

///|
pub impl Show for ArrayValue with output(self, logger) {
  let _ = self
  logger.write_string("[object Array]")
}

///|
pub impl ToJson for ArrayValue with to_json(self) {
  let _ = self
  Json::string("[object Array]")
}

///|
pub struct ArgumentsValue {
  id : Int
  env : Env
  params : Array[String]
  mapped : Array[Bool]
  elements : Array[Value?]
  props : Map[String, Property]
  mut proto : Value?
  mut extensible : Bool
}

///|
pub impl Show for ArgumentsValue with output(self, logger) {
  let _ = self
  logger.write_string("[object Arguments]")
}

///|
pub impl ToJson for ArgumentsValue with to_json(self) {
  let _ = self
  Json::string("[object Arguments]")
}

///|
pub fn Value::undefined() -> Value {
  Undefined
}
