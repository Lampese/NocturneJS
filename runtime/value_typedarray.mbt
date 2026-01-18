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
struct TypedArrayData {
  kind : TypedArrayKind
  buffer : ObjectValue
  byte_offset : Int
  length : Int
  length_tracking : Bool
  bytes_per_element : Int
}
