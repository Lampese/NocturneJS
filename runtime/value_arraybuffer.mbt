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
