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
