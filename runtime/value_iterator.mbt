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
