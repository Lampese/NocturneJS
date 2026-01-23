///|
pub struct MapData {
  entries : Array[(Value, Value)?]
}

///|
pub struct WeakMapData {
  entries : Array[(Value, Value)]
  index : Map[Int, Int]
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
