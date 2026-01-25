///|
priv struct ShapeEntry {
  name : String
  writable : Bool
  enumerable : Bool
  configurable : Bool
}

///|
priv struct Shape {
  id : Int
  entries : Array[ShapeEntry]
  entry_map : Map[String, Int]
  transitions : Map[String, Shape]
}

///|
priv enum PropSlot {
  Data(Value)
  Accessor(Value, Value)
}

///|
priv struct PropsDict {
  map : Map[String, Property]
  order : Array[String]
}

///|
priv enum PropsRepr {
  Shape(Shape, Array[PropSlot])
  Dict(PropsDict)
}

///|
struct Props {
  mut repr : PropsRepr
  mut version : Int
}

///|
const PROPS_DICT_THRESHOLD : Int = 256

///|
fn clone_shape_entries(entries : Array[ShapeEntry]) -> Array[ShapeEntry] {
  let out : Array[ShapeEntry] = []
  for entry in entries {
    out.push(entry)
  }
  out
}

///|
fn shape_new(entries : Array[ShapeEntry]) -> Shape {
  let map = Map::new()
  let mut index = 0
  for entry in entries {
    map.set(entry.name, index)
    index = index + 1
  }
  Shape::{
    id: alloc_id(),
    entries,
    entry_map: map,
    transitions: Map::new(),
  }
}

///|
let empty_shape_ref : Ref[Shape?] = Ref::new(None)

///|
fn set_shape_ref(cell : Ref[Shape?], value : Shape?) -> Unit {
  cell.update(fn(_) { value })
}

///|
fn get_shape_ref(cell : Ref[Shape?]) -> Shape? {
  let mut value : Shape? = None
  cell.update(fn(current) {
    value = current
    current
  })
  value
}

///|
fn empty_shape() -> Shape {
  match get_shape_ref(empty_shape_ref) {
    Some(shape) => shape
    None => {
      let shape = shape_new([])
      set_shape_ref(empty_shape_ref, Some(shape))
      shape
    }
  }
}

///|
fn props_shape_cache_clear() -> Unit {
  set_shape_ref(empty_shape_ref, None)
}

///|
fn prop_attr_bits(writable : Bool, enumerable : Bool, configurable : Bool) -> Int {
  let mut bits = 0
  if writable {
    bits = bits + 1
  }
  if enumerable {
    bits = bits + 2
  }
  if configurable {
    bits = bits + 4
  }
  bits
}

///|
fn shape_transition_key(kind : String, name : String, bits : Int) -> String {
  kind + "|" + name + "|" + Int::to_string(bits)
}

///|
fn shape_transition_add(
  shape : Shape,
  name : String,
  writable : Bool,
  enumerable : Bool,
  configurable : Bool,
) -> Shape {
  let bits = prop_attr_bits(writable, enumerable, configurable)
  let key = shape_transition_key("A", name, bits)
  match shape.transitions.get(key) {
    Some(next) => next
    None => {
      let entries = clone_shape_entries(shape.entries)
      entries.push(ShapeEntry::{ name, writable, enumerable, configurable })
      let next = shape_new(entries)
      shape.transitions.set(key, next)
      next
    }
  }
}

///|
fn shape_transition_update(
  shape : Shape,
  name : String,
  writable : Bool,
  enumerable : Bool,
  configurable : Bool,
) -> Shape {
  let bits = prop_attr_bits(writable, enumerable, configurable)
  let key = shape_transition_key("U", name, bits)
  match shape.transitions.get(key) {
    Some(next) => next
    None => {
      let entries = clone_shape_entries(shape.entries)
      match shape.entry_map.get(name) {
        Some(index) =>
          if index >= 0 && index < entries.length() {
            entries[index] =
              ShapeEntry::{ name, writable, enumerable, configurable }
          }
        None => ()
      }
      let next = shape_new(entries)
      shape.transitions.set(key, next)
      next
    }
  }
}

///|
fn shape_transition_remove(shape : Shape, name : String) -> Shape {
  let key = shape_transition_key("R", name, 0)
  match shape.transitions.get(key) {
    Some(next) => next
    None => {
      let entries : Array[ShapeEntry] = []
      for entry in shape.entries {
        if entry.name != name {
          entries.push(entry)
        }
      }
      let next =
        if entries.is_empty() {
          empty_shape()
        } else {
          shape_new(entries)
        }
      shape.transitions.set(key, next)
      next
    }
  }
}

///|
fn slot_from_property(prop : Property) -> PropSlot {
  if prop.getter is Some(_) || prop.setter is Some(_) {
    let getter = match prop.getter {
      Some(value) => value
      None => Undefined
    }
    let setter = match prop.setter {
      Some(value) => value
      None => Undefined
    }
    PropSlot::Accessor(getter, setter)
  } else {
    PropSlot::Data(prop.value)
  }
}

///|
fn property_from_entry_slot(entry : ShapeEntry, slot : PropSlot) -> Property {
  match slot {
    PropSlot::Data(value) =>
      Property::{
        value,
        writable: entry.writable,
        enumerable: entry.enumerable,
        configurable: entry.configurable,
        getter: None,
        setter: None,
      }
    PropSlot::Accessor(getter, setter) =>
      Property::{
        value: Undefined,
        writable: entry.writable,
        enumerable: entry.enumerable,
        configurable: entry.configurable,
        getter: Some(getter),
        setter: Some(setter),
      }
  }
}

///|
fn props_new() -> Props {
  Props::{ repr: PropsRepr::Shape(empty_shape(), []), version: 0 }
}

///|
fn props_is_empty(props : Props) -> Bool {
  match props.repr {
    PropsRepr::Shape(shape, _) => shape.entries.is_empty()
    PropsRepr::Dict(dict) => dict.map.is_empty()
  }
}

///|
fn props_has_only_length(props : Props) -> Bool {
  match props.repr {
    PropsRepr::Shape(shape, _) =>
      shape.entries.length() == 1 && shape.entries[0].name == "length"
    PropsRepr::Dict(dict) =>
      dict.order.length() == 1 && dict.order[0] == "length"
  }
}

///|
fn props_get(props : Props, name : String) -> Property? {
  match props.repr {
    PropsRepr::Shape(shape, slots) =>
      match shape.entry_map.get(name) {
        Some(index) =>
          if index >= 0 && index < slots.length() {
            let entry = shape.entries[index]
            let slot = slots[index]
            Some(property_from_entry_slot(entry, slot))
          } else {
            None
          }
        None => None
      }
    PropsRepr::Dict(dict) => dict.map.get(name)
  }
}

///|
fn props_contains(props : Props, name : String) -> Bool {
  match props.repr {
    PropsRepr::Shape(shape, _) => shape.entry_map.contains(name)
    PropsRepr::Dict(dict) => dict.map.contains(name)
  }
}

///|
fn props_iter(props : Props, f : (String, Property) -> Unit) -> Unit {
  match props.repr {
    PropsRepr::Shape(shape, slots) => {
      let entries = shape.entries
      let mut index = 0
      let count = entries.length()
      while index < count && index < slots.length() {
        let entry = entries[index]
        let slot = slots[index]
        f(entry.name, property_from_entry_slot(entry, slot))
        index = index + 1
      }
    }
    PropsRepr::Dict(dict) =>
      for name in dict.order {
        match dict.map.get(name) {
          Some(prop) => f(name, prop)
          None => ()
        }
      }
  }
}

///|
fn props_version(props : Props) -> Int {
  props.version
}

///|
fn props_shape_id(props : Props) -> Int? {
  match props.repr {
    PropsRepr::Shape(shape, _) => Some(shape.id)
    _ => None
  }
}

///|
fn props_get_data_slot(
  props : Props,
  name : String,
) -> (Int, Bool, Value)? {
  match props.repr {
    PropsRepr::Shape(shape, slots) =>
      match shape.entry_map.get(name) {
        Some(index) =>
          if index >= 0 && index < slots.length() {
            let entry = shape.entries[index]
            match slots[index] {
              PropSlot::Data(value) => Some((index, entry.writable, value))
              _ => None
            }
          } else {
            None
          }
        None => None
      }
    _ => None
  }
}

///|
fn props_get_data_slot_value(props : Props, index : Int) -> Value? {
  match props.repr {
    PropsRepr::Shape(_, slots) =>
      if index >= 0 && index < slots.length() {
        match slots[index] {
          PropSlot::Data(value) => Some(value)
          _ => None
        }
      } else {
        None
      }
    _ => None
  }
}

///|
fn props_set_data_slot_value(
  props : Props,
  index : Int,
  value : Value,
) -> Bool {
  match props.repr {
    PropsRepr::Shape(shape, slots) =>
      if index >= 0 && index < slots.length() {
        let entry = shape.entries[index]
        match slots[index] {
          PropSlot::Data(old) =>
            if entry.writable {
              rc_replace_value(old, value)
              slots[index] = PropSlot::Data(value)
              props.repr = PropsRepr::Shape(shape, slots)
              props.version = props.version + 1
              true
            } else {
              false
            }
          _ => false
        }
      } else {
        false
      }
    _ => false
  }
}

///|
fn props_iter_raise(
  props : Props,
  f : (String, Property) -> Unit raise,
) -> Unit raise {
  match props.repr {
    PropsRepr::Shape(shape, slots) => {
      let entries = shape.entries
      let mut index = 0
      let count = entries.length()
      while index < count && index < slots.length() {
        let entry = entries[index]
        let slot = slots[index]
        let _ = f(entry.name, property_from_entry_slot(entry, slot))
        index = index + 1
      }
    }
    PropsRepr::Dict(dict) =>
      for name in dict.order {
        match dict.map.get(name) {
          Some(prop) => {
            let _ = f(name, prop)
          }
          None => ()
        }
      }
  }
}

///|
fn props_reset_empty(props : Props) -> Unit {
  props.repr = PropsRepr::Shape(empty_shape(), [])
  props.version = props.version + 1
}

///|
fn props_convert_to_dict(props : Props) -> Unit {
  match props.repr {
    PropsRepr::Dict(_) => ()
    PropsRepr::Shape(shape, slots) => {
      let map : Map[String, Property] = Map::new()
      let order : Array[String] = []
      let entries = shape.entries
      let mut index = 0
      let count = entries.length()
      while index < count && index < slots.length() {
        let entry = entries[index]
        let slot = slots[index]
        let prop = property_from_entry_slot(entry, slot)
        map.set(entry.name, prop)
        order.push(entry.name)
        index = index + 1
      }
      props.repr = PropsRepr::Dict(PropsDict::{ map, order })
    }
  }
}
