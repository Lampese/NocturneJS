///|
fn new_map_iterator(data : MapData, kind : Int) -> Value {
  let iter_value = new_object_value_with_proto(
    value_from_object(map_iterator_proto()),
  )
  match iter_value {
    Object(obj) =>
      obj.map_iter_data = Some(MapIteratorData::{
        entries: data.entries,
        index: 0,
        kind,
        done: false,
      })
    _ => ()
  }
  iter_value
}

///|
fn new_set_iterator(data : SetData, kind : Int) -> Value {
  let iter_value = new_object_value_with_proto(
    value_from_object(set_iterator_proto()),
  )
  match iter_value {
    Object(obj) =>
      obj.set_iter_data = Some(SetIteratorData::{
        entries: data.entries,
        index: 0,
        kind,
        done: false,
      })
    _ => ()
  }
  iter_value
}

///|
fn require_map_iterator(
  this_value : Value?,
) -> (ObjectValue, MapIteratorData) raise {
  match this_value {
    Some(Object(obj)) =>
      match obj.map_iter_data {
        Some(data) => (obj, data)
        None => {
          let _ = throw_type_error("not a map iterator")
          (obj, MapIteratorData::{ entries: [], index: 0, kind: 0, done: true })
        }
      }
    _ => {
      let _ = throw_type_error("not a map iterator")
      (
        new_object_struct(None),
        MapIteratorData::{ entries: [], index: 0, kind: 0, done: true },
      )
    }
  }
}

///|
fn require_set_iterator(
  this_value : Value?,
) -> (ObjectValue, SetIteratorData) raise {
  match this_value {
    Some(Object(obj)) =>
      match obj.set_iter_data {
        Some(data) => (obj, data)
        None => {
          let _ = throw_type_error("not a set iterator")
          (obj, SetIteratorData::{ entries: [], index: 0, kind: 0, done: true })
        }
      }
    _ => {
      let _ = throw_type_error("not a set iterator")
      (
        new_object_struct(None),
        SetIteratorData::{ entries: [], index: 0, kind: 0, done: true },
      )
    }
  }
}

///|
fn map_iterator_next(this_value : Value?) -> Value raise {
  let (_, data) = require_map_iterator(this_value)
  if data.done {
    return iterator_result(Undefined, true)
  }
  let mut index = data.index
  while index < data.entries.length() {
    match data.entries[index] {
      Some((key, value)) => {
        data.index = index + 1
        return match data.kind {
          0 => iterator_result(key, false)
          1 => iterator_result(value, false)
          _ => {
            let pair = new_array_value([Some(key), Some(value)])
            iterator_result(pair, false)
          }
        }
      }
      None => index = index + 1
    }
  }
  data.index = index
  data.done = true
  iterator_result(Undefined, true)
}

///|
fn set_iterator_next(this_value : Value?) -> Value raise {
  let (_, data) = require_set_iterator(this_value)
  if data.done {
    return iterator_result(Undefined, true)
  }
  let mut index = data.index
  while index < data.entries.length() {
    match data.entries[index] {
      Some(value) => {
        data.index = index + 1
        return match data.kind {
          1 => {
            let pair = new_array_value([Some(value), Some(value)])
            iterator_result(pair, false)
          }
          _ => iterator_result(value, false)
        }
      }
      None => index = index + 1
    }
  }
  data.index = index
  data.done = true
  iterator_result(Undefined, true)
}
