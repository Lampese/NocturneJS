///|
fn require_iterator_wrap(
  this_value : Value?,
) -> (ObjectValue, IteratorWrapData) raise {
  match this_value {
    Some(Object(obj)) =>
      match obj.iterator_wrap_data {
        Some(data) => (obj, data)
        None => {
          let _ = throw_type_error("not an iterator")
          (
            obj,
            IteratorWrapData::{
              wrapped_iter: Undefined,
              wrapped_next: Undefined,
            },
          )
        }
      }
    _ => {
      let _ = throw_type_error("not an iterator")
      (
        new_object_struct(None),
        IteratorWrapData::{ wrapped_iter: Undefined, wrapped_next: Undefined },
      )
    }
  }
}

///|
fn require_iterator_concat(
  this_value : Value?,
) -> (ObjectValue, IteratorConcatData) raise {
  match this_value {
    Some(Object(obj)) =>
      match obj.iterator_concat_data {
        Some(data) => (obj, data)
        None => {
          let _ = throw_type_error("not an iterator")
          (
            obj,
            IteratorConcatData::{
              index: 0,
              count: 0,
              running: false,
              iter: Undefined,
              next: Undefined,
              values: [],
            },
          )
        }
      }
    _ => {
      let _ = throw_type_error("not an iterator")
      (
        new_object_struct(None),
        IteratorConcatData::{
          index: 0,
          count: 0,
          running: false,
          iter: Undefined,
          next: Undefined,
          values: [],
        },
      )
    }
  }
}

///|
fn require_iterator_helper(
  this_value : Value?,
) -> (ObjectValue, IteratorHelperData) raise {
  match this_value {
    Some(Object(obj)) =>
      match obj.iterator_helper_data {
        Some(data) => (obj, data)
        None => {
          let _ = throw_type_error("not an iterator")
          (
            obj,
            IteratorHelperData::{
              obj: Undefined,
              next: Undefined,
              func: Undefined,
              inner: Undefined,
              inner_next: Undefined,
              count: 0L,
              kind: 0,
              executing: false,
              done: true,
            },
          )
        }
      }
    _ => {
      let _ = throw_type_error("not an iterator")
      (
        new_object_struct(None),
        IteratorHelperData::{
          obj: Undefined,
          next: Undefined,
          func: Undefined,
          inner: Undefined,
          inner_next: Undefined,
          count: 0L,
          kind: 0,
          executing: false,
          done: true,
        },
      )
    }
  }
}
