///|
fn array_length_from_props(arr : ArrayValue) -> Int64 {
  match arr.props.get("length") {
    Some(prop) =>
      match prop.value {
        Number(num) =>
          if Double::is_nan(num) || Double::is_inf(num) {
            0L
          } else {
            let trunc = Double::trunc(num)
            if trunc <= 0.0 {
              0L
            } else if trunc > Int64::to_double(max_array_length) {
              max_array_length
            } else {
              Double::to_int64(trunc)
            }
          }
        _ => Int64::from_int(arr.elements.length())
      }
    None => Int64::from_int(arr.elements.length())
  }
}

///|
fn array_length_value_from_props(
  arr : ArrayValue,
  receiver : Value,
) -> Value raise {
  match property_get_from_props(arr.props, "length", receiver) {
    Some(value) => value
    None => Number(Double::from_int(arr.elements.length()))
  }
}

///|
fn array_set_length_int64(
  arr : ArrayValue,
  target_len : Int64,
  allow_nonwritable_equal : Bool,
) -> Bool {
  let current_len = array_length_from_props(arr)
  match arr.props.get("length") {
    Some(prop) =>
      if !prop.writable {
        if allow_nonwritable_equal && target_len == current_len {
          return true
        }
        return false
      }
    None => ()
  }
  if target_len >= current_len {
    let max_dense_len = 2147483647
    if target_len <= Int64::from_int(max_dense_len) {
      let target_int = Int64::to_int(target_len)
      while arr.elements.length() < target_int {
        arr.elements.push(None)
      }
    }
    set_length_prop(arr.props, Number(Int64::to_double(target_len)))
    return true
  }
  let mut final_len = target_len
  for key, prop in arr.props {
    match array_index_from_name(key) {
      Some(index) =>
        if index >= target_len && !prop.configurable {
          let blocked_len = index + 1L
          if blocked_len > final_len {
            final_len = blocked_len
          }
        }
      None => ()
    }
  }
  let to_remove : Array[String] = []
  for key, prop in arr.props {
    match array_index_from_name(key) {
      Some(index) =>
        if index >= final_len && prop.configurable {
          to_remove.push(key)
        }
      None => ()
    }
  }
  for key in to_remove {
    props_remove(arr.props, key)
  }
  if final_len <= Int64::from_int(arr.elements.length()) {
    let final_int = Int64::to_int(final_len)
    rc_truncate_optional_values(arr.elements, final_int)
  }
  set_length_prop(arr.props, Number(Int64::to_double(final_len)))
  final_len == target_len
}

///|
fn array_set_sparse(
  arr : ArrayValue,
  name : String,
  value : Value,
  receiver : Value,
  index64 : Int64,
) -> Bool raise {
  if arr.props.contains(name) {
    return set_property_in(arr.props, name, value, receiver)
  }
  match arr.proto {
    Some(proto) => return set_value_with_receiver(proto, name, value, receiver)
    None => ()
  }
  if !arr.extensible {
    return false
  }
  let _ = set_property_in(arr.props, name, value, receiver)
  let current_len = array_length_from_props(arr)
  let next_len = index64 + 1L
  if next_len > current_len {
    set_length_prop(arr.props, Number(Int64::to_double(next_len)))
  }
  true
}

///|
fn array_set(arr : ArrayValue, name : String, value : Value) -> Bool raise {
  match arr.typed_array_data {
    Some(data) => return typed_array_set(arr, data, name, value)
    None => ()
  }
  let receiver = Array(arr)
  if name == "length" {
    let new_len_uint = to_uint32(to_number(value))
    let number_len = to_number(value)
    let new_len_num = UInt::to_double(new_len_uint)
    if Double::is_nan(number_len) || new_len_num != number_len {
      let _ = throw_range_error("invalid array length")
      return false
    }
    let target_len = Double::to_int64(new_len_num)
    return array_set_length_int64(arr, target_len, false)
  }
  match array_index_from_name(name) {
    Some(index64) => {
      if index64 < 0L {
        return false
      }
      let current_len = array_length_from_props(arr)
      match arr.props.get("length") {
        Some(prop) =>
          if !prop.writable && index64 >= current_len {
            return false
          }
        None => ()
      }
      let max_dense_index = 2147483646
      if index64 <= Int64::from_int(max_dense_index) {
        let index = Int64::to_int(index64)
        let elements_len = arr.elements.length()
        if index > elements_len {
          return array_set_sparse(arr, name, value, receiver, index64)
        }
        let mut has_existing = arr.props.contains(name)
        if !has_existing && index < elements_len {
          has_existing = match arr.elements[index] {
            Some(_) => true
            None => false
          }
        }
        if has_existing {
          if arr.props.contains(name) {
            return set_property_in(arr.props, name, value, receiver)
          }
          if index < arr.elements.length() {
            rc_replace_optional_value(arr.elements[index], Some(value))
            arr.elements[index] = Some(value)
          }
          let current_len = array_length_from_props(arr)
          let elem_len = Int64::from_int(arr.elements.length())
          let new_len = if elem_len > current_len {
            elem_len
          } else {
            current_len
          }
          set_length_prop(arr.props, Number(Int64::to_double(new_len)))
          return true
        }
        match arr.proto {
          Some(proto) =>
            return set_value_with_receiver(proto, name, value, receiver)
          None => ()
        }
        if !arr.extensible {
          return false
        }
        while arr.elements.length() < index {
          arr.elements.push(None)
        }
        if index == arr.elements.length() {
          rc_incref_value(value)
          arr.elements.push(Some(value))
        } else {
          rc_replace_optional_value(arr.elements[index], Some(value))
          arr.elements[index] = Some(value)
        }
        let current_len = array_length_from_props(arr)
        let elem_len = Int64::from_int(arr.elements.length())
        let new_len = if elem_len > current_len {
          elem_len
        } else {
          current_len
        }
        set_length_prop(arr.props, Number(Int64::to_double(new_len)))
        true
      } else {
        array_set_sparse(arr, name, value, receiver, index64)
      }
    }
    None =>
      set_property_with_proto(
        arr.props,
        arr.proto,
        name,
        value,
        receiver,
        arr.extensible,
      )
  }
}

///|
fn set_length_prop(props : Map[String, Property], value : Value) -> Unit {
  match props.get("length") {
    Some(prop) =>
      props_set(props, "length", Property::{
        value,
        writable: prop.writable,
        configurable: false,
        enumerable: false,
        getter: None,
        setter: None,
      })
    None => props_set(props, "length", property_data_non_enum_non_config(value))
  }
}

///|
fn array_proto_length(obj : ObjectValue) -> Int {
  match obj.props.get("length") {
    Some(prop) =>
      match prop.value {
        Number(num) => Int64::to_int(Double::to_int64(Double::trunc(num)))
        _ => 0
      }
    None => 0
  }
}

///|
fn array_proto_set_length(obj : ObjectValue, value : Value) -> Bool raise {
  match obj.props.get("length") {
    Some(prop) => if !prop.writable { return false }
    None => ()
  }
  let new_len = to_number(value)
  if Double::is_nan(new_len) || new_len < 0.0 {
    return false
  }
  let len = Double::to_int64(Double::trunc(new_len))
  if len < 0L {
    return false
  }
  let target = Int64::to_int(len)
  let current_len = array_proto_length(obj)
  if target < current_len {
    let mut i = current_len - 1
    while i >= target {
      let index_name = Int::to_string(i)
      match obj.props.get(index_name) {
        Some(prop) =>
          if !prop.configurable {
            let final_len = i + 1
            set_length_prop(obj.props, Number(Double::from_int(final_len)))
            return false
          } else {
            props_remove(obj.props, index_name)
          }
        None => ()
      }
      if i == 0 {
        break
      }
      i = i - 1
    }
  }
  set_length_prop(obj.props, Number(Double::from_int(target)))
  true
}

///|
fn array_proto_set(
  obj : ObjectValue,
  name : String,
  value : Value,
) -> Bool raise {
  let receiver = Object(obj)
  if name == "length" {
    return array_proto_set_length(obj, value)
  }
  match parse_array_index(name) {
    Some(index) => {
      if index < 0 {
        return false
      }
      let has_existing = obj.props.contains(name)
      if !has_existing && !obj.extensible {
        return false
      }
      if obj.props.contains(name) {
        return set_property_in(obj.props, name, value, receiver)
      }
      match get_property_in_chain(obj.proto, name) {
        Some(desc) =>
          if descriptor_is_accessor(desc) {
            match property_get(desc, "set") {
              Undefined | Null => return false
              setter => {
                let _ = call_value_with_this(setter, [value], receiver)
                return true
              }
            }
          } else {
            let writable = is_truthy(property_get(desc, "writable"))
            if !writable {
              return false
            }
          }
        None => ()
      }
      let current_len = array_proto_length(obj)
      match obj.props.get("length") {
        Some(prop) => if !prop.writable && index >= current_len { return false }
        None => ()
      }
      let _ = set_property_in(obj.props, name, value, receiver)
      if index >= current_len {
        set_length_prop(obj.props, Number(Double::from_int(index + 1)))
      }
      true
    }
    None =>
      set_property_with_proto(
        obj.props,
        obj.proto,
        name,
        value,
        receiver,
        obj.extensible,
      )
  }
}
