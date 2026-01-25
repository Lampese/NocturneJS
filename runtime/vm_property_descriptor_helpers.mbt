///|
fn descriptor_is_accessor(desc : Value) -> Bool raise {
  has_property_value(desc, "get") || has_property_value(desc, "set")
}

///|
fn descriptor_is_data(desc : Value) -> Bool raise {
  has_property_value(desc, "value") || has_property_value(desc, "writable")
}

///|
priv struct PropertyDescInfo {
  value : Value
  writable : Bool
  enumerable : Bool
  configurable : Bool
  getter : Value
  setter : Value
  has_value : Bool
  has_writable : Bool
  has_enumerable : Bool
  has_configurable : Bool
  has_get : Bool
  has_set : Bool
}

///|
fn to_property_desc_info(desc : Value) -> PropertyDescInfo raise {
  if !is_object_like(desc) {
    let _ = throw_type_error("property description must be an object")
    return PropertyDescInfo::{
      value: Undefined,
      writable: false,
      enumerable: false,
      configurable: false,
      getter: Undefined,
      setter: Undefined,
      has_value: false,
      has_writable: false,
      has_enumerable: false,
      has_configurable: false,
      has_get: false,
      has_set: false,
    }
  }
  let mut value = Undefined
  let mut writable = false
  let mut enumerable = false
  let mut configurable = false
  let mut getter = Undefined
  let mut setter = Undefined
  let mut has_value = false
  let mut has_writable = false
  let mut has_enumerable = false
  let mut has_configurable = false
  let mut has_get = false
  let mut has_set = false
  if has_property_value(desc, "enumerable") {
    has_enumerable = true
    enumerable = is_truthy(property_get(desc, "enumerable"))
  }
  if has_property_value(desc, "configurable") {
    has_configurable = true
    configurable = is_truthy(property_get(desc, "configurable"))
  }
  if has_property_value(desc, "value") {
    has_value = true
    value = property_get(desc, "value")
  }
  if has_property_value(desc, "writable") {
    has_writable = true
    writable = is_truthy(property_get(desc, "writable"))
  }
  if has_property_value(desc, "get") {
    has_get = true
    let getter_value = property_get(desc, "get")
    if !(getter_value is Undefined) && !is_callable(getter_value) {
      let _ = throw_type_error("get is not callable")
      return PropertyDescInfo::{
        value,
        writable,
        enumerable,
        configurable,
        getter,
        setter,
        has_value,
        has_writable,
        has_enumerable,
        has_configurable,
        has_get,
        has_set,
      }
    }
    getter = getter_value
  }
  if has_property_value(desc, "set") {
    has_set = true
    let setter_value = property_get(desc, "set")
    if !(setter_value is Undefined) && !is_callable(setter_value) {
      let _ = throw_type_error("set is not callable")
      return PropertyDescInfo::{
        value,
        writable,
        enumerable,
        configurable,
        getter,
        setter,
        has_value,
        has_writable,
        has_enumerable,
        has_configurable,
        has_get,
        has_set,
      }
    }
    setter = setter_value
  }
  if (has_get || has_set) && (has_value || has_writable) {
    let _ = throw_type_error("invalid property descriptor")
    return PropertyDescInfo::{
      value,
      writable,
      enumerable,
      configurable,
      getter,
      setter,
      has_value,
      has_writable,
      has_enumerable,
      has_configurable,
      has_get,
      has_set,
    }
  }
  PropertyDescInfo::{
    value,
    writable,
    enumerable,
    configurable,
    getter,
    setter,
    has_value,
    has_writable,
    has_enumerable,
    has_configurable,
    has_get,
    has_set,
  }
}

///|
fn property_descriptor_object(prop : Property) -> Value {
  let desc = new_object_value()
  match desc {
    Object(obj) => {
      if prop.getter is Some(_) || prop.setter is Some(_) {
        let getter = match prop.getter {
          Some(value) => value
          None => Undefined
        }
        let setter = match prop.setter {
          Some(value) => value
          None => Undefined
        }
        props_set(obj.props, "get", property_data(getter))
        props_set(obj.props, "set", property_data(setter))
      } else {
        props_set(obj.props, "value", property_data(prop.value))
        props_set(obj.props, "writable", property_data(Bool(prop.writable)))
      }
      props_set(obj.props, "enumerable", property_data(Bool(prop.enumerable)))
      props_set(obj.props, "configurable", property_data(Bool(prop.configurable)))
    }
    _ => ()
  }
  desc
}

///|
fn property_descriptor_object_from_info(info : PropertyDescInfo) -> Value {
  let desc = new_object_value()
  match desc {
    Object(obj) => {
      if info.has_get {
        props_set(obj.props, "get", property_data(info.getter))
      }
      if info.has_set {
        props_set(obj.props, "set", property_data(info.setter))
      }
      if info.has_value {
        props_set(obj.props, "value", property_data(info.value))
      }
      if info.has_writable {
        props_set(obj.props, "writable", property_data(Bool(info.writable)))
      }
      if info.has_enumerable {
        props_set(obj.props, "enumerable", property_data(Bool(info.enumerable)))
      }
      if info.has_configurable {
        props_set(obj.props, "configurable", property_data(Bool(info.configurable)))
      }
    }
    _ => ()
  }
  desc
}

///|
fn property_data(value : Value) -> Property {
  Property::{
    value,
    writable: true,
    configurable: true,
    enumerable: true,
    getter: None,
    setter: None,
  }
}

///|
fn property_data_readonly(value : Value) -> Property {
  Property::{
    value,
    writable: false,
    configurable: true,
    enumerable: false,
    getter: None,
    setter: None,
  }
}

///|
fn property_data_const(value : Value) -> Property {
  Property::{
    value,
    writable: false,
    configurable: false,
    enumerable: false,
    getter: None,
    setter: None,
  }
}

///|
fn property_data_non_enum_non_config(value : Value) -> Property {
  Property::{
    value,
    writable: true,
    configurable: false,
    enumerable: false,
    getter: None,
    setter: None,
  }
}

///|
fn property_data_non_enum(value : Value) -> Property {
  Property::{
    value,
    writable: true,
    configurable: true,
    enumerable: false,
    getter: None,
    setter: None,
  }
}

///|
fn property_accessor(getter : Value, setter : Value) -> Property {
  Property::{
    value: Undefined,
    writable: false,
    configurable: true,
    enumerable: false,
    getter: Some(getter),
    setter: Some(setter),
  }
}

///|
fn property_accessor_non_config(getter : Value, setter : Value) -> Property {
  Property::{
    value: Undefined,
    writable: false,
    configurable: false,
    enumerable: false,
    getter: Some(getter),
    setter: Some(setter),
  }
}

///|
fn set_accessor_property(
  props : Props,
  name : String,
  getter : Value?,
  setter : Value?,
) -> Unit {
  match props_get(props, name) {
    Some(prop) =>
      if prop.getter is Some(_) || prop.setter is Some(_) {
        let next_getter = match getter {
          Some(_) => getter
          None => prop.getter
        }
        let next_setter = match setter {
          Some(_) => setter
          None => prop.setter
        }
        props_set(props, name, Property::{
          value: Undefined,
          writable: false,
          configurable: prop.configurable,
          enumerable: prop.enumerable,
          getter: next_getter,
          setter: next_setter,
        })
      } else {
        props_set(props, name, Property::{
          value: Undefined,
          writable: false,
          configurable: true,
          enumerable: true,
          getter,
          setter,
        })
      }
    None =>
      props_set(props, name, Property::{
        value: Undefined,
        writable: false,
        configurable: true,
        enumerable: true,
        getter,
        setter,
      })
  }
}

///|
fn set_accessor_property_with_enum(
  props : Props,
  name : String,
  getter : Value?,
  setter : Value?,
  enumerable : Bool,
) -> Unit {
  match props_get(props, name) {
    Some(prop) =>
      if prop.getter is Some(_) || prop.setter is Some(_) {
        let next_getter = match getter {
          Some(_) => getter
          None => prop.getter
        }
        let next_setter = match setter {
          Some(_) => setter
          None => prop.setter
        }
        props_set(props, name, Property::{
          value: Undefined,
          writable: false,
          configurable: prop.configurable,
          enumerable,
          getter: next_getter,
          setter: next_setter,
        })
      } else {
        props_set(props, name, Property::{
          value: Undefined,
          writable: false,
          configurable: true,
          enumerable,
          getter,
          setter,
        })
      }
    None =>
      props_set(props, name, Property::{
        value: Undefined,
        writable: false,
        configurable: true,
        enumerable,
        getter,
        setter,
      })
  }
}
