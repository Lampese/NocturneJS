///|
fn has_own_property(value : Value, name : String) -> Bool raise {
  let desc = get_own_property_descriptor(value, name)
  match desc {
    Undefined => false
    _ =>
      if !is_object_like(desc) {
        let _ = throw_type_error("invalid property descriptor")
        false
      } else {
        true
      }
  }
}
