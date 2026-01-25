///|
struct Props {
  mut map : Map[String, Property]
  mut shared_empty : Bool
}

///|
let shared_empty_props_map : Map[String, Property] = Map::new()

///|
fn props_new() -> Props {
  Props::{ map: shared_empty_props_map, shared_empty: true }
}

///|
fn props_is_empty(props : Props) -> Bool {
  props.shared_empty || props.map.is_empty()
}

///|
fn props_get(props : Props, name : String) -> Property? {
  if props.shared_empty {
    None
  } else {
    props.map.get(name)
  }
}

///|
fn props_contains(props : Props, name : String) -> Bool {
  if props.shared_empty {
    false
  } else {
    props.map.contains(name)
  }
}

///|
fn props_iter(props : Props, f : (String, Property) -> Unit) -> Unit {
  if props.shared_empty {
    return ()
  }
  for key, prop in props.map {
    f(key, prop)
  }
}

///|
fn props_iter_raise(
  props : Props,
  f : (String, Property) -> Unit raise,
) -> Unit raise {
  if props.shared_empty {
    return ()
  }
  for key, prop in props.map {
    let _ = f(key, prop)
  }
}

///|
fn props_ensure_map(props : Props) -> Map[String, Property] {
  if props.shared_empty {
    props.map = Map::new()
    props.shared_empty = false
  }
  props.map
}

///|
fn props_reset_empty(props : Props) -> Unit {
  if !props.shared_empty {
    props.map = shared_empty_props_map
    props.shared_empty = true
  }
}
