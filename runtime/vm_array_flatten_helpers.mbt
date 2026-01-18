///|
fn flatten_into_array(
  target : Value,
  source : Value,
  source_len : Int,
  target_index : Int,
  depth : Int,
  mapper_fn : Value?,
  this_arg : Value,
) -> Int raise {
  let mut next_index = target_index
  let mut source_index = 0
  let max_safe = Double::to_int(9007199254740991.0)
  while source_index < source_len {
    let name = Int::to_string(source_index)
    if has_property_value(source, name) {
      let mut element = property_get(source, name)
      match mapper_fn {
        Some(func) =>
          element = call_value_with_this(
            func,
            [element, Number(Double::from_int(source_index)), source],
            this_arg,
          )
        None => ()
      }
      if depth > 0 && is_array_value(element) {
        let inner = to_object(element)
        let inner_len = array_like_length(inner)
        next_index = flatten_into_array(
          target,
          inner,
          inner_len,
          next_index,
          depth - 1,
          None,
          Undefined,
        )
      } else {
        if next_index >= max_safe {
          let _ = throw_type_error("Array too long")
          return next_index
        }
        create_data_property_or_throw(
          target,
          Int::to_string(next_index),
          element,
        )
        next_index = next_index + 1
      }
    }
    source_index = source_index + 1
  }
  next_index
}

///|
fn array_flatten_value(
  this_value : Value,
  args : Array[Value],
  map : Bool,
) -> Value raise {
  let obj = to_object(this_value)
  let source_len = array_like_length(obj)
  let mut depth = 1
  let mut mapper : Value? = None
  let mut this_arg = Undefined
  if map {
    let func = if args.is_empty() { Undefined } else { args[0] }
    if !is_callable(func) {
      return throw_type_error("not a function")
    }
    mapper = Some(func)
    if args.length() > 1 {
      this_arg = args[1]
    }
    depth = 1
  } else if args.length() > 0 && !(args[0] is Undefined) {
    depth = to_int32_sat(to_number(args[0]))
  }
  let target = array_species_create(obj, 0)
  let _ = flatten_into_array(
    target, obj, source_len, 0, depth, mapper, this_arg,
  )
  target
}
