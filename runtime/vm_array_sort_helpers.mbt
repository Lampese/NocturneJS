///|
fn array_sort_slot_string(slots : Array[ArraySortSlot], index : Int) -> String {
  match slots[index].str_cache {
    Some(text) => text
    None => {
      let text = to_string_value(slots[index].value)
      slots[index].str_cache = Some(text)
      text
    }
  }
}

///|
fn array_sort_compare(
  slots : Array[ArraySortSlot],
  left : Int,
  right : Int,
  compare_fn : Value?,
) -> Int raise {
  let left_value = slots[left].value
  let right_value = slots[right].value
  let mut cmp = 0
  match compare_fn {
    Some(func) =>
      if strict_eq(left_value, right_value) {
        cmp = 0
      } else {
        let res =
          call_value_with_this_twoargs(func, Undefined, left_value, right_value)
        let num = to_number(res)
        if Double::is_nan(num) {
          cmp = 0
        } else if num > 0.0 {
          cmp = 1
        } else if num < 0.0 {
          cmp = -1
        } else {
          cmp = 0
        }
      }
    None => {
      let left_str = array_sort_slot_string(slots, left)
      let right_str = array_sort_slot_string(slots, right)
      let raw = string_lex_compare(left_str, right_str)
      if raw < 0 {
        cmp = -1
      } else if raw > 0 {
        cmp = 1
      } else {
        cmp = 0
      }
    }
  }
  if cmp == 0 {
    let left_pos = slots[left].pos
    let right_pos = slots[right].pos
    if left_pos < right_pos {
      -1
    } else if left_pos > right_pos {
      1
    } else {
      0
    }
  } else {
    cmp
  }
}

///|
fn array_sort_slots(
  slots : Array[ArraySortSlot],
  compare_fn : Value?,
) -> Unit raise {
  let _ = with_gc_frame(fn() raise {
    for slot in slots {
      let _ = gc_root_push(slot.value)
    }
    let len = slots.length()
    if len < 2 {
      return Undefined
    }
    let mut gc_tick = 0
    let mut i = 1
    while i < len {
      let mut j = i
      while j > 0 {
        gc_tick = gc_tick + 1
        if Int::land(gc_tick, 1023) == 0 && gc_is_pending() {
          gc_maybe_collect()
        }
        let cmp = array_sort_compare(slots, j - 1, j, compare_fn)
        if cmp <= 0 {
          break
        }
        let tmp = slots[j - 1]
        slots[j - 1] = slots[j]
        slots[j] = tmp
        j = j - 1
      }
      i = i + 1
    }
    Undefined
  })
  ()
}

///|
fn array_sort_object(obj : Value, compare_fn : Value?) -> Value raise {
  let len = array_like_length(obj)
  let slots : Array[ArraySortSlot] = []
  let mut undefined_count = 0
  let mut index = 0
  while index < len {
    let key = Int::to_string(index)
    if has_property(obj, key) {
      let value = property_get(obj, key)
      match value {
        Undefined => undefined_count = undefined_count + 1
        _ => slots.push(ArraySortSlot::{ value, str_cache: None, pos: index })
      }
    }
    index = index + 1
  }
  array_sort_slots(slots, compare_fn)
  match obj {
    Array(arr) =>
      match arr.typed_array_data {
        Some(data) => {
          let write_limit = typed_array_effective_length(data)
          let mut write_index = 0
          for slot in slots {
            if write_index >= write_limit {
              break
            }
            let _ = property_set(
              obj,
              Int::to_string(write_index),
              slot.value,
              true,
            )
            write_index = write_index + 1
          }
          return obj
        }
        None => ()
      }
    _ => ()
  }
  let mut write_index = 0
  for slot in slots {
    let _ = property_set(obj, Int::to_string(write_index), slot.value, true)
    write_index = write_index + 1
  }
  let mut remaining = undefined_count
  while remaining > 0 {
    let _ = property_set(obj, Int::to_string(write_index), Undefined, true)
    write_index = write_index + 1
    remaining = remaining - 1
  }
  while write_index < len {
    delete_property_or_throw(obj, Int::to_string(write_index))
    write_index = write_index + 1
  }
  obj
}

///|
priv struct ArraySortSlot {
  value : Value
  mut str_cache : String?
  pos : Int
}
