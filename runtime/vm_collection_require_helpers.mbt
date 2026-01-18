///|
fn require_map(this_value : Value?) -> (ObjectValue, MapData) raise {
  match this_value {
    Some(Object(obj)) =>
      match obj.map_data {
        Some(data) => (obj, data)
        None => {
          let _ = throw_type_error("not a map")
          (obj, MapData::{ entries: [] })
        }
      }
    _ => {
      let _ = throw_type_error("not a map")
      (new_object_struct(None), MapData::{ entries: [] })
    }
  }
}

///|
fn require_set(this_value : Value?) -> (ObjectValue, SetData) raise {
  match this_value {
    Some(Object(obj)) =>
      match obj.set_data {
        Some(data) => (obj, data)
        None => {
          let _ = throw_type_error("not a set")
          (obj, SetData::{ entries: [] })
        }
      }
    _ => {
      let _ = throw_type_error("not a set")
      (new_object_struct(None), SetData::{ entries: [] })
    }
  }
}

///|
fn require_weakmap(this_value : Value?) -> (ObjectValue, WeakMapData) raise {
  match this_value {
    Some(Object(obj)) =>
      match obj.weakmap_data {
        Some(data) => (obj, data)
        None => {
          let _ = throw_type_error("not a weakmap")
          (obj, WeakMapData::{ entries: [] })
        }
      }
    _ => {
      let _ = throw_type_error("not a weakmap")
      (new_object_struct(None), WeakMapData::{ entries: [] })
    }
  }
}

///|
fn require_weakset(this_value : Value?) -> (ObjectValue, WeakSetData) raise {
  match this_value {
    Some(Object(obj)) =>
      match obj.weakset_data {
        Some(data) => (obj, data)
        None => {
          let _ = throw_type_error("not a weakset")
          (obj, WeakSetData::{ entries: [] })
        }
      }
    _ => {
      let _ = throw_type_error("not a weakset")
      (new_object_struct(None), WeakSetData::{ entries: [] })
    }
  }
}

///|
fn require_weakref(this_value : Value?) -> WeakRefData raise {
  match this_value {
    Some(Object(obj)) =>
      match obj.weakref_data {
        Some(data) => data
        None => {
          let _ = throw_type_error("not a WeakRef")
          WeakRefData::{ target: Undefined }
        }
      }
    _ => {
      let _ = throw_type_error("not a WeakRef")
      WeakRefData::{ target: Undefined }
    }
  }
}

///|
fn require_finreg(this_value : Value?) -> FinalizationRegistryData raise {
  let fallback_env = match root_env() {
    Some(env) => env
    None => Env::new(None)
  }
  match this_value {
    Some(Object(obj)) =>
      match obj.finreg_data {
        Some(data) => data
        None => {
          let _ = throw_type_error("not a FinalizationRegistry")
          FinalizationRegistryData::{
            callback: Undefined,
            entries: [],
            realm_env: fallback_env,
          }
        }
      }
    _ => {
      let _ = throw_type_error("not a FinalizationRegistry")
      FinalizationRegistryData::{
        callback: Undefined,
        entries: [],
        realm_env: fallback_env,
      }
    }
  }
}
