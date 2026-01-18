///|
struct ProxyData {
  mut target : Value
  mut handler : Value
  callable : Bool
  constructable : Bool
}

///|
pub struct ObjectValue {
  id : Int
  props : Map[String, Property]
  mut proto : Value?
  string_data : String?
  number_data : Double?
  bool_data : Bool?
  bigint_data : @bigint.BigInt?
  symbol_data : SymbolValue?
  mut regexp_data : RegexpData?
  mut generator_data : GeneratorData?
  mut async_generator_data : AsyncGeneratorData?
  mut proxy_data : ProxyData?
  mut map_data : MapData?
  mut set_data : SetData?
  mut map_iter_data : MapIteratorData?
  mut set_iter_data : SetIteratorData?
  mut iterator_wrap_data : IteratorWrapData?
  mut iterator_concat_data : IteratorConcatData?
  mut iterator_helper_data : IteratorHelperData?
  mut weakmap_data : WeakMapData?
  mut weakset_data : WeakSetData?
  mut weakref_data : WeakRefData?
  mut finreg_data : FinalizationRegistryData?
  mut date_data : Double?
  mut array_buffer_data : ArrayBufferData?
  mut dataview_data : DataViewData?
  is_html_dda : Bool
  mut is_error : Bool
  mut is_module_namespace : Bool
  mut realm_env : Env?
  mut immutable_proto : Bool
  mut extensible : Bool
}

///|
pub impl Show for ObjectValue with output(self, logger) {
  let _ = self
  logger.write_string("[object Object]")
}

///|
pub impl ToJson for ObjectValue with to_json(self) {
  let _ = self
  Json::string("[object Object]")
}
