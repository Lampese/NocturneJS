///|
fn builtin_name_module(func : BuiltinFunction) -> String {
  match func {
    BuiltinFunction::ModuleImport => "__import"
    BuiltinFunction::ModuleImportDynamic => "__import_dynamic"
    BuiltinFunction::ModuleImportDynamicJob => ""
    BuiltinFunction::ModuleImportBinding => "__import_binding"
    BuiltinFunction::ModuleBinding => "__binding"
    BuiltinFunction::ModuleExport => "__export"
    BuiltinFunction::ModuleExportStar => "__export_star"
    BuiltinFunction::ModuleAsyncFulfilled => ""
    BuiltinFunction::ModuleAsyncRejected => ""
    _ => ""
  }
}

///|
fn builtin_length_module(func : BuiltinFunction) -> Int {
  match func {
    BuiltinFunction::ModuleImport => 1
    BuiltinFunction::ModuleImportDynamic => 1
    BuiltinFunction::ModuleImportDynamicJob => 0
    BuiltinFunction::ModuleImportBinding => 2
    BuiltinFunction::ModuleBinding => 1
    BuiltinFunction::ModuleExport => 2
    BuiltinFunction::ModuleExportStar => 1
    BuiltinFunction::ModuleAsyncFulfilled => 1
    BuiltinFunction::ModuleAsyncRejected => 1
    _ => 0
  }
}
