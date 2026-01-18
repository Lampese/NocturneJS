///|
fn call_builtin_iterator(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match builtin.kind {
    BuiltinFunction::IteratorReturnSelf =>
      iterator_return_self(builtin.this_value)
    BuiltinFunction::AsyncIteratorReturnSelf =>
      iterator_return_self(builtin.this_value)
    BuiltinFunction::AsyncFromSyncIteratorNext =>
      async_from_sync_iterator_step(
        builtin.this_value,
        args,
        async_from_sync_iter_kind_next,
      )
    BuiltinFunction::AsyncFromSyncIteratorReturn =>
      async_from_sync_iterator_step(
        builtin.this_value,
        args,
        async_from_sync_iter_kind_return,
      )
    BuiltinFunction::AsyncFromSyncIteratorThrow =>
      async_from_sync_iterator_step(
        builtin.this_value,
        args,
        async_from_sync_iter_kind_throw,
      )
    BuiltinFunction::AsyncFromSyncIteratorUnwrap =>
      async_from_sync_iterator_unwrap(builtin, args)
    BuiltinFunction::AsyncFromSyncIteratorCloseWrap =>
      async_from_sync_iterator_close_wrap(builtin, args)
    BuiltinFunction::IteratorConstructor =>
      throw_type_error("constructor requires 'new'")
    BuiltinFunction::IteratorFrom =>
      iterator_from_value(if args.is_empty() { Undefined } else { args[0] })
    BuiltinFunction::IteratorConcat => iterator_concat_value(args)
    BuiltinFunction::IteratorConcatNext =>
      iterator_concat_next(builtin.this_value)
    BuiltinFunction::IteratorConcatReturn =>
      iterator_concat_return(builtin.this_value)
    BuiltinFunction::IteratorWrapNext => iterator_wrap_next(builtin.this_value)
    BuiltinFunction::IteratorWrapReturn =>
      iterator_wrap_return(builtin.this_value)
    BuiltinFunction::IteratorHelperNext =>
      iterator_helper_next(builtin.this_value)
    BuiltinFunction::IteratorHelperReturn =>
      iterator_helper_return(builtin.this_value)
    BuiltinFunction::IteratorDrop =>
      iterator_helper_create(
        iterator_helper_kind_drop,
        builtin.this_value,
        args,
      )
    BuiltinFunction::IteratorFilter =>
      iterator_helper_create(
        iterator_helper_kind_filter,
        builtin.this_value,
        args,
      )
    BuiltinFunction::IteratorFlatMap =>
      iterator_helper_create(
        iterator_helper_kind_flat_map,
        builtin.this_value,
        args,
      )
    BuiltinFunction::IteratorMap =>
      iterator_helper_create(iterator_helper_kind_map, builtin.this_value, args)
    BuiltinFunction::IteratorTake =>
      iterator_helper_create(
        iterator_helper_kind_take,
        builtin.this_value,
        args,
      )
    BuiltinFunction::IteratorEvery =>
      iterator_proto_iterate(
        iterator_proto_kind_every,
        builtin.this_value,
        args,
      )
    BuiltinFunction::IteratorFind =>
      iterator_proto_iterate(iterator_proto_kind_find, builtin.this_value, args)
    BuiltinFunction::IteratorForEach =>
      iterator_proto_iterate(
        iterator_proto_kind_for_each,
        builtin.this_value,
        args,
      )
    BuiltinFunction::IteratorSome =>
      iterator_proto_iterate(iterator_proto_kind_some, builtin.this_value, args)
    BuiltinFunction::IteratorReduce =>
      iterator_proto_reduce(builtin.this_value, args)
    BuiltinFunction::IteratorToArray =>
      iterator_proto_to_array(builtin.this_value)
    BuiltinFunction::IteratorConstructorGetter =>
      iterator_constructor_getter(builtin.this_value)
    BuiltinFunction::IteratorConstructorSetter =>
      iterator_constructor_setter(
        builtin.this_value,
        if args.is_empty() {
          Undefined
        } else {
          args[0]
        },
      )
    BuiltinFunction::IteratorToStringTagGetter =>
      iterator_to_string_tag_getter(builtin.this_value)
    BuiltinFunction::IteratorToStringTagSetter =>
      iterator_to_string_tag_setter(
        builtin.this_value,
        if args.is_empty() {
          Undefined
        } else {
          args[0]
        },
      )
    _ => throw_type_error("invalid builtin")
  }
}
