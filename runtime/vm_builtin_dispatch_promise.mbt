///|
fn call_builtin_promise(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match builtin.kind {
    BuiltinFunction::PromiseConstructor =>
      throw_type_error("constructor requires 'new'")
    BuiltinFunction::PromiseResolve => {
      let value = if args.is_empty() { Undefined } else { args[0] }
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      promise_resolve_value(this_value, value, false)
    }
    BuiltinFunction::PromiseReject => {
      let value = if args.is_empty() { Undefined } else { args[0] }
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      promise_resolve_value(this_value, value, true)
    }
    BuiltinFunction::PromiseThen => {
      let data = require_promise_data(builtin.this_value)
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let on_fulfilled = if args.is_empty() { Undefined } else { args[0] }
      let on_rejected = if args.length() > 1 { args[1] } else { Undefined }
      let ctor = promise_species_constructor(this_value)
      let (result_promise, resolve, reject) = new_promise_capability(ctor)
      perform_promise_then(
        this_value, data, on_fulfilled, on_rejected, resolve, reject,
      )
      result_promise
    }
    BuiltinFunction::PromiseCatch => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let on_rejected = if args.is_empty() { Undefined } else { args[0] }
      let target = to_object(this_value)
      let then_method = property_get_with_receiver(target, "then", this_value)
      if !is_callable(then_method) {
        return throw_type_error("not a function")
      }
      call_value_with_this(then_method, [Undefined, on_rejected], this_value)
    }
    BuiltinFunction::PromiseFinally => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let on_finally = if args.is_empty() { Undefined } else { args[0] }
      let ctor = promise_species_constructor(this_value)
      let mut then_fulfill = Undefined
      let mut then_reject = Undefined
      if !is_callable(on_finally) {
        then_fulfill = on_finally
        then_reject = on_finally
      } else {
        then_fulfill = new_promise_then_finally_function(PromiseThenFinallyData::{
          ctor,
          on_finally,
          is_reject: false,
        })
        then_reject = new_promise_then_finally_function(PromiseThenFinallyData::{
          ctor,
          on_finally,
          is_reject: true,
        })
      }
      let target = to_object(this_value)
      let then_method = property_get_with_receiver(target, "then", this_value)
      if !is_callable(then_method) {
        return throw_type_error("not a function")
      }
      call_value_with_this(then_method, [then_fulfill, then_reject], this_value)
    }
    BuiltinFunction::PromiseReactionJob => {
      let resolve = if args.is_empty() { Undefined } else { args[0] }
      let reject = if args.length() > 1 { args[1] } else { Undefined }
      let handler = if args.length() > 2 { args[2] } else { Undefined }
      let is_reject = if args.length() > 3 { is_truthy(args[3]) } else { false }
      let mut result = if args.length() > 4 { args[4] } else { Undefined }
      let mut rejected = is_reject
      if !(handler is Undefined) {
        try {
          result = call_value_with_this(handler, [result], Undefined)
          rejected = false
        } catch {
          ThrowSignal(value) => {
            result = value
            rejected = true
          }
          err => raise err
        }
      }
      let target = if rejected { reject } else { resolve }
      if target is Undefined {
        return Undefined
      }
      let _ = call_value_with_this(target, [result], Undefined)
      Undefined
    }
    BuiltinFunction::PromiseResolveFunction =>
      match promise_resolve_func_data_get(builtin.id) {
        Some(data) => {
          if data.shared.already_resolved {
            return Undefined
          }
          data.shared.already_resolved = true
          let resolution = if args.is_empty() { Undefined } else { args[0] }
          if data.is_reject || !is_object_like(resolution) {
            let state = if data.is_reject {
              PromiseState::Rejected
            } else {
              PromiseState::Fulfilled
            }
            match promise_data_from_value(data.promise) {
              Some(promise_data) =>
                promise_settle(promise_data, state, resolution)
              None => ()
            }
            return Undefined
          }
          if strict_eq(resolution, data.promise) {
            let error = type_error_value("promise self resolution")
            match promise_data_from_value(data.promise) {
              Some(promise_data) =>
                promise_settle(promise_data, PromiseState::Rejected, error)
              None => ()
            }
            return Undefined
          }
          let mut then_value = Undefined
          try {
            then_value = property_get(resolution, "then")
          } catch {
            ThrowSignal(value) => {
              match promise_data_from_value(data.promise) {
                Some(promise_data) =>
                  promise_settle(promise_data, PromiseState::Rejected, value)
                None => ()
              }
              return Undefined
            }
            err => raise err
          }
          if !is_callable(then_value) {
            match promise_data_from_value(data.promise) {
              Some(promise_data) =>
                promise_settle(
                  promise_data,
                  PromiseState::Fulfilled,
                  resolution,
                )
              None => ()
            }
            return Undefined
          }
          enqueue_job(
            new_builtin_value(BuiltinFunction::PromiseResolveThenableJob),
            [data.promise, resolution, then_value],
          )
          Undefined
        }
        None => Undefined
      }
    BuiltinFunction::PromiseRejectFunction =>
      call_builtin_with_this(
        BuiltinValue::{
          kind: BuiltinFunction::PromiseResolveFunction,
          id: builtin.id,
          props: builtin.props,
          this_value: builtin.this_value,
          proto: builtin.proto,
          extensible: builtin.extensible,
          is_html_dda: builtin.is_html_dda,
          realm_env: builtin.realm_env,
        },
        args,
        Undefined,
      )
    BuiltinFunction::PromiseResolveThenableJob => {
      let promise = if args.is_empty() { Undefined } else { args[0] }
      let thenable = if args.length() > 1 { args[1] } else { Undefined }
      let then_func = if args.length() > 2 { args[2] } else { Undefined }
      let (resolve, reject) = create_promise_resolving_functions(promise)
      try {
        let _ = call_value_with_this(then_func, [resolve, reject], thenable)

      } catch {
        ThrowSignal(value) => {
          let _ = call_value_with_this(reject, [value], Undefined)

        }
        err => raise err
      }
      Undefined
    }
    BuiltinFunction::PromiseAll => {
      let iterable = if args.is_empty() { Undefined } else { args[0] }
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      promise_all_value(this_value, iterable, 0)
    }
    BuiltinFunction::PromiseAllSettled => {
      let iterable = if args.is_empty() { Undefined } else { args[0] }
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      promise_all_value(this_value, iterable, 1)
    }
    BuiltinFunction::PromiseAny => {
      let iterable = if args.is_empty() { Undefined } else { args[0] }
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      promise_all_value(this_value, iterable, 2)
    }
    BuiltinFunction::PromiseRace => {
      let iterable = if args.is_empty() { Undefined } else { args[0] }
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      promise_race_value(this_value, iterable)
    }
    BuiltinFunction::PromiseTry => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      promise_try_value(this_value, args)
    }
    BuiltinFunction::PromiseWithResolvers => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      promise_with_resolvers_value(this_value)
    }
    BuiltinFunction::PromiseAllResolveElement =>
      match promise_all_element_data_get(builtin.id) {
        Some(data) => {
          if data.shared.already_called {
            return Undefined
          }
          data.shared.already_called = true
          let value = if args.is_empty() { Undefined } else { args[0] }
          let result = if data.mode == 1 {
            let obj = new_object_value()
            let status = if data.is_reject { "rejected" } else { "fulfilled" }
            create_data_property_or_throw(obj, "status", String(status))
            let name = if data.is_reject { "reason" } else { "value" }
            create_data_property_or_throw(obj, name, value)
            obj
          } else {
            value
          }
          set_array_index(data.values, data.index, result)
          if promise_all_remaining_add(data.remaining, -1) {
            if data.mode == 2 {
              let error = new_aggregate_error_value([data.values])
              let _ = call_value_with_this(
                data.result_resolve,
                [error],
                Undefined,
              )

            } else {
              let _ = call_value_with_this(
                data.result_resolve,
                [data.values],
                Undefined,
              )

            }
          }
          Undefined
        }
        None => Undefined
      }
    BuiltinFunction::PromiseThenFinally =>
      match promise_then_finally_data_get(builtin.id) {
        Some(data) => {
          let value = if args.is_empty() { Undefined } else { args[0] }
          let result = call_value_with_this(data.on_finally, [], Undefined)
          let promise = promise_resolve_value(data.ctor, result, false)
          let then_func = if data.is_reject {
            new_promise_finally_thrower(value)
          } else {
            new_promise_finally_value_thunk(value)
          }
          let then_method = property_get(promise, "then")
          if !is_callable(then_method) {
            return throw_type_error("not a function")
          }
          call_value_with_this(then_method, [then_func], promise)
        }
        None => Undefined
      }
    BuiltinFunction::PromiseFinallyValueThunk =>
      match promise_finally_value_data_get(builtin.id) {
        Some(value) => value
        None => Undefined
      }
    BuiltinFunction::PromiseFinallyThrower => {
      let value = match promise_finally_value_data_get(builtin.id) {
        Some(value) => value
        None => Undefined
      }
      raise ThrowSignal(value)
    }
    BuiltinFunction::PromiseCapabilityExecutor => {
      match promise_executor_data_get(builtin.id) {
        Some(data) => {
          let resolve_set = match data.resolve {
            Some(value) => !(value is Undefined)
            None => false
          }
          let reject_set = match data.reject {
            Some(value) => !(value is Undefined)
            None => false
          }
          if resolve_set || reject_set {
            return throw_type_error("resolving function already set")
          }
          let resolve = if args.is_empty() { Undefined } else { args[0] }
          let reject = if args.length() > 1 { args[1] } else { Undefined }
          rc_replace_optional_value(data.resolve, Some(resolve))
          data.resolve = Some(resolve)
          rc_replace_optional_value(data.reject, Some(reject))
          data.reject = Some(reject)
        }
        None => ()
      }
      Undefined
    }
    _ => throw_type_error("invalid builtin")
  }
}
