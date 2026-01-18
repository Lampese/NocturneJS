///|
fn await_value(value : Value) -> Value raise {
  let promise_ctor = promise_ctor_value_for_current_env()
  let promise = promise_resolve_value(promise_ctor, value, false)
  match promise_data_from_value(promise) {
    Some(data) => {
      run_pending_jobs()
      match data.state {
        PromiseState::Fulfilled => data.value
        PromiseState::Rejected => raise ThrowSignal(data.value)
        PromiseState::Pending => promise
      }
    }
    None => promise
  }
}
