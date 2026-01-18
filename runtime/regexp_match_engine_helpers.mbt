///|
fn match_backreference(
  indices : Array[Int],
  input : String,
  state : MatchState,
  flags : RegexpFlags,
) -> Array[MatchState] {
  let mut chosen : CaptureSpan? = None
  for index in indices {
    if index < 0 || index >= state.captures.length() {
      continue
    }
    match state.captures[index] {
      Some(span) => {
        chosen = Some(span)
        break
      }
      None => ()
    }
  }
  match chosen {
    None => [state]
    Some(span) => {
      let text = input.unsafe_substring(start=span.start, end=span.end)
      match match_string_at(input, state.index, text, flags) {
        Some(next_index) =>
          [MatchState::{ index: next_index, captures: state.captures }]
        None => []
      }
    }
  }
}

///|
fn match_lookbehind(
  inner : RegexNode,
  input : String,
  state : MatchState,
  flags : RegexpFlags,
  positive : Bool,
) -> Array[MatchState] {
  let len = input.length()
  let unicode = flags.unicode || flags.unicode_sets
  let reversed_input = reverse_input_for_flags(input, flags)
  let reversed_inner = reverse_regex_node(inner, unicode)
  let reversed_captures = reverse_captures(len, state.captures)
  let reversed_state = MatchState::{
    index: len - state.index,
    captures: reversed_captures,
  }
  let matches = match_node_with_policy(
    reversed_inner, reversed_input, reversed_state, flags, false,
  )
  if matches.is_empty() {
    if positive {
      []
    } else {
      [state]
    }
  } else if positive {
    let next = matches[0]
    let restored = reverse_captures(len, next.captures)
    [MatchState::{ index: state.index, captures: restored }]
  } else {
    []
  }
}

///|
fn match_node_with_policy(
  node : RegexNode,
  input : String,
  state : MatchState,
  flags : RegexpFlags,
  capture_first : Bool,
) -> Array[MatchState] {
  match node {
    Empty => [state]
    Sequence(nodes) => match_sequence(nodes, input, state, flags, capture_first)
    Alternation(branches) => {
      let results : Array[MatchState] = []
      let all_indices : Array[Int] = []
      for branch in branches {
        collect_capture_indices_into(branch, all_indices)
      }
      for branch in branches {
        let branch_indices = collect_capture_indices(branch)
        let clear_indices = capture_indices_diff(all_indices, branch_indices)
        for
          next in match_node_with_policy(
            branch, input, state, flags, capture_first,
          ) {
          results.push(clear_captures(next, clear_indices))
        }
      }
      results
    }
    Literal(code) =>
      match
        read_codepoint(input, state.index, flags.unicode || flags.unicode_sets) {
        Some((value, next_index)) => {
          let a = canonicalize_codepoint(value, flags)
          let b = canonicalize_codepoint(code, flags)
          if a == b {
            [MatchState::{ index: next_index, captures: state.captures }]
          } else {
            []
          }
        }
        None => []
      }
    Dot =>
      match
        read_codepoint(input, state.index, flags.unicode || flags.unicode_sets) {
        Some((value, next_index)) =>
          if flags.dot_all || !is_line_terminator(value) {
            [MatchState::{ index: next_index, captures: state.captures }]
          } else {
            []
          }
        None => []
      }
    CharClass(set) => {
      let results : Array[MatchState] = []
      for next_index in match_class_set(set, input, state.index, flags) {
        results.push(MatchState::{ index: next_index, captures: state.captures })
      }
      results
    }
    Capture(index, inner) => {
      let results : Array[MatchState] = []
      let start = state.index
      for
        next in match_node_with_policy(
          inner, input, state, flags, capture_first,
        ) {
        let captures = clone_captures(next.captures)
        if !(capture_first && captures[index] is Some(_)) {
          captures[index] = Some(CaptureSpan::{ start, end: next.index })
        }
        results.push(MatchState::{ index: next.index, captures })
      }
      results
    }
    BackReference(indices) => match_backreference(indices, input, state, flags)
    NamedBackReference(_) => []
    NumericBackReference(_) => []
    ScopedFlags(scoped_flags, inner) =>
      match_node_with_policy(inner, input, state, scoped_flags, capture_first)
    LookAhead(inner, positive) => {
      let matches = match_node_with_policy(
        inner, input, state, flags, capture_first,
      )
      if positive {
        if matches.is_empty() {
          []
        } else {
          let first = matches[0]
          [MatchState::{ index: state.index, captures: first.captures }]
        }
      } else if matches.is_empty() {
        [state]
      } else {
        []
      }
    }
    LookBehind(inner, positive) =>
      match_lookbehind(inner, input, state, flags, positive)
    AnchorStart =>
      if state.index == 0 {
        [state]
      } else if flags.multiline {
        match read_codepoint(input, state.index - 1, false) {
          Some((prev, _)) => if is_line_terminator(prev) { [state] } else { [] }
          None => []
        }
      } else {
        []
      }
    AnchorEnd => {
      let len = input.length()
      if state.index == len {
        [state]
      } else if flags.multiline {
        match read_codepoint(input, state.index, false) {
          Some((curr, _)) => if is_line_terminator(curr) { [state] } else { [] }
          None => []
        }
      } else {
        []
      }
    }
    WordBoundary(pos) => {
      let before = if state.index == 0 {
        false
      } else {
        match read_codepoint(input, state.index - 1, false) {
          Some((prev, _)) => is_word_char_for_flags(prev, flags)
          None => false
        }
      }
      let after = match read_codepoint(input, state.index, false) {
        Some((curr, _)) => is_word_char_for_flags(curr, flags)
        None => false
      }
      let boundary = before != after
      if boundary == pos {
        [state]
      } else {
        []
      }
    }
    Repeat(inner, min, max, greedy) =>
      match_repeat(
        inner, input, state, flags, min, max, greedy, capture_first, true,
      )
  }
}

///|
fn match_sequence_iter(
  nodes : Array[RegexNode],
  start : Int,
  input : String,
  state : MatchState,
  flags : RegexpFlags,
  capture_first : Bool,
  emit : (MatchState) -> Bool,
) -> Bool {
  if start >= nodes.length() {
    return emit(state)
  }
  let node = nodes[start]
  match_node_with_policy_iter(node, input, state, flags, capture_first, fn(
    next,
  ) {
    match_sequence_iter(
      nodes,
      start + 1,
      input,
      next,
      flags,
      capture_first,
      emit,
    )
  })
}

///|
fn match_node_with_policy_iter(
  node : RegexNode,
  input : String,
  state : MatchState,
  flags : RegexpFlags,
  capture_first : Bool,
  emit : (MatchState) -> Bool,
) -> Bool {
  match node {
    Empty => emit(state)
    Sequence(nodes) =>
      match_sequence_iter(nodes, 0, input, state, flags, capture_first, emit)
    Alternation(branches) => {
      let all_indices : Array[Int] = []
      for branch in branches {
        collect_capture_indices_into(branch, all_indices)
      }
      for branch in branches {
        let branch_indices = collect_capture_indices(branch)
        let clear_indices = capture_indices_diff(all_indices, branch_indices)
        let stopped = match_node_with_policy_iter(
          branch,
          input,
          state,
          flags,
          capture_first,
          fn(next) { emit(clear_captures(next, clear_indices)) },
        )
        if stopped {
          return true
        }
      }
      false
    }
    Literal(code) =>
      match
        read_codepoint(input, state.index, flags.unicode || flags.unicode_sets) {
        Some((value, next_index)) => {
          let a = canonicalize_codepoint(value, flags)
          let b = canonicalize_codepoint(code, flags)
          if a == b {
            emit(MatchState::{ index: next_index, captures: state.captures })
          } else {
            false
          }
        }
        None => false
      }
    Dot =>
      match
        read_codepoint(input, state.index, flags.unicode || flags.unicode_sets) {
        Some((value, next_index)) =>
          if flags.dot_all || !is_line_terminator(value) {
            emit(MatchState::{ index: next_index, captures: state.captures })
          } else {
            false
          }
        None => false
      }
    CharClass(set) => {
      for next_index in match_class_set(set, input, state.index, flags) {
        if emit(MatchState::{ index: next_index, captures: state.captures }) {
          return true
        }
      }
      false
    }
    Capture(index, inner) => {
      let start = state.index
      match_node_with_policy_iter(inner, input, state, flags, capture_first, fn(
        next,
      ) {
        let captures = clone_captures(next.captures)
        if !(capture_first && captures[index] is Some(_)) {
          captures[index] = Some(CaptureSpan::{ start, end: next.index })
        }
        emit(MatchState::{ index: next.index, captures })
      })
    }
    BackReference(indices) => {
      for next in match_backreference(indices, input, state, flags) {
        if emit(next) {
          return true
        }
      }
      false
    }
    NamedBackReference(_) => false
    NumericBackReference(_) => false
    ScopedFlags(scoped_flags, inner) =>
      match_node_with_policy_iter(
        inner, input, state, scoped_flags, capture_first, emit,
      )
    LookAhead(inner, positive) => {
      let mut first : MatchState? = None
      let _ = match_node_with_policy_iter(
        inner,
        input,
        state,
        flags,
        capture_first,
        fn(next) {
          first = Some(next)
          true
        },
      )
      match (positive, first) {
        (true, Some(next)) =>
          emit(MatchState::{ index: state.index, captures: next.captures })
        (true, None) => false
        (false, Some(_)) => false
        (false, None) => emit(state)
      }
    }
    LookBehind(inner, positive) => {
      for next in match_lookbehind(inner, input, state, flags, positive) {
        if emit(next) {
          return true
        }
      }
      false
    }
    AnchorStart =>
      if state.index == 0 {
        emit(state)
      } else if flags.multiline {
        match read_codepoint(input, state.index - 1, false) {
          Some((prev, _)) =>
            if is_line_terminator(prev) {
              emit(state)
            } else {
              false
            }
          None => false
        }
      } else {
        false
      }
    AnchorEnd => {
      let len = input.length()
      if state.index == len {
        emit(state)
      } else if flags.multiline {
        match read_codepoint(input, state.index, false) {
          Some((curr, _)) =>
            if is_line_terminator(curr) {
              emit(state)
            } else {
              false
            }
          None => false
        }
      } else {
        false
      }
    }
    WordBoundary(pos) => {
      let before = if state.index == 0 {
        false
      } else {
        match read_codepoint(input, state.index - 1, false) {
          Some((prev, _)) => is_word_char_for_flags(prev, flags)
          None => false
        }
      }
      let after = match read_codepoint(input, state.index, false) {
        Some((curr, _)) => is_word_char_for_flags(curr, flags)
        None => false
      }
      let boundary = before != after
      if boundary == pos {
        emit(state)
      } else {
        false
      }
    }
    Repeat(inner, min, max, greedy) =>
      match_repeat_iter(
        inner, input, state, flags, min, max, greedy, capture_first, true, emit,
      )
  }
}

///|
fn match_repeat_iter(
  inner : RegexNode,
  input : String,
  state : MatchState,
  flags : RegexpFlags,
  min : Int,
  max : Int,
  greedy : Bool,
  capture_first : Bool,
  clear_on_zero : Bool,
  emit : (MatchState) -> Bool,
) -> Bool {
  let inner_indices = collect_capture_indices(inner)
  let iter_state = repeat_iteration_state(state, inner_indices)
  match inner {
    LookAhead(_, _) => {
      let zero_state = if min == 0 && clear_on_zero {
        clear_captures(state, inner_indices)
      } else {
        state
      }
      if max == 0 {
        return if min == 0 { emit(zero_state) } else { false }
      }
      if min == 0 {
        if !greedy {
          if emit(zero_state) {
            return true
          }
        }
        let stopped = match_node_with_policy_iter(
          inner,
          input,
          iter_state,
          flags,
          capture_first,
          fn(next) {
            if next.index != state.index {
              emit(next)
            } else {
              false
            }
          },
        )
        if stopped {
          return true
        }
        if greedy {
          return emit(zero_state)
        }
        return false
      }
      return match_node_with_policy_iter(
        inner, input, state, flags, capture_first, emit,
      )
    }
    _ => ()
  }
  match inner {
    CharClass(set) =>
      if !set.strings.is_empty() {
        match
          match_repeat_string_set(
            set, input, state, flags, min, max, greedy, clear_on_zero,
          ) {
          Some(results) => {
            for next in results {
              if emit(next) {
                return true
              }
            }
            return false
          }
          None => ()
        }
      }
    _ => ()
  }
  match
    match_repeat_simple(
      inner, input, state, flags, min, max, greedy, capture_first, clear_on_zero,
    ) {
    Some(results) => {
      for next in results {
        if emit(next) {
          return true
        }
      }
      return false
    }
    None => ()
  }
  let zero_state = if min == 0 && clear_on_zero {
    clear_captures(state, inner_indices)
  } else {
    state
  }
  if max == 0 {
    return if min == 0 { emit(zero_state) } else { false }
  } else if min == 0 {
    if !greedy {
      if emit(zero_state) {
        return true
      }
    }
    let stopped = match_node_with_policy_iter(
      inner,
      input,
      iter_state,
      flags,
      capture_first,
      fn(next) {
        if next.index == state.index {
          false
        } else {
          let next_max = if max == -1 { -1 } else { max - 1 }
          match_repeat_iter(
            inner, input, next, flags, 0, next_max, greedy, capture_first, false,
            emit,
          )
        }
      },
    )
    if stopped {
      return true
    }
    if greedy {
      return emit(zero_state)
    }
    false
  } else {
    match_node_with_policy_iter(
      inner,
      input,
      iter_state,
      flags,
      capture_first,
      fn(next) {
        if next.index == state.index {
          if min <= 1 {
            emit(next)
          } else {
            false
          }
        } else {
          let next_max = if max == -1 { -1 } else { max - 1 }
          match_repeat_iter(
            inner,
            input,
            next,
            flags,
            min - 1,
            next_max,
            greedy,
            capture_first,
            false,
            emit,
          )
        }
      },
    )
  }
}

///|
fn match_sequence_first_at(
  nodes : Array[RegexNode],
  start : Int,
  input : String,
  state : MatchState,
  flags : RegexpFlags,
  capture_first : Bool,
) -> MatchState? {
  if start >= nodes.length() {
    return Some(state)
  }
  match nodes[start] {
    Repeat(inner, min, max, greedy) =>
      match_repeat_first(
        inner,
        min,
        max,
        greedy,
        nodes,
        start + 1,
        input,
        state,
        flags,
        capture_first,
      )
    node => {
      let mut found : MatchState? = None
      let stopped = match_node_with_policy_iter(
        node,
        input,
        state,
        flags,
        capture_first,
        fn(next) {
          match
            match_sequence_first_at(
              nodes,
              start + 1,
              input,
              next,
              flags,
              capture_first,
            ) {
            Some(result) => {
              found = Some(result)
              true
            }
            None => false
          }
        },
      )
      if stopped {
        return found
      }
      None
    }
  }
}

///|
fn match_repeat_first(
  inner : RegexNode,
  min : Int,
  max : Int,
  greedy : Bool,
  nodes : Array[RegexNode],
  next_index : Int,
  input : String,
  state : MatchState,
  flags : RegexpFlags,
  capture_first : Bool,
) -> MatchState? {
  let inner_indices = collect_capture_indices(inner)
  let iter_state = repeat_iteration_state(state, inner_indices)
  match inner {
    LookAhead(_, _) => {
      let zero_state = if min == 0 {
        clear_captures(state, inner_indices)
      } else {
        state
      }
      if max == 0 {
        if min == 0 {
          return match_sequence_first_at(
            nodes, next_index, input, zero_state, flags, capture_first,
          )
        }
        return None
      }
      if min == 0 {
        return match_sequence_first_at(
          nodes, next_index, input, zero_state, flags, capture_first,
        )
      }
      let mut found : MatchState? = None
      let stopped = match_node_with_policy_iter(
        inner,
        input,
        iter_state,
        flags,
        capture_first,
        fn(next) {
          match
            match_sequence_first_at(
              nodes, next_index, input, next, flags, capture_first,
            ) {
            Some(result) => {
              found = Some(result)
              true
            }
            None => false
          }
        },
      )
      if stopped {
        return found
      }
      return None
    }
    _ => ()
  }
  if is_simple_repeat_node(inner) {
    return match_repeat_first_simple(
      inner, min, max, greedy, nodes, next_index, input, state, flags, capture_first,
    )
  }
  fn repeat_step(
    inner : RegexNode,
    min : Int,
    max : Int,
    greedy : Bool,
    nodes : Array[RegexNode],
    next_index : Int,
    input : String,
    state : MatchState,
    flags : RegexpFlags,
    capture_first : Bool,
    count : Int,
  ) -> MatchState? {
    let iter_state = repeat_iteration_state(state, inner_indices)
    if greedy {
      if max == -1 || count < max {
        let mut found : MatchState? = None
        let stopped = match_node_with_policy_iter(
          inner,
          input,
          iter_state,
          flags,
          capture_first,
          fn(next) {
            if next.index == state.index {
              if min > 0 && count + 1 >= min {
                match
                  match_sequence_first_at(
                    nodes, next_index, input, next, flags, capture_first,
                  ) {
                  Some(result) => {
                    found = Some(result)
                    true
                  }
                  None => false
                }
              } else {
                false
              }
            } else {
              match
                repeat_step(
                  inner,
                  min,
                  max,
                  greedy,
                  nodes,
                  next_index,
                  input,
                  next,
                  flags,
                  capture_first,
                  count + 1,
                ) {
                Some(result) => {
                  found = Some(result)
                  true
                }
                None => false
              }
            }
          },
        )
        if stopped {
          return found
        }
      }
      if count >= min {
        return match_sequence_first_at(
          nodes, next_index, input, state, flags, capture_first,
        )
      }
      None
    } else {
      if count >= min {
        match
          match_sequence_first_at(
            nodes, next_index, input, state, flags, capture_first,
          ) {
          Some(result) => return Some(result)
          None => ()
        }
      }
      if max != -1 && count >= max {
        return None
      }
      let mut found : MatchState? = None
      let stopped = match_node_with_policy_iter(
        inner,
        input,
        iter_state,
        flags,
        capture_first,
        fn(next) {
          if next.index == state.index {
            if min > 0 && count + 1 >= min {
              match
                match_sequence_first_at(
                  nodes, next_index, input, next, flags, capture_first,
                ) {
                Some(result) => {
                  found = Some(result)
                  true
                }
                None => false
              }
            } else {
              false
            }
          } else {
            match
              repeat_step(
                inner,
                min,
                max,
                greedy,
                nodes,
                next_index,
                input,
                next,
                flags,
                capture_first,
                count + 1,
              ) {
              Some(result) => {
                found = Some(result)
                true
              }
              None => false
            }
          }
        },
      )
      if stopped {
        return found
      }
      None
    }
  }

  repeat_step(
    inner, min, max, greedy, nodes, next_index, input, state, flags, capture_first,
    0,
  )
}

///|
fn is_simple_repeat_node(node : RegexNode) -> Bool {
  match node {
    Literal(_) => true
    Dot => true
    CharClass(set) => set.strings.is_empty()
    _ => false
  }
}

///|
fn match_repeat_first_simple(
  inner : RegexNode,
  min : Int,
  max : Int,
  greedy : Bool,
  nodes : Array[RegexNode],
  next_index : Int,
  input : String,
  state : MatchState,
  flags : RegexpFlags,
  capture_first : Bool,
) -> MatchState? {
  let positions : Array[Int] = [state.index]
  let mut count = 0
  let mut current = state.index
  while max == -1 || count < max {
    match match_simple_repeat_next(inner, input, current, flags) {
      Some(next_index) => {
        if next_index == current {
          break
        }
        positions.push(next_index)
        current = next_index
        count = count + 1
      }
      None => break
    }
  }
  let total = positions.length() - 1
  if total < min {
    return None
  }
  if greedy {
    let mut i = total
    while true {
      if i < min {
        break
      }
      let idx = positions[i]
      let next_state = MatchState::{ index: idx, captures: state.captures }
      match
        match_sequence_first_at(
          nodes, next_index, input, next_state, flags, capture_first,
        ) {
        Some(result) => return Some(result)
        None => ()
      }
      if i == min {
        break
      }
      i = i - 1
    }
  } else {
    let mut i = min
    while i <= total {
      let idx = positions[i]
      let next_state = MatchState::{ index: idx, captures: state.captures }
      match
        match_sequence_first_at(
          nodes, next_index, input, next_state, flags, capture_first,
        ) {
        Some(result) => return Some(result)
        None => ()
      }
      i = i + 1
    }
  }
  None
}

///|
fn match_node_first(
  node : RegexNode,
  input : String,
  state : MatchState,
  flags : RegexpFlags,
) -> MatchState? {
  match node {
    Sequence(nodes) =>
      match_sequence_first_at(nodes, 0, input, state, flags, false)
    _ => match_sequence_first_at([node], 0, input, state, flags, false)
  }
}

///|
fn match_sequence(
  nodes : Array[RegexNode],
  input : String,
  state : MatchState,
  flags : RegexpFlags,
  capture_first : Bool,
) -> Array[MatchState] {
  let mut states : Array[MatchState] = [state]
  for node in nodes {
    let next_states : Array[MatchState] = []
    for current in states {
      for
        next in match_node_with_policy(
          node, input, current, flags, capture_first,
        ) {
        next_states.push(next)
      }
    }
    states = next_states
    if states.is_empty() {
      break
    }
  }
  states
}

///|
fn match_simple_repeat_next(
  inner : RegexNode,
  input : String,
  index : Int,
  flags : RegexpFlags,
) -> Int? {
  match inner {
    Literal(code) =>
      match read_codepoint(input, index, flags.unicode || flags.unicode_sets) {
        Some((value, next_index)) => {
          let a = canonicalize_codepoint(value, flags)
          let b = canonicalize_codepoint(code, flags)
          if a == b {
            Some(next_index)
          } else {
            None
          }
        }
        None => None
      }
    Dot =>
      match read_codepoint(input, index, flags.unicode || flags.unicode_sets) {
        Some((value, next_index)) =>
          if flags.dot_all || !is_line_terminator(value) {
            Some(next_index)
          } else {
            None
          }
        None => None
      }
    CharClass(set) =>
      if !set.strings.is_empty() {
        None
      } else {
        match
          read_codepoint(input, index, flags.unicode || flags.unicode_sets) {
          Some((codepoint, next_index)) => {
            let check = canonicalize_codepoint(codepoint, flags)
            if class_set_contains_range(set, check) {
              Some(next_index)
            } else {
              None
            }
          }
          None => None
        }
      }
    _ => None
  }
}

///|
fn match_repeat_simple(
  inner : RegexNode,
  input : String,
  state : MatchState,
  flags : RegexpFlags,
  min : Int,
  max : Int,
  greedy : Bool,
  capture_first : Bool,
  clear_on_zero : Bool,
) -> Array[MatchState]? {
  let _ = capture_first
  match match_simple_repeat_next(inner, input, state.index, flags) {
    None => None
    Some(_) => {
      if max == 0 {
        if min == 0 {
          let zero_state = if clear_on_zero {
            clear_captures(state, collect_capture_indices(inner))
          } else {
            state
          }
          return Some([zero_state])
        }
        return Some([])
      }
      let zero_state = if min == 0 && clear_on_zero {
        clear_captures(state, collect_capture_indices(inner))
      } else {
        state
      }
      let positions : Array[Int] = [state.index]
      let mut count = 0
      let mut current = state.index
      while max == -1 || count < max {
        match match_simple_repeat_next(inner, input, current, flags) {
          Some(next_index) => {
            if next_index == current {
              break
            }
            positions.push(next_index)
            current = next_index
            count = count + 1
          }
          None => break
        }
      }
      let total = positions.length() - 1
      if total < min {
        return Some([])
      }
      let results : Array[MatchState] = []
      if greedy {
        let mut i = total
        while true {
          if i < min {
            break
          }
          let idx = positions[i]
          if i == 0 {
            results.push(MatchState::{
              index: idx,
              captures: zero_state.captures,
            })
          } else {
            results.push(MatchState::{ index: idx, captures: state.captures })
          }
          if i == min {
            break
          }
          i = i - 1
        }
      } else {
        let mut i = min
        while i <= total {
          let idx = positions[i]
          if i == 0 {
            results.push(MatchState::{
              index: idx,
              captures: zero_state.captures,
            })
          } else {
            results.push(MatchState::{ index: idx, captures: state.captures })
          }
          i = i + 1
        }
      }
      Some(results)
    }
  }
}

///|
fn match_repeat_string_set(
  set : ClassSet,
  input : String,
  state : MatchState,
  flags : RegexpFlags,
  min : Int,
  max : Int,
  greedy : Bool,
  clear_on_zero : Bool,
) -> Array[MatchState]? {
  if class_set_has_empty_string(set) {
    return None
  }
  if max == 0 {
    if min == 0 {
      let zero_state = if clear_on_zero {
        clear_captures(state, collect_capture_indices(CharClass(set)))
      } else {
        state
      }
      return Some([zero_state])
    }
    return Some([])
  }
  let zero_state = if min == 0 && clear_on_zero {
    clear_captures(state, collect_capture_indices(CharClass(set)))
  } else {
    state
  }
  let input_len = input.length()
  let max_count = if max == -1 || max > input_len { input_len } else { max }
  let positions_by_count : Array[Array[Int]] = [[state.index]]
  let next_cache : Map[Int, Array[Int]] = Map::new()
  let mut count = 0
  while count < max_count {
    let current = positions_by_count[count]
    if current.is_empty() {
      break
    }
    let next_positions : Array[Int] = []
    let seen : Map[Int, Bool] = Map::new()
    for pos in current {
      let nexts = match next_cache.get(pos) {
        Some(value) => value
        None => {
          let computed = match_class_set(set, input, pos, flags)
          next_cache.set(pos, computed)
          computed
        }
      }
      for next in nexts {
        if next == pos {
          continue
        }
        if !seen.contains(next) {
          seen.set(next, true)
          next_positions.push(next)
        }
      }
    }
    positions_by_count.push(next_positions)
    count = count + 1
  }
  let max_reached = positions_by_count.length() - 1
  if max_reached < min {
    return Some([])
  }
  let results : Array[MatchState] = []
  if greedy {
    let mut i = max_reached
    while true {
      if i < min {
        break
      }
      let positions = positions_by_count[i]
      for idx in positions {
        if i == 0 {
          results.push(MatchState::{ index: idx, captures: zero_state.captures })
        } else {
          results.push(MatchState::{ index: idx, captures: state.captures })
        }
      }
      if i == min {
        break
      }
      i = i - 1
    }
  } else {
    let mut i = min
    while i <= max_reached {
      let positions = positions_by_count[i]
      for idx in positions {
        if i == 0 {
          results.push(MatchState::{ index: idx, captures: zero_state.captures })
        } else {
          results.push(MatchState::{ index: idx, captures: state.captures })
        }
      }
      i = i + 1
    }
  }
  Some(results)
}

///|
fn repeat_iteration_state(
  state : MatchState,
  inner_indices : Array[Int],
) -> MatchState {
  if inner_indices.is_empty() {
    state
  } else {
    clear_captures(state, inner_indices)
  }
}

///|
fn match_repeat(
  inner : RegexNode,
  input : String,
  state : MatchState,
  flags : RegexpFlags,
  min : Int,
  max : Int,
  greedy : Bool,
  capture_first : Bool,
  clear_on_zero : Bool,
) -> Array[MatchState] {
  let inner_indices = collect_capture_indices(inner)
  let iter_state = repeat_iteration_state(state, inner_indices)
  match inner {
    LookAhead(_, _) => {
      let zero_state = if min == 0 && clear_on_zero {
        clear_captures(state, inner_indices)
      } else {
        state
      }
      if max == 0 {
        return if min == 0 { [zero_state] } else { [] }
      }
      if min == 0 {
        let results : Array[MatchState] = []
        if !greedy {
          results.push(zero_state)
        }
        let next_states = match_node_with_policy(
          inner, input, iter_state, flags, capture_first,
        )
        for next in next_states {
          if next.index != state.index {
            results.push(next)
          }
        }
        if greedy {
          results.push(zero_state)
        }
        return results
      } else {
        return match_node_with_policy(inner, input, state, flags, capture_first)
      }
    }
    _ => ()
  }
  match inner {
    CharClass(set) =>
      if !set.strings.is_empty() {
        match
          match_repeat_string_set(
            set, input, state, flags, min, max, greedy, clear_on_zero,
          ) {
          Some(results) => return results
          None => ()
        }
      }
    _ => ()
  }
  match
    match_repeat_simple(
      inner, input, state, flags, min, max, greedy, capture_first, clear_on_zero,
    ) {
    Some(results) => return results
    None => ()
  }
  let zero_state = if min == 0 && clear_on_zero {
    clear_captures(state, inner_indices)
  } else {
    state
  }
  if max == 0 {
    if min == 0 {
      [zero_state]
    } else {
      []
    }
  } else if min == 0 {
    let results : Array[MatchState] = []
    if !greedy {
      results.push(zero_state)
    }
    let next_states = match_node_with_policy(
      inner, input, iter_state, flags, capture_first,
    )
    for next in next_states {
      if next.index != state.index {
        let next_max = if max == -1 { -1 } else { max - 1 }
        for
          tail in match_repeat(
            inner, input, next, flags, 0, next_max, greedy, capture_first, false,
          ) {
          results.push(tail)
        }
      }
    }
    if greedy {
      results.push(zero_state)
    }
    results
  } else {
    let results : Array[MatchState] = []
    let next_states = match_node_with_policy(
      inner, input, iter_state, flags, capture_first,
    )
    for next in next_states {
      if next.index == state.index {
        if min <= 1 {
          results.push(next)
        }
      } else {
        let next_max = if max == -1 { -1 } else { max - 1 }
        for
          tail in match_repeat(
            inner,
            input,
            next,
            flags,
            min - 1,
            next_max,
            greedy,
            capture_first,
            false,
          ) {
          results.push(tail)
        }
      }
    }
    results
  }
}

///|
fn regexp_match_from_program(
  program : RegexpProgram,
  input : String,
  start : Int,
  sticky : Bool,
) -> RegexpMatch? {
  let len = input.length()
  if start > len {
    return None
  }
  let mut index = start
  while true {
    let initial = MatchState::{
      index,
      captures: Array::make(program.capture_count, None),
    }
    match match_node_first(program.root, input, initial, program.flags) {
      Some(chosen) => {
        let start_index = index
        let end_index = chosen.index
        let groups : Array[String?] = []
        groups.push(
          Some(input.unsafe_substring(start=start_index, end=end_index)),
        )
        for entry in chosen.captures {
          match entry {
            Some(span) =>
              groups.push(
                Some(input.unsafe_substring(start=span.start, end=span.end)),
              )
            None => groups.push(None)
          }
        }
        let indices = chosen.captures
        return Some(RegexpMatch::{
          start: start_index,
          end: end_index,
          groups,
          indices,
        })
      }
      None => ()
    }
    if sticky {
      return None
    }
    if index >= len {
      return None
    }
    index = advance_string_index(
      input,
      index,
      program.flags.unicode || program.flags.unicode_sets,
    )
  }
  None
}
