///|
enum TestMode {
  DefaultNoStrict
  DefaultStrict
  NoStrict
  Strict
  All
} derive(Eq)

///|
enum ConfigSection {
  None
  Config
  Exclude
  Features
  Tests
}

///|
struct RunnerConfig {
  mut new_style : Bool
  mut test_mode : TestMode
  mut skip_async : Bool
  mut skip_module : Bool
  mut verbose : Bool
  mut harness_dir : String
  harness_exclude : Map[String, Bool]
  features : Map[String, Bool]
  skip_features : Map[String, Bool]
  tests : Array[String]
  excludes : Array[String]
}

///|
struct TestMeta {
  mut is_nostrict : Bool
  mut is_onlystrict : Bool
  mut is_negative : Bool
  mut is_async : Bool
  mut is_module : Bool
  mut can_block : Bool
  mut skip : Bool
  mut error_type : String?
  includes : Array[String]
}

///|
enum TestOutcome {
  Passed
  Failed
  Skipped
  Excluded
}

///|
fn default_config() -> RunnerConfig {
  RunnerConfig::{
    new_style: false,
    test_mode: TestMode::DefaultNoStrict,
    skip_async: false,
    skip_module: false,
    verbose: false,
    harness_dir: "",
    harness_exclude: Map::new(),
    features: Map::new(),
    skip_features: Map::new(),
    tests: [],
    excludes: [],
  }
}

///|
fn map_has(map : Map[String, Bool], key : String) -> Bool {
  match map.get(key) {
    Some(_) => true
    None => false
  }
}

///|
fn compose_path(base : String, name : String) -> String {
  if name.is_empty() {
    return name
  }
  let target = @path.Path(name)
  if base.is_empty() || base == "." || target.is_absolute() {
    return name
  }
  let base_path = @path.Path(base)
  base_path.join(target).to_string()
}

///|
fn split_words(value : String) -> Array[String] {
  let words : Array[String] = []
  let len = value.length()
  let mut start = -1
  let mut i = 0
  while i < len {
    let unit = UInt16::to_int(value.code_unit_at(i))
    let is_sep = unit == Char::to_int(' ') ||
      unit == Char::to_int('\t') ||
      unit == Char::to_int('\r') ||
      unit == Char::to_int('\n') ||
      unit == Char::to_int(',')
    if is_sep {
      if start >= 0 {
        words.push(value.unsafe_substring(start~, end=i))
        start = -1
      }
    } else if start < 0 {
      start = i
    }
    i = i + 1
  }
  if start >= 0 {
    words.push(value.unsafe_substring(start~, end=len))
  }
  words
}

///|
fn split_key_value(line : String) -> (String, String?) {
  match line.find("=") {
    Some(idx) => {
      let key = line.unsafe_substring(start=0, end=idx).trim().to_string()
      let value = line
        .unsafe_substring(start=idx + 1, end=line.length())
        .trim()
        .to_string()
      (key, Some(value))
    }
    None => (line, None)
  }
}

///|
fn load_list_file(path : String, out : Array[String]) -> Unit raise {
  let base = @path.Path(path).dirname().to_string()
  let text = @fs.read_file_to_string(path)
  for line in text.split("\n") {
    let trimmed = line.trim().to_string()
    if trimmed.is_empty() || trimmed.has_prefix("#") || trimmed.has_prefix(";") {
      continue
    }
    out.push(compose_path(base, trimmed))
  }
}

///|
fn try_read_dir(path : String) -> Array[String]? {
  try {
    if @fs.is_dir(path) {
      Some(@fs.read_dir(path))
    } else {
      None
    }
  } catch {
    _ => None
  }
}

///|
fn enumerate_tests(path : String, out : Array[String]) -> Unit {
  match try_read_dir(path) {
    Some(entries) => {
      entries.sort()
      for name in entries {
        if name == "." || name == ".." {
          continue
        }
        let full = compose_path(path, name)
        match try_read_dir(full) {
          Some(_) => enumerate_tests(full, out)
          None =>
            if full.has_suffix(".js") && !full.has_suffix("_FIXTURE.js") {
              out.push(full)
            }
        }
      }
    }
    None => ()
  }
}

///|
fn parse_config(path : String, ignore_testdir : Bool) -> RunnerConfig raise {
  let cfg = default_config()
  let base = @path.Path(path).dirname().to_string()
  let mut section = ConfigSection::None
  let text = @fs.read_file_to_string(path)
  for raw_line in text.split("\n") {
    let trimmed = raw_line.trim().to_string()
    if trimmed.is_empty() || trimmed.has_prefix("#") || trimmed.has_prefix(";") {
      continue
    }
    if trimmed.has_prefix("[") {
      match trimmed.find("]") {
        Some(end) => {
          let name = trimmed.unsafe_substring(start=1, end~).trim().to_string()
          section = match name {
            "config" => ConfigSection::Config
            "exclude" => ConfigSection::Exclude
            "features" => ConfigSection::Features
            "tests" => ConfigSection::Tests
            _ => ConfigSection::None
          }
        }
        None => section = ConfigSection::None
      }
      continue
    }
    match section {
      ConfigSection::Config => {
        let (key, value_opt) = split_key_value(trimmed)
        match value_opt {
          Some(value) =>
            match key {
              "style" => cfg.new_style = value == "new"
              "testdir" =>
                if !ignore_testdir {
                  let testdir = compose_path(base, value)
                  enumerate_tests(testdir, cfg.tests)
                }
              "harnessdir" => cfg.harness_dir = compose_path(base, value)
              "harnessexclude" =>
                for word in split_words(value) {
                  cfg.harness_exclude.set(word, true)
                }
              "features" =>
                for word in split_words(value) {
                  cfg.features.set(word, true)
                }
              "skip-features" =>
                for word in split_words(value) {
                  cfg.skip_features.set(word, true)
                }
              "mode" =>
                cfg.test_mode = match value {
                  "default" | "default-nostrict" => TestMode::DefaultNoStrict
                  "default-strict" => TestMode::DefaultStrict
                  "nostrict" => TestMode::NoStrict
                  "strict" => TestMode::Strict
                  "all" | "both" => TestMode::All
                  _ => cfg.test_mode
                }
              "strict" =>
                if value == "skip" || value == "no" {
                  cfg.test_mode = TestMode::NoStrict
                }
              "nostrict" =>
                if value == "skip" || value == "no" {
                  cfg.test_mode = TestMode::Strict
                }
              "async" => cfg.skip_async = value != "yes"
              "module" => cfg.skip_module = value != "yes"
              "verbose" => cfg.verbose = value == "yes"
              "excludefile" => {
                let file_path = compose_path(base, value)
                load_list_file(file_path, cfg.excludes) catch {
                  err => println("error: \{err}")
                }
              }
              _ => ()
            }
          None => ()
        }
      }
      ConfigSection::Exclude => cfg.excludes.push(compose_path(base, trimmed))
      ConfigSection::Features => {
        let (key, value_opt) = split_key_value(trimmed)
        match value_opt {
          None => cfg.features.set(key, true)
          Some(value) =>
            if value == "yes" {
              cfg.features.set(key, true)
            } else {
              cfg.skip_features.set(key, true)
            }
        }
      }
      ConfigSection::Tests => cfg.tests.push(compose_path(base, trimmed))
      ConfigSection::None => ()
    }
  }
  cfg
}

///|
fn build_exclude_sets(
  excludes : Array[String],
) -> (Array[String], Map[String, Bool]) {
  let dirs : Array[String] = []
  let files : Map[String, Bool] = Map::new()
  for entry in excludes {
    if entry.has_suffix("/") {
      dirs.push(entry)
    } else {
      files.set(entry, true)
    }
  }
  dirs.sort()
  (dirs, files)
}

///|
fn is_excluded(
  filename : String,
  exclude_dirs : Array[String],
  exclude_files : Map[String, Bool],
) -> Bool {
  if map_has(exclude_files, filename) {
    return true
  }
  for prefix in exclude_dirs {
    if filename.has_prefix(prefix) {
      return true
    }
  }
  false
}

///|
fn extract_desc(buf : String, style : Char) -> String? {
  let len = buf.length()
  let style_unit = Char::to_int(style)
  let mut i = 0
  while i + 3 < len {
    let u0 = UInt16::to_int(buf.code_unit_at(i))
    let u1 = UInt16::to_int(buf.code_unit_at(i + 1))
    let u2 = UInt16::to_int(buf.code_unit_at(i + 2))
    let u3 = UInt16::to_int(buf.code_unit_at(i + 3))
    if u0 == Char::to_int('/') &&
      u1 == Char::to_int('*') &&
      u2 == style_unit &&
      u3 != Char::to_int('/') {
      let start = i + 3
      let mut j = start
      while j + 1 < len {
        let a = UInt16::to_int(buf.code_unit_at(j))
        let b = UInt16::to_int(buf.code_unit_at(j + 1))
        if a == Char::to_int('*') && b == Char::to_int('/') {
          return Some(buf.unsafe_substring(start~, end=j))
        }
        j = j + 1
      }
      return None
    }
    i = i + 1
  }
  None
}

///|
fn find_tag(desc : String, tag : String) -> Int? {
  match desc.find(tag) {
    Some(idx) => Some(idx + tag.length())
    None => None
  }
}

///|
fn find_tag_from(desc : String, start : Int, tag : String) -> Int? {
  if start < 0 || start >= desc.length() {
    return None
  }
  let suffix = desc.unsafe_substring(start~, end=desc.length())
  match suffix.find(tag) {
    Some(idx) => Some(start + idx + tag.length())
    None => None
  }
}

///|
fn get_option(desc : String, start : Int, state : Int) -> (String?, Int, Int) {
  if start < 0 {
    return (None, -1, state)
  }
  let len = desc.length()
  let mut i = start
  let mut st = state
  while i < len {
    let unit = UInt16::to_int(desc.code_unit_at(i))
    if unit == Char::to_int('[') {
      st = st + 1
      i = i + 1
      continue
    }
    if unit == Char::to_int(']') {
      st = st - 1
      if st > 0 {
        i = i + 1
        continue
      }
      return (None, -1, st)
    }
    if unit == Char::to_int(' ') ||
      unit == Char::to_int('\t') ||
      unit == Char::to_int('\r') ||
      unit == Char::to_int(',') ||
      unit == Char::to_int('-') {
      i = i + 1
      continue
    }
    if unit == Char::to_int('\n') {
      if st > 0 {
        i = i + 1
        continue
      }
      let next_is_space = if i + 1 < len {
        let next = UInt16::to_int(desc.code_unit_at(i + 1))
        next == Char::to_int(' ')
      } else {
        false
      }
      if next_is_space {
        i = i + 1
        continue
      }
      return (None, -1, st)
    }
    let start_i = i
    i = i + 1
    while i < len {
      let u2 = UInt16::to_int(desc.code_unit_at(i))
      if u2 == Char::to_int(' ') ||
        u2 == Char::to_int('\t') ||
        u2 == Char::to_int('\r') ||
        u2 == Char::to_int('\n') ||
        u2 == Char::to_int(',') ||
        u2 == Char::to_int(']') {
        break
      }
      i = i + 1
    }
    let option = desc.unsafe_substring(start=start_i, end=i)
    return (Some(option), i, st)
  }
  (None, -1, st)
}

///|
fn parse_tag_value(desc : String, start : Int) -> String? {
  let len = desc.length()
  let mut i = start
  while i < len {
    let unit = UInt16::to_int(desc.code_unit_at(i))
    if unit != Char::to_int(' ') &&
      unit != Char::to_int('\t') &&
      unit != Char::to_int('\r') &&
      unit != Char::to_int('\n') {
      break
    }
    i = i + 1
  }
  if i >= len {
    return None
  }
  let start_i = i
  while i < len {
    let unit = UInt16::to_int(desc.code_unit_at(i))
    if unit == Char::to_int(' ') ||
      unit == Char::to_int('\t') ||
      unit == Char::to_int('\r') ||
      unit == Char::to_int('\n') {
      break
    }
    i = i + 1
  }
  Some(desc.unsafe_substring(start=start_i, end=i))
}

///|
fn find_include_entries(source : String) -> Array[String] {
  let includes : Array[String] = []
  let pattern = "$INCLUDE(\""
  let mut pos = 0
  let len = source.length()
  while pos < len {
    let suffix = source.unsafe_substring(start=pos, end=len)
    match suffix.find(pattern) {
      Some(idx) => {
        let start = pos + idx + pattern.length()
        if start >= len {
          break
        }
        let rest = source.unsafe_substring(start~, end=len)
        match rest.find("\"") {
          Some(end_rel) => {
            let end = start + end_rel
            includes.push(source.unsafe_substring(start~, end~))
            pos = end + 1
          }
          None => break
        }
      }
      None => break
    }
  }
  includes
}

///|
fn new_test_meta(new_style : Bool) -> TestMeta {
  let includes = if new_style { ["sta.js", "assert.js"] } else { ["sta.js"] }
  TestMeta::{
    is_nostrict: false,
    is_onlystrict: false,
    is_negative: false,
    is_async: false,
    is_module: false,
    can_block: true,
    skip: false,
    error_type: None,
    includes,
  }
}

///|
fn parse_test_meta(
  cfg : RunnerConfig,
  filename : String,
  source : String,
) -> TestMeta {
  let meta = new_test_meta(cfg.new_style)
  if cfg.new_style {
    match extract_desc(source, '-') {
      Some(desc) => {
        match find_tag(desc, "includes:") {
          Some(idx) => {
            let mut state = 0
            let mut pos = idx
            while pos >= 0 {
              let (opt, next, st) = get_option(desc, pos, state)
              state = st
              match opt {
                Some(name) =>
                  if map_has(cfg.harness_exclude, name) {
                    meta.skip = true
                  } else {
                    meta.includes.push(name)
                  }
                None => break
              }
              pos = next
            }
          }
          None => ()
        }
        match find_tag(desc, "flags:") {
          Some(idx) => {
            let mut state = 0
            let mut pos = idx
            while pos >= 0 {
              let (opt, next, st) = get_option(desc, pos, state)
              state = st
              match opt {
                Some(flag) =>
                  if flag == "noStrict" || flag == "raw" {
                    meta.is_nostrict = true
                    if cfg.test_mode == TestMode::Strict {
                      meta.skip = true
                    }
                  } else if flag == "onlyStrict" {
                    meta.is_onlystrict = true
                    if cfg.test_mode == TestMode::NoStrict {
                      meta.skip = true
                    }
                  } else if flag == "async" {
                    meta.is_async = true
                    if cfg.skip_async {
                      meta.skip = true
                    }
                  } else if flag == "module" {
                    meta.is_module = true
                    if cfg.skip_module {
                      meta.skip = true
                    }
                  } else if flag == "CanBlockIsFalse" {
                    meta.can_block = false
                  } else if flag == "CanBlockIsTrue" {
                    meta.can_block = true
                  }
                None => break
              }
              pos = next
            }
          }
          None => ()
        }
        match find_tag(desc, "negative:") {
          Some(idx) => {
            meta.is_negative = true
            match find_tag_from(desc, idx, "type:") {
              Some(t_idx) => meta.error_type = parse_tag_value(desc, t_idx)
              None => ()
            }
          }
          None => ()
        }
        match find_tag(desc, "features:") {
          Some(idx) => {
            let mut state = 0
            let mut pos = idx
            while pos >= 0 {
              let (opt, next, st) = get_option(desc, pos, state)
              state = st
              match opt {
                Some(feature) =>
                  if map_has(cfg.features, feature) {
                    ()
                  } else if map_has(cfg.skip_features, feature) {
                    meta.skip = true
                  } else {
                    if cfg.verbose {
                      println("\{filename}:1: unknown feature: \{feature}")
                    }
                    meta.skip = true
                  }
                None => break
              }
              pos = next
            }
          }
          None => ()
        }
      }
      None => ()
    }
    if meta.is_async {
      meta.includes.push("doneprintHandle.js")
    }
  } else {
    for include_name in find_include_entries(source) {
      if map_has(cfg.harness_exclude, include_name) {
        meta.skip = true
      } else {
        meta.includes.push(include_name)
      }
    }
    match extract_desc(source, '*') {
      Some(desc) => {
        if desc.contains("@noStrict") {
          meta.is_nostrict = true
          if cfg.test_mode == TestMode::Strict {
            meta.skip = true
          }
        }
        if desc.contains("@onlyStrict") {
          meta.is_onlystrict = true
          if cfg.test_mode == TestMode::NoStrict {
            meta.skip = true
          }
        }
        if desc.contains("@negative") {
          meta.is_negative = true
        }
      }
      None => ()
    }
  }
  meta
}

///|
fn compute_modes(cfg : RunnerConfig, meta : TestMeta) -> (Bool, Bool) {
  let mut use_strict = false
  let mut use_nostrict = false
  match cfg.test_mode {
    TestMode::DefaultNoStrict =>
      if meta.is_onlystrict {
        use_strict = true
      } else {
        use_nostrict = true
      }
    TestMode::DefaultStrict =>
      if meta.is_nostrict {
        use_nostrict = true
      } else {
        use_strict = true
      }
    TestMode::NoStrict => if !meta.is_onlystrict { use_nostrict = true }
    TestMode::Strict => if !meta.is_nostrict { use_strict = true }
    TestMode::All =>
      if meta.is_module {
        use_nostrict = true
      } else {
        if !meta.is_nostrict {
          use_strict = true
        }
        if !meta.is_onlystrict {
          use_nostrict = true
        }
      }
  }
  (use_strict, use_nostrict)
}

///|
fn default_harness_dir(filename : String, new_style : Bool) -> String {
  let marker = "test/"
  match filename.find(marker) {
    Some(idx) => {
      let prefix = filename.unsafe_substring(start=0, end=idx)
      if new_style {
        prefix + "harness"
      } else {
        prefix + "test/harness"
      }
    }
    None => ""
  }
}

///|
fn join_harness_path(harness_dir : String, name : String) -> String {
  if harness_dir.is_empty() {
    return name
  }
  let base = @path.Path(harness_dir)
  base.join(@path.Path(name)).to_string()
}

///|
fn read_text(path : String) -> Result[String, String] {
  try {
    let bytes = @fs.read_file_to_bytes(path)
    Ok(@encoding/utf8.decode(bytes))
  } catch {
    err => Err(err.to_string())
  }
}

///|
fn run_test_variant(
  meta : TestMeta,
  filename : String,
  source : String,
  harness_dir : String,
  force_strict : Bool,
  gc_threshold : Int?,
  gc_debug : Bool,
) -> String? {
  @runtime.reset_runtime_state()
  let engine = @qjs.Engine::new()
  match gc_threshold {
    Some(threshold) => engine.set_gc_threshold(threshold)
    None => ()
  }
  engine.set_gc_debug(gc_debug)
  engine.set_can_block(meta.can_block)
  for include_name in meta.includes {
    let include_path = join_harness_path(harness_dir, include_name)
    let include_source = match read_text(include_path) {
      Ok(text) => text
      Err(msg) => return Some("harness load \{include_path}: \{msg}")
    }
    let include_result = engine.eval_result_with_path_mode(
      include_source, include_path, false, false,
    ) catch {
      err => return Some("harness error \{include_path}: \{err}")
    }
    match include_result {
      Ok(_) => ()
      Err(err_value) =>
        return Some(
          "harness error \{include_path}: \{engine.format_error(err_value)}",
        )
    }
  }
  let result = engine.eval_result_with_path_mode(
    source,
    filename,
    meta.is_module,
    force_strict,
  ) catch {
    err => return Some("eval error: \{err}")
  }
  match result {
    Ok(_) => {
      if meta.is_negative {
        return Some("expected error")
      }
      if meta.is_async {
        let done = engine.async_done_count()
        if done != 1 {
          return Some("$DONE() not called")
        }
      }
      None
    }
    Err(err_value) =>
      if meta.is_negative {
        match meta.error_type {
          Some(expected) =>
            match engine.error_name(err_value) {
              Some(actual) =>
                if actual == expected {
                  None
                } else {
                  Some("expected \{expected} but got \{actual}")
                }
              None =>
                Some(
                  "expected \{expected} but got \{engine.format_error(err_value)}",
                )
            }
          None => None
        }
      } else {
        Some(engine.format_error(err_value))
      }
  }
}

///|
fn run_test(
  cfg : RunnerConfig,
  filename : String,
  exclude_dirs : Array[String],
  exclude_files : Map[String, Bool],
  gc_threshold : Int?,
  gc_debug : Bool,
) -> TestOutcome {
  if is_excluded(filename, exclude_dirs, exclude_files) {
    return TestOutcome::Excluded
  }
  let source = match read_text(filename) {
    Ok(text) => text
    Err(msg) => {
      println("error: \{filename}: \{msg}")
      return TestOutcome::Failed
    }
  }
  let meta = parse_test_meta(cfg, filename, source)
  let (use_strict, use_nostrict) = compute_modes(cfg, meta)
  if meta.skip || (!use_strict && !use_nostrict) {
    return TestOutcome::Skipped
  }
  let harness_dir = if !cfg.harness_dir.is_empty() {
    cfg.harness_dir
  } else {
    default_harness_dir(filename, cfg.new_style)
  }
  let mut failed : String? = None
  if use_nostrict {
    match
      run_test_variant(
        meta, filename, source, harness_dir, false, gc_threshold, gc_debug,
      ) {
      Some(msg) => failed = Some("nostrict: \{msg}")
      None => ()
    }
  }
  if use_strict {
    match
      run_test_variant(
        meta, filename, source, harness_dir, true, gc_threshold, gc_debug,
      ) {
      Some(msg) => failed = Some("strict: \{msg}")
      None => ()
    }
  }
  match failed {
    Some(msg) => {
      println("FAIL \{filename}: \{msg}")
      TestOutcome::Failed
    }
    None => TestOutcome::Passed
  }
}

///|
fn add_tests_from_dir(dir : String, out : Array[String]) -> Unit {
  enumerate_tests(dir, out)
}

///|
fn add_tests_from_files(files : Array[String], out : Array[String]) -> Unit {
  for name in files {
    out.push(name)
  }
}

///|
fn parse_args(
  args : Array[String],
) -> (String, Array[String], Array[String], Bool, Int, Int, Int?, Bool) {
  let mut config = "quickjs/test262.conf"
  let files : Array[String] = []
  let dirs : Array[String] = []
  let mut ignore_testdir = false
  let mut start_index = 0
  let mut count_limit = -1
  let mut gc_threshold : Int? = None
  let mut gc_debug = false
  let mut i = 1
  while i < args.length() {
    let arg = args[i]
    if arg == "-c" && i + 1 < args.length() {
      config = args[i + 1]
      i = i + 2
    } else if arg == "--start" && i + 1 < args.length() {
      let value = args[i + 1]
      match parse_nonneg_int(value) {
        Some(index) => start_index = index
        None => println("error: invalid --start \{value}")
      }
      i = i + 2
    } else if arg == "--count" && i + 1 < args.length() {
      let value = args[i + 1]
      match parse_nonneg_int(value) {
        Some(count) => count_limit = count
        None => println("error: invalid --count \{value}")
      }
      i = i + 2
    } else if arg == "--gc-threshold" && i + 1 < args.length() {
      let value = args[i + 1]
      match parse_int_value(value) {
        Some(threshold) => gc_threshold = Some(threshold)
        None => println("error: invalid --gc-threshold \{value}")
      }
      i = i + 2
    } else if arg == "--gc-debug" {
      gc_debug = true
      i = i + 1
    } else if arg == "-f" && i + 1 < args.length() {
      files.push(args[i + 1])
      ignore_testdir = true
      i = i + 2
    } else if arg == "-d" && i + 1 < args.length() {
      dirs.push(args[i + 1])
      ignore_testdir = true
      i = i + 2
    } else {
      files.push(arg)
      ignore_testdir = true
      i = i + 1
    }
  }
  (
    config, files, dirs, ignore_testdir, start_index, count_limit, gc_threshold,
    gc_debug,
  )
}

///|
fn parse_int_value(value : String) -> Int? {
  let parsed : Result[Int, Error] = try? @strconv.parse_int(value, base=10)
  match parsed {
    Ok(index) => Some(index)
    Err(_) => None
  }
}

///|
fn parse_nonneg_int(value : String) -> Int? {
  let parsed : Result[Int, Error] = try? @strconv.parse_int(value, base=10)
  match parsed {
    Ok(index) => if index >= 0 { Some(index) } else { None }
    Err(_) => None
  }
}

///|
fn main {
  let args = @env.args()
  let (
    config_path,
    files,
    dirs,
    ignore_testdir,
    start_index,
    count_limit,
    gc_threshold,
    gc_debug,
  ) = parse_args(args)
  let cfg = parse_config(config_path, ignore_testdir) catch {
    err => {
      println("error: \{err}")
      return
    }
  }
  let tests : Array[String] = []
  add_tests_from_files(cfg.tests, tests)
  for dir in dirs {
    add_tests_from_dir(dir, tests)
  }
  if !files.is_empty() {
    add_tests_from_files(files, tests)
  }
  if tests.is_empty() {
    println("error: no tests found")
    return
  }
  tests.sort()
  let run_tests = if start_index > 0 || count_limit >= 0 {
    let selected : Array[String] = []
    let mut index = 0
    let mut taken = 0
    for test_path in tests {
      if index >= start_index {
        if count_limit >= 0 && taken >= count_limit {
          break
        }
        selected.push(test_path)
        taken = taken + 1
      }
      index = index + 1
    }
    selected
  } else {
    tests
  }
  if run_tests.is_empty() {
    println("error: no tests found")
    return
  }
  let (exclude_dirs, exclude_files) = build_exclude_sets(cfg.excludes)
  let mut total = 0
  let mut failed = 0
  let mut skipped = 0
  let mut excluded = 0
  for test_path in run_tests {
    if cfg.verbose {
      println("RUN \{test_path}")
    }
    let outcome = run_test(
      cfg, test_path, exclude_dirs, exclude_files, gc_threshold, gc_debug,
    )
    match outcome {
      TestOutcome::Passed => total = total + 1
      TestOutcome::Failed => {
        total = total + 1
        failed = failed + 1
      }
      TestOutcome::Skipped => {
        total = total + 1
        skipped = skipped + 1
      }
      TestOutcome::Excluded => excluded = excluded + 1
    }
  }
  println(
    "Result: \{failed}/\{total} errors, \{skipped} skipped, \{excluded} excluded",
  )
}
