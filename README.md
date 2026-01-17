# NocturneJS

QuickJS-aligned JavaScript runtime in MoonBit.

## Usage

Run a single JavaScript file:

```
moon run --target native --release cmd/main -- path/to/file.js
```

Library usage (MoonBit):

```sh
moon add Lampese/nocturnejs
```

```json
{
  "import": [
    {
      "path": "Lampese/nocturnejs",
      "alias": "njs"
    }
  ]
}
```

```mbt
let engine = @njs.Engine::new()
let result = engine.eval_result("1 + 2") catch {
  err => {
    println("error: \{err}")
    return
  }
}
match result {
  Ok(value) => println(engine.value_to_string(value))
  Err(err) => println("error: \{engine.format_error(err)}")
}
```

## Testing

Run test262 with the QuickJS-aligned config:

```bash
moon update
git submodule update --init --recursive
git -C test262 apply --check ../test262-config/test262.patch && \
git -C test262 apply ../test262-config/test262.patch
moon run --target native --release cmd/test262 -- -c test262-config/test262.conf
```

## Test262 Coverage

<table>
  <tr>
    <td>NJS</td>
    <td>
      <img src="assets/test262-njs.svg" width="600" height="16" alt="NocturneJS test262: 43,616 passed, 49 failed, 5,767 skipped">
    </td>
    <td>pass 43,616 (88.23%)  fail 49 (0.10%)  skipped 5,767 (11.67%)  excluded 2,567 (not shown)</td>
  </tr>
  <tr>
    <td>QJS</td>
    <td>
      <img src="assets/test262-qjs.svg" width="600" height="16" alt="QuickJS test262: 43,606 passed, 52 failed, 5,767 skipped">
    </td>
    <td>pass 43,606 (88.23%)  fail 52 (0.11%)  skipped 5,767 (11.67%)</td>
  </tr>
</table>

<small>* Total differs by 7 tests because QuickJS data comes from a historical report snapshot, while we enumerate the current test262 directory.</small>
