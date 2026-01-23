///|
test "engine construct" {
  let _ = @nocturnejs.Engine::new()

}

///|
test "eval numbers" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval("1 + 2 * 3")
  match result {
    Ok(Value::Number(value)) => inspect(value, content="7")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval var assign" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval("var r; r = 1 + 2; r")
  match result {
    Ok(Value::Number(value)) => inspect(value, content="3")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval if else" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "var x = 1; if (x > 1) { x = 2; } else { x = 3; } x",
  )
  match result {
    Ok(Value::Number(value)) => inspect(value, content="3")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval while" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "var i = 0; var sum = 0; while (i < 3) { sum = sum + i; i = i + 1; } sum",
  )
  match result {
    Ok(Value::Number(value)) => inspect(value, content="3")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval for loop" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "var sum = 0; for (var i = 0; i < 3; i = i + 1) { sum = sum + i; } sum",
  )
  match result {
    Ok(Value::Number(value)) => inspect(value, content="3")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval break continue" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "var sum = 0; for (var i = 0; i < 5; i = i + 1) { if (i == 2) continue; if (i == 4) break; sum = sum + i; } sum",
  )
  match result {
    Ok(Value::Number(value)) => inspect(value, content="4")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval do while" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "var i = 0; var sum = 0; do { sum = sum + i; i = i + 1; } while (i < 3); sum",
  )
  match result {
    Ok(Value::Number(value)) => inspect(value, content="3")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval switch" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "var x = 1; var y = 0; switch (x) { case 0: y = 1; break; case 1: y = 2; break; default: y = 3; } y",
  )
  match result {
    Ok(Value::Number(value)) => inspect(value, content="2")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval switch fallthrough" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "var x = 1; var y = 0; switch (x) { case 0: y = 1; break; case 1: y = 2; default: y = 3; } y",
  )
  match result {
    Ok(Value::Number(value)) => inspect(value, content="3")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval try catch" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "var x = 0; try { throw 3; } catch (e) { x = e + 1; } x",
  )
  match result {
    Ok(Value::Number(value)) => inspect(value, content="4")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval try finally" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "var x = 0; try { x = 1; } finally { x = x + 2; } x",
  )
  match result {
    Ok(Value::Number(value)) => inspect(value, content="3")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval try finally break continue" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "var log = \"\"; for (var i = 0; i < 3; i = i + 1) { try { if (i == 1) break; log = log + \"t\"; } finally { log = log + \"f\"; } } log",
  )
  match result {
    Ok(Value::String(value)) => inspect(value, content="tff")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
  let result2 = try? engine.eval(
    "var log = \"\"; for (var i = 0; i < 3; i = i + 1) { try { if (i == 1) continue; log = log + \"t\"; } finally { log = log + \"f\"; } } log",
  )
  match result2 {
    Ok(Value::String(value)) => inspect(value, content="tfftf")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval try finally throw" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "var x = 0; try { throw 1; } finally { x = 2; } x",
  )
  match result {
    Ok(_) => fail("expected error")
    Err(_) => ()
  }
  let result2 = try? engine.eval("x")
  match result2 {
    Ok(Value::Number(value)) => inspect(value, content="2")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval typeof void delete" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "var x = 1; typeof x == \"number\" && typeof unknown == \"undefined\" && void 0 == undefined && delete x == false",
  )
  match result {
    Ok(Value::Bool(value)) => inspect(value, content="true")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval object literal and member" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "var x = 1; var a = { x, y: 2, \"z\": 3 }; a.x + a[\"y\"] + a.z",
  )
  match result {
    Ok(Value::Number(value)) => inspect(value, content="6")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval json stringify" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval("var a = { x: 1, y: 2 }; JSON.stringify(a)")
  match result {
    Ok(Value::String(value)) => inspect(value, content="{\"x\":1,\"y\":2}")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval typeof builtin object" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval("typeof Object")
  match result {
    Ok(Value::String(value)) => inspect(value, content="function")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval object define property" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "var gvar1 = 1; Object.defineProperty(globalThis, \"gvar1\", { writable: false }); gvar1 = 2; gvar1",
  )
  match result {
    Ok(Value::Number(value)) => inspect(value, content="1")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval object define property getter" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "var v = 0; Object.defineProperty(globalThis, \"gvar2\", { get: function() { return 5; }, set: function(x) { v = x; } }); var a = gvar2; gvar2 = 7; a + v",
  )
  match result {
    Ok(Value::Number(value)) => inspect(value, content="12")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval function prototype" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "function g() {} Object.defineProperty(g, \"prototype\", { writable: false }); g.prototype.constructor === g",
  )
  match result {
    Ok(Value::Bool(value)) => inspect(value, content="true")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval array literal and toString" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "var a = [1,,3]; a.length === 3 && a[1] === undefined && a.toString() === \"1,,3\"",
  )
  match result {
    Ok(Value::Bool(value)) => inspect(value, content="true")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval json stringify array" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval("JSON.stringify([1,,3])")
  match result {
    Ok(Value::String(value)) => inspect(value, content="[1,null,3]")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval inc dec" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "var a = 1; var r = a++; var ok = (r === 1 && a === 2); a = 1; r = ++a; ok = ok && (r === 2 && a === 2); a = 1; r = a--; ok = ok && (r === 1 && a === 0); a = 1; r = --a; ok = ok && (r === 0 && a === 0); a = { x: true }; a.x++; ok = ok && (a.x === 2); a = { x: true }; r = a.x++; ok = ok && (r === 1 && a.x === 2); a = [true]; a[0]++; ok = ok && (a[0] === 2); a = [true]; r = a[0]--; ok = ok && (r === 1 && a[0] === 0); ok",
  )
  match result {
    Ok(Value::Bool(value)) => inspect(value, content="true")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval new in instanceof arguments" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "function F(x) { this.x = x; } var a = new Object; a.x = 1; var b = new F(2); var ok = (a.x === 1 && b.x === 2); ok = ok && (\"x\" in a) && !(\"y\" in a); ok = ok && (a instanceof Object) && !(a instanceof String); ok = ok && (new String(\"abc\") == \"abc\"); function f2() { return arguments.length === 2 && arguments[0] === 1 && arguments[1] === 3; } ok = ok && f2(1, 3); ok",
  )
  match result {
    Ok(Value::Bool(value)) => inspect(value, content="true")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval arguments mapping" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "var ok = true; function f(a) { arguments[0] = 2; ok = ok && (a === 2 && arguments[0] === 2); a = 5; ok = ok && (arguments[0] === 5); return ok; } ok = ok && f(1); function g(a) { Object.defineProperty(arguments, \"0\", { value: 10 }); return a === 10 && arguments[0] === 10; } ok = ok && g(1); function h(a) { delete arguments[0]; a = 3; return arguments[0] === undefined; } ok = ok && h(1); ok",
  )
  match result {
    Ok(Value::Bool(value)) => inspect(value, content="true")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval function caller arguments accessors" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "function f() {} var ok = (f.caller === undefined) && (f.arguments === undefined); var threw = false; try { f.caller = 1; } catch(e) { threw = e instanceof TypeError; } var threw2 = false; try { f.arguments = 2; } catch(e) { threw2 = e instanceof TypeError; } ok && threw && threw2",
  )
  match result {
    Ok(Value::Bool(value)) => inspect(value, content="true")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval array properties" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "var a = [1]; a.foo = 2; var ok = a.foo === 2; var names = Object.getOwnPropertyNames(a); ok = ok && (names.length === 3); var hasFoo = false; var hasLength = false; for (var i = 0; i < names.length; i = i + 1) { if (names[i] === \"foo\") hasFoo = true; if (names[i] === \"length\") hasLength = true; } ok = ok && hasFoo && hasLength; delete a.foo; ok && (a.foo === undefined)",
  )
  match result {
    Ok(Value::Bool(value)) => inspect(value, content="true")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval function length and name" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "function f(a, b) {} f.length === 2 && f.name === \"f\"",
  )
  match result {
    Ok(Value::Bool(value)) => inspect(value, content="true")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval function call apply" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "function f(a, b) { return this.x + a + b; } var obj = { x: 1 }; var ok = (f.call(obj, 2, 3) === 6); ok = ok && (f.apply(obj, [2, 3]) === 6); ok",
  )
  match result {
    Ok(Value::Bool(value)) => inspect(value, content="true")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval function bind" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "function f(a, b, c) { return [this.x, a, b, c].toString(); } var obj = { x: 1 }; var g = f.bind(obj, 2); var ok = (g(3, 4) === \"1,2,3,4\"); ok = ok && (g.length === 2) && (g.name === \"bound f\"); function C(a) { this.x = a; } var h = C.bind(null, 7); var o = new h(); ok && (o.x === 7)",
  )
  match result {
    Ok(Value::Bool(value)) => inspect(value, content="true")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval in instanceof delete errors" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "var ok = false; try { \"x\" in 1; } catch(e) { ok = e instanceof TypeError; } var ok2 = false; try { 1 instanceof 2; } catch(e) { ok2 = e instanceof TypeError; } var ok3 = false; try { delete null.a; } catch(e) { ok3 = e instanceof TypeError; } ok && ok2 && ok3",
  )
  match result {
    Ok(Value::Bool(value)) => inspect(value, content="true")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval object builtins" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "var ok = true; var a = { x: 1 }; ok = ok && (Object.getPrototypeOf(a) === Object.prototype); var b = Object.create(a); ok = ok && (Object.getPrototypeOf(b) === a); var c = { y: 2 }; Object.setPrototypeOf(a, c); ok = ok && (Object.getPrototypeOf(a) === c); var keys = Object.keys({x:1,\"18014398509481984\":1,\"9007199254740992\":1,\"9007199254740991\":1,\"4294967296\":1,\"4294967295\":1,y:1,\"4294967294\":1,\"1\":2}); ok = ok && (keys.toString() === \"1,4294967294,x,18014398509481984,9007199254740992,9007199254740991,4294967296,4294967295,y\"); var p = {}; Object.preventExtensions(p); p.z = 2; ok = ok && (Object.isExtensible(p) === false) && (typeof p.z === \"undefined\"); ok",
  )
  match result {
    Ok(Value::Bool(value)) => inspect(value, content="true")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval array constructor push join" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "var ok = true; var a = new Array(3); ok = ok && (a.length === 3) && (a[0] === undefined); var b = Array(1, 2); ok = ok && (b.length === 2) && (b[0] === 1) && (b[1] === 2); var c = [1, 2]; c.push(3, 4); ok = ok && (c.join() === \"1,2,3,4\"); ok",
  )
  match result {
    Ok(Value::Bool(value)) => inspect(value, content="true")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval array length non configurable" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "var a = [1,2,3,4,5]; Object.defineProperty(a, \"3\", { configurable: false }); var threw = false; try { a.length = 2; } catch(e) { threw = e instanceof TypeError; } !threw && (a.length === 4) && (a.toString() === \"1,2,3,4\")",
  )
  match result {
    Ok(Value::Bool(value)) => inspect(value, content="true")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval function constructor" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "var f = new Function(\"a\", \"b\", \"return a + b;\"); f(2, 3) === 5",
  )
  match result {
    Ok(Value::Bool(value)) => inspect(value, content="true")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval strict this binding" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "\"use strict\"; function f() { return this === undefined; } f()",
  )
  match result {
    Ok(Value::Bool(value)) => inspect(value, content="true")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "eval strict arguments callee" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "\"use strict\"; function f() { var ok = false; try { arguments.callee; } catch(e) { ok = e instanceof TypeError; } return ok; } f()",
  )
  match result {
    Ok(Value::Bool(value)) => inspect(value, content="true")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "banana" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval("'b' + 'a' + + 'a' + 'a'")
  match result {
    Ok(Value::String(value)) => inspect(value, content="baNaNa")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}

///|
test "reduce" {
  let engine = @nocturnejs.Engine::new()
  let result = try? engine.eval(
    "(()=>[...Array(7).keys()].reduce((a,i)=>((f,n)=>n<2?1:f(n-1)+f(n-2))((g=>n=>n<2?1:g(g)(n-1)+g(g)(n-2))(g=>n=>n<2?1:g(g)(n-1)+g(g)(n-2)),i)+a,0))()",
  )
  match result {
    Ok(Value::Number(value)) => inspect(value, content="33")
    Ok(other) => fail("unexpected value: \{other}")
    Err(_) => fail("eval failed")
  }
}
