package interpreter

import (
	"testing"
)

func TestExecWithData(t *testing.T) {
	script := `
	let result = x + y
	let nameUpper = user.name.upper()
	let active = user.active
	`

	data := map[string]interface{}{
		"x": 10,
		"y": 20,
		"user": map[string]interface{}{
			"name":   "alice",
			"active": true,
		},
	}

	_, err := Exec(script, data)
	if err != nil {
		t.Fatalf("Exec failed: %v", err)
	}

	// Exec evalProgram returns the LAST statement result.
	// In the script:
	// 1. let result = 30
	// 2. let nameUpper = "ALICE"
	// 3. let active = true
	// The return value of a LetStatement in this interpreter is typically NULL or the value assigned?
	// In existing Eval implementation:
	// case *LetStatement: returns nil (or val if we changed it, but standard monkey is nil/updates env)
	// EvalProgram loops and returns 'result'.
	// If LetStatement returns val, last one is true.

	// Wait, let's check core.go Eval logic for LetStatement.
	// We edited it recently.

	// "func evalProgram" loops. "result = Eval(statement, env)".
	// "Eval(*LetStatement)" -> returns val? Or nil?
	//
	// In my edit to "interpreter/main.go" (now core.go):
	// case *LetStatement:
	//    val := Eval(node.Value, env)
	//    ...
	//    env.Set(...)
	//    (implicit return nil or val?)
	// It falls through? No, switch case.
	//
	// Looking at read code earlier:
	// case *LetStatement:
	//    val := Eval(...)
	//    if isError(...) return val
	//    env.Set(...)
	//    // No return here? Implicitly returns nil?
	//
	// If it returns nil (which is likely if not specified in switch case returning Object), then `evalProgram` sees nil.

	// If I want to verify variables were set, I might need to inspect the Env... code doesn't return Env.
	// `Exec` creates a local environment.
	// If `Exec` returns the last expression value, I should use an expression at the end?

	script2 := `
		let result = x + y
		result
	`
	res2, err := Exec(script2, data)
	if err != nil {
		t.Fatalf("Exec2 failed: %v", err)
	}

	if res2 == nil {
		t.Fatal("Exec2 returned nil")
	}

	intVal, ok := res2.(*Integer)
	if !ok {
		t.Fatalf("Expected Integer, got %T", res2)
	}
	if intVal.Value != 30 {
		t.Errorf("Expected 30, got %d", intVal.Value)
	}
}

func TestExecStructData(t *testing.T) {
	type Config struct {
		Port int
		Host string
	}

	data := map[string]interface{}{
		"cfg": Config{Port: 8080, Host: "localhost"},
	}

	script := `cfg.Host`

	res, err := Exec(script, data)
	if err != nil {
		t.Fatalf("Exec failed: %v", err)
	}

	strVal, ok := res.(*String)
	if !ok {
		t.Fatalf("Expected String, got %T", res)
	}
	if strVal.Value != "localhost" {
		t.Errorf("Expected 'localhost', got '%s'", strVal.Value)
	}

	// Verify Int access
	script2 := `cfg.Port`
	res2, err := Exec(script2, data)
	if err != nil { t.Fatal(err) }
	if res2.(*Integer).Value != 8080 {
		t.Errorf("Expected 8080, got %d", res2.(*Integer).Value)
	}
}
