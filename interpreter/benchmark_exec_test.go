package interpreter

import (
	"testing"

	"github.com/oarkflow/expr"
)

// --- Exec with Data Map (Parse + Inject + Eval) ---

func Benchmark_SPL_Exec_Math(b *testing.B) {
	script := "x + y"
	data := map[string]interface{}{
		"x": 10,
		"y": 20,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Exec(script, data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Expr_Exec_Math(b *testing.B) {
	script := "x + y"
	data := map[string]interface{}{
		"x": 10,
		"y": 20,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// expr.Compile needs to trace types from env if strictly typed, but here passed at runtime
		// If we re-compile every time (equivalent to Exec):
		program, err := expr.Compile(script, expr.Env(data))
		if err != nil {
			b.Fatal(err)
		}
		_, err = expr.Run(program, data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_SPL_Exec_NestedStruct(b *testing.B) {
	script := "user.age"
	data := map[string]interface{}{
		"user": map[string]interface{}{
			"name": "Alice",
			"age":  30,
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Exec(script, data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Expr_Exec_NestedStruct(b *testing.B) {
	script := "user.age"
	data := map[string]interface{}{
		"user": map[string]interface{}{
			"name": "Alice",
			"age":  30,
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		program, err := expr.Compile(script, expr.Env(data))
		if err != nil {
			b.Fatal(err)
		}
		_, err = expr.Run(program, data)
		if err != nil {
			b.Fatal(err)
		}
	}
}
