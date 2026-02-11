package interpreter

import (
	"testing"

	"github.com/oarkflow/expr"
)

// --- Simple Arithmetic: 1 + 2 * 3 ---

func Benchmark_SPL_Math_ParseAndEval(b *testing.B) {
	input := "1 + 2 * 3"
	for i := 0; i < b.N; i++ {
		l := NewLexer(input)
		p := NewParser(l)
		program := p.ParseProgram()
		env := NewEnvironment()
		Eval(program, env)
	}
}

func Benchmark_Expr_Math_ParseAndEval(b *testing.B) {
	input := "1 + 2 * 3"
	for i := 0; i < b.N; i++ {
		program, err := expr.Compile(input)
		if err != nil {
			b.Fatal(err)
		}
		expr.Run(program, nil)
	}
}

func Benchmark_SPL_Math_EvalOnly(b *testing.B) {
	input := "1 + 2 * 3"
	l := NewLexer(input)
	p := NewParser(l)
	program := p.ParseProgram()
	env := NewEnvironment()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Eval(program, env)
	}
}

func Benchmark_Expr_Math_EvalOnly(b *testing.B) {
	input := "1 + 2 * 3"
	program, err := expr.Compile(input)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		expr.Run(program, nil)
	}
}

// --- Variables: x + y ---

func Benchmark_SPL_Vars_EvalOnly(b *testing.B) {
	input := "x + y"
	l := NewLexer(input)
	p := NewParser(l)
	program := p.ParseProgram()

	env := NewEnvironment()
	env.Set("x", &Integer{Value: 10})
	env.Set("y", &Integer{Value: 20})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Eval(program, env)
	}
}

func Benchmark_Expr_Vars_EvalOnly(b *testing.B) {
	input := "x + y"
	// expr needs to know types at compile time usually via Compile(input, expr.Env(env))
	// or it infers dynamic access if passing map[string]interface{}

	env := map[string]interface{}{
		"x": 10,
		"y": 20,
	}

	program, err := expr.Compile(input, expr.Env(env))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		expr.Run(program, env)
	}
}

// --- Property Access: user.age ---

func Benchmark_SPL_Prop_EvalOnly(b *testing.B) {
	input := "user.age"
	l := NewLexer(input)
	p := NewParser(l)
	program := p.ParseProgram()

	env := NewEnvironment()
    // Setup nested hash for SPL
	userHash := &Hash{Pairs: make(map[HashKey]HashPair)}

    // Key "age" (String)
    ageKey := &String{Value: "age"}
    hashedAgeKey := ageKey.HashKey()

    // Value 30
    userHash.Pairs[hashedAgeKey] = HashPair{Key: ageKey, Value: &Integer{Value: 30}}

    env.Set("user", userHash)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Eval(program, env)
	}
}

func Benchmark_Expr_Prop_EvalOnly(b *testing.B) {
	input := "user.age"

	env := map[string]interface{}{
		"user": map[string]interface{}{
            "age": 30,
        },
	}

	program, err := expr.Compile(input, expr.Env(env))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		expr.Run(program, env)
	}
}
