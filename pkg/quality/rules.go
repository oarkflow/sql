package quality

import (
	"context"
	"fmt"
	"regexp"

	"github.com/oarkflow/sql/pkg/utils"
)

type ValidationResult struct {
	Valid   bool
	Errors  []error
	Warning bool
}

type Validator interface {
	Validate(ctx context.Context, record utils.Record) (*ValidationResult, error)
}

type Rule func(value any) error

var (
	emailRegex = regexp.MustCompile(`^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,4}$`)
)

func GetRule(name string, args ...any) Rule {
	switch name {
	case "NotNull":
		return func(v any) error {
			if v == nil {
				return fmt.Errorf("value is null")
			}
			if s, ok := v.(string); ok && s == "" {
				return fmt.Errorf("value is empty")
			}
			return nil
		}
	case "Unique":
		// This usually requires state, implementing strictly local check or using external cache
		// For now, placeholder or needs context
		return func(v any) error { return nil }
	case "Range":
		if len(args) < 2 {
			return func(v any) error { return nil }
		}
		min, _ := utils.ToFloat64(args[0])
		max, _ := utils.ToFloat64(args[1])
		return func(v any) error {
			val, err := utils.ToFloat64(v)
			if err != nil {
				return err
			}
			if val < min || val > max {
				return fmt.Errorf("value %v out of range [%v, %v]", val, min, max)
			}
			return nil
		}
	case "Regex":
		if len(args) < 1 {
			return func(v any) error { return nil }
		}
		pattern := args[0].(string)
		re := regexp.MustCompile(pattern)
		return func(v any) error {
			s := fmt.Sprintf("%v", v)
			if !re.MatchString(s) {
				return fmt.Errorf("value %s does not match pattern %s", s, pattern)
			}
			return nil
		}
	case "Email":
		return func(v any) error {
			s, ok := v.(string)
			if !ok {
				return fmt.Errorf("not a string")
			}
			if !emailRegex.MatchString(s) {
				return fmt.Errorf("invalid email")
			}
			return nil
		}
	default:
		return func(v any) error { return nil }
	}
}
