package transformers

import (
	"context"
	"fmt"
	"strings"
	"time"

	convert "github.com/oarkflow/convert/v2"

	"github.com/oarkflow/sql/pkg/utils"
)

// FieldFormatterTransformer allows declarative per-field formatting pipelines defined in configuration.
type FieldFormatterTransformer struct {
	pipelines map[string][]formatStep
}

type formatStep struct {
	Name string
	Args []formatArg
}

type formatArg struct {
	Value   string
	Literal bool
}

// NewFieldFormatterTransformer builds a transformer from field->pipeline expressions.
func NewFieldFormatterTransformer(fields map[string]string) (*FieldFormatterTransformer, error) {
	if len(fields) == 0 {
		return nil, fmt.Errorf("field formatter requires at least one field specification")
	}
	pipelines := make(map[string][]formatStep, len(fields))
	for field, expr := range fields {
		steps, err := parseFormatPipeline(expr)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", field, err)
		}
		pipelines[field] = steps
	}
	return &FieldFormatterTransformer{pipelines: pipelines}, nil
}

// Name implements contracts.Transformer to identify this transformer.
func (f *FieldFormatterTransformer) Name() string {
	return "FieldFormatterTransformer"
}

// Transform applies the configured formatting pipelines to the supplied record.
func (f *FieldFormatterTransformer) Transform(ctx context.Context, rec utils.Record) (utils.Record, error) {
	for field, steps := range f.pipelines {
		current := rec[field]
		value, err := f.applySteps(ctx, current, steps, rec)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", field, err)
		}
		rec[field] = value
	}
	return rec, nil
}

func (f *FieldFormatterTransformer) applySteps(ctx context.Context, current any, steps []formatStep, rec utils.Record) (any, error) {
	val := current
	var err error
	for _, step := range steps {
		val, err = f.executeStep(ctx, step, val, rec)
		if err != nil {
			return nil, err
		}
	}
	return val, nil
}

func (f *FieldFormatterTransformer) executeStep(ctx context.Context, step formatStep, current any, rec utils.Record) (any, error) {
	switch step.Name {
	case "transform":
		return f.stepTransform(ctx, step.Args, current, rec)
	case "copy":
		return f.stepCopy(ctx, step.Args, current, rec)
	case "set":
		return f.stepSet(ctx, step.Args, current, rec)
	case "default":
		return f.stepDefault(ctx, step.Args, current, rec)
	case "uppercase", "upper":
		return applyStringOp(current, strings.ToUpper), nil
	case "lowercase", "lower":
		return applyStringOp(current, strings.ToLower), nil
	case "trim":
		return applyStringOp(current, strings.TrimSpace), nil
	case "prefix":
		return f.stepAffix(ctx, step.Args, current, rec, true)
	case "suffix":
		return f.stepAffix(ctx, step.Args, current, rec, false)
	case "replace":
		return f.stepReplace(ctx, step.Args, current, rec)
	default:
		if step.Name == "" {
			return current, nil
		}
		return nil, fmt.Errorf("unsupported formatting operation %s", step.Name)
	}
}

func (f *FieldFormatterTransformer) stepTransform(ctx context.Context, args []formatArg, current any, rec utils.Record) (any, error) {
	if len(args) < 2 {
		return current, fmt.Errorf("transform requires at least source field and output pattern")
	}
	sourceVal, ok := f.resolveArgValue(ctx, args[0], rec, current)
	if !ok {
		return nil, fmt.Errorf("transform source %s not found", args[0].Value)
	}
	if sourceVal == nil {
		return nil, nil
	}
	outputPattern := args[1].Value
	inputPatterns := make([]string, 0, len(args)-2)
	for _, arg := range args[2:] {
		inputPatterns = append(inputPatterns, arg.Value)
	}
	formatted, err := formatTimeValue(sourceVal, outputPattern, inputPatterns)
	if err != nil {
		return nil, err
	}
	return formatted, nil
}

func (f *FieldFormatterTransformer) stepCopy(ctx context.Context, args []formatArg, current any, rec utils.Record) (any, error) {
	if len(args) == 0 {
		return current, fmt.Errorf("copy requires a field reference")
	}
	val, _ := f.resolveArgValue(ctx, args[0], rec, current)
	return val, nil
}

func (f *FieldFormatterTransformer) stepSet(ctx context.Context, args []formatArg, current any, rec utils.Record) (any, error) {
	if len(args) == 0 {
		return current, fmt.Errorf("set requires a value")
	}
	val, _ := f.resolveArgValue(ctx, args[0], rec, current)
	return val, nil
}

func (f *FieldFormatterTransformer) stepDefault(ctx context.Context, args []formatArg, current any, rec utils.Record) (any, error) {
	if !hasValue(current) {
		if len(args) == 0 {
			return current, nil
		}
		val, _ := f.resolveArgValue(ctx, args[0], rec, current)
		return val, nil
	}
	return current, nil
}

func (f *FieldFormatterTransformer) stepAffix(ctx context.Context, args []formatArg, current any, rec utils.Record, prefix bool) (any, error) {
	if len(args) == 0 {
		return current, fmt.Errorf("%s requires a value", map[bool]string{true: "prefix", false: "suffix"}[prefix])
	}
	additionVal, _ := f.resolveArgValue(ctx, args[0], rec, current)
	addition := fmt.Sprint(additionVal)
	base := fmt.Sprint(current)
	if current == nil {
		base = ""
	}
	if prefix {
		return addition + base, nil
	}
	return base + addition, nil
}

func (f *FieldFormatterTransformer) stepReplace(ctx context.Context, args []formatArg, current any, rec utils.Record) (any, error) {
	if len(args) < 2 {
		return current, fmt.Errorf("replace requires old and new values")
	}
	original := fmt.Sprint(current)
	oldVal, _ := f.resolveArgValue(ctx, args[0], rec, current)
	newVal, _ := f.resolveArgValue(ctx, args[1], rec, current)
	return strings.ReplaceAll(original, fmt.Sprint(oldVal), fmt.Sprint(newVal)), nil
}

func (f *FieldFormatterTransformer) resolveArgValue(ctx context.Context, arg formatArg, rec utils.Record, current any) (any, bool) {
	if arg.Literal {
		return arg.Value, true
	}
	key := strings.TrimSpace(arg.Value)
	if key == "" {
		return current, true
	}
	switch strings.ToLower(key) {
	case "$current", "current", "self", "this", "_":
		return current, true
	}
	if val, ok := rec[key]; ok {
		return val, true
	}
	_, value := utils.GetValue(ctx, key, rec)
	if value != nil {
		return value, true
	}
	return nil, false
}

func applyStringOp(val any, op func(string) string) any {
	if val == nil {
		return nil
	}
	str := fmt.Sprint(val)
	return op(str)
}

func hasValue(val any) bool {
	if val == nil {
		return false
	}
	switch v := val.(type) {
	case string:
		return strings.TrimSpace(v) != ""
	case []byte:
		return len(v) > 0
	}
	return true
}

func parseFormatPipeline(expr string) ([]formatStep, error) {
	parts := splitPipeline(expr)
	steps := make([]formatStep, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		step, err := parseStep(part)
		if err != nil {
			return nil, err
		}
		steps = append(steps, step)
	}
	if len(steps) == 0 {
		return nil, fmt.Errorf("no formatting operations defined")
	}
	return steps, nil
}

func splitPipeline(expr string) []string {
	var parts []string
	var current strings.Builder
	inQuotes := false
	quoteChar := rune(0)
	for _, r := range expr {
		switch r {
		case '|':
			if inQuotes {
				current.WriteRune(r)
			} else {
				parts = append(parts, current.String())
				current.Reset()
			}
		case '\'':
			if inQuotes && quoteChar == '\'' {
				inQuotes = false
				quoteChar = 0
			} else if !inQuotes {
				inQuotes = true
				quoteChar = '\''
			}
			current.WriteRune(r)
		case '"':
			if inQuotes && quoteChar == '"' {
				inQuotes = false
				quoteChar = 0
			} else if !inQuotes {
				inQuotes = true
				quoteChar = '"'
			}
			current.WriteRune(r)
		default:
			current.WriteRune(r)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts
}

func parseStep(part string) (formatStep, error) {
	part = strings.TrimSpace(part)
	step := formatStep{}
	if part == "" {
		return step, fmt.Errorf("empty formatting step")
	}
	idx := strings.Index(part, "(")
	if idx == -1 {
		step.Name = strings.ToLower(part)
		return step, nil
	}
	step.Name = strings.ToLower(strings.TrimSpace(part[:idx]))
	end := strings.LastIndex(part, ")")
	if end == -1 || end < idx {
		return step, fmt.Errorf("missing closing parenthesis in %s", part)
	}
	argStr := strings.TrimSpace(part[idx+1 : end])
	if argStr == "" {
		return step, nil
	}
	step.Args = splitArgs(argStr)
	return step, nil
}

func splitArgs(input string) []formatArg {
	var args []formatArg
	var current strings.Builder
	inQuotes := false
	quoteChar := rune(0)
	depth := 0
	for _, r := range input {
		switch r {
		case '\'':
			if inQuotes && quoteChar == '\'' {
				inQuotes = false
				quoteChar = 0
			} else if !inQuotes {
				inQuotes = true
				quoteChar = '\''
			}
			current.WriteRune(r)
		case '"':
			if inQuotes && quoteChar == '"' {
				inQuotes = false
				quoteChar = 0
			} else if !inQuotes {
				inQuotes = true
				quoteChar = '"'
			}
			current.WriteRune(r)
		case '(':
			if inQuotes {
				current.WriteRune(r)
			} else {
				depth++
				current.WriteRune(r)
			}
		case ')':
			if inQuotes {
				current.WriteRune(r)
			} else if depth > 0 {
				depth--
				current.WriteRune(r)
			} else {
				current.WriteRune(r)
			}
		case ',':
			if inQuotes || depth > 0 {
				current.WriteRune(r)
			} else {
				raw := strings.TrimSpace(current.String())
				if raw != "" {
					args = append(args, buildFormatArg(raw))
				}
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}
	}
	raw := strings.TrimSpace(current.String())
	if raw != "" {
		args = append(args, buildFormatArg(raw))
	}
	return args
}

func buildFormatArg(raw string) formatArg {
	if len(raw) >= 2 {
		if (raw[0] == '"' && raw[len(raw)-1] == '"') || (raw[0] == '\'' && raw[len(raw)-1] == '\'') {
			unquoted := raw[1 : len(raw)-1]
			unquoted = strings.ReplaceAll(unquoted, `\"`, `"`)
			unquoted = strings.ReplaceAll(unquoted, `\'`, `'`)
			return formatArg{Value: unquoted, Literal: true}
		}
	}
	return formatArg{Value: raw}
}

func formatTimeValue(value any, outputPattern string, inputPatterns []string) (string, error) {
	if !hasValue(value) {
		return "", nil
	}
	var parsed time.Time
	switch v := value.(type) {
	case time.Time:
		parsed = v
	default:
		str := fmt.Sprint(v)
		t, err := parseTimeWithPatterns(str, inputPatterns)
		if err != nil {
			return "", err
		}
		parsed = t
	}
	layout := layoutFromPattern(outputPattern)
	return parsed.Format(layout), nil
}

func parseTimeWithPatterns(value string, patterns []string) (time.Time, error) {
	layouts := make([]string, 0, len(patterns))
	for _, pattern := range patterns {
		layouts = append(layouts, layoutFromPattern(pattern))
	}
	if len(layouts) == 0 {
		layouts = []string{
			time.RFC3339,
			"2006-01-02",
			"20060102",
			"2006-01-02 15:04:05",
			"2006/01/02",
			"02-01-2006",
		}
	}
	trimmed := strings.TrimSpace(value)
	for _, layout := range layouts {
		if t, err := time.Parse(layout, trimmed); err == nil {
			return t, nil
		}
	}
	if t, err := convert.ToTime(trimmed); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("unable to parse time value %s", value)
}

var formatTokens = []struct {
	token  string
	layout string
}{
	{"YYYY", "2006"},
	{"yyyy", "2006"},
	{"YY", "06"},
	{"yy", "06"},
	{"MMMM", "January"},
	{"MMM", "Jan"},
	{"MM", "01"},
	{"M", "1"},
	{"DD", "02"},
	{"D", "2"},
	{"HH", "15"},
	{"H", "15"},
	{"hh", "03"},
	{"h", "3"},
	{"mm", "04"},
	{"m", "4"},
	{"ss", "05"},
	{"s", "5"},
	{"SSS", "000"},
	{"A", "PM"},
	{"a", "pm"},
	{"ZZ", "-0700"},
	{"Z", "-07"},
}

func layoutFromPattern(pattern string) string {
	if strings.TrimSpace(pattern) == "" {
		return time.RFC3339
	}
	var b strings.Builder
	inLiteral := false
	literalChar := byte(0)
	for i := 0; i < len(pattern); {
		ch := pattern[i]
		if ch == '\'' || ch == '"' {
			if inLiteral && ch == literalChar {
				inLiteral = false
				literalChar = 0
				i++
				continue
			}
			if !inLiteral {
				inLiteral = true
				literalChar = ch
				i++
				continue
			}
		}
		if inLiteral {
			b.WriteByte(ch)
			i++
			continue
		}
		matched := false
		for _, token := range formatTokens {
			if strings.HasPrefix(pattern[i:], token.token) {
				b.WriteString(token.layout)
				i += len(token.token)
				matched = true
				break
			}
		}
		if !matched {
			b.WriteByte(ch)
			i++
		}
	}
	return b.String()
}
