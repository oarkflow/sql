package transformers

import (
	"fmt"
	"strings"

	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/contracts"
)

// BuildTransformers converts transformation configs into executable transformers.
func BuildTransformers(cfgs []config.TransformerConfig) ([]contracts.Transformer, error) {
	transformers := make([]contracts.Transformer, 0, len(cfgs))
	for _, tCfg := range cfgs {
		transformer, err := BuildTransformer(tCfg)
		if err != nil {
			return nil, err
		}
		transformers = append(transformers, transformer)
	}
	return transformers, nil
}

// BuildTransformer instantiates a transformer from config.
func BuildTransformer(cfg config.TransformerConfig) (contracts.Transformer, error) {
	switch strings.ToLower(cfg.Type) {
	case "hl7", "hl7_to_json", "hl7_to_xml":
		opts := HL7TransformerOptions{}
		if cfg.Options != nil {
			assignStringOption(&opts.InputField, cfg.Options, "input_field")
			assignStringOption(&opts.OutputJSONField, cfg.Options, "json_field")
			assignStringOption(&opts.OutputXMLField, cfg.Options, "xml_field")
			assignStringOption(&opts.OutputSegmentsField, cfg.Options, "segments_field")
			assignStringOption(&opts.OutputTypedField, cfg.Options, "typed_field")
			assignStringOption(&opts.OutputDocumentField, cfg.Options, "document_field")
			assignStringOption(&opts.MessageTypeField, cfg.Options, "message_type_field")
			assignStringOption(&opts.ControlIDField, cfg.Options, "control_id_field")
			assignStringOption(&opts.TimestampField, cfg.Options, "timestamp_field")
			assignStringOption(&opts.OutputTypedJSONField, cfg.Options, "typed_json_field")
			assignStringOption(&opts.OutputTypedXMLField, cfg.Options, "typed_xml_field")
		}
		return NewHL7Transformer(opts), nil
	case "field_formatter", "field_format", "format_fields", "formatter", "formatting":
		fields, err := extractFormatterFields(cfg.Options)
		if err != nil {
			return nil, err
		}
		return NewFieldFormatterTransformer(fields)
	default:
		return nil, fmt.Errorf("unsupported transformer type %s", cfg.Type)
	}
}

func assignStringOption(target *string, options map[string]any, key string) {
	if val, ok := options[key]; ok {
		if str, ok := val.(string); ok {
			*target = str
		}
	}
}

func extractFormatterFields(options map[string]any) (map[string]string, error) {
	if options == nil {
		return nil, fmt.Errorf("field formatter transformer requires options")
	}
	raw, ok := options["fields"]
	if !ok {
		return nil, fmt.Errorf("field formatter transformer requires a 'fields' option")
	}
	return normalizeFieldMap(raw)
}

func normalizeFieldMap(raw any) (map[string]string, error) {
	switch v := raw.(type) {
	case map[string]string:
		return v, nil
	case map[string]any:
		result := make(map[string]string, len(v))
		for key, value := range v {
			result[key] = fmt.Sprint(value)
		}
		return result, nil
	case []any:
		result := make(map[string]string, len(v))
		for _, item := range v {
			entry, ok := item.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("fields array entries must be objects, got %T", item)
			}
			fieldName := fmt.Sprint(entry["field"])
			if fieldName == "" {
				fieldName = fmt.Sprint(entry["name"])
			}
			if fieldName == "" {
				return nil, fmt.Errorf("fields array entry missing field/name")
			}
			expr, ok := entry["expression"]
			if !ok {
				expr, ok = entry["value"]
			}
			if !ok {
				return nil, fmt.Errorf("fields array entry missing expression/value for %s", fieldName)
			}
			result[fieldName] = fmt.Sprint(expr)
		}
		return result, nil
	default:
		return nil, fmt.Errorf("fields option must be a map or array, got %T", raw)
	}
}
