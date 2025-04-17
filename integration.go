package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/oarkflow/json"
	"github.com/oarkflow/xid"

	"github.com/oarkflow/sql/integrations"
	"github.com/oarkflow/sql/pkg/utils"
)

var (
	manager = integrations.New()
)

func RegisterIntegration(service integrations.Service, credentials ...integrations.Credential) error {
	if len(credentials) > 0 {
		service.RequireAuth = true
		credential := credentials[0]
		if credential.Key == "" {
			credential.Key = xid.New().String()
		}
		if err := manager.UpdateCredential(credential); err != nil {
			if err := manager.AddCredential(credential); err != nil {
				return err
			}
		}
		if service.CredentialKey == "" {
			service.CredentialKey = credential.Key
		}
	}
	if err := manager.UpdateService(service); err != nil {
		if err := manager.AddService(service); err != nil {
			return err
		}
	}
	return nil
}

func ReadService(identifier string) ([]utils.Record, error) {
	parts := strings.SplitN(identifier, ".", 2)
	integrationKey := parts[0]
	var query any
	var source string
	if len(parts) > 1 {
		source = parts[1]
		query = fmt.Sprintf("SELECT * FROM %s", source)
	}
	result, err := manager.Execute(context.Background(), integrationKey, query)
	if err != nil {
		return nil, err
	}
	switch result := result.(type) {
	case []map[string]any:
		return result, nil
	case map[string]any:
		return []map[string]any{result}, nil
	case integrations.HTTPResponse:
		var dest []map[string]any
		err = json.Unmarshal(result.Body, &dest)
		if err == nil {
			return dest, nil
		}
		var tmp map[string]any
		err = json.Unmarshal(result.Body, &tmp)
		if err == nil {
			return []map[string]any{tmp}, nil
		}
		return nil, err
	case *integrations.HTTPResponse:
		var dest []map[string]any
		err = json.Unmarshal(result.Body, &dest)
		if err == nil {
			return dest, nil
		}
		var tmp map[string]any
		err = json.Unmarshal(result.Body, &tmp)
		if err == nil {
			return []map[string]any{tmp}, nil
		}
		return nil, err
	}
	return nil, fmt.Errorf("integration %s not found with type %T", integrationKey, result)
}
