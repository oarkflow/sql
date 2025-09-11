package sql

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/json"
	"github.com/oarkflow/log"
	"github.com/oarkflow/xid/wuid"

	"github.com/oarkflow/sql/integrations"
	"github.com/oarkflow/sql/pkg/utils"
)

// defaultManager is used when no specific user manager is provided.
var defaultManager = integrations.New()

// userManagers holds managers keyed by user id.
var (
	userManagers = map[string]*integrations.Manager{}
	umMutex      sync.RWMutex
)

func toString(val any) string {
	switch val := val.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	default:
		return fmt.Sprint(val)
	}
}

// getManager returns the user-specific manager if userID is non-empty;
// otherwise, it returns the default manager. If a manager does not exist for a given
// user, one will be created lazily.
func getManager(ctx context.Context) *integrations.Manager {
	userID := toString(ctx.Value("user_id"))
	if userID != "" {
		umMutex.RLock()
		mgr, ok := userManagers[userID]
		umMutex.RUnlock()
		if ok {
			return mgr
		}
		// Create a new manager for the user
		umMutex.Lock()
		defer umMutex.Unlock()
		// Double-check whether it was created in the meantime.
		if mgr, ok = userManagers[userID]; !ok {
			mgr = integrations.New()
			userManagers[userID] = mgr
		}
		return mgr
	}
	return defaultManager
}

// RegisterUserManager allows you to explicitly register a manager for a given user.
func RegisterUserManager(ctx context.Context, mgr *integrations.Manager) error {
	userID := toString(ctx.Value("user_id"))
	if userID == "" {
		return fmt.Errorf("userID cannot be empty; use default manager instead")
	}
	umMutex.Lock()
	defer umMutex.Unlock()
	userManagers[userID] = mgr
	return nil
}

// RegisterIntegrationForUser registers an integration service for the specified user.
// If userID is empty it will use the default manager.
func RegisterIntegrationForUser(ctx context.Context, service integrations.Service, credentials ...integrations.Credential) error {
	mgr := getManager(ctx)
	if len(credentials) > 0 {
		service.RequireAuth = true
		credential := credentials[0]
		if credential.Key == "" {
			credential.Key = wuid.New().String()
		}
		if err := mgr.UpdateCredential(credential); err != nil {
			if err := mgr.AddCredential(credential); err != nil {
				return err
			}
		}
		if service.CredentialKey == "" {
			service.CredentialKey = credential.Key
		}
	}
	if err := mgr.UpdateService(service); err != nil {
		if err := mgr.AddService(service); err != nil {
			return err
		}
	}
	return nil
}

// ReadServiceForUser executes a service based on an identifier for the specified user.
// If userID is empty it will use the default manager.
func ReadServiceForUser(ctx context.Context, identifier string) ([]utils.Record, error) {
	parts := strings.SplitN(identifier, ".", 2)
	integrationKey := parts[0]
	var query any
	var source string
	if len(parts) > 1 {
		source = parts[1]
		query = fmt.Sprintf("SELECT * FROM %s", source)
	}
	mgr := getManager(ctx)
	start := time.Now()
	userID := ctx.Value("user_id")
	result, err := mgr.Execute(ctx, integrationKey, query)
	if err != nil {
		return nil, err
	}
	if userID != nil {
		latency := time.Since(start)
		log.Info().Any("user_id", userID).Str("integration", integrationKey).Str("latency", fmt.Sprintf("%s", latency)).Msg("Executed integration")
	}
	switch result := result.(type) {
	case []map[string]any:
		return result, nil
	case map[string]any:
		return []map[string]any{result}, nil
	case integrations.ServiceResponse:
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
	case *integrations.ServiceResponse:
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
