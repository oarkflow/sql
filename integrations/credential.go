package integrations

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/json"
)

type Credential struct {
	Key         string         `json:"key"`
	Type        CredentialType `json:"type"`
	Data        any            `json:"data"`
	Description string         `json:"description"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
}

// UnmarshalJSON for Credential decodes the data field based on the credential type.
func (c *Credential) UnmarshalJSON(data []byte) error {
	type Alias Credential
	aux := &struct {
		Data json.RawMessage `json:"data"`
		*Alias
	}{
		Alias: (*Alias)(c),
	}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	switch c.Type {
	case CredentialTypeAPIKey:
		var d APIKeyCredential
		if err := json.Unmarshal(aux.Data, &d); err != nil {
			return err
		}
		c.Data = d
	case CredentialTypeBearer:
		var d BearerCredential
		if err := json.Unmarshal(aux.Data, &d); err != nil {
			return err
		}
		c.Data = d
	case CredentialTypeBasicAuth, CredentialTypeSMTP:
		var d SMTPAuthCredential
		if err := json.Unmarshal(aux.Data, &d); err != nil {
			return err
		}
		c.Data = d
	case CredentialTypeOAuth2:
		var d OAuth2Credential
		if err := json.Unmarshal(aux.Data, &d); err != nil {
			return err
		}
		c.Data = &d
	case CredentialTypeSMPP:
		var d SMPPCredential
		if err := json.Unmarshal(aux.Data, &d); err != nil {
			return err
		}
		c.Data = d
	case CredentialTypeDatabase:
		var d DatabaseCredential
		if err := json.Unmarshal(aux.Data, &d); err != nil {
			return err
		}
		c.Data = d
	default:
		var d map[string]any
		if err := json.Unmarshal(aux.Data, &d); err != nil {
			return err
		}
		c.Data = d
	}
	return nil
}

type CredentialStore interface {
	AddCredential(Credential) error
	GetCredential(key string, requireAuth ...bool) (Credential, error)
	UpdateCredential(Credential) error
	DeleteCredential(string) error
	ListCredentials() ([]Credential, error)
}

type InMemoryCredentialStore struct {
	credentials map[string]Credential
	mu          sync.RWMutex
}

func NewInMemoryCredentialStore() *InMemoryCredentialStore {
	return &InMemoryCredentialStore{
		credentials: make(map[string]Credential),
	}
}

func (c *InMemoryCredentialStore) AddCredential(cred Credential) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.credentials[cred.Key]; exists {
		return fmt.Errorf("credential already exists: %s", cred.Key)
	}
	c.credentials[cred.Key] = cred
	return nil
}

func (c *InMemoryCredentialStore) GetCredential(key string, requireAuth ...bool) (Credential, error) {
	hasCredential := false
	if len(requireAuth) > 0 && requireAuth[0] {
		hasCredential = true
	}
	key = strings.TrimSpace(key)
	if key == "" {
		if hasCredential {
			return Credential{}, fmt.Errorf("credential key cannot be empty")
		}
		return Credential{}, nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	cred, exists := c.credentials[key]
	if !exists {
		if hasCredential {
			return Credential{}, fmt.Errorf("credential not found: %s", key)
		}
		return Credential{}, nil
	}
	return cred, nil
}

func (c *InMemoryCredentialStore) UpdateCredential(cred Credential) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.credentials[cred.Key] = cred
	return nil
}

func (c *InMemoryCredentialStore) DeleteCredential(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.credentials[key]; !exists {
		return fmt.Errorf("credential not found: %s", key)
	}
	delete(c.credentials, key)
	return nil
}

func (c *InMemoryCredentialStore) ListCredentials() ([]Credential, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	creds := make([]Credential, 0, len(c.credentials))
	for _, cred := range c.credentials {
		creds = append(creds, cred)
	}
	return creds, nil
}
