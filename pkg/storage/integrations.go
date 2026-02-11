package storage

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/oarkflow/json"
	"github.com/oarkflow/sql/integrations"
)

// ErrIntegrationNotFound signals lookups that failed to resolve a stored service.
var ErrIntegrationNotFound = errors.New("integration not found")

type integrationRecord struct {
	Name          string
	Type          integrations.ServiceType
	Description   sql.NullString
	Status        sql.NullString
	RequireAuth   bool
	CredentialKey sql.NullString
	Config        string
	Enabled       bool
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// CreateIntegration inserts a new integration definition.
func (s *Store) CreateIntegration(ctx context.Context, svc integrations.Service) error {
	now := time.Now().UTC()
	if svc.CreatedAt.IsZero() {
		svc.CreatedAt = now
	}
	if svc.Status == "" {
		svc.Status = "pending"
	}
	if svc.Description == "" {
		svc.Description = ""
	}
	svc.UpdatedAt = now
	return s.persistIntegration(ctx, svc, false)
}

// UpdateIntegration updates an existing integration record.
func (s *Store) UpdateIntegration(ctx context.Context, svc integrations.Service) error {
	svc.UpdatedAt = time.Now().UTC()
	if svc.Status == "" {
		svc.Status = "pending"
	}
	return s.persistIntegration(ctx, svc, true)
}

func (s *Store) persistIntegration(ctx context.Context, svc integrations.Service, isUpdate bool) error {
	cfgBytes, err := json.Marshal(svc.Config)
	if err != nil {
		return err
	}

	if isUpdate {
		res, err := s.db.ExecContext(
			ctx,
			`UPDATE integrations SET
                type = ?,
                description = ?,
                status = ?,
                require_auth = ?,
                credential_key = ?,
                config = ?,
                enabled = ?,
                updated_at = ?
            WHERE name = ?`,
			svc.Type,
			nullString(svc.Description),
			svc.Status,
			boolToInt(svc.RequireAuth),
			nullString(svc.CredentialKey),
			string(cfgBytes),
			boolToInt(svc.Enabled),
			svc.UpdatedAt,
			svc.Name,
		)
		if err != nil {
			return err
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			return ErrIntegrationNotFound
		}
		return nil
	}

	_, err = s.db.ExecContext(
		ctx,
		`INSERT INTO integrations (
            name, type, description, status, require_auth, credential_key, config, enabled, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		svc.Name,
		svc.Type,
		nullString(svc.Description),
		svc.Status,
		boolToInt(svc.RequireAuth),
		nullString(svc.CredentialKey),
		string(cfgBytes),
		boolToInt(svc.Enabled),
		svc.CreatedAt,
		svc.UpdatedAt,
	)
	return err
}

// ListIntegrations returns all persisted services.
func (s *Store) ListIntegrations(ctx context.Context) ([]integrations.Service, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT name, type, description, status, require_auth, credential_key, config, enabled, created_at, updated_at FROM integrations ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var services []integrations.Service
	for rows.Next() {
		rec := integrationRecord{}
		var requireAuth, enabled int
		if err := rows.Scan(&rec.Name, &rec.Type, &rec.Description, &rec.Status, &requireAuth, &rec.CredentialKey, &rec.Config, &enabled, &rec.CreatedAt, &rec.UpdatedAt); err != nil {
			return nil, err
		}
		rec.RequireAuth = requireAuth == 1
		rec.Enabled = enabled == 1
		svc, err := s.hydrateIntegration(rec)
		if err != nil {
			return nil, err
		}
		services = append(services, svc)
	}
	return services, rows.Err()
}

// GetIntegration fetches a single integration.
func (s *Store) GetIntegration(ctx context.Context, name string) (integrations.Service, error) {
	rec := integrationRecord{}
	var requireAuth, enabled int
	row := s.db.QueryRowContext(ctx, `SELECT name, type, description, status, require_auth, credential_key, config, enabled, created_at, updated_at FROM integrations WHERE name = ?`, name)
	if err := row.Scan(&rec.Name, &rec.Type, &rec.Description, &rec.Status, &requireAuth, &rec.CredentialKey, &rec.Config, &enabled, &rec.CreatedAt, &rec.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return integrations.Service{}, ErrIntegrationNotFound
		}
		return integrations.Service{}, err
	}
	rec.RequireAuth = requireAuth == 1
	rec.Enabled = enabled == 1
	return s.hydrateIntegration(rec)
}

// DeleteIntegration removes a service definition.
func (s *Store) DeleteIntegration(ctx context.Context, name string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM integrations WHERE name = ?`, name)
	return err
}

// UpdateIntegrationStatus updates the last-known connection status.
func (s *Store) UpdateIntegrationStatus(ctx context.Context, name, status string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE integrations SET status = ?, updated_at = ? WHERE name = ?`, status, time.Now().UTC(), name)
	return err
}

func (s *Store) hydrateIntegration(rec integrationRecord) (integrations.Service, error) {
	wrapper := struct {
		Name          string                   `json:"name"`
		Type          integrations.ServiceType `json:"type"`
		Config        json.RawMessage          `json:"config"`
		RequireAuth   bool                     `json:"require_auth"`
		CredentialKey string                   `json:"credential_key"`
		Enabled       bool                     `json:"enabled"`
	}{
		Name:        rec.Name,
		Type:        rec.Type,
		Config:      json.RawMessage(rec.Config),
		RequireAuth: rec.RequireAuth,
		Enabled:     rec.Enabled,
	}
	if rec.CredentialKey.Valid {
		wrapper.CredentialKey = rec.CredentialKey.String
	}

	bytes, err := json.Marshal(wrapper)
	if err != nil {
		return integrations.Service{}, err
	}
	var svc integrations.Service
	if err := json.Unmarshal(bytes, &svc); err != nil {
		return integrations.Service{}, err
	}
	if rec.Description.Valid {
		svc.Description = rec.Description.String
	}
	if rec.Status.Valid {
		svc.Status = rec.Status.String
	}
	svc.CreatedAt = rec.CreatedAt
	svc.UpdatedAt = rec.UpdatedAt
	return svc, nil
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func nullString(v string) interface{} {
	if v == "" {
		return nil
	}
	return v
}
