package storage

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/oarkflow/json"
	"github.com/oarkflow/sql/integrations"
)

type credentialRecord struct {
	Key         string
	Type        integrations.CredentialType
	Description sql.NullString
	Data        []byte
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// UpsertCredential inserts or updates a credential.
func (s *Store) UpsertCredential(ctx context.Context, cred integrations.Credential) error {
	if cred.Key == "" {
		return errors.New("credential key is required")
	}
	now := time.Now().UTC()
	if cred.CreatedAt.IsZero() {
		cred.CreatedAt = now
	}
	cred.UpdatedAt = now

	payload, err := json.Marshal(cred.Data)
	if err != nil {
		return err
	}
	blob, err := s.secret.encrypt(payload)
	if err != nil {
		return err
	}

	_, err = s.db.ExecContext(
		ctx,
		`INSERT INTO credentials (key, type, description, data, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(key) DO UPDATE SET
            type=excluded.type,
            description=excluded.description,
            data=excluded.data,
            updated_at=excluded.updated_at`,
		cred.Key,
		cred.Type,
		cred.Description,
		blob,
		cred.CreatedAt,
		cred.UpdatedAt,
	)
	return err
}

// GetCredential retrieves a credential by key.
func (s *Store) GetCredential(ctx context.Context, key string) (integrations.Credential, error) {
	row := s.db.QueryRowContext(ctx, `SELECT key, type, description, data, created_at, updated_at FROM credentials WHERE key = ?`, key)
	rec := credentialRecord{}
	if err := row.Scan(&rec.Key, &rec.Type, &rec.Description, &rec.Data, &rec.CreatedAt, &rec.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return integrations.Credential{}, integrations.ErrCredentialNotFound
		}
		return integrations.Credential{}, err
	}
	return s.hydrateCredential(rec)
}

// ListCredentials returns all stored credentials.
func (s *Store) ListCredentials(ctx context.Context) ([]integrations.Credential, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT key, type, description, data, created_at, updated_at FROM credentials ORDER BY key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var creds []integrations.Credential
	for rows.Next() {
		rec := credentialRecord{}
		if err := rows.Scan(&rec.Key, &rec.Type, &rec.Description, &rec.Data, &rec.CreatedAt, &rec.UpdatedAt); err != nil {
			return nil, err
		}
		cred, err := s.hydrateCredential(rec)
		if err != nil {
			return nil, err
		}
		creds = append(creds, cred)
	}
	return creds, rows.Err()
}

// DeleteCredential removes a credential and nulls dependent integrations.
func (s *Store) DeleteCredential(ctx context.Context, key string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM credentials WHERE key = ?`, key)
	return err
}

func (s *Store) hydrateCredential(rec credentialRecord) (integrations.Credential, error) {
	payload, err := s.secret.decrypt(rec.Data)
	if err != nil {
		return integrations.Credential{}, err
	}
	wrapper := struct {
		Key  string                      `json:"key"`
		Type integrations.CredentialType `json:"type"`
		Data json.RawMessage             `json:"data"`
	}{
		Key:  rec.Key,
		Type: rec.Type,
		Data: json.RawMessage(payload),
	}

	bytes, err := json.Marshal(wrapper)
	if err != nil {
		return integrations.Credential{}, err
	}
	var cred integrations.Credential
	if err := json.Unmarshal(bytes, &cred); err != nil {
		return integrations.Credential{}, err
	}
	if rec.Description.Valid {
		cred.Description = rec.Description.String
	}
	cred.CreatedAt = rec.CreatedAt
	cred.UpdatedAt = rec.UpdatedAt
	return cred, nil
}
