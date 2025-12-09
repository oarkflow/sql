package storage

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

// SavedQueryRecord represents a persisted saved query definition.
type SavedQueryRecord struct {
	ID          string
	Name        string
	Integration string
	Query       string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// ErrSavedQueryNotFound indicates missing saved query entries.
var ErrSavedQueryNotFound = errors.New("saved query not found")

// QueryHistoryRecord represents a stored query execution event.
type QueryHistoryRecord struct {
	ID            string
	QueryID       string
	Name          string
	Query         string
	Integration   string
	RowCount      int
	ExecutionTime float64
	Success       bool
	Error         string
	CreatedAt     time.Time
}

// SaveQuery persists a named query for later reuse.
func (s *Store) SaveQuery(ctx context.Context, name, query, integration string) (SavedQueryRecord, error) {
	recName := strings.TrimSpace(name)
	recIntegration := strings.TrimSpace(integration)
	rec := SavedQueryRecord{
		ID:          uuid.New().String(),
		Name:        recName,
		Integration: recIntegration,
		Query:       query,
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}

	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO saved_queries (id, name, integration, query, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
		rec.ID,
		nullString(recName),
		nullString(recIntegration),
		rec.Query,
		rec.CreatedAt,
		rec.UpdatedAt,
	)
	if err != nil {
		return SavedQueryRecord{}, err
	}
	return rec, nil
}

// GetSavedQuery fetches a single saved query by identifier.
func (s *Store) GetSavedQuery(ctx context.Context, id string) (SavedQueryRecord, error) {
	rec := SavedQueryRecord{}
	var name sql.NullString
	var integration sql.NullString
	row := s.db.QueryRowContext(ctx, `SELECT id, name, integration, query, created_at, updated_at FROM saved_queries WHERE id = ?`, id)
	if err := row.Scan(&rec.ID, &name, &integration, &rec.Query, &rec.CreatedAt, &rec.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SavedQueryRecord{}, ErrSavedQueryNotFound
		}
		return SavedQueryRecord{}, err
	}
	if name.Valid {
		rec.Name = name.String
	}
	if integration.Valid {
		rec.Integration = integration.String
	}
	return rec, nil
}

// ListSavedQueries returns the stored named queries ordered by most recent update.
func (s *Store) ListSavedQueries(ctx context.Context) ([]SavedQueryRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, name, integration, query, created_at, updated_at FROM saved_queries ORDER BY updated_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var queries []SavedQueryRecord
	for rows.Next() {
		var name sql.NullString
		var integration sql.NullString
		rec := SavedQueryRecord{}
		if err := rows.Scan(&rec.ID, &name, &integration, &rec.Query, &rec.CreatedAt, &rec.UpdatedAt); err != nil {
			return nil, err
		}
		if name.Valid {
			rec.Name = name.String
		}
		if integration.Valid {
			rec.Integration = integration.String
		}
		queries = append(queries, rec)
	}
	return queries, rows.Err()
}

// UpdateSavedQuery updates an existing saved query definition.
func (s *Store) UpdateSavedQuery(ctx context.Context, rec SavedQueryRecord) (SavedQueryRecord, error) {
	rec.Name = strings.TrimSpace(rec.Name)
	rec.Integration = strings.TrimSpace(rec.Integration)
	rec.UpdatedAt = time.Now().UTC()
	result, err := s.db.ExecContext(
		ctx,
		`UPDATE saved_queries SET name = ?, integration = ?, query = ?, updated_at = ? WHERE id = ?`,
		nullString(rec.Name),
		nullString(rec.Integration),
		rec.Query,
		rec.UpdatedAt,
		rec.ID,
	)
	if err != nil {
		return SavedQueryRecord{}, err
	}
	rows, err := result.RowsAffected()
	if err == nil && rows == 0 {
		return SavedQueryRecord{}, ErrSavedQueryNotFound
	}
	updated, err := s.GetSavedQuery(ctx, rec.ID)
	if err != nil {
		return SavedQueryRecord{}, err
	}
	return updated, nil
}

// DeleteSavedQuery removes a saved query by identifier.
func (s *Store) DeleteSavedQuery(ctx context.Context, id string) error {
	result, err := s.db.ExecContext(ctx, `DELETE FROM saved_queries WHERE id = ?`, id)
	if err != nil {
		return err
	}
	if rows, err := result.RowsAffected(); err == nil && rows == 0 {
		return ErrSavedQueryNotFound
	}
	return nil
}

// RecordQueryHistory stores the outcome of a query execution.
func (s *Store) RecordQueryHistory(ctx context.Context, entry QueryHistoryRecord) (QueryHistoryRecord, error) {
	if entry.ID == "" {
		entry.ID = uuid.New().String()
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now().UTC()
	}

	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO query_history (
            id, query_id, integration, query, row_count, execution_time, success, error, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		entry.ID,
		nullString(entry.QueryID),
		nullString(entry.Integration),
		entry.Query,
		entry.RowCount,
		entry.ExecutionTime,
		boolToInt(entry.Success),
		nullString(entry.Error),
		entry.CreatedAt,
	)
	if err != nil {
		return QueryHistoryRecord{}, err
	}
	return entry, nil
}

// ListQueryHistory returns the most recent query executions up to the provided limit.
func (s *Store) ListQueryHistory(ctx context.Context, limit int) ([]QueryHistoryRecord, error) {
	if limit <= 0 {
		limit = 50
	}

	rows, err := s.db.QueryContext(
		ctx,
		`SELECT
            h.id,
            h.query_id,
            sq.name,
            h.query,
            h.integration,
            h.row_count,
            h.execution_time,
            h.success,
            h.error,
            h.created_at
        FROM query_history h
        LEFT JOIN saved_queries sq ON sq.id = h.query_id
        ORDER BY h.created_at DESC
        LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var history []QueryHistoryRecord
	for rows.Next() {
		var (
			queryID     sql.NullString
			name        sql.NullString
			integration sql.NullString
			success     int
			errText     sql.NullString
		)
		rec := QueryHistoryRecord{}
		if err := rows.Scan(&rec.ID, &queryID, &name, &rec.Query, &integration, &rec.RowCount, &rec.ExecutionTime, &success, &errText, &rec.CreatedAt); err != nil {
			return nil, err
		}
		if queryID.Valid {
			rec.QueryID = queryID.String
		}
		if name.Valid {
			rec.Name = name.String
		}
		if integration.Valid {
			rec.Integration = integration.String
		}
		rec.Success = success == 1
		if errText.Valid {
			rec.Error = errText.String
		}
		history = append(history, rec)
	}
	return history, rows.Err()
}

// ClearQueryHistory removes all stored query execution entries.
func (s *Store) ClearQueryHistory(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM query_history`)
	return err
}
