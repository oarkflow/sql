package adapters

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/contracts"
	"github.com/oarkflow/sql/pkg/utils"
	"github.com/oarkflow/sql/pkg/utils/fileutil"
	"github.com/oarkflow/sql/pkg/utils/sqlutil"
)

func NewLookupLoader(lkup config.DataConfig) (contracts.LookupLoader, error) {
	switch strings.ToLower(lkup.Type) {
	case "postgresql", "mysql", "sqlite":
		db, err := config.OpenDB(lkup)
		if err != nil {
			return nil, fmt.Errorf("error connecting to lookup DB: %v", err)
		}
		return NewSQLAdapterAsSource(db, "", lkup.Source), nil
	case "csv", "json":
		return NewFileAdapter(lkup.File, "source", false), nil
	case "nosql":
		return NewNoSQLAdapter(lkup), nil
	case "rest":
		return NewRESTAdapter(lkup), nil
	case "mq":
		return NewMQAdapter(lkup), nil
	default:
		return nil, fmt.Errorf("unsupported lookup type: %s", lkup.Type)
	}
}

type FileAdapter struct {
	mode            string
	Filename        string
	extension       string
	appendMode      bool
	file            *os.File
	jsonFirstRecord bool
	csvHeader       []string
	appender        contracts.Appender[utils.Record]
	headerWritten   bool
}

func NewFileAdapter(fileName, mode string, appendMode bool) *FileAdapter {
	extension := strings.TrimPrefix(filepath.Ext(fileName), ".")
	return &FileAdapter{
		Filename:        fileName,
		extension:       extension,
		appendMode:      appendMode,
		mode:            mode,
		jsonFirstRecord: true,
	}
}

func (fl *FileAdapter) Setup(_ context.Context) error {
	if fl.mode != "loader" {
		_, err := os.Stat(fl.Filename)
		return err
	}
	appender, err := fileutil.NewAppender[utils.Record](fl.Filename, fl.extension, fl.appendMode)
	if err != nil {
		return err
	}
	fl.appender = appender
	return nil
}

func (fl *FileAdapter) StoreBatch(_ context.Context, records []utils.Record) error {
	switch fl.extension {
	case "csv", "json":
		err := fl.appender.AppendBatch(records)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported file extension: %s", fl.extension)
	}
	return nil
}

func (fl *FileAdapter) Close() error {
	if fl.appender != nil {
		err := fl.appender.Close()
		if err != nil {
			return err
		}
	}
	if fl.file != nil {
		return fl.file.Close()
	}
	return nil
}

func (fl *FileAdapter) LoadData(_ ...contracts.Option) ([]utils.Record, error) {
	ch, err := fl.Extract(context.Background())
	if err != nil {
		return nil, err
	}
	var records []utils.Record
	for rec := range ch {
		records = append(records, rec)
	}
	return records, nil
}

func (fl *FileAdapter) Extract(_ context.Context, _ ...contracts.Option) (<-chan utils.Record, error) {
	out := make(chan utils.Record)
	go func() {
		defer close(out)
		_, err := fileutil.ProcessFile(fl.Filename, func(record utils.Record) {
			out <- record
		})
		if err != nil {
			log.Printf("File extraction error: %v", err)
		}
	}()
	return out, nil
}

type SQLAdapter struct {
	Db              *sql.DB
	mode            string
	Table           string
	truncate        bool
	updateSequence  bool
	destType        string
	update          bool
	delete          bool
	query           string
	AutoCreate      bool
	Created         bool
	Driver          string
	NormalizeSchema map[string]string
}

func NewSQLAdapterAsLoader(db *sql.DB, destType, driver string, cfg config.TableMapping, normalizeSchema map[string]string) *SQLAdapter {
	autoCreate := false
	if !cfg.KeyValueTable && cfg.AutoCreateTable {
		autoCreate = true
	}
	return &SQLAdapter{
		Db:              db,
		destType:        destType,
		Table:           cfg.NewName,
		truncate:        cfg.TruncateDestination,
		updateSequence:  cfg.UpdateSequence,
		update:          cfg.Update,
		delete:          cfg.Delete,
		query:           cfg.Query,
		AutoCreate:      autoCreate,
		Created:         false,
		Driver:          driver,
		NormalizeSchema: normalizeSchema,
		mode:            "loader",
	}
}

func NewSQLAdapterAsSource(db *sql.DB, table, query string) *SQLAdapter {
	return &SQLAdapter{Db: db, Table: table, query: query}
}

func (l *SQLAdapter) Setup(ctx context.Context) error {
	if l.mode != "loader" {
		return nil
	}
	if l.truncate {
		exists, err := tableExists(l.Db, l.Table, l.Driver)
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}
		_, err = l.Db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s", l.Table))
		if err != nil {
			return fmt.Errorf("truncate error for table %s: %v", l.Table, err)
		}
	}
	return nil
}

func tableExists(db *sql.DB, tableName, dbType string) (bool, error) {
	var count int
	var query string
	switch dbType {
	case "mysql", "postgres":
		query = fmt.Sprintf("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '%s'", tableName)
	case "sqlite":
		query = fmt.Sprintf("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='%s'", tableName)
	default:
		return false, fmt.Errorf("unsupported DBMS type: %s", dbType)
	}
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (l *SQLAdapter) StoreBatch(ctx context.Context, batch []utils.Record) error {
	if l.update {
		for _, rec := range batch {
			var q string
			var args []any
			if l.query != "" {
				q = l.query
			} else {
				q, args = sqlutil.BuildUpdateStatement(l.Table, rec)
			}
			if _, err := l.Db.ExecContext(ctx, q, args...); err != nil {
				return err
			}
		}
		return nil
	}
	if l.delete {
		for _, rec := range batch {
			var q string
			var args []any
			if l.query != "" {
				q = l.query
			} else {
				q, args = sqlutil.BuildDeleteStatement(l.Table, rec)
			}
			if _, err := l.Db.ExecContext(ctx, q, args...); err != nil {
				return err
			}
		}
		return nil
	}
	if len(batch) == 0 {
		return nil
	}
	if l.AutoCreate && !l.Created {
		if err := sqlutil.CreateTableFromRecord(l.Db, l.Driver, l.Table, l.NormalizeSchema); err != nil {
			return err
		}
		l.Created = true
	}
	var keys []string
	for k := range batch[0] {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var placeholders []string
	var args []any
	argCounter := 1
	for _, rec := range batch {
		var valPlaceholders []string
		for _, k := range keys {
			valPlaceholders = append(valPlaceholders, fmt.Sprintf("$%d", argCounter))
			args = append(args, rec[k])
			argCounter++
		}
		placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(valPlaceholders, ", ")))
	}
	q := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		l.Table,
		strings.Join(keys, ", "),
		strings.Join(placeholders, ", "),
	)
	_, err := l.Db.ExecContext(ctx, q, args...)
	return err
}

func (l *SQLAdapter) LoadData(opts ...contracts.Option) ([]utils.Record, error) {
	ch, err := l.Extract(context.Background(), opts...)
	if err != nil {
		return nil, err
	}
	var records []utils.Record
	for rec := range ch {
		records = append(records, rec)
	}
	return records, nil
}

func (l *SQLAdapter) Extract(ctx context.Context, opts ...contracts.Option) (<-chan utils.Record, error) {
	opt := &contracts.SourceOption{Table: "", Query: ""}
	for _, op := range opts {
		op(opt)
	}
	table, query := l.Table, l.query
	if opt.Table != "" {
		table = opt.Table
	}
	if opt.Query != "" {
		query = opt.Query
	}
	var q string
	if query != "" {
		q = query
	} else {
		q = fmt.Sprintf("SELECT * FROM %s", table)
	}
	out := make(chan utils.Record, 100)
	go func(query string) {
		defer close(out)

		rows, err := l.Db.QueryContext(ctx, q)
		if err != nil {
			log.Printf("SQL query error: %v", err)
			return
		}
		defer func() {
			_ = rows.Close()
		}()
		cols, err := rows.Columns()
		if err != nil {
			log.Printf("Error getting columns: %v", err)
			return
		}
		// Get column types for type conversion
		colTypes, err := rows.ColumnTypes()
		if err != nil {
			log.Printf("Error getting column types: %v", err)
			return
		}
		for rows.Next() {
			columns := make([]any, len(cols))
			columnPointers := make([]any, len(cols))
			for i := range columns {
				columnPointers[i] = &columns[i]
			}
			if err := rows.Scan(columnPointers...); err != nil {
				log.Printf("Scan error: %v", err)
				continue
			}
			rec := make(utils.Record)
			for i, colName := range cols {
				var val any
				// if the scanned value is a []byte, try converting it based on the column type
				if b, ok := columns[i].([]byte); ok {
					if b == nil {
						val = nil
						continue
					}
					dbType := colTypes[i].DatabaseTypeName()
					_, scale, _ := colTypes[i].DecimalSize()
					switch dbType {
					case "INT", "INTEGER", "BIGINT", "TINYINT", "SMALLINT", "MEDIUMINT":
						if num, err := strconv.ParseInt(string(b), 10, 64); err == nil {
							val = num
						} else {
							val = string(b)
						}
					case "NUMERIC":
						if scale > 0 {
							if num, err := strconv.ParseInt(string(b), 10, 64); err == nil {
								val = num
							} else {
								val = string(b)
							}
						} else {
							if num, err := strconv.ParseFloat(string(b), 64); err == nil {
								val = num
							} else {
								val = string(b)
							}
						}
					case "FLOAT", "DOUBLE", "DECIMAL":
						if num, err := strconv.ParseFloat(string(b), 64); err == nil {
							val = num
						} else {
							val = string(b)
						}
					default:
						// default conversion: treat it as a string
						val = string(b)
					}
				} else {
					val = columns[i]
				}
				rec[colName] = val
			}
			out <- rec
		}
	}(q)
	return out, nil
}

func (l *SQLAdapter) Close() error {
	if l.mode != "loader" {
		return l.Db.Close()
	}
	if l.destType == "postgresql" && l.updateSequence {
		return sqlutil.UpdateSequence(l.Db, l.Table)
	}
	return l.Db.Close()
}

type IOAdapter struct {
	mode   string
	reader io.Reader
	writer io.Writer
	format string
}

func NewIOAdapterSource(reader io.Reader, format string) *IOAdapter {
	return &IOAdapter{
		mode:   "source",
		reader: reader,
		format: strings.ToLower(format),
	}
}

func NewIOAdapterLoader(writer io.Writer, format string) *IOAdapter {
	return &IOAdapter{
		mode:   "loader",
		writer: writer,
		format: strings.ToLower(format),
	}
}

func (ioa *IOAdapter) Setup(_ context.Context) error {
	return nil
}

func (ioa *IOAdapter) Extract(_ context.Context, _ ...contracts.Option) (<-chan utils.Record, error) {
	if ioa.mode != "source" {
		return nil, fmt.Errorf("IOAdapter not configured as source")
	}
	out := make(chan utils.Record)
	go func() {
		switch ioa.format {
		case "csv":
			var data string
			if isInteractive(ioa.reader) {
				_, _ = fmt.Fprintln(os.Stdout, "Enter CSV data (press Enter twice to finish):")
				data = readUntilDoubleEnter(ioa.reader)
			} else {

				b, err := io.ReadAll(ioa.reader)
				if err != nil {
					log.Printf("Error reading input: %v", err)
					break
				}
				data = string(b)
			}
			csvReader := csv.NewReader(strings.NewReader(data))
			records, err := csvReader.ReadAll()
			if err != nil {
				log.Printf("CSV read error: %v", err)
				break
			}
			if len(records) < 1 {
				break
			}

			header := records[0]
			for _, row := range records[1:] {
				rec := make(utils.Record)
				for i, field := range row {
					if i < len(header) {
						rec[header[i]] = field
					}
				}
				out <- rec
			}
		case "json":
			var data string
			if isInteractive(ioa.reader) {
				_, _ = fmt.Fprintln(os.Stdout, "Enter JSON data (press Enter to finish):")
				data = readUntilDoubleEnter(ioa.reader)
			} else {
				b, err := io.ReadAll(ioa.reader)
				if err != nil {
					log.Printf("Error reading input: %v", err)
					break
				}
				data = string(b)
			}
			trimmed := strings.TrimSpace(data)
			dec := json.NewDecoder(strings.NewReader(data))
			if len(trimmed) > 0 && trimmed[0] == '[' {

				_, err := dec.Token()
				if err != nil {
					log.Printf("JSON array token error: %v", err)
					break
				}
				for dec.More() {
					var record map[string]any
					if err := dec.Decode(&record); err != nil {
						log.Printf("JSON decode error: %v", err)
						continue
					}
					out <- record
				}
				_, _ = dec.Token()
			} else {

				scanner := bufio.NewScanner(strings.NewReader(data))
				for scanner.Scan() {
					line := scanner.Text()
					if strings.TrimSpace(line) == "" {
						continue
					}
					var record map[string]any
					if err := json.Unmarshal([]byte(line), &record); err != nil {
						log.Printf("JSON unmarshal error: %v", err)
						continue
					}
					out <- record
				}
			}
		default:

			scanner := bufio.NewScanner(ioa.reader)

			const maxCapacity = 1024 * 1024
			buf := make([]byte, 0, 64*1024)
			scanner.Buffer(buf, maxCapacity)
			for scanner.Scan() {
				line := scanner.Text()
				rec := utils.Record{"line": line}
				out <- rec
			}
			if err := scanner.Err(); err != nil {
				log.Printf("Scanner error: %v", err)
			}
		}
		close(out)
	}()
	return out, nil
}

func (ioa *IOAdapter) StoreBatch(_ context.Context, records []utils.Record) error {
	if ioa.mode != "loader" {
		return fmt.Errorf("IOAdapter not configured as loader")
	}
	switch ioa.format {
	case "csv":
		w := csv.NewWriter(ioa.writer)
		if len(records) == 0 {
			return nil
		}
		header := extractCSVHeader(records[0])
		if err := w.Write(header); err != nil {
			return err
		}
		for _, rec := range records {
			row, err := buildCSVRow(header, rec)
			if err != nil {
				return err
			}
			if err := w.Write(row); err != nil {
				return err
			}
		}
		w.Flush()
		if err := w.Error(); err != nil {
			return err
		}
	case "json":
		for _, rec := range records {
			data, err := json.Marshal(rec)
			if err != nil {
				return err
			}
			_, err = ioa.writer.Write(data)
			if err != nil {
				return err
			}
			_, err = ioa.writer.Write([]byte("\n"))
			if err != nil {
				return err
			}
		}
	default:
		for _, rec := range records {
			line := fmt.Sprintf("%v", rec)
			_, err := ioa.writer.Write([]byte(line + "\n"))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (ioa *IOAdapter) Close() error {
	if ioa.mode == "source" {
		if closer, ok := ioa.reader.(io.Closer); ok {
			return closer.Close()
		}
	} else if ioa.mode == "loader" {
		if closer, ok := ioa.writer.(io.Closer); ok {
			return closer.Close()
		}
	}
	return nil
}

func extractCSVHeader(rec utils.Record) []string {
	var header []string
	for k := range rec {
		header = append(header, k)
	}
	sort.Strings(header)
	return header
}

func buildCSVRow(header []string, rec utils.Record) ([]string, error) {
	row := make([]string, len(header))
	for i, key := range header {
		if val, ok := rec[key]; ok {
			row[i] = fmt.Sprintf("%v", val)
		} else {
			row[i] = ""
		}
	}
	return row, nil
}

func isInteractive(r io.Reader) bool {
	f, ok := r.(*os.File)
	if !ok {
		return false
	}
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

func readUntilDoubleEnter(r io.Reader) string {
	scanner := bufio.NewScanner(r)
	var lines []string
	blankCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			blankCount++
			if blankCount >= 1 {
				break
			}
		} else {
			blankCount = 0
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

// New adapter implementations for additional sources

// ---------- NoSQL Adapter (MongoDB) ----------

type NoSQLAdapter struct {
	config     config.DataConfig
	client     *mongo.Client
	collection *mongo.Collection
}

func NewNoSQLAdapter(cfg config.DataConfig) contracts.LookupLoader {
	return &NoSQLAdapter{config: cfg}
}

func (a *NoSQLAdapter) Setup(ctx context.Context) error {
	// use config.Source as MongoDB connection URI, and config.File as "database.collection" (e.g., "test.users")
	clientOpts := options.Client().ApplyURI(a.config.Source)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return err
	}
	// Check connection
	if err = client.Ping(ctx, nil); err != nil {
		return err
	}
	a.client = client
	// Assume config.File has value "dbname.collection"
	parts := strings.Split(a.config.File, ".")
	if len(parts) != 2 {
		return fmt.Errorf("config.File must be in format 'database.collection'")
	}
	dbName, collName := parts[0], parts[1]
	a.collection = client.Database(dbName).Collection(collName)
	return nil
}

func (a *NoSQLAdapter) StoreBatch(ctx context.Context, records []utils.Record) error {
	var docs []interface{}
	for _, rec := range records {
		docs = append(docs, rec)
	}
	_, err := a.collection.InsertMany(ctx, docs)
	return err
}

func (a *NoSQLAdapter) LoadData(opts ...contracts.Option) ([]utils.Record, error) {
	ch, err := a.Extract(context.Background(), opts...)
	if err != nil {
		return nil, err
	}
	var records []utils.Record
	for rec := range ch {
		records = append(records, rec)
	}
	return records, nil
}

func (a *NoSQLAdapter) Extract(ctx context.Context, opts ...contracts.Option) (<-chan utils.Record, error) {
	out := make(chan utils.Record, 100)
	go func() {
		defer close(out)
		cursor, err := a.collection.Find(ctx, struct{}{})
		if err != nil {
			log.Printf("NoSQL query error: %v", err)
			return
		}
		defer cursor.Close(ctx)
		for cursor.Next(ctx) {
			var rec utils.Record
			if err := cursor.Decode(&rec); err != nil {
				log.Printf("Decode error: %v", err)
				continue
			}
			out <- rec
		}
	}()
	return out, nil
}

func (a *NoSQLAdapter) Close() error {
	if a.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return a.client.Disconnect(ctx)
	}
	return nil
}

// ---------- REST Adapter ----------

type RESTAdapter struct {
	config     config.DataConfig
	httpClient *http.Client
}

func NewRESTAdapter(cfg config.DataConfig) contracts.LookupLoader {
	return &RESTAdapter{
		config:     cfg,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

func (a *RESTAdapter) Setup(ctx context.Context) error {
	// Optionally, test connectivity with a GET request.
	resp, err := a.httpClient.Get(a.config.Source)
	if err != nil {
		return err
	}
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	return nil
}

func (a *RESTAdapter) StoreBatch(ctx context.Context, records []utils.Record) error {
	// POST the records as a JSON array to the REST endpoint.
	data, err := json.Marshal(records)
	if err != nil {
		return err
	}
	resp, err := a.httpClient.Post(a.config.Source, "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("REST POST returned status %s", resp.Status)
	}
	return nil
}

func (a *RESTAdapter) LoadData(opts ...contracts.Option) ([]utils.Record, error) {
	ch, err := a.Extract(context.Background(), opts...)
	if err != nil {
		return nil, err
	}
	var records []utils.Record
	for rec := range ch {
		records = append(records, rec)
	}
	return records, nil
}

func (a *RESTAdapter) Extract(ctx context.Context, opts ...contracts.Option) (<-chan utils.Record, error) {
	out := make(chan utils.Record, 100)
	go func() {
		defer close(out)
		resp, err := a.httpClient.Get(a.config.Source)
		if err != nil {
			log.Printf("REST GET error: %v", err)
			return
		}
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Printf("Error reading REST response: %v", err)
			return
		}
		// Expecting a JSON array of objects
		var recs []utils.Record
		if err := json.Unmarshal(data, &recs); err != nil {
			log.Printf("JSON unmarshal error: %v", err)
			return
		}
		for _, rec := range recs {
			out <- rec
		}
	}()
	return out, nil
}

func (a *RESTAdapter) Close() error {
	// nothing to close for HTTP client
	return nil
}

// ---------- MQ Adapter (AMQP) ----------

type MQAdapter struct {
	config    config.DataConfig
	conn      *amqp.Connection
	channel   *amqp.Channel
	queueName string
	consumer  <-chan amqp.Delivery
}

func NewMQAdapter(cfg config.DataConfig) contracts.LookupLoader {
	return &MQAdapter{config: cfg}
}

func (a *MQAdapter) Setup(ctx context.Context) error {
	// use config.Source as the AMQP connection URI
	conn, err := amqp.Dial(a.config.Source)
	if err != nil {
		return err
	}
	a.conn = conn
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	a.channel = ch
	// use config.Table as queue name (if empty, use "default")
	a.queueName = a.config.Table
	if a.queueName == "" {
		a.queueName = "default"
	}
	_, err = ch.QueueDeclare(
		a.queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	return nil
}

func (a *MQAdapter) StoreBatch(ctx context.Context, records []utils.Record) error {
	for _, rec := range records {
		data, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		err = a.channel.Publish(
			"", // default exchange
			a.queueName,
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        data,
			},
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *MQAdapter) LoadData(opts ...contracts.Option) ([]utils.Record, error) {
	ch, err := a.Extract(context.Background(), opts...)
	if err != nil {
		return nil, err
	}
	var records []utils.Record
	for rec := range ch {
		records = append(records, rec)
	}
	return records, nil
}

func (a *MQAdapter) Extract(ctx context.Context, opts ...contracts.Option) (<-chan utils.Record, error) {
	if a.consumer == nil {
		consumer, err := a.channel.Consume(
			a.queueName,
			"",
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			return nil, err
		}
		a.consumer = consumer
	}
	out := make(chan utils.Record, 100)
	go func() {
		defer close(out)
		for d := range a.consumer {
			var rec utils.Record
			if err := json.Unmarshal(d.Body, &rec); err != nil {
				log.Printf("MQ unmarshal error: %v", err)
				continue
			}
			out <- rec
		}
	}()
	return out, nil
}

func (a *MQAdapter) Close() error {
	if a.channel != nil {
		_ = a.channel.Close()
	}
	if a.conn != nil {
		return a.conn.Close()
	}
	return nil
}
