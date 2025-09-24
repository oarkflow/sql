package nosqladapter

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/oarkflow/log"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/contracts"
	"github.com/oarkflow/sql/pkg/utils"
)

type Adapter struct {
	config     config.DataConfig
	client     *mongo.Client
	collection *mongo.Collection
}

func New(cfg config.DataConfig) contracts.LookupLoader {
	return &Adapter{config: cfg}
}

func (a *Adapter) Setup(ctx context.Context) error {
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

func (a *Adapter) StoreBatch(ctx context.Context, records []utils.Record) error {
	var docs []any
	for _, rec := range records {
		docs = append(docs, rec)
	}
	_, err := a.collection.InsertMany(ctx, docs)
	return err
}

func (a *Adapter) StoreSingle(ctx context.Context, rec utils.Record) error {
	return a.StoreBatch(ctx, []utils.Record{rec})
}

func (a *Adapter) LoadData(opts ...contracts.Option) ([]utils.Record, error) {
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

func (a *Adapter) Extract(ctx context.Context, opts ...contracts.Option) (<-chan utils.Record, error) {
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

func (a *Adapter) Close() error {
	if a.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return a.client.Disconnect(ctx)
	}
	return nil
}
