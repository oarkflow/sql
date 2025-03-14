package main

import (
	"fmt"
	"log"

	"github.com/oarkflow/etl"
	"github.com/oarkflow/etl/pkg/config"
)

// LoggerPlugin is a simple plugin that logs its initialization.
type LoggerPlugin struct{}

func (p *LoggerPlugin) Name() string {
	return "LoggerPlugin"
}

func (p *LoggerPlugin) Init(e *etl.ETL) error {
	// Subscribe to the "AfterLoad" event.
	if e.EventBus() != nil {
		e.EventBus().Subscribe("AfterLoad", func(evt etl.Event) {
			log.Printf("[SamplePlugin] Received AfterLoad event with payload: %v", evt.Payload)
			// For example, perform extra logging or trigger external notifications.
		})
	}
	// You could also modify ETL's configuration, add new sources/transformers, etc.
	log.Println("[SamplePlugin] Initialized successfully.")
	return nil
}

func main() {
	paths := []string{
		"assets/prod.yaml",
		// "assets/multiple-source.yaml",
		// "assets/std.yaml",
	}
	for _, path := range paths {
		fmt.Println("Started executing", path)
		err := RunETL(path)
		if err != nil {
			panic(err)
		}
		fmt.Println("Execution Completed\n", path)
	}
}

func RunETL(configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		log.Printf("Error loading config: %v", err)
		return err
	}
	/*// Define lifecycle hooks.
	hooks := &etl.LifecycleHooks{
		BeforeExtract: func(ctx context.Context) error {
			log.Println("[Hook] BeforeExtract: starting extraction")
			return nil
		},
		AfterExtract: func(ctx context.Context, count int) error {
			log.Printf("[Hook] AfterExtract: extracted %d records", count)
			return nil
		},
		BeforeMapper: func(ctx context.Context, rec utils.Record) error {
			log.Println("[Hook] BeforeMapper: processing record", rec)
			return nil
		},
		AfterMapper: func(ctx context.Context, rec utils.Record) error {
			log.Println("[Hook] AfterMapper: record after mapping", rec)
			return nil
		},
		BeforeTransform: func(ctx context.Context, rec utils.Record) error {
			log.Println("[Hook] BeforeTransform: about to transform", rec)
			return nil
		},
		AfterTransform: func(ctx context.Context, rec utils.Record) error {
			log.Println("[Hook] AfterTransform: record transformed", rec)
			return nil
		},
		BeforeLoad: func(ctx context.Context, batch []utils.Record) error {
			log.Printf("[Hook] BeforeLoad: about to load batch of %d records", len(batch))
			return nil
		},
		AfterLoad: func(ctx context.Context, batch []utils.Record) error {
			log.Printf("[Hook] AfterLoad: loaded batch of %d records", len(batch))
			return nil
		},
	}*/

	// Define validations.
	/*validations := &etl.Validations{
		ValidateBeforeExtract: func(ctx context.Context) error {
			log.Println("[Validation] ValidateBeforeExtract: OK")
			return nil
		},
		ValidateAfterExtract: func(ctx context.Context, count int) error {
			log.Printf("[Validation] ValidateAfterExtract: %d records validated", count)
			return nil
		},
		ValidateBeforeLoad: func(ctx context.Context, batch []utils.Record) error {
			log.Printf("[Validation] ValidateBeforeLoad: batch size %d", len(batch))
			return nil
		},
		ValidateAfterLoad: func(ctx context.Context, batch []utils.Record) error {
			log.Printf("[Validation] ValidateAfterLoad: batch size %d", len(batch))
			return nil
		},
	}*/

	// Create an EventBus and subscribe to some events.
	eventBus := etl.NewEventBus()
	eventBus.Subscribe("BeforeExtract", func(e etl.Event) {
		log.Println("[EventBus] Received event:", e.Name)
	})
	/*eventBus.Subscribe("AfterLoad", func(e etl.Event) {
		log.Printf("[EventBus] AfterLoad event with payload: %v", e.Name)
	})*/
	opts := []etl.Option{
		// etl.WithValidations(validations),
		etl.WithPlugin(&LoggerPlugin{}),
		etl.WithEventBus(eventBus),
		// etl.WithLifecycleHooks(hooks),
		etl.WithDashboardAuth("admin", "password"),
	}
	return etl.Run(cfg, opts...)
}
