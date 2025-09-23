package platform

import (
	"context"
	"fmt"
	"log"
	"sync"
)

// ProcessingPipelineExecutor executes the processing pipeline
type ProcessingPipelineExecutor struct {
	pipeline    ProcessingPipeline
	registry    SchemaRegistry
	parsers     map[string]Parser
	normalizers map[string]Normalizer
	validators  map[string]Validator
	enrichers   map[string]Enricher
	sinks       map[string]Sink
}

func NewProcessingPipelineExecutor(pipeline ProcessingPipeline, registry SchemaRegistry) *ProcessingPipelineExecutor {
	return &ProcessingPipelineExecutor{
		pipeline:    pipeline,
		registry:    registry,
		parsers:     make(map[string]Parser),
		normalizers: make(map[string]Normalizer),
		validators:  make(map[string]Validator),
		enrichers:   make(map[string]Enricher),
		sinks:       make(map[string]Sink),
	}
}

func (e *ProcessingPipelineExecutor) RegisterParser(name string, parser Parser) {
	e.parsers[name] = parser
}

func (e *ProcessingPipelineExecutor) RegisterNormalizer(name string, normalizer Normalizer) {
	e.normalizers[name] = normalizer
}

func (e *ProcessingPipelineExecutor) RegisterValidator(name string, validator Validator) {
	e.validators[name] = validator
}

func (e *ProcessingPipelineExecutor) RegisterEnricher(name string, enricher Enricher) {
	e.enrichers[name] = enricher
}

func (e *ProcessingPipelineExecutor) RegisterSink(name string, sink Sink) {
	e.sinks[name] = sink
}

func (e *ProcessingPipelineExecutor) Execute(ctx context.Context, ingestMsg IngestMessage) error {
	log.Printf("Executing pipeline %s for message %s", e.pipeline.Name, ingestMsg.ID)

	// Start with parsing
	records, err := e.executeParse(ctx, ingestMsg)
	if err != nil {
		return fmt.Errorf("parse stage failed: %w", err)
	}

	// Process each record through the pipeline
	for _, record := range records {
		if err := e.executeRecordPipeline(ctx, record, ingestMsg); err != nil {
			log.Printf("Pipeline execution failed for record: %v", err)
			// Continue with other records
		}
	}

	return nil
}

func (e *ProcessingPipelineExecutor) executeParse(ctx context.Context, msg IngestMessage) ([]Record, error) {
	// Detect content type if not provided
	contentType := msg.ContentType
	if contentType == "" {
		contentType = DetectContentType(msg.Payload)
	}

	// Get parser based on content type or configuration
	parser := e.getParserForContentType(contentType)
	if parser == nil {
		return nil, fmt.Errorf("no parser available for content type: %s", contentType)
	}

	records, err := parser.Parse(msg.Payload, contentType)
	if err != nil {
		return nil, fmt.Errorf("parsing failed: %w", err)
	}

	log.Printf("Parsed %d records from message %s", len(records), msg.ID)
	return records, nil
}

func (e *ProcessingPipelineExecutor) executeRecordPipeline(ctx context.Context, record Record, msg IngestMessage) error {
	currentRecord := record

	for _, stage := range e.pipeline.Stages {
		switch stage.Type {
		case "normalize":
			if err := e.executeNormalize(ctx, &currentRecord, stage); err != nil {
				return fmt.Errorf("normalize stage failed: %w", err)
			}
		case "validate":
			if err := e.executeValidate(ctx, currentRecord, stage); err != nil {
				return fmt.Errorf("validate stage failed: %w", err)
			}
		case "enrich":
			if err := e.executeEnrich(ctx, &currentRecord, stage); err != nil {
				return fmt.Errorf("enrich stage failed: %w", err)
			}
		case "store":
			if err := e.executeStore(ctx, currentRecord, stage); err != nil {
				return fmt.Errorf("store stage failed: %w", err)
			}
		case "forward":
			if err := e.executeForward(ctx, currentRecord, stage); err != nil {
				return fmt.Errorf("forward stage failed: %w", err)
			}
		default:
			log.Printf("Unknown pipeline stage type: %s", stage.Type)
		}
	}

	return nil
}

func (e *ProcessingPipelineExecutor) executeNormalize(ctx context.Context, record *Record, stage PipelineStage) error {
	schemaName, ok := stage.Config["schema"].(string)
	if !ok {
		return fmt.Errorf("normalize stage missing schema configuration")
	}

	normalizer := e.normalizers["default"]
	if normalizer == nil {
		return fmt.Errorf("no default normalizer registered")
	}

	normalized, err := normalizer.Normalize(*record, schemaName)
	if err != nil {
		return err
	}

	*record = normalized
	log.Printf("Normalized record to schema: %s", schemaName)
	return nil
}

func (e *ProcessingPipelineExecutor) executeValidate(ctx context.Context, record Record, stage PipelineStage) error {
	schemaName, ok := stage.Config["schema"].(string)
	if !ok {
		return fmt.Errorf("validate stage missing schema configuration")
	}

	validator := e.validators["default"]
	if validator == nil {
		return fmt.Errorf("no default validator registered")
	}

	if err := validator.Validate(record, schemaName); err != nil {
		return err
	}

	log.Printf("Validated record against schema: %s", schemaName)
	return nil
}

func (e *ProcessingPipelineExecutor) executeEnrich(ctx context.Context, record *Record, stage PipelineStage) error {
	enricherName, ok := stage.Config["enricher"].(string)
	if !ok {
		enricherName = "default"
	}

	enricher := e.enrichers[enricherName]
	if enricher == nil {
		return fmt.Errorf("enricher not found: %s", enricherName)
	}

	enriched, err := enricher.Enrich(*record)
	if err != nil {
		return err
	}

	*record = enriched
	log.Printf("Enriched record using enricher: %s", enricherName)
	return nil
}

func (e *ProcessingPipelineExecutor) executeStore(ctx context.Context, record Record, stage PipelineStage) error {
	schemaName, ok := stage.Config["schema"].(string)
	if !ok {
		return fmt.Errorf("store stage missing schema configuration")
	}

	sinkName, ok := stage.Config["sink"].(string)
	if !ok {
		sinkName = "default"
	}

	sink := e.sinks[sinkName]
	if sink == nil {
		return fmt.Errorf("sink not found: %s", sinkName)
	}

	if err := sink.Store(record, schemaName); err != nil {
		return err
	}

	log.Printf("Stored record using sink: %s", sinkName)
	return nil
}

func (e *ProcessingPipelineExecutor) executeForward(ctx context.Context, record Record, stage PipelineStage) error {
	topic, ok := stage.Config["topic"].(string)
	if !ok {
		return fmt.Errorf("forward stage missing topic configuration")
	}

	sink := e.sinks["message-bus"]
	if sink == nil {
		return fmt.Errorf("message bus sink not found")
	}

	if err := sink.Forward(record, topic); err != nil {
		return err
	}

	log.Printf("Forwarded record to topic: %s", topic)
	return nil
}

func (e *ProcessingPipelineExecutor) getParserForContentType(contentType string) Parser {
	// Try to find a registered parser for this content type
	for _, parser := range e.parsers {
		// Simple matching - in production, this could be more sophisticated
		if parser.Name() == "json" && (contentType == "application/json" || contentType == "") {
			return parser
		}
		if parser.Name() == "csv" && contentType == "text/csv" {
			return parser
		}
		if parser.Name() == "xml" && contentType == "application/xml" {
			return parser
		}
		if parser.Name() == "hl7" && contentType == "application/hl7" {
			return parser
		}
	}

	// Default to string parser
	return e.parsers["string"]
}

// DefaultNormalizer implements the Normalizer interface
type DefaultNormalizer struct {
	registry SchemaRegistry
	mapper   *SchemaMapper
}

func NewDefaultNormalizer(registry SchemaRegistry, fieldMappings map[string]string) *DefaultNormalizer {
	return &DefaultNormalizer{
		registry: registry,
		mapper:   NewSchemaMapper(fieldMappings),
	}
}

func (n *DefaultNormalizer) Normalize(record Record, schemaName string) (Record, error) {
	// First, map fields if mappings are provided
	mapped := n.mapper.Map(record)

	// Then normalize using the schema
	normalizer := NewRecordNormalizer(n.registry)
	return normalizer.Normalize(mapped, schemaName)
}

func (n *DefaultNormalizer) Name() string {
	return "default"
}

// DefaultValidator implements the Validator interface
type DefaultValidator struct {
	registry SchemaRegistry
}

func NewDefaultValidator(registry SchemaRegistry) *DefaultValidator {
	return &DefaultValidator{
		registry: registry,
	}
}

func (v *DefaultValidator) Validate(record Record, schemaName string) error {
	return v.registry.Validate(record, schemaName)
}

func (v *DefaultValidator) Name() string {
	return "default"
}

// DefaultEnricher implements the Enricher interface
type DefaultEnricher struct {
	enrichments map[string]interface{}
}

func NewDefaultEnricher(enrichments map[string]interface{}) *DefaultEnricher {
	return &DefaultEnricher{
		enrichments: enrichments,
	}
}

func (e *DefaultEnricher) Enrich(record Record) (Record, error) {
	enriched := make(Record)

	// Copy original record
	for k, v := range record {
		enriched[k] = v
	}

	// Add enrichments
	for k, v := range e.enrichments {
		if _, exists := enriched[k]; !exists {
			enriched[k] = v
		}
	}

	return enriched, nil
}

func (e *DefaultEnricher) Name() string {
	return "default"
}

// PipelineWorkerPool manages a pool of workers for processing
type PipelineWorkerPool struct {
	workers  int
	executor *ProcessingPipelineExecutor
	jobChan  chan IngestMessage
	stopChan chan struct{}
	wg       sync.WaitGroup
}

func NewPipelineWorkerPool(workers int, executor *ProcessingPipelineExecutor) *PipelineWorkerPool {
	return &PipelineWorkerPool{
		workers:  workers,
		executor: executor,
		jobChan:  make(chan IngestMessage, workers*10),
		stopChan: make(chan struct{}),
	}
}

func (p *PipelineWorkerPool) Start() error {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}

	log.Printf("Started pipeline worker pool with %d workers", p.workers)
	return nil
}

func (p *PipelineWorkerPool) Stop() error {
	close(p.stopChan)
	p.wg.Wait()
	log.Printf("Stopped pipeline worker pool")
	return nil
}

func (p *PipelineWorkerPool) Submit(job IngestMessage) {
	select {
	case p.jobChan <- job:
		// Job submitted
	default:
		log.Printf("Worker pool queue full, dropping job %s", job.ID)
	}
}

func (p *PipelineWorkerPool) worker(id int) {
	defer p.wg.Done()

	for {
		select {
		case job := <-p.jobChan:
			if err := p.executor.Execute(context.Background(), job); err != nil {
				log.Printf("Worker %d failed to process job %s: %v", id, job.ID, err)
			}
		case <-p.stopChan:
			return
		}
	}
}
