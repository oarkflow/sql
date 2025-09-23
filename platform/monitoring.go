package platform

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// MetricsCollector collects platform metrics
type MetricsCollector struct {
	metrics map[string]interface{}
	mu      sync.RWMutex
}

func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		metrics: make(map[string]interface{}),
	}
}

func (mc *MetricsCollector) RecordMetric(name string, value interface{}) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.metrics[name] = value
}

func (mc *MetricsCollector) IncrementCounter(name string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if val, exists := mc.metrics[name]; exists {
		if counter, ok := val.(int64); ok {
			mc.metrics[name] = counter + 1
		} else {
			mc.metrics[name] = int64(1)
		}
	} else {
		mc.metrics[name] = int64(1)
	}
}

func (mc *MetricsCollector) GetMetrics() map[string]interface{} {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	// Return a copy
	result := make(map[string]interface{})
	for k, v := range mc.metrics {
		result[k] = v
	}
	return result
}

// PrometheusMetricsExporter exports metrics in Prometheus format
type PrometheusMetricsExporter struct {
	collector *MetricsCollector
}

func NewPrometheusMetricsExporter(collector *MetricsCollector) *PrometheusMetricsExporter {
	return &PrometheusMetricsExporter{
		collector: collector,
	}
}

func (e *PrometheusMetricsExporter) Export() string {
	metrics := e.collector.GetMetrics()

	var output string
	for name, value := range metrics {
		// Convert to Prometheus format
		switch v := value.(type) {
		case int64:
			output += fmt.Sprintf("# TYPE %s counter\n%s %d\n", name, name, v)
		case float64:
			output += fmt.Sprintf("# TYPE %s gauge\n%s %f\n", name, name, v)
		case int:
			output += fmt.Sprintf("# TYPE %s counter\n%s %d\n", name, name, v)
		default:
			// Skip complex types
			continue
		}
	}

	return output
}

// StructuredLogger provides structured logging
type StructuredLogger struct {
	service string
}

func NewStructuredLogger(service string) *StructuredLogger {
	return &StructuredLogger{
		service: service,
	}
}

func (l *StructuredLogger) Info(message string, fields map[string]interface{}) {
	l.log("INFO", message, fields)
}

func (l *StructuredLogger) Error(message string, err error, fields map[string]interface{}) {
	if fields == nil {
		fields = make(map[string]interface{})
	}
	fields["error"] = err.Error()
	l.log("ERROR", message, fields)
}

func (l *StructuredLogger) Warn(message string, fields map[string]interface{}) {
	l.log("WARN", message, fields)
}

func (l *StructuredLogger) Debug(message string, fields map[string]interface{}) {
	l.log("DEBUG", message, fields)
}

func (l *StructuredLogger) log(level, message string, fields map[string]interface{}) {
	entry := map[string]interface{}{
		"timestamp": time.Now().Format(time.RFC3339),
		"level":     level,
		"service":   l.service,
		"message":   message,
	}

	// Add additional fields
	for k, v := range fields {
		entry[k] = v
	}

	// Convert to JSON and log
	if jsonBytes, err := json.Marshal(entry); err == nil {
		log.Println(string(jsonBytes))
	} else {
		log.Printf("[%s] %s: %v", level, message, fields)
	}
}

// HealthChecker provides health check functionality
type HealthChecker struct {
	checks map[string]HealthCheck
	mu     sync.RWMutex
}

type HealthCheck func() error

func NewHealthChecker() *HealthChecker {
	return &HealthChecker{
		checks: make(map[string]HealthCheck),
	}
}

func (hc *HealthChecker) RegisterCheck(name string, check HealthCheck) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.checks[name] = check
}

func (hc *HealthChecker) Check() map[string]string {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	results := make(map[string]string)

	for name, check := range hc.checks {
		if err := check(); err != nil {
			results[name] = fmt.Sprintf("FAIL: %v", err)
		} else {
			results[name] = "PASS"
		}
	}

	return results
}

// MonitoringServer provides HTTP endpoints for monitoring
type MonitoringServer struct {
	metricsCollector *MetricsCollector
	healthChecker    *HealthChecker
	port             string
}

func NewMonitoringServer(collector *MetricsCollector, checker *HealthChecker, port string) *MonitoringServer {
	return &MonitoringServer{
		metricsCollector: collector,
		healthChecker:    checker,
		port:             port,
	}
}

func (ms *MonitoringServer) Start() error {
	http.HandleFunc("/metrics", ms.handleMetrics)
	http.HandleFunc("/health", ms.handleHealth)
	http.HandleFunc("/status", ms.handleStatus)

	log.Printf("Starting monitoring server on port %s", ms.port)
	return http.ListenAndServe(":"+ms.port, nil)
}

func (ms *MonitoringServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	exporter := NewPrometheusMetricsExporter(ms.metricsCollector)
	metrics := exporter.Export()

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write([]byte(metrics))
}

func (ms *MonitoringServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	results := ms.healthChecker.Check()

	status := "ok"
	statusCode := http.StatusOK

	for _, result := range results {
		if len(result) >= 4 && result[:4] == "FAIL" {
			status = "error"
			statusCode = http.StatusServiceUnavailable
			break
		}
	}

	response := map[string]interface{}{
		"status":    status,
		"checks":    results,
		"timestamp": time.Now().Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	json.NewEncoder(w).Encode(response)
}

func (ms *MonitoringServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	metrics := ms.metricsCollector.GetMetrics()
	health := ms.healthChecker.Check()

	response := map[string]interface{}{
		"metrics":   metrics,
		"health":    health,
		"timestamp": time.Now().Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// TracingSpan represents a simple tracing span
type TracingSpan struct {
	ID        string                 `json:"id"`
	ParentID  string                 `json:"parent_id,omitempty"`
	Name      string                 `json:"name"`
	StartTime time.Time              `json:"start_time"`
	EndTime   time.Time              `json:"end_time"`
	Tags      map[string]interface{} `json:"tags,omitempty"`
}

func (s *TracingSpan) Duration() time.Duration {
	return s.EndTime.Sub(s.StartTime)
}

// SimpleTracer provides basic tracing functionality
type SimpleTracer struct {
	spans []TracingSpan
	mu    sync.RWMutex
}

func NewSimpleTracer() *SimpleTracer {
	return &SimpleTracer{
		spans: make([]TracingSpan, 0),
	}
}

func (t *SimpleTracer) StartSpan(name string, parentID string) *TracingSpan {
	span := &TracingSpan{
		ID:        generateSpanID(),
		ParentID:  parentID,
		Name:      name,
		StartTime: time.Now(),
		Tags:      make(map[string]interface{}),
	}

	return span
}

func (t *SimpleTracer) FinishSpan(span *TracingSpan) {
	span.EndTime = time.Now()

	t.mu.Lock()
	defer t.mu.Unlock()
	t.spans = append(t.spans, *span)
}

func (t *SimpleTracer) GetSpans() []TracingSpan {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Return a copy
	result := make([]TracingSpan, len(t.spans))
	copy(result, t.spans)
	return result
}

func generateSpanID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
