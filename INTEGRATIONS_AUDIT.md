# Integrations Package Audit & Enhancement Checklist

## Critical Bugs & Logical Issues
- [x] **Race Condition in OAuth2 Refresh**: `refreshOAuth2Token` modifies shared `OAuth2Credential` state without locking.
- [x] **Circuit Breaker Half-Open Flood**: `CircuitBreaker.AllowRequest` allows all traffic in `HalfOpen` state instead of probing.
- [x] **Inefficient HTTP Client**: `getHTTPClient` creates new `http.Client` and `Transport` per request, preventing connection pooling.
- [x] **Inefficient DB Querying**: `ExecuteDatabaseQuery` creates and closes a DB conn pool per query.
- [x] **Config Reload Error Ignored**: `LoadIntegrationsFromFile` goroutine logs but ignores `Init` errors during reload.

## Enhancements
- [x] **DB Connection Pooling**: Cache DB pools in `Manager`.
- [x] **HTTP Client Pooling**: Cache HTTP clients based on config (timeout/insecure).
- [x] **Structured Logging**: Ensure consistent usage of the logger.
- [ ] **Metrics**: Add call counters, latency histograms, and error counters for integrations.
- [x] **Secure Config**: Support ENV variable expansion in config strings (e.g. `${DB_PASSWORD}`).

## Code Quality
- [x] **Dedup Unmarshalling**: Review `UnmarshalService` vs `Service.UnmarshalJSON`.
