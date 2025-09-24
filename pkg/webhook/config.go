package webhook

// Config holds the configuration for the webhook server
type Config struct {
	Port       string `json:"port"`
	MaxWorkers int    `json:"max_workers"`
	Secret     string `json:"secret"` // For HMAC verification
}

// DefaultConfig returns a default configuration
func DefaultConfig() *Config {
	return &Config{
		Port:       "8080",
		MaxWorkers: 10,
		Secret:     "",
	}
}
