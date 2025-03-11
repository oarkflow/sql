package main // main.go
import (
	"log"
	"net/http"

	"github.com/oarkflow/json"

	"github.com/oarkflow/etl"
	"github.com/oarkflow/etl/pkg/config"
)

var currentConfig config.Config

func main() {
	http.HandleFunc("/api/config", configHandler)
	http.HandleFunc("/api/etl/run", runETLHandler)

	// Serve the frontend files from the public directory.
	fs := http.FileServer(http.Dir("./static"))
	http.Handle("/", fs)

	log.Println("Server started on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// configHandler handles GET and POST to view and update the ETL configuration.
func configHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(currentConfig)
	case http.MethodPost:
		var cfg config.Config
		if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
			http.Error(w, "Invalid config data", http.StatusBadRequest)
			return
		}
		currentConfig = cfg
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Configuration updated successfully"))
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// runETLHandler triggers the ETL process.
func runETLHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Run the ETL process in a separate goroutine.
	go func(cfg config.Config) {
		log.Println("Starting ETL process...")
		etl.Run(&cfg)
		log.Println("ETL process completed.")
	}(currentConfig)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ETL process started"))
}
