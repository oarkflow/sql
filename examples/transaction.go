package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/oarkflow/etl/pkg/transactions"
)

func main() {
	fmt.Println("File Transaction Example")
	err := transactions.RunInTransaction(context.Background(), func(tx *transactions.Transaction) error {
		tmpFile, err := os.CreateTemp("", "transfile_*")
		if err != nil {
			return fmt.Errorf("failed to create temp file: %w", err)
		}
		tmpFileName := tmpFile.Name()
		log.Printf("Created temporary file: %s", tmpFileName)
		if err := tx.RegisterRollback(func(ctx context.Context) error {
			log.Printf("Rolling back: deleting temporary file %s", tmpFileName)
			if removeErr := os.Remove(tmpFileName); removeErr != nil {
				return fmt.Errorf("failed to remove temp file: %w", removeErr)
			}
			return nil
		}); err != nil {
			tmpFile.Close()
			return err
		}
		data := []byte("Hello, transactional file write!\n")
		if _, err := tmpFile.Write(data); err != nil {
			tmpFile.Close()
			return fmt.Errorf("failed to write data to file: %w", err)
		}
		tmpFile.Close()
		log.Printf("Data written to temporary file: %s", tmpFileName)
		return errors.New("simulated error after writing file")
	})
	if err != nil {
		log.Printf("File transaction rolled back: %v", err)
	} else {
		log.Println("File transaction committed successfully")
	}
}
