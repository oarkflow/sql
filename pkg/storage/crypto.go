package storage

import (
	"crypto/aes"
	cip "crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
)

type secretCipher struct {
	block   cip.AEAD
	enabled bool
}

func newSecretCipher(secret string) (*secretCipher, error) {
	if secret == "" {
		return &secretCipher{enabled: false}, nil
	}
	sum := sha256.Sum256([]byte(secret))
	block, err := aes.NewCipher(sum[:])
	if err != nil {
		return nil, err
	}
	gcm, err := cip.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &secretCipher{block: gcm, enabled: true}, nil
}

func (c *secretCipher) encrypt(data []byte) ([]byte, error) {
	if c == nil || !c.enabled {
		return data, nil
	}
	nonce := make([]byte, c.block.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	ciphertext := c.block.Seal(nil, nonce, data, nil)
	return append(nonce, ciphertext...), nil
}

func (c *secretCipher) decrypt(blob []byte) ([]byte, error) {
	if c == nil || !c.enabled {
		return blob, nil
	}
	if len(blob) < c.block.NonceSize() {
		return nil, errors.New("ciphertext too short")
	}
	nonce := blob[:c.block.NonceSize()]
	payload := blob[c.block.NonceSize():]
	return c.block.Open(nil, nonce, payload, nil)
}
