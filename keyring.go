// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package lightpool

import (
	"bytes"
	"crypto/aes"
	"errors"
	"sync"
)

// Keyring keeps a primary encryption key followed by accepted decryption keys.
// All buffers cross this boundary by copy. Encryption cannot be disabled by
// removing the primary key; rotate by installing, selecting, then removing.
type Keyring struct {
	mu   sync.RWMutex
	keys [][]byte
}

func NewKeyring(primary []byte, secondary ...[]byte) (*Keyring, error) {
	k := &Keyring{}
	if err := k.Add(primary); err != nil {
		return nil, err
	}
	for _, key := range secondary {
		if err := k.Add(key); err != nil {
			return nil, err
		}
	}
	return k, nil
}
func (k *Keyring) Add(key []byte) error {
	if _, err := aes.NewCipher(key); err != nil {
		return err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	for _, old := range k.keys {
		if bytes.Equal(old, key) {
			return nil
		}
	}
	k.keys = append(k.keys, bytes.Clone(key))
	return nil
}
func (k *Keyring) Use(key []byte) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	for j, old := range k.keys {
		if bytes.Equal(old, key) {
			copy(k.keys[1:j+1], k.keys[:j])
			k.keys[0] = old
			return nil
		}
	}
	return errors.New("key is not installed")
}
func (k *Keyring) Remove(key []byte) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	for j, old := range k.keys {
		if bytes.Equal(old, key) {
			if j == 0 {
				return errors.New("cannot remove primary key")
			}
			k.keys = append(k.keys[:j], k.keys[j+1:]...)
			return nil
		}
	}
	return nil
}
func (k *Keyring) Keys() [][]byte {
	k.mu.RLock()
	defer k.mu.RUnlock()
	out := make([][]byte, len(k.keys))
	for j, key := range k.keys {
		out[j] = bytes.Clone(key)
	}
	return out
}
