// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package lightpool

import (
	"bytes"
	"errors"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/modulecollective/lightpool/wire"
)

// Keyring keeps a primary encryption key followed by accepted decryption keys.
// Keys are 32 bytes (AES-256). All buffers cross this boundary by copy.
// Encryption cannot be disabled by removing the primary key; rotate by
// installing, selecting, then removing.
type Keyring struct {
	mu      sync.RWMutex
	entries []keyEntry
	// built is the primary-first frame keys, replaced whole on every change so
	// each frame reads it without locking or rebuilding ciphers.
	built atomic.Pointer[[]wire.Key]
}

type keyEntry struct {
	secret []byte
	key    wire.Key
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
	built, err := wire.NewKey(key)
	if err != nil {
		return err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	for _, old := range k.entries {
		if bytes.Equal(old.secret, key) {
			return nil
		}
	}
	k.entries = append(k.entries, keyEntry{bytes.Clone(key), built})
	k.publish()
	return nil
}

func (k *Keyring) Use(key []byte) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	for j, old := range k.entries {
		if bytes.Equal(old.secret, key) {
			copy(k.entries[1:j+1], k.entries[:j])
			k.entries[0] = old
			k.publish()
			return nil
		}
	}
	return errors.New("key is not installed")
}

func (k *Keyring) Remove(key []byte) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	for j, old := range k.entries {
		if bytes.Equal(old.secret, key) {
			if j == 0 {
				return errors.New("cannot remove primary key")
			}
			k.entries = slices.Delete(k.entries, j, j+1)
			k.publish()
			return nil
		}
	}
	return nil
}

func (k *Keyring) Keys() [][]byte {
	k.mu.RLock()
	defer k.mu.RUnlock()
	out := make([][]byte, len(k.entries))
	for j, e := range k.entries {
		out[j] = bytes.Clone(e.secret)
	}
	return out
}

// publish replaces the frame keys; the caller holds the write lock.
func (k *Keyring) publish() {
	built := make([]wire.Key, len(k.entries))
	for j, e := range k.entries {
		built[j] = e.key
	}
	k.built.Store(&built)
}

func (k *Keyring) frameKeys() []wire.Key {
	if built := k.built.Load(); built != nil {
		return *built
	}
	return nil // a zero Keyring; New refuses it as empty
}
