package keys

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type Keys struct {
	stream    jetstream.Stream
	js        jetstream.JetStream
	tenantID  string
	cryptoKey []byte
}

var (
	ErrBadInput  = errors.New("bad input provided")
	ErrTransient = errors.New("transient")
)

type Opt func(*Keys)

func New(
	stream jetstream.Stream,
	js jetstream.JetStream,
	opts ...Opt,
) *Keys {
	i := Keys{stream, js, "", []byte{}}
	for _, o := range opts {
		o(&i)
	}
	return &i
}

func WithSubjectSuffix(s string) Opt {
	return func(i *Keys) {
		i.tenantID = s
	}
}

// WithEncryptionKey sets a 32-byte encryption key to have all all keys encrypted
//
// Uses AES-GCM
func WithEncryptionKey(k []byte) Opt {
	return func(i *Keys) {
		i.cryptoKey = k
	}
}

func (s *Keys) SetKey(ctx context.Context, key string, val any, ttl *time.Duration) error {
	if key == "" {
		return fmt.Errorf("%w - cannot set empty key", ErrBadInput)
	}

	data, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("%w - marshaling failed: %w", ErrBadInput, err)
	}

	opts := []jetstream.PublishOpt{}

	if ttl != nil {
		opts = append(opts, jetstream.WithMsgTTL(*ttl))
	}

	if len(s.cryptoKey) > 0 {
		data, err = s.encrypt(data)
		if err != nil {
			return fmt.Errorf("encryption failed: %w", err)
		}
	}

	_, err = s.js.Publish(
		ctx,
		s.InTenantSubjectSpace(fmt.Sprintf("keys.%s", key)),
		data,
		opts...,
	)

	return err
}

func (s *Keys) DelKey(ctx context.Context, key string) error {
	msg, err := s.stream.GetLastMsgForSubject(ctx, s.InTenantSubjectSpace(fmt.Sprintf("keys.%s", key)))
	if err != nil {
		return fmt.Errorf("%w - failed to get key: %w", ErrTransient, err)
	}

	return s.stream.DeleteMsg(ctx, msg.Sequence)
}

func (s *Keys) GetKey(ctx context.Context, key string) ([]byte, error) {
	msg, err := s.stream.GetLastMsgForSubject(ctx, s.InTenantSubjectSpace(fmt.Sprintf("keys.%s", key)))
	if err != nil {
		return nil, fmt.Errorf("%w - failed to get key: %w", ErrTransient, err)
	}

	if len(s.cryptoKey) > 0 {
		decri, err := s.decrypt(msg.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt key: %w", err)
		}

		return decri, nil
	}

	return msg.Data, nil
}

func (s *Keys) InTenantSubjectSpace(sub string) string {
	if s.tenantID == "" {
		return sub
	}
	return fmt.Sprintf("%s.%s", s.tenantID, sub)
}

// encrypt uses AES-GCM to encrypt data
func (s *Keys) encrypt(plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(s.cryptoKey)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// decrypt uses AES-GCM to decrypt data
func (s *Keys) decrypt(ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(s.cryptoKey)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	return gcm.Open(nil, nonce, ciphertext, nil)
}
