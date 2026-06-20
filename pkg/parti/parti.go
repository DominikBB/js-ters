package parti

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sync/errgroup"
)

type Parti struct {
	subPrefix      string // A subject which will be used as suffix to all eventstore stream operations
	metadataStream jetstream.Stream
	eventStream    jetstream.Stream
	js             jetstream.JetStream
}

type Opt = func(*Parti)

var (
	ErrBadInput  = errors.New("bad input provided")
	ErrTransient = errors.New("transient")
	ErrNotFound  = errors.New("not found")
)

func NewEventStore(
	eventStream jetstream.Stream,
	js jetstream.JetStream,
	opts ...Opt,
) *Parti {
	es := Parti{eventStream: eventStream, js: js}
	for _, opt := range opts {
		opt(&es)
	}
	return &es
}

// WithSubjectPrefix allows you to add a particular suffix to all messages read and produced by the eventstore,
// which is usaful if you would like to control locations/tenants by subject parts.
func WithSubjectPrefix(s string) Opt {
	return func(es *Parti) {
		es.subPrefix = s
	}
}

func WithMetadataStream(s jetstream.Stream) Opt {
	return func(es *Parti) {
		es.metadataStream = s
	}
}

type Aggregate struct {
	Events      []uint64
	SoftDeleted bool
}

func (s *Parti) Get(ctx context.Context, key string) (iter.Seq2[*jetstream.RawStreamMsg, error], error) {
	msg, err := s.metadataStream.GetLastMsgForSubject(ctx, s.subject(key))
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("%w - failed to get key: %w", ErrTransient, err)
	}

	var agg Aggregate
	if err := json.Unmarshal(msg.Data, &agg); err != nil {
		return nil, fmt.Errorf("%w - bad aggregate message: %w", ErrBadInput, err)
	}

	if agg.SoftDeleted {
		return nil, ErrNotFound
	}

	// TODO: sort sequences
	return s.iterateOver(ctx, agg), nil
}

func (s *Parti) Del(ctx context.Context, key string) error {
	msg, err := s.metadataStream.GetLastMsgForSubject(ctx, s.subject(key))
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("%w - failed to get key: %w", ErrTransient, err)
	}

	var agg Aggregate
	if err := json.Unmarshal(msg.Data, &agg); err != nil {
		return fmt.Errorf("%w - bad aggregate message: %w", ErrBadInput, err)
	}

	agg.SoftDeleted = true

	data, err := json.Marshal(agg)
	if err != nil {
		return err
	}

	_, err = s.js.Publish(
		ctx,
		s.subject(key),
		data,
	)

	return err
}

type partitioner func(jetstream.Msg) string

// SubjectPartition will group messages based on a part of a subject
//
// SubjectPartition(1) will group below
// test.1.some.message -> under key 1
// test.1.other.message -> under key 1
// test.3.some.message -> under key 3
func SubjectPartition(subjectIndex int) partitioner {
	return func(m jetstream.Msg) string {
		parts := strings.Split(m.Subject(), ".")
		if len(parts) == 0 || len(parts)-1 < subjectIndex {
			return ""
		}
		return parts[subjectIndex]
	}
}

// HeaderPartition will group messages based on a value of a particular header
func HeaderPartition(header string) partitioner {
	return func(m jetstream.Msg) string {
		h := m.Headers().Get(header)
		return h
	}
}

// DataPartition will group messages based on a custom function over the message payload
func DataPartition[T any](partitionFunc func(T) string) partitioner {
	return func(m jetstream.Msg) string {
		var input T
		if err := json.Unmarshal(m.Data(), &input); err != nil {
			return ""
		}

		return partitionFunc(input)
	}
}

// Subscribe pulls 300 messages at a time, groups them based on partitioning strategy
// and updates the group metadata in the metadata stream.
//
// Messages where a partition returned an empty string will be Nak'd
func (s *Parti) Subscribe(ctx context.Context, cons jetstream.Consumer, strategy partitioner) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		batch, err := cons.Fetch(
			300,
			jetstream.FetchContext(ctx),
			jetstream.FetchMaxWait(5*time.Second),
		)
		if err != nil {
			return err
		}

		groups := map[string][]jetstream.Msg{}
		for m := range batch.Messages() {
			part := strategy(m)
			_, ok := groups[part]
			if !ok {
				groups[part] = []jetstream.Msg{m}
			}
			groups[part] = append(groups[part], m)
		}

		eg, cctx := errgroup.WithContext(ctx)
		for part, msgs := range groups {
			if part == "" {
				for _, msg := range msgs {
					msg.Nak()
				}
				return nil
			}
			eg.Go(func() error {
				sqs := []uint64{}
				for _, m := range msgs {
					meta, err := m.Metadata()
					if err != nil {
						return err
					}
					sqs = append(sqs, meta.Sequence.Stream)
				}

				err := s.updateAggregate(cctx, part, sqs)
				if err != nil {
					for _, msg := range msgs {
						msg.Nak()
					}
					return nil
				}

				for _, msg := range msgs {
					msg.Ack()
				}
				return nil
			})
		}

		eg.Wait()
	}
}

func (s *Parti) updateAggregate(ctx context.Context, key string, seq []uint64) error {
	msg, err := s.metadataStream.GetLastMsgForSubject(ctx, s.subject(key))

	if errors.Is(err, jetstream.ErrMsgNotFound) {
		agg := Aggregate{
			Events: seq,
		}

		data, err := json.Marshal(agg)
		if err != nil {
			return err
		}

		_, err = s.js.Publish(
			ctx,
			s.subject(key),
			data,
		)

		return err
	}

	if err != nil {
		return fmt.Errorf("%w - failed to get key: %w", ErrTransient, err)
	}

	var agg Aggregate
	if err := json.Unmarshal(msg.Data, &agg); err != nil {
		return fmt.Errorf("%w - bad aggregate message: %w", ErrBadInput, err)
	}

	agg.Events = append(agg.Events, seq...)

	data, err := json.Marshal(agg)
	if err != nil {
		return err
	}

	_, err = s.js.Publish(
		ctx,
		s.subject(key),
		data,
	)

	return err
}

func (s *Parti) subject(key string) string {
	var subject []string
	if s.subPrefix != "" {
		subject = append(subject, s.subPrefix)
	}
	subject = append(subject, "EVENT_STORE", key)
	return strings.Join(subject, ".")
}

func (s *Parti) iterateOver(ctx context.Context, agg Aggregate) iter.Seq2[*jetstream.RawStreamMsg, error] {
	return func(yield func(*jetstream.RawStreamMsg, error) bool) {
		for _, seq := range agg.Events {

			msg, err := s.eventStream.GetMsg(ctx, seq)
			if err != nil {
				if !yield(msg, err) {
					return
				}
			}
		}
	}
}
