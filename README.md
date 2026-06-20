# js-ters

A small set of NATS JetStream utils.

## keys

A very light version of JetStream KV, but capable of using any provided streams and stream subjects. This allows it to be fully permission controlled by subject based nats user rules.

```go

 k := New(
  stream,
  js,
  WithEncryptionKey(aesKey),          // Encrypts all messages
  WithSubjectPrefix("my.tenant_123"), // Will ensure all messages are stored in a particular subject space
 )

 _ = k.SetKey(ctx, "my_secret", "1245")
 _ = k.SetExpiringKey(ctx, "my_secret_2", "54321", 3*time.Hour) // TTL requires AllowMsgTTL on the stream

 value, _ := k.GetKey(ctx, "my_secret")
 _ = k.DelKey(ctx, "my_secret")

```

# parti

A kind of event store, keeps metadata on groups of messages from a stream.

```go

 ms, _ := js.Stream(ctx, "META_STREAM") // Stream to store metadata in, can keep 1 msg per subject
 es, _ := js.Stream(ctx, "EVENT_STREAM") // Persistent immutable stream

 p, _ := parti.New(
  es,
  ms,
  js,
  parti.WithSubjectPrefix("my.tenant"), // Metadata will be stored under my.tenant.parti_meta...
 )

 // Subscriber consumes the EVENT_STREAM, groups messages by their 3rd part of the subject into an easily retrievable log
 go p.Subscribe(ctx, parti.SubjectPartition(3))

 // Input
 // eg. my.tenant123.some_entity123.created
 // eg. my.tenant123.some_entity123.updated
 // eg. my.tenant123.some_entity321.created

 // Result
 // my.tenant123.parti_meta.some_entity123
 // my.tenant123.parti_meta.some_entity321


 // Getting messages costs O(n+1)
 iter, _ := p.Get(ctx, "some_entity123")
 for msg, err := range iter {
    if errors.Is(parti.ErrRetrievalFailed){
     break
   }

   // Do stuff with messages
 }

 _ = p.Del(ctx, "some_entity123") // Soft delete the entity, retaining metadata and messages


```
