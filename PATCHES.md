# Runtime transport and allocation bounds

Base: librdkafka 2.12.1, `e1db7eaa517f0a6438bc846a9c49ede73b9ea211`.

The optional runtime connect callback receives the actual logical broker authority and a stable broker-instance identity before any external connect. Runtime supplies the socket's transport. The existing native Kafka codec, broker routing, group, producer, transaction and management engines remain in use.

The extra `runtime.maximum.brokers` quota was removed in the simple resource stage; upstream broker metadata bounds apply. Runtime metadata bounds aggregate topics/partitions (4096) and record headers (128). Existing gzip, Snappy and LZ4 decoders enforce the configured receive size before allocating expanded bodies; Zstd retains its native limit. The gzip and Java Snappy bounded entry points have direct tests in the companion rust-rdkafka fork.

Validation on macOS arm64: Kafka 4.3.1 interoperation with all five compression codecs, binary/null records and headers, manual offsets, classic/consumer groups, idempotent production, transaction commit/abort and group-offset transfer, finite topic/partition/config/group operations, PLAIN/SCRAM-256/SCRAM-512/OAuth and Runtime HTTP CONNECT target authorization. This is not a claim of verification on other operating systems or every broker release.

## SASL admission callback

The optional `runtime_sasl_admit_cb` runs before every SASL provider start,
including reauthentication on an established socket. It receives the actual
broker authority and may refuse before any credential token is produced.
No native codec, reconnect policy, or successful-session lifetime is replaced.
The companion Rust wrapper catches callback panics and maps refusal to a fixed,
credential-free native error. The companion library suite builds this native
source and passes 19 tests on macOS arm64; Boundless owns the frozen-expiry and
wire-level reauthentication regression.

## Caller-owned SASL mechanisms

Every SASL mechanism is a caller-owned exchange through `runtime_sasl_cb`; a
SASL client without that callback fails at creation. The native PLAIN, SCRAM,
OAUTHBEARER, Cyrus and SSPI engines are never selected, so a peer's challenge
never reaches their stack buffers (SCRAM's `AuthMessage` and salt are sized by
the peer) and no locally derived proof reaches an error string. librdkafka
still sends the configured `sasl.mechanisms` name in SaslHandshake, frames each
token, bounds every token in either direction to 65536 bytes, and schedules
reauthentication from the broker's session lifetime. A step that consumes the
peer's final message and completes with no output (SCRAM's server-final) ends
the exchange without another SaslAuthenticate. Callback failures surface only
as the fixed "Caller-owned SASL exchange failed".
