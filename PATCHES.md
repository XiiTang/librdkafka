# Boundless librdkafka native patches

Keep the 2.12.1 native baseline and caller-owned SASL/transport/lifecycle hooks.
Selected upstream backports: #5545 length-aware cluster ID copy, #5136 CMake
atomic detection/exchange plus the related OIDC preprocessing guard, #5089
timer callback/argument snapshot under the timer lock, #5488 consumer heartbeat
error classification and deferred leave/rejoin, #5574 FETCH_STOP pending guard,
#5457 send_offsets_to_transaction keeping its timeout (Boundless #66).
No share-consumer implementation or unrelated CI changes are imported.

CMake native tests 0000, 0146 and 0147 pass on macOS arm64; set
TEST_CONSUMER_GROUP_PROTOCOL=consumer for 0147. Added atomic_exchange verifies
previous-value returns for 32/64 bits, including the full-width 64-bit value.
Retain assignment-removal unit tests from #5574 and cluster-ID mock vectors
from #5545. Other platforms remain separate build/test gates.

A destroy with `RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE` ends the commits waiting
for a group coordinator at once instead of `session.timeout.ms` after
termination began: that flag calls for immediate termination and nothing waits
for those commits, while rust-rdkafka's caller-owned close destroys with it
(Boundless #66). Test 0084_destroy_flags_mock: destroy took 30000 ms before,
under 1 ms after. Before #5457, Boundless's send_offsets test without a group coordinator
waited without end; its 0105 subtest passes after. Tests 0084, 0105
(full), 0106, 0113, 0116 and 0117 pass on macOS arm64.

A manual OffsetCommit is sent again only while it was never transmitted
(Boundless W7b). Upstream (`e1db7eaa`) retries a transmitted OffsetCommit up to
twice (#3398), including COORDINATOR_LOAD_IN_PROGRESS and an all-partitions
UNKNOWN_TOPIC_OR_PART reply. It also defers a commit that failed with a
transport error, NOT_COORDINATOR or COORDINATOR_NOT_AVAILABLE until the
coordinator returns, then sends it again. These retries do not serialize
overlapping commits: a later commit can pass an older request during retry
backoff, and the stale retry can overwrite the newer commit.

For ordinary nontransactional commits, COORDINATOR_LOAD_IN_PROGRESS and an
all-partitions UNKNOWN_TOPIC_OR_PART reply prove that the requested offsets
were not appended. The fork still returns these refusals to the program,
which decides whether to commit again, at the cost of upstream's automatic
recovery. A mixed reply is not proof that every partition was unapplied.
Transport failures, NOT_COORDINATOR, COORDINATOR_NOT_AVAILABLE and
REQUEST_TIMED_OUT can instead leave the result ambiguous. Even a certainly
unapplied request can become stale before its retry, so the fork keeps the
same no-transmitted-retry policy for both kinds of failure.

The request uses upstream's RD_KAFKA_REQUEST_NO_RETRIES, under which
rd_kafka_buf_retry() retries a buffer only while rd_kafka_buf_was_sent() is
false, and the consumer group defers only a commit whose request was never
transmitted. A transmitted commit that fails ends with its error. KIP-848's
re-commit of automatic commits after STALE_MEMBER_EPOCH is unchanged. Test 0117
offset_commit_resent_only_if_never_transmitted: before, the transmitted commit
was sent again and returned NO_ERROR (with only the retry limit, the deferral
still sent it again); after, it ends with ERR__TRANSPORT, the
never-transmitted commit is retried once and succeeds, the broker receives 2 OffsetCommits and offset 2
stays committed. 0117's REQUEST_TIMED_OUT test and 0143's offset_commit test
now assert one OffsetCommit and the commit error (before: 2 requests and
success). All local tests pass on macOS arm64 with the classic and consumer
group protocols; 0143's FindCoordinator timing failed twice under the consumer
protocol on a loaded host and passed three reruns.

## W17d: configured populations and linear aborted transaction storage

The pinned upstream client creates bootstrap, metadata and coordinator brokers
and topic/desired partition handles without aggregate admission. The new optional
`broker.population.max` and `partition.population.max` properties default to zero
(upstream behavior); positive selections admit at their native owners before
creation and retain places until final destruction. Logical coordinator places
are included at client construction; the fixed INTERNAL dummy is infrastructure.
Exhaustion retains a distinct fatal population code. Topic, produce, lookup and
assignment paths propagate refusal without null dereferences, retries or
fabricated success. An eager assignment can already have cleared its previous
assignment when addition refuses, as upstream does for other addition errors.
The existing first pass obtains and checks each handle before marking the assignment; it uses actual per-creation admission, without a quota predictor. The patch preserves those actual effects. Broker list removal no longer refunds
a place while its broker object remains alive. Native threads, supplied transport,
close and transmitted-commit retry behavior are unchanged.

Upstream preallocates the complete Fetch aborted-transaction count for every
PID, giving quadratic storage for distinct PIDs. Per-PID offset vectors now grow
with their actual entries using checked sizes, without a new limit; the existing
Fetch array-count decoder bound and read-committed lookup/pop/sort behavior stay.

Regression evidence (macOS arm64, work branch): `TESTS=0146,0000
RD_UT_TEST=aborted_txns .../test-runner -p1 -Q -l` passed. Population paths cover
bootstrap/learned brokers, logical group allowance, retained replacement after
list removal, UA/desired/metadata growth and assignment refusal. The retained
broker test gates its real thread-exit interceptor, then observes refusal while
the decommissioned object remains alive; the retained toppar test holds its real
handle. The aborted suite checks linear aggregate slots, repeated PID offsets,
next/pop and the existing Fetch count decoder at and beyond its bound. These
focused results do not claim a complete native lane.

## W5-D6: preserve the reply selected by the existing error owner

Upstream v2.12.1 (`e1db7eaa`) reduces decoded errors to native integers and
fallback C strings. SaslAuthenticate 58 becomes `_AUTHENTICATION`, admin timeout
7 can become success, and unrelated requests purged by a broker failure inherit
its native error. These values cannot establish which request the peer refused.
The optional immutable `rd_kafka_broker_reply_t` keeps the original API/version,
signed wire code, addressed broker, resource key and decoded-length explanation
on the existing response, error, event, message or keyed result owner. Public
getters borrow it; copies and destruction follow those owners. No public setter,
client error history or new completion event is introduced.

The richer synchronous metadata/watermark/offset/group entries share upstream's
implementation and return its unchanged overall status. They transfer completed
keyed results before the existing reply destruction, including error completion.
Legacy entry points retain their success-only conventions. Group listing retains
each broker's failed callback independently of the last scalar status, and
publishes only fully decoded groups/members; upstream's early counts otherwise
expose null fields when returning partial data. Unanswered offset inputs are
invalidated by the richer entry rather than reported as broker-returned offsets.

Transaction and first-fatal reply copies use the existing locks and source
selection. A previously selected local abortable error remains the EndTxn
answer even if EndTxn subsequently receives a peer error. Produce results,
metadata cache copies, DeleteRecords fanout and consumer error events carry the
selected source through their existing transfers. Transport failures, local
parse/state errors, intermediate retry replies and synthesized EOF have no reply.

Upstream's Fetch EOF synthesis can overwrite a received partition refusal on an
empty partition. It now requires a successful response before synthesizing EOF;
Apache Kafka's FetchCollector.initialize likewise handles errors before successful
fetch state. The real empty-partition Fetch regression otherwise receives no
application error. Fetch request-level errors retain request scope. This changes
neither the existing application-error branches nor their suppression/backoff.

Comparison with upstream: existing codec/correlation, retry actions, transaction
flags, deadlines, callback dispatch, purge and close stay in place; the prior
no-transmitted-OffsetCommit-retry patch remains intact. Reply custody is attached
only after successful decoding and only to the result selected for publication.
Length-bearing peer text stays separate from native fallback diagnostics.

Regression entry points: native `0000`, `RD_UT_TEST=failure_rows`, drives the
actual ListGroups/DescribeGroups callbacks (callback ordering, multiple broker
refusals and incomplete tails) and EndTxn's earlier-local-error selection. The
consumer fork's `test_failure_rows` drives actual admin/offset/group frames,
empty-partition Fetch through both Rust routes, transaction/fatal custody and
owner destruction. Focused revision/platform results are recorded with the
consumer implementation; no complete native lane is claimed here.


### W5-D6 review corrections: Produce keys and full-i32 formatting

The Produce decoder skipped TopicName below version 10, although D6's selected
reply used it for every supported version. It now uses the existing string
decoder unconditionally; version-specific leader tags and retry selection stay
unchanged. The consumer's existing real Produce outcome regression negotiates
version 9 as well as retaining the 30000 default-version case, and checks the
ordinary result and publication-child capture after native destruction.

At upstream v2.12.1 (`e1db7eaa`) and the prior pin, err2str/err2name subtract
RD_KAFKA_RESP_ERR__BEGIN before checking the range. Safe UnknownCode(i32::MAX)
therefore reaches signed overflow. Both native entries now short-circuit on the
existing range guards before index arithmetic, preserving known output and the
native Err-N?/ERR_N? fallbacks. The existing Rust native-formatting test exercises
both i32 extremes through name and Display; sanitizer results are recorded with
the review correction validation.

The existing group callback unit now identifies errors by distinct decoded broker
ids rather than collection position. Both callback schedules, two refusals,
overall native status and complete-group-prefix assertions remain.


Review-correction validation on macOS arm64: the native build and both
`RD_UT_TEST=failure_rows` units pass (including keyed broker lookup and both
callback schedules). At the same native source, Rust 1.97.0's existing
`types::tests::test_display_error` passes for EOF and both full-i32 extremes
through name and Display with native CFLAGS `-fsanitize=undefined
-fno-sanitize-recover=undefined` and `UBSAN_OPTIONS=halt_on_error=1`. The Rust
harness explicitly links the Apple Clang UBSan runtime; an initial link without
that runtime failed before running the test. No sanitizer report occurs in the
passing run. This remains a focused correction check, not a full native lane.
