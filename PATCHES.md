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

An OffsetCommit is sent again only while it was never transmitted (Boundless
W7b): a transmitted commit may have been processed, and resending it can
overwrite a later commit. Upstream retried a transmitted OffsetCommit up to
twice (#3398) and deferred one that failed with a transport error,
NOT_COORDINATOR or COORDINATOR_NOT_AVAILABLE until the coordinator returned,
then sent it again; Kafka's group coordinator also returns those two errors
after the commit was appended (REQUEST_TIMED_OUT, NOT_ENOUGH_REPLICAS and
NOT_LEADER_OR_FOLLOWER are mapped to them). The request now uses upstream's
RD_KAFKA_REQUEST_NO_RETRIES, under which rd_kafka_buf_retry() retries a buffer
only while rd_kafka_buf_was_sent() is false, and the consumer group defers
only a commit whose request was never transmitted. A transmitted commit that
fails ends with its error. KIP-848's re-commit of automatic commits after
STALE_MEMBER_EPOCH is unchanged. Test 0117
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
