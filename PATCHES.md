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
