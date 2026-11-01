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
protocol on a loaded host and passed three reruns, a fixture defect that
Boundless #309 below corrects.

## Boundless #309: 0143 FindCoordinator fixture

Test only; native behavior is unchanged. 0143's `test_find_coordinator` created
the consumer before installing the four FindCoordinator errors. Coordinator
discovery starts at creation on the native thread, so a query answered first
leaves the next one 600 s away and the errors are never consumed. It also
measured the gap after the first FindCoordinator, whose reply retires the
bootstrap broker and resets the query interval by design
(`rdkafka_cgrp.c` `_DESTROY_BROKER`), and asserted no minimum number of gaps.
The subtest now installs the errors before creating the consumer, skips the
gap after the first FindCoordinator, measures only the `num_retries - 1` gaps
that follow an error reply (the query after the last error is answered, and a
later one is not a retry) with the existing bounds, and asserts that all of
them were measured. On macOS arm64 (CMake Debug, static, no SSL/SASL), full
0143 at `d8fbe0cf` passed with the classic (load 21.8) and consumer (load 7.9)
group protocols, the race not reproducing; with this change both passed
(loads 58.7 and 85.1).

## Boundless #308: transactional requests sent again only if unprocessed

A failed AddOffsetsToTxn, TxnOffsetCommit or EndTxn attempt is sent again only
when the coordinator cannot have processed it: it was never transmitted, or
the coordinator refused it before acting. Upstream (`d8fbe0cf`) resends a
transmitted AddOffsetsToTxn and EndTxn through `rd_kafka_buf_retry()` with
`RD_KAFKA_REQUEST_MAX_RETRIES` after transport loss, request expiry,
COORDINATOR_NOT_AVAILABLE, NOT_COORDINATOR, REQUEST_TIMED_OUT,
COORDINATOR_LOAD_IN_PROGRESS, CONCURRENT_TRANSACTIONS and (AddOffsetsToTxn)
UNKNOWN_TOPIC_OR_PART, and a TxnOffsetCommit through a fresh coordinator
request after 500 ms for the same errors; the Java client re-enqueues all
three on disconnect. A transmitted original can still be waiting in a broker
request handler when the automatic copy succeeds and the transaction moves
on; it then acts on later work: a late AddOffsetsToTxn reopens a completed
transaction as ONGOING, which later times out and fences the epoch, a late
TxnOffsetCommit replaces a later offset in the same transaction, and a late
EndTxn (v0-1 carries only ID, epoch and result) commits or aborts the next
transaction.

`rd_kafka_txn_may_resend()` in `rdkafka_txnmgr.c` is the one check the three
resend sites call. It allows a resend when `rd_kafka_buf_was_sent()` is false,
or the answer is CONCURRENT_TRANSACTIONS (a pending transition or PREPARE state
for AddPartitions and EndTxn, failed transaction verification before the
TxnOffsetCommit write), or, for TxnOffsetCommit only, COORDINATOR_LOAD_IN_PROGRESS
(the loading check precedes record generation). The transaction coordinator
can answer COORDINATOR_LOAD_IN_PROGRESS for AddOffsetsToTxn and EndTxn after a
successful log append, so it proves nothing there. Unlike W7b, refusals are
still resent: the transactional API runs one call at a time, so a refused
attempt, which had no effect, cannot act on later work. Every other failure
is returned as a retriable error:

- AddOffsetsToTxn keeps the existing retriable result. An attempt the
  coordinator may have appended (NOT_COORDINATOR, COORDINATOR_NOT_AVAILABLE,
  REQUEST_TIMED_OUT, COORDINATOR_LOAD_IN_PROGRESS, UNKNOWN_TOPIC_OR_PART)
  keeps its participation in `txn_req_cnt`, as `_TRANSPORT`/`_TIMED_OUT`
  already did, so abort_transaction() of an offsets-only transaction still
  sends EndTxn instead of completing locally while the broker transaction
  stays ONGOING. The cost upstream already accepts after transport loss:
  when such an attempt was in fact not appended, that EndTxn finds no ongoing
  transaction and INVALID_TXN_STATE is fatal.
- TxnOffsetCommit returns the retriable error without requiring abort; the
  API-timeout branch is unchanged.
- EndTxn steps COMMITTING_TRANSACTION back to BEGIN_COMMIT (a new valid
  transition) and ABORTING_TRANSACTION back to BEGIN_ABORT, as upstream does
  for a failed abort, so the application's resumed commit_transaction() or
  abort_transaction() sends a new EndTxn instead of waiting for one no longer
  in flight.

Builders keep `MAX_RETRIES`: a never-transmitted or refused attempt is retried
until the API timeout as before. The other group requests keep upstream's
retries:

- SyncGroup: a repeat in another generation gets ILLEGAL_GENERATION; in STABLE
  the coordinator returns the current assignment and ignores the repeated one,
  and a pending write is re-checked by generation. No stale payload overwrites
  later state.
- Classic Heartbeat validates member and generation and only reschedules the
  session timer; it carries no payload to replay.
- ConsumerGroupHeartbeat keeps upstream's retries: the next ordinary heartbeat
  can carry the same epoch and subscription, leaving the late-original route;
  heartbeats carry no sequence to fence it.

Regression, 0105 on macOS arm64 (CMake Debug, static, no SSL/SASL):
`do_test_txn_resent_only_if_unprocessed` runs each of AddOffsetsToTxn,
TxnOffsetCommit, EndTxn(commit) and EndTxn(abort) against a transmitted
request answered only after it timed out, NOT_COORDINATOR,
CONCURRENT_TRANSACTIONS and COORDINATOR_LOAD_IN_PROGRESS, counting requests
with mock request tracking. Before (new test against `d8fbe0cf` source): the
no-answer, NOT_COORDINATOR and (except TxnOffsetCommit) loading cases were
sent again and succeeded, so the expected failure was missing; the
CONCURRENT_TRANSACTIONS and TxnOffsetCommit loading cases passed. After: those
cases fail retriable, not abortable or fatal, after 1 request and the
application's repeat sends exactly 1 more and succeeds; refusals still take 2
requests and succeed. `do_test_txn_abort_after_ambiguous_add_offsets`: an
offsets-only transaction whose AddOffsetsToTxn is answered NOT_COORDINATOR
fails send_offsets retriable and abort then sends 1 EndTxn (before:
send_offsets succeeded after the resend). Changed to assert the rule:
`recoverable_errors`, `endtxn_infinite`, `endtxn_timeout` and `txn_req_cnt`
now inject refusals for the internal retries they test; `endtxn_errors`
scenarios #0-#2 expect a retriable COORDINATOR_NOT_AVAILABLE, NOT_COORDINATOR
or COORDINATOR_LOAD_IN_PROGRESS and an explicit repeat;
`endtxn_timeout_inflight` expects the resumed call to fail retriable when the
in-flight EndTxn times out, and a third call to succeed; `switch_coordinator`
and `switch_coordinator_refresh` repeat commit/abort or send_offsets after a
retriable NOT_COORDINATOR; `offset_commit_doesnt_retry_too_quickly` uses
COORDINATOR_LOAD_IN_PROGRESS for the 500 ms retry delay it measures.
`send_offsets_concurrent_is_retried` and `disconnected_group_coord` (the
never-transmitted resend) are unchanged. Full 0105 passes with the classic
and consumer group protocols. The group test 0117 passes with both
protocols; 0147 and 0148 pass with the consumer protocol and skip with the
classic one, which they do not support; and 0102's mock subtest passes with the consumer protocol (the
mock cluster does not implement static membership for the classic one). The
full local suite (`-l`) passes with both protocols apart from 0138, whose
failure is the W5-D6 fault corrected below (Boundless #440).

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

### W5-D6 correction: reply custody for admin offset results (Boundless #440)

W5-D6 read the API key and version for a partition's or group's reply from
`request` in the shared `rd_kafka_handle_OffsetCommit` and
`rd_kafka_handle_OffsetFetch`. The admin parsers
(`rd_kafka_AlterConsumerGroupOffsetsResponse_parse`,
`rd_kafka_ListConsumerGroupOffsetsResponse_parse`) call those handlers
without a request buffer, so a broker error in either admin result
dereferenced NULL in `rd_kafka_broker_reply_new`: 0138's
`AlterConsumerGroupOffsets_errors` never got its result (the process faults;
on macOS it shows as a hang until the runner's timeout). Upstream
(`e1db7eaa`) passes 0138. W7b was not involved: the admin parser calls the
handler with `ignore_cgrp`, which never resends.

Both handlers now take that identity from the response they parsed, which
carries its request's header (`rd_kafka_req_response`), as the admin
topic/config/group parsers already do. All 35 reply-custody calls were
checked; only these handlers can see a NULL request. No API, lock or retry
changes.

Regression, 0138 on macOS arm64 (CMake Debug, static, no SSL/SASL): the
existing `AlterConsumerGroupOffsets_errors` subtests and the new
`ListConsumerGroupOffsets_errors` (an injected OffsetFetch
GROUP_AUTHORIZATION_FAILED reaches the result) pass with the classic and
consumer group protocols. Before (work-branch test, unfixed source), the new
subtest got no result until the runner killed it.

## Boundless #179: complete header-name octets

Upstream-derived `rd_kafka_header_get_all` discards the stored name length,
although `rd_kafka_header_add` and copy retain it. The additive
`rd_kafka_header_get_all_with_name_size` borrows the same name/value pointers
and also returns the complete name length, excluding the appended terminator.
Embedded NUL is data; empty names and null versus empty values are preserved.
An absent index returns `_NOENT` without writing outputs. Legacy getters,
storage, codecs, ownership, retry and transport behavior are unchanged.

Regression: `rdunittest.c::unittest_header_name_size` drives add/get/copy/get/
destroy with invalid UTF-8, NUL, empty and duplicate names, null/empty/binary
values and absent indices. On macOS arm64 at the #179 work-branch source,
CMake static native build and `TESTS=0000 RD_UT_TEST=header_name_size
build/tests/test-runner -p1 -Q -l` both exited 0. This is a broker-free getter
check, not full native/platform acceptance.

## Boundless #442: compressed batches decoded to their end

A Fetch batch's gzip or LZ4 payload yields records only when it decodes to
its end. Upstream (`e1db7eaa`) and the prior pin decoded only a gzip batch's
first member, dropping further members and ignoring any bytes after it, and
`rd_gz_decompress` never returned for a member cut short: pass 2 sizes its
buffer from pass 1, so a stream without its end met `Z_BUF_ERROR` with
`avail_out == 0` and its loop spun on the broker thread, which client
destruction then waits for. `rd_kafka_lz4_decompress` stopped when its input
ran out, so a frame without its end mark or content checksum was accepted
(the checksum was never verified).

`rd_gz_decompress_bounded` now decodes every member (RFC 1952 2.2) in both
passes; a following member is read with gzip-only window bits, while the
first keeps upstream's gzip/zlib detection. It returns NULL, which the reader
reports as `_BAD_COMPRESSION`, when the input ends inside a member, when bytes
after a member do not start one, or when `inflate` can make no progress; its
`maximum` bounds all members' output together, as `inflateReset2` clears
`total_out`. `rd_kafka_lz4_decompress` answers `_BAD_COMPRESSION` unless
`LZ4F_decompress` returned 0, the finished frame (`lz4frame.h`); output growth,
its receive bound and `_BAD_MSG` for bytes after a finished frame are
unchanged. The consumer's handling of a failed batch is upstream's: an error
for the partition at the batch offset, repeated on each fetch.

Mature decoders: the JDK 21 `GZIPInputStream` that Kafka's Java consumer uses
decodes further members and ignores other bytes after a member;
CPython's `gzip.decompress` and async-compression (`multiple_members`) decode
further members and refuse other bytes, as this patch does, and all refuse a
cut member. Kafka's `Lz4BlockInputStream` refuses a frame without its end
mark (`PREMATURE_EOS`).

Regression, macOS arm64 (CMake Debug, static, no SSL/SASL):
`RD_UT_TEST=gzip` (one member; two members in order; a sum over the bound
refused and at the bound accepted; garbage after a member, a member without
its trailer, cut inside its deflate data or a second member cut short, and no
input refused) and `RD_UT_TEST=lz4` (complete frames with and without content
checksum; without end mark or checksum `_BAD_COMPRESSION`; bytes after the
frame `_BAD_MSG`; a 1 MiB frame without content size that must grow its
output decodes fully) pass. Against the previous decoders the gzip unit fails
at the two members (only the first decoded) and the LZ4 unit at the frame
without its end mark (accepted). Boundless's
`protocols::kafka::tests::codecs::a_compressed_batch_reaches_the_program_only_when_decoded_to_its_end`
drives crafted batches through the Fetch reader. With this patch and #440's
correction, 0000 (every unit), 0138, 0105 and 0143 pass with the classic and
consumer group protocols.
