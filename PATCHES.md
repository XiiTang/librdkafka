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
