# Boundless librdkafka native patches

Keep the 2.12.1 native baseline and caller-owned SASL/transport/lifecycle hooks.
Selected upstream backports: #5545 length-aware cluster ID copy, #5136 CMake
atomic detection/exchange plus the related OIDC preprocessing guard, #5089
timer callback/argument snapshot under the timer lock, #5488 consumer heartbeat
error classification and deferred leave/rejoin, #5574 FETCH_STOP pending guard.
No share-consumer implementation or unrelated CI changes are imported.

CMake native tests 0000, 0146 and 0147 pass on macOS arm64; set
TEST_CONSUMER_GROUP_PROTOCOL=consumer for 0147. Added atomic_exchange verifies
previous-value returns for 32/64 bits, including the full-width 64-bit value.
Retain assignment-removal unit tests from #5574 and cluster-ID mock vectors
from #5545. Other platforms remain separate build/test gates.
