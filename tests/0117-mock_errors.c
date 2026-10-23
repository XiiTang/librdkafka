/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2020-2022, Magnus Edenhill
 *               2025, Confluent Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "test.h"

#include "rdkafka.h"

#include "../src/rdkafka_proto.h"
#include "../src/rdunittest.h"

#include <stdarg.h>


/**
 * @name Misc mock-injected errors.
 *
 */

/**
 * @brief Test producer handling (retry) of ERR_KAFKA_STORAGE_ERROR.
 */
static void do_test_producer_storage_error(rd_bool_t too_few_retries) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_resp_err_t err;

        SUB_TEST_QUICK("%s", too_few_retries ? "with too few retries" : "");

        test_conf_init(&conf, NULL, 10);

        test_conf_set(conf, "test.mock.num.brokers", "3");
        test_conf_set(conf, "retries", too_few_retries ? "1" : "10");
        test_conf_set(conf, "retry.backoff.ms", "500");
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);

        test_curr->ignore_dr_err = rd_false;
        if (too_few_retries) {
                test_curr->exp_dr_err = RD_KAFKA_RESP_ERR_KAFKA_STORAGE_ERROR;
                test_curr->exp_dr_status = RD_KAFKA_MSG_STATUS_NOT_PERSISTED;
        } else {
                test_curr->exp_dr_err    = RD_KAFKA_RESP_ERR_NO_ERROR;
                test_curr->exp_dr_status = RD_KAFKA_MSG_STATUS_PERSISTED;
        }

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        mcluster = rd_kafka_handle_mock_cluster(rk);
        TEST_ASSERT(mcluster, "missing mock cluster");

        rd_kafka_mock_push_request_errors(
            mcluster, RD_KAFKAP_Produce, 3,
            RD_KAFKA_RESP_ERR_KAFKA_STORAGE_ERROR,
            RD_KAFKA_RESP_ERR_KAFKA_STORAGE_ERROR,
            RD_KAFKA_RESP_ERR_KAFKA_STORAGE_ERROR);

        err = rd_kafka_producev(rk, RD_KAFKA_V_TOPIC("mytopic"),
                                RD_KAFKA_V_VALUE("hi", 2), RD_KAFKA_V_END);
        TEST_ASSERT(!err, "produce failed: %s", rd_kafka_err2str(err));

        /* Wait for delivery report. */
        test_flush(rk, 5000);

        rd_kafka_destroy(rk);

        SUB_TEST_PASS();
}


/**
 * @brief Issue #2933. Offset commit being retried when failing due to
 *        RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS and then causing fetchers
 *        to not start.
 */
static void do_test_offset_commit_error_during_rebalance(void) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *c1, *c2;
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        const char *topic            = "test";
        const int msgcnt             = 100;
        const int exp_msg_cnt_intial = 1;
        int exp_msg_cnt_final        = msgcnt;
        rd_kafka_resp_err_t err;

        SUB_TEST();

        test_conf_init(&conf, NULL, 60);

        mcluster = test_mock_cluster_new(3, &bootstraps);

        rd_kafka_mock_topic_create(mcluster, topic, 4, 3);

        /* Seed the topic with messages */
        test_produce_msgs_easy_v(topic, 0, RD_KAFKA_PARTITION_UA, 0, msgcnt, 10,
                                 "bootstrap.servers", bootstraps,
                                 "batch.num.messages", "1", NULL);

        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "false");

        /* Make sure we don't consume the entire partition in one Fetch */
        test_conf_set(conf, "fetch.message.max.bytes", "100");

        c1 = test_create_consumer("mygroup", test_rebalance_cb,
                                  rd_kafka_conf_dup(conf), NULL);

        c2 = test_create_consumer("mygroup", test_rebalance_cb, conf, NULL);

        test_consumer_subscribe(c1, topic);
        test_consumer_subscribe(c2, topic);


        /* Wait for assignment and one message */
        test_consumer_poll("C1.PRE", c1, 0, -1, -1, exp_msg_cnt_intial, NULL);
        test_consumer_poll("C2.PRE", c2, 0, -1, -1, exp_msg_cnt_intial, NULL);

        /* Trigger rebalance */
        test_consumer_close(c2);
        rd_kafka_destroy(c2);

        rd_kafka_mock_push_request_errors(
            mcluster, RD_KAFKAP_OffsetCommit, 6,
            RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS,
            RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS,
            RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS,
            RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS,
            RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS,
            RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS);

        /* This commit should fail (async) */
        TEST_SAY("Committing (should fail)\n");
        err = rd_kafka_commit(c1, NULL, 0 /*sync*/);
        TEST_SAY("Commit returned %s\n", rd_kafka_err2name(err));
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS,
                    "Expected commit to fail with ERR_REBALANCE_IN_PROGRESS, "
                    "not %s",
                    rd_kafka_err2name(err));

        /* Since all the assignors in the `consumer` protocol are COOPERATIVE
         * only the new partitions are assigned to the consumer. All the
         * previously assigned partitions will start consuming from the last
         * offset. */
        if (!test_consumer_group_protocol_classic())
                exp_msg_cnt_final = msgcnt - exp_msg_cnt_intial;

        /* Wait for new assignment and able to read all messages */
        test_consumer_poll("C1.POST", c1, 0, -1, -1, exp_msg_cnt_final, NULL);

        rd_kafka_destroy(c1);

        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}



static rd_bool_t is_offset_commit(rd_kafka_mock_request_t *request,
                                  void *opaque) {
        return rd_kafka_mock_request_api_key(request) == RD_KAFKAP_OffsetCommit;
}

/**
 * @brief An OffsetCommit answered with ERR_REQUEST_TIMED_OUT was transmitted
 *        and may have been processed: it is not sent again, and the commit
 *        ends with that error.
 */
static void do_test_offset_commit_request_timed_out(rd_bool_t auto_commit) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *c1, *c2;
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        const char *topic = "test";
        const int msgcnt  = 1;
        rd_kafka_topic_partition_list_t *partitions;
        rd_kafka_resp_err_t err;
        size_t commit_cnt;

        SUB_TEST_QUICK("enable.auto.commit=%s", auto_commit ? "true" : "false");

        test_conf_init(&conf, NULL, 60);

        mcluster = test_mock_cluster_new(1, &bootstraps);

        rd_kafka_mock_topic_create(mcluster, topic, 1, 1);

        /* Seed the topic with messages */
        test_produce_msgs_easy_v(topic, 0, RD_KAFKA_PARTITION_UA, 0, msgcnt, 10,
                                 "bootstrap.servers", bootstraps,
                                 "batch.num.messages", "1", NULL);

        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit",
                      auto_commit ? "true" : "false");
        /* Too high to be done by interval in this test */
        test_conf_set(conf, "auto.commit.interval.ms", "90000");

        /* Make sure we don't consume the entire partition in one Fetch */
        test_conf_set(conf, "fetch.message.max.bytes", "100");

        c1 = test_create_consumer("mygroup", NULL, rd_kafka_conf_dup(conf),
                                  NULL);


        test_consumer_subscribe(c1, topic);

        /* Wait for assignment and one message */
        test_consumer_poll("C1.PRE", c1, 0, -1, -1, 1, NULL);

        rd_kafka_mock_start_request_tracking(mcluster);

        rd_kafka_mock_push_request_errors(mcluster, RD_KAFKAP_OffsetCommit, 1,
                                          RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT);

        if (!auto_commit) {
                err = rd_kafka_commit(c1, NULL, 0 /*sync*/);
                TEST_ASSERT(err == RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT,
                            "Expected commit to fail with "
                            "ERR_REQUEST_TIMED_OUT, not %s",
                            rd_kafka_err2name(err));
        }

        /* Rely on consumer_close() doing final commit
         * when auto commit is enabled */

        test_consumer_close(c1);

        rd_kafka_destroy(c1);

        commit_cnt =
            test_mock_get_matching_request_cnt(mcluster, is_offset_commit, NULL);
        TEST_ASSERT(commit_cnt == 1,
                    "Expected 1 OffsetCommit request, not %" PRIusz,
                    commit_cnt);

        /* Create a new consumer and retrieve the committed offsets to verify
         * nothing was committed */
        c2 = test_create_consumer("mygroup", NULL, conf, NULL);

        partitions = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(partitions, topic, 0)->offset =
            RD_KAFKA_OFFSET_INVALID;

        TEST_CALL_ERR__(rd_kafka_committed(c2, partitions, 10 * 1000));
        TEST_ASSERT(partitions->elems[0].offset == RD_KAFKA_OFFSET_INVALID,
                    "Expected no committed offset, not %" PRId64,
                    partitions->elems[0].offset);

        rd_kafka_topic_partition_list_destroy(partitions);

        rd_kafka_destroy(c2);

        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief Verify that a cluster roll does not cause consumer_poll() to return
 * the temporary and retriable COORDINATOR_LOAD_IN_PROGRESS error. We should
 * backoff and retry in that case.
 */
static void do_test_joingroup_coordinator_load_in_progress() {
        rd_kafka_conf_t *conf;
        rd_kafka_t *consumer;
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        const char *topic = "test";
        const int msgcnt  = 1;

        SUB_TEST();

        test_conf_init(&conf, NULL, 60);

        mcluster = test_mock_cluster_new(1, &bootstraps);

        rd_kafka_mock_topic_create(mcluster, topic, 1, 1);

        test_produce_msgs_easy_v(topic, 0, RD_KAFKA_PARTITION_UA, 0, msgcnt, 10,
                                 "bootstrap.servers", bootstraps,
                                 "batch.num.messages", "1", NULL);

        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "auto.offset.reset", "earliest");

        rd_kafka_mock_push_request_errors(
            mcluster, RD_KAFKAP_FindCoordinator, 1,
            RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS);

        consumer = test_create_consumer("mygroup", NULL, conf, NULL);

        test_consumer_subscribe(consumer, topic);

        /* Wait for assignment and one message */
        test_consumer_poll("consumer", consumer, 0, -1, -1, msgcnt, NULL);

        test_consumer_close(consumer);

        rd_kafka_destroy(consumer);

        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


static rd_bool_t offset_commit_queued;

/**
 * @brief Notes when a connection holds one request in its output queue
 *        behind one awaiting its reply: only the coordinator connection
 *        does, while the first commit's reply is held.
 */
static int
queued_stats_cb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque) {
        if (strstr(json, "\"outbuf_cnt\":1, \"outbuf_msg_cnt\":0, "
                         "\"waitresp_cnt\":1,"))
                offset_commit_queued = rd_true;
        return 0;
}

static void commit_offset_queue(rd_kafka_t *c,
                                const char *topic,
                                int64_t offset,
                                rd_kafka_queue_t *rkqu) {
        rd_kafka_topic_partition_list_t *offsets =
            rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(offsets, topic, 0)->offset = offset;
        TEST_CALL_ERR__(rd_kafka_commit_queue(c, offsets, rkqu, NULL, NULL));
        rd_kafka_topic_partition_list_destroy(offsets);
}

/**
 * @brief Records the result of the next commit on \p rkqu, if one arrives
 *        within \p timeout_ms, in \p errs indexed by its offset.
 *
 * @returns rd_true if a result arrived.
 */
static rd_bool_t next_commit_result(rd_kafka_queue_t *rkqu,
                                    int timeout_ms,
                                    rd_kafka_resp_err_t *errs) {
        rd_kafka_event_t *rkev = rd_kafka_queue_poll(rkqu, timeout_ms);
        const rd_kafka_topic_partition_list_t *offsets;
        int64_t offset;

        if (!rkev)
                return rd_false;

        TEST_ASSERT(rd_kafka_event_type(rkev) == RD_KAFKA_EVENT_OFFSET_COMMIT,
                    "Expected an offset commit result, not %s",
                    rd_kafka_event_name(rkev));
        offsets = rd_kafka_event_topic_partition_list(rkev);
        TEST_ASSERT(offsets && offsets->cnt == 1,
                    "Expected the result of one partition's commit");
        offset = offsets->elems[0].offset;
        TEST_ASSERT(offset == 1 || offset == 2,
                    "Unexpected committed offset %" PRId64, offset);
        errs[offset] = rd_kafka_event_error(rkev);
        TEST_SAY("Commit of offset %" PRId64 " returned %s\n", offset,
                 rd_kafka_err2name(errs[offset]));
        rd_kafka_event_destroy(rkev);
        return rd_true;
}

/**
 * @brief An OffsetCommit whose request was transmitted may have been
 *        processed when its connection fails: it ends with
 *        ERR__TRANSPORT and is not sent again, so a later commit on the
 *        new connection cannot be overwritten by it.
 *        An OffsetCommit that was never transmitted when its connection
 *        failed is still sent again, once, on the next connection.
 */
static void do_test_offset_commit_resent_only_if_never_transmitted(void) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *c;
        rd_kafka_queue_t *rkqu;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_topic_partition_list_t *committed;
        const char *bootstraps;
        const char *topic           = "test";
        rd_kafka_resp_err_t errs[3] = {RD_KAFKA_RESP_ERR__IN_PROGRESS,
                                       RD_KAFKA_RESP_ERR__IN_PROGRESS,
                                       RD_KAFKA_RESP_ERR__IN_PROGRESS};
        int results                 = 0;
        size_t commit_cnt;

        SUB_TEST_QUICK();

        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 1, 1);

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "enable.auto.commit", "false");
        /* With one request in flight per connection the second commit
         * stays in the output queue while the first awaits its reply. */
        test_conf_set(conf, "max.in.flight", "1");
        test_conf_set(conf, "statistics.interval.ms", "100");
        rd_kafka_conf_set_stats_cb(conf, queued_stats_cb);

        c    = test_create_consumer("mygroup", NULL, conf, NULL);
        rkqu = rd_kafka_queue_new(c);

        /* A commit made before the coordinator is known waits for it once
         * and is not deferred again. With the coordinator known, the
         * connection failure is what could send the commit again. */
        committed = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(committed, topic, 0)->offset =
            RD_KAFKA_OFFSET_INVALID;
        TEST_CALL_ERR__(rd_kafka_committed(c, committed, 10 * 1000));
        rd_kafka_topic_partition_list_destroy(committed);

        rd_kafka_mock_start_request_tracking(mcluster);

        /* The broker stores the first commit and holds its reply. */
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 1, RD_KAFKAP_OffsetCommit, 1, RD_KAFKA_RESP_ERR_NO_ERROR,
            20 * 1000);

        commit_offset_queue(c, topic, 1, rkqu);
        test_mock_wait_matching_requests(mcluster, 1, 0, is_offset_commit,
                                         NULL);

        offset_commit_queued = rd_false;
        commit_offset_queue(c, topic, 2, rkqu);
        while (!offset_commit_queued) {
                rd_kafka_message_t *rkm = rd_kafka_consumer_poll(c, 100);
                if (rkm)
                        rd_kafka_message_destroy(rkm);
        }

        /* The connection fails: the first commit was transmitted,
         * the second never was. */
        TEST_SAY("Coordinator connection goes down\n");
        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;
        rd_kafka_mock_broker_set_down(mcluster, 1);

        /* The transmitted commit ends at once, while the broker is down. */
        if (next_commit_result(rkqu, 5000, errs))
                results++;

        rd_kafka_mock_broker_set_up(mcluster, 1);

        while (results < 2) {
                TEST_ASSERT(next_commit_result(rkqu, 30 * 1000, errs),
                            "Timed out waiting for the commit results");
                results++;
        }

        TEST_ASSERT(errs[1] == RD_KAFKA_RESP_ERR__TRANSPORT,
                    "Expected the transmitted commit to end with "
                    "ERR__TRANSPORT, not %s",
                    rd_kafka_err2name(errs[1]));
        TEST_ASSERT(errs[2] == RD_KAFKA_RESP_ERR_NO_ERROR,
                    "Expected the never-transmitted commit to be sent again "
                    "and succeed, not %s",
                    rd_kafka_err2name(errs[2]));

        /* Each commit reached the broker once. */
        commit_cnt =
            test_mock_get_matching_request_cnt(mcluster, is_offset_commit, NULL);
        TEST_ASSERT(commit_cnt == 2,
                    "Expected the broker to receive 2 OffsetCommit requests, "
                    "not %" PRIusz,
                    commit_cnt);

        committed = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(committed, topic, 0)->offset =
            RD_KAFKA_OFFSET_INVALID;
        TEST_CALL_ERR__(rd_kafka_committed(c, committed, 10 * 1000));
        TEST_ASSERT(committed->elems[0].offset == 2,
                    "Expected committed offset 2, not %" PRId64,
                    committed->elems[0].offset);
        rd_kafka_topic_partition_list_destroy(committed);

        rd_kafka_queue_destroy(rkqu);
        rd_kafka_destroy(c);
        test_curr->is_fatal_cb = NULL;

        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

int main_0117_mock_errors(int argc, char **argv) {

        TEST_SKIP_MOCK_CLUSTER(0);

        do_test_producer_storage_error(rd_false);
        do_test_producer_storage_error(rd_true);

        do_test_offset_commit_error_during_rebalance();

        do_test_offset_commit_request_timed_out(rd_true);
        do_test_offset_commit_request_timed_out(rd_false);

        do_test_joingroup_coordinator_load_in_progress();

        do_test_offset_commit_resent_only_if_never_transmitted();

        return 0;
}
