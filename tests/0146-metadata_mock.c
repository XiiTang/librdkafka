/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2024, Confluent Inc.
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

#include "../src/rdkafka_proto.h"
#include "../src/rdkafka_int.h" /* rk_partition_cnt, rk_topic_cnt */

static rd_bool_t is_metadata_request(rd_kafka_mock_request_t *request,
                                     void *opaque) {
        return rd_kafka_mock_request_api_key(request) == RD_KAFKAP_Metadata;
}

static rd_bool_t is_fetch_request(rd_kafka_mock_request_t *request,
                                  void *opaque) {
        int32_t *broker_id = (int32_t *)opaque;
        rd_bool_t ret =
            rd_kafka_mock_request_api_key(request) == RD_KAFKAP_Fetch;
        if (broker_id)
                ret &= rd_kafka_mock_request_id(request) == *broker_id;
        return ret;
}

/**
 * @brief Metadata should persists in cache after
 *        a full metadata refresh.
 *
 * @param assignor Assignor to use
 */
static void do_test_metadata_persists_in_cache(const char *assignor) {
        rd_kafka_t *rk;
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        const char *topic = test_mk_topic_name(__FUNCTION__, 1);
        rd_kafka_conf_t *conf;
        rd_kafka_topic_t *rkt;
        const rd_kafka_metadata_t *md;
        rd_kafka_topic_partition_list_t *subscription;

        SUB_TEST_QUICK("%s", assignor);

        mcluster = test_mock_cluster_new(3, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 1, 1);

        test_conf_init(&conf, NULL, 10);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "partition.assignment.strategy", assignor);
        test_conf_set(conf, "group.id", topic);

        rk = test_create_handle(RD_KAFKA_CONSUMER, conf);

        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, topic, 0);

        rkt = test_create_consumer_topic(rk, topic);

        /* Metadata for topic is available */
        TEST_CALL_ERR__(rd_kafka_metadata(rk, 0, rkt, &md, 1000));
        rd_kafka_metadata_destroy(md);
        md = NULL;

        /* Subscribe to same topic */
        TEST_CALL_ERR__(rd_kafka_subscribe(rk, subscription));

        /* Request full metadata */
        TEST_CALL_ERR__(rd_kafka_metadata(rk, 1, NULL, &md, 1000));
        rd_kafka_metadata_destroy(md);
        md = NULL;

        /* Subscribing shouldn't give UNKNOWN_TOPIC_OR_PART err.
         * Verify no error was returned. */
        test_consumer_poll_no_msgs("no error", rk, 0, 100);

        rd_kafka_topic_partition_list_destroy(subscription);
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief Tests about the fast metadata refresh feature.
 *
 * - variation 0: no loop of metadata requests should be started
 *   when a metadata request is made without leader epoch change.
 *   It's expected it stops after the first request.
 *   See issue #4577
 *
 * - variation 1: an error is returned on subsequent metadata response too,
 *   metadata refreshes continue every second until the error is cleared.
 *
 * It's expected that the temporary error doesn't remove the topic from cache,
 * produce requests aren't blocked from metadata errors and no error is
 * to be surfaced to the application.
 */
static void do_test_fast_metadata_refresh(int variation) {
        rd_kafka_t *rk;
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        const char *topic = test_mk_topic_name(__FUNCTION__, 1);
        rd_kafka_conf_t *conf;
        int metadata_requests;
        int expected_metadata_requests;
        switch (variation) {
        case 0:
                expected_metadata_requests = 2;
                break;
        case 1:
                expected_metadata_requests = 7;
                break;
        default:
                TEST_FAIL("Invalid variation %d", variation);
                break;
        }

        SUB_TEST_QUICK("%s", variation == 0 ? "stops" : "retries");

        mcluster = test_mock_cluster_new(3, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 1, 1);

        test_conf_init(&conf, NULL, 10);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        rd_kafka_mock_start_request_tracking(mcluster);

        if (variation == 1) {
                /* Produce some messages to the topic
                 * and keep metadata in cache. */
                test_produce_msgs2(rk, topic, 0, 0, 0, 3, NULL, 5);
        }

        /* This error triggers a metadata refresh but no leader change
         * happened */
        rd_kafka_mock_push_request_errors(
            mcluster, RD_KAFKAP_Produce, 1,
            RD_KAFKA_RESP_ERR_KAFKA_STORAGE_ERROR);

        if (variation == 1) {
                rd_kafka_mock_topic_set_error(
                    mcluster, topic, RD_KAFKA_RESP_ERR_KAFKA_STORAGE_ERROR);
        }

        test_produce_msgs2(rk, topic, 0, 0, 0, 3, NULL, 5);

        /* Wait some time for seeing the retries */
        rd_sleep(3);

        if (variation == 1) {
                /* Clear topic error to stop the retries */
                rd_kafka_mock_topic_set_error(mcluster, topic,
                                              RD_KAFKA_RESP_ERR_NO_ERROR);
        }

        /* First call is for getting initial metadata,
         * second one happens after the error,
         * it should stop refreshing metadata after that.
         *
         * There can be an additional metadata request originating from
         * the 1s timer when the partition is being delegated or
         * the broker is connecting but still not up. */
        metadata_requests = test_mock_wait_matching_requests(
            mcluster, expected_metadata_requests, 500, is_metadata_request,
            NULL);
        TEST_ASSERT(expected_metadata_requests <= metadata_requests &&
                        metadata_requests <= expected_metadata_requests + 1,
                    "Expected %d or %d metadata request, got %d",
                    expected_metadata_requests, expected_metadata_requests + 1,
                    metadata_requests);
        rd_kafka_mock_stop_request_tracking(mcluster);

        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief A stale leader received while validating shouldn't
 *        migrate back the partition to that stale broker.
 */
static void do_test_stale_metadata_doesnt_migrate_partition(void) {
        int i, fetch_requests;
        rd_kafka_t *rk;
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        const char *topic = test_mk_topic_name(__FUNCTION__, 1);
        rd_kafka_conf_t *conf;
        int32_t expected_broker_id;

        SUB_TEST_QUICK();

        mcluster = test_mock_cluster_new(3, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 1, 3);
        rd_kafka_mock_partition_set_leader(mcluster, topic, 0, 1);

        test_conf_init(&conf, NULL, 10);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", topic);
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "false");
        test_conf_set(conf, "fetch.error.backoff.ms", "10");
        test_conf_set(conf, "fetch.wait.max.ms", "10");
        test_conf_set(conf, "fetch.queue.backoff.ms", "10");

        rk = test_create_handle(RD_KAFKA_CONSUMER, conf);

        test_consumer_subscribe(rk, topic);

        /* Produce and consume to leader 1 */
        test_produce_msgs_easy_v(topic, 0, 0, 0, 1, 0, "bootstrap.servers",
                                 bootstraps, NULL);
        test_consumer_poll_exact("read first", rk, 0, 0, 0, 1, rd_true, NULL);

        /* Change leader to 2, Fetch fails, refreshes metadata. */
        rd_kafka_mock_partition_set_leader(mcluster, topic, 0, 2);

        /* Validation fails, metadata refreshed again */
        rd_kafka_mock_broker_push_request_error_rtts(
            mcluster, 2, RD_KAFKAP_OffsetForLeaderEpoch, 1,
            RD_KAFKA_RESP_ERR_KAFKA_STORAGE_ERROR, 1000);

        /* Wait partition migrates to broker 2 */
        rd_usleep(100 * 1000, 0);

        /* Ask to return stale metadata while calling OffsetForLeaderEpoch */
        rd_kafka_mock_start_request_tracking(mcluster);
        for (i = 0; i < 10; i++) {
                rd_kafka_mock_partition_push_leader_response(
                    mcluster, topic, 0, 1 /*leader id*/, 0 /*leader epoch*/);
        }

        /* After the error on OffsetForLeaderEpoch metadata is refreshed
         * and it returns the stale metadata.
         * 1s for the OffsetForLeaderEpoch plus at least 500ms for
         * restarting the fetch requests */
        rd_usleep(2000 * 1000, 0);

        /* Partition doesn't have to migrate back to broker 1 */
        expected_broker_id = 1;
        fetch_requests     = test_mock_wait_matching_requests(
            mcluster, 0, 500, is_fetch_request, &expected_broker_id);
        TEST_ASSERT(fetch_requests == 0,
                    "No fetch request should be received by broker 1, got %d",
                    fetch_requests);
        rd_kafka_mock_stop_request_tracking(mcluster);

        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief A metadata call for an existing topic, just after subscription,
 *        must not cause a UNKNOWN_TOPIC_OR_PART error.
 *        See issue #4589.
 */
static void do_test_metadata_call_before_join(void) {
        rd_kafka_t *rk;
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        const char *topic = test_mk_topic_name(__FUNCTION__, 1);
        rd_kafka_conf_t *conf;
        const struct rd_kafka_metadata *metadata;

        SUB_TEST_QUICK();

        mcluster = test_mock_cluster_new(3, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 1, 3);

        test_conf_init(&conf, NULL, 10);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", topic);

        rk = test_create_handle(RD_KAFKA_CONSUMER, conf);

        test_consumer_subscribe(rk, topic);

        TEST_CALL_ERR__(rd_kafka_metadata(rk, 1, 0, &metadata, 5000));
        rd_kafka_metadata_destroy(metadata);

        test_consumer_poll_no_msgs("no errors", rk, 0, 1000);

        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

typedef struct expected_request_s {
        int16_t api_key;
        int32_t broker;
} expected_request_t;

/**
 * @brief Verify that a request with the expected ApiKey and broker
 *        was sent to the cluster.
 */
rd_bool_t verify_requests_after_metadata_update_operation(
    rd_kafka_mock_cluster_t *mcluster,
    expected_request_t *expected_request) {
        size_t cnt, i;
        rd_kafka_mock_request_t **requests =
            rd_kafka_mock_get_requests(mcluster, &cnt);
        rd_bool_t found = rd_false;

        for (i = 0; i < cnt; i++) {
                int16_t api_key;
                int32_t broker;
                rd_kafka_mock_request_t *request = requests[i];
                api_key = rd_kafka_mock_request_api_key(request);
                broker  = rd_kafka_mock_request_id(request);
                if (api_key == expected_request->api_key &&
                    broker == expected_request->broker) {
                        found = rd_true;
                        break;
                }
        }

        rd_kafka_mock_request_destroy_array(requests, cnt);

        return found;
}

/**
 * @brief A metadata update request should be triggered when a leader change
 *        happens while producing or consuming and cause a migration
 *        to the new leader.
 *
 * @param producer If true, the test will be for a producer, otherwise
 *                 for a consumer.
 * @param second_leader_change If true, a leader change will be triggered
 *                             for two partitions, otherwise for one.
 */
static void do_test_metadata_update_operation(rd_bool_t producer,
                                              rd_bool_t second_leader_change) {
        rd_kafka_t *rk;
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        const char *topic = test_mk_topic_name(__FUNCTION__, 1);
        rd_kafka_conf_t *conf;
        test_timing_t timing;
        rd_bool_t found;
        expected_request_t expected_request = {
            .api_key = producer ? RD_KAFKAP_Produce : RD_KAFKAP_Fetch,
            .broker  = 3};

        SUB_TEST_QUICK("%s, %s", producer ? "producer" : "consumer",
                       second_leader_change ? "two leader changes"
                                            : "single leader change");

        mcluster = test_mock_cluster_new(4, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 2, 4);
        rd_kafka_mock_partition_set_leader(mcluster, topic, 0, 1);
        rd_kafka_mock_partition_set_leader(mcluster, topic, 1, 2);

        test_conf_init(&conf, NULL, 20);
        test_conf_set(conf, "bootstrap.servers", bootstraps);

        if (producer) {
                test_conf_set(conf, "batch.num.messages", "1");
                rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);
                rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

                /* Start producing to leader 1 and 2 */
                test_produce_msgs2(rk, topic, 0, 0, 0, 1, NULL, 0);
                test_produce_msgs2(rk, topic, 0, 1, 0, 1, NULL, 0);
                rd_kafka_flush(rk, 1000);
        } else {
                test_produce_msgs_easy_v(topic, 0, 0, 0, 1, 0,
                                         "bootstrap.servers", bootstraps, NULL);
                test_produce_msgs_easy_v(topic, 0, 1, 0, 1, 0,
                                         "bootstrap.servers", bootstraps, NULL);

                rd_kafka_topic_partition_list_t *assignment;
                test_conf_set(conf, "group.id", topic);
                test_conf_set(conf, "fetch.wait.max.ms", "100");
                test_conf_set(conf, "auto.offset.reset", "earliest");

                rk = test_create_handle(RD_KAFKA_CONSUMER, conf);

                assignment = rd_kafka_topic_partition_list_new(1);
                rd_kafka_topic_partition_list_add(assignment, topic, 0);
                rd_kafka_topic_partition_list_add(assignment, topic, 1);
                test_consumer_assign("2 partitions", rk, assignment);
                rd_kafka_topic_partition_list_destroy(assignment);

                /* Start consuming from leader 1 and 2 */
                test_consumer_poll_timeout("initial leaders", rk, 0, -1, -1, 2,
                                           NULL, 5000);
        }

        TIMING_START(&timing, "Metadata update and partition migration");
        rd_kafka_mock_start_request_tracking(mcluster);
        rd_kafka_mock_partition_set_leader(mcluster, topic, 0, 3);
        if (second_leader_change)
                rd_kafka_mock_partition_set_leader(mcluster, topic, 1, 4);


        if (producer) {
                /* Produce two new messages to the new leaders */
                test_produce_msgs2(rk, topic, 0, 0, 1, 1, NULL, 0);
                test_produce_msgs2(rk, topic, 0, 1, 1, 1, NULL, 0);
                rd_kafka_flush(rk, 1000);
        } else {
                /* Produce two new messages and consume them from
                 * the new leaders */
                test_produce_msgs_easy_v(topic, 0, 0, 0, 1, 0,
                                         "bootstrap.servers", bootstraps, NULL);
                test_produce_msgs_easy_v(topic, 0, 1, 0, 1, 0,
                                         "bootstrap.servers", bootstraps, NULL);
                test_consumer_poll_timeout("changed leaders", rk, 0, -1, -1, 2,
                                           NULL, 5000);
        }
        TIMING_ASSERT_LATER(&timing, 0, 500);

        /* Leader change triggers the metadata update and migration
         * of partition 0 to brokers 3 and with 'second_leader_change' also
         * of partition 1 to broker 4. */
        found = verify_requests_after_metadata_update_operation(
            mcluster, &expected_request);
        if (!found)
                TEST_FAIL(
                    "Requests with ApiKey %s"
                    " were not found on broker %" PRId32,
                    rd_kafka_ApiKey2str(expected_request.api_key),
                    expected_request.broker);

        if (second_leader_change) {
                expected_request.broker = 4;
        } else {
                expected_request.broker = 2;
        }

        found = verify_requests_after_metadata_update_operation(
            mcluster, &expected_request);
        if (!found)
                TEST_FAIL(
                    "Requests with ApiKey %s"
                    " were not found on broker %" PRId32,
                    rd_kafka_ApiKey2str(expected_request.api_key),
                    expected_request.broker);

        rd_kafka_mock_stop_request_tracking(mcluster);
        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);

        TEST_LATER_CHECK();
        SUB_TEST_PASS();
}

/**
 * @brief cluster_id must be copied by its exact wire length,
 *        not via strlen() during DescribeCluster parsing.
 *
 * On the wire cluster_id is immediately followed by the controller_id int32,
 * and cluster_id.str is not nul-terminated, so a strlen()-based copy would
 * over-read into the controller_id bytes when they are non-zero. Forcing
 * controller_id = -1 (all 0xFF bytes) triggers this; the cluster id from
 * DescribeCluster (the parser's per-response copy under test) must equal the
 * one from rd_kafka_clusterid() (a length-based copy).
 */
static void do_test_cluster_id_not_overread(void) {
        rd_kafka_t *rk;
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        char *clusterid;
        rd_kafka_queue_t *q;
        rd_kafka_AdminOptions_t *options;
        rd_kafka_event_t *rkev;
        const rd_kafka_DescribeCluster_result_t *res;
        const char *describe_clusterid;
        char errstr[256];

        SUB_TEST_QUICK();

        mcluster = test_mock_cluster_new(1, &bootstraps);

        /* Force controller_id to -1 (0xFFFFFFFF) so the byte following the
         * cluster_id on the wire is non-zero */
        rd_kafka_mock_set_controller_id(mcluster, -1);

        test_conf_init(&conf, NULL, 10);
        test_conf_set(conf, "bootstrap.servers", bootstraps);

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        /* Authoritative cluster id: rk_clusterid uses a length-based copy
         * (RD_KAFKAP_STR_DUP) */
        clusterid = rd_kafka_clusterid(rk, tmout_multip(5000));
        TEST_ASSERT(clusterid, "Expected to retrieve clusterid");

        /* DescribeCluster surfaces the internal per-response cluster_id copy */
        q = rd_kafka_queue_new(rk);
        options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_DESCRIBECLUSTER);
        /* DescribeCluster defaults to routing to the controller; since we set
         * controller_id = -1 (no controller), target broker 1 explicitly so
         * the request is served and the response parsed. */
        TEST_CALL_ERR__(rd_kafka_AdminOptions_set_broker(options, 1, errstr,
                                                         sizeof(errstr)));
        rd_kafka_DescribeCluster(rk, options, q);
        rd_kafka_AdminOptions_destroy(options);

        rkev = test_wait_admin_result(q, RD_KAFKA_EVENT_DESCRIBECLUSTER_RESULT,
                                      tmout_multip(10 * 1000));
        TEST_ASSERT(rkev, "Expected DescribeCluster result event");
        TEST_ASSERT(!rd_kafka_event_error(rkev), "DescribeCluster failed: %s",
                    rd_kafka_event_error_string(rkev));

        res                = rd_kafka_event_DescribeCluster_result(rkev);
        describe_clusterid = rd_kafka_DescribeCluster_result_cluster_id(res);

        TEST_ASSERT(describe_clusterid &&
                        !strcmp(describe_clusterid, clusterid),
                    "DescribeCluster cluster id \"%s\" (len %d) does not match "
                    "expected \"%s\" (len %d): cluster_id was over-read",
                    describe_clusterid ? describe_clusterid : "(null)",
                    describe_clusterid ? (int)strlen(describe_clusterid) : -1,
                    clusterid, (int)strlen(clusterid));

        rd_kafka_event_destroy(rkev);
        rd_kafka_queue_destroy(q);
        rd_kafka_mem_free(rk, clusterid);
        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief Waits for \p rk to raise a fatal error and returns its code,
 *        or RD_KAFKA_RESP_ERR_NO_ERROR when none came within \p timeout_ms.
 */
static rd_kafka_resp_err_t population_wait_fatal(rd_kafka_t *rk,
                                                 int timeout_ms) {
        rd_kafka_resp_err_t err;
        char errstr[512];
        test_timing_t t;

        TIMING_START(&t, "wait for fatal error");
        while (!(err = rd_kafka_fatal_error(rk, errstr, sizeof(errstr))) &&
               TIMING_DURATION(&t) < (int64_t)timeout_ms * 1000)
                rd_kafka_poll(rk, 100);
        TIMING_STOP(&t);
        if (err)
                TEST_SAY("Fatal error: %s: %s\n", rd_kafka_err2name(err),
                         errstr);
        return err;
}

/**
 * @brief broker.population.max counts the bootstrap broker, the brokers
 *        learned from metadata and the logical group coordinator, and
 *        refuses the broker past it with a typed fatal error. Unset, all
 *        brokers are kept.
 *
 * The cluster has 3 brokers and the client one bootstrap broker, decommissioned
 * only after the learned brokers are added: a producer holds 4 handles at
 * once, a consumer in a group 5.
 */
static void do_test_broker_population_max(int role, int max) {
        rd_kafka_t *rk;
        const char *bootstraps;
        char bootstrap[64];
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        const rd_kafka_metadata_t *md;
        int32_t *ids;
        size_t cnt;
        char max_str[16];
        int needed = 1 + 3 + (role ? 1 : 0);
        rd_kafka_resp_err_t err;

        SUB_TEST_QUICK("%s, broker.population.max %d",
                       role == 1 ? "consumer in a group" : role == 2 ? "transactional producer" : "producer", max);

        mcluster = test_mock_cluster_new(3, &bootstraps);
        rd_snprintf(bootstrap, sizeof(bootstrap), "%.*s",
                    (int)strcspn(bootstraps, ","), bootstraps);

        test_conf_init(&conf, NULL, 10);
        test_conf_set(conf, "bootstrap.servers", bootstrap);
        rd_snprintf(max_str, sizeof(max_str), "%d", max);
        test_conf_set(conf, "broker.population.max", max_str);
        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;

        if (role == 2)
                test_conf_set(conf, "transactional.id", "population");
        if (role == 1) {
                test_conf_set(conf, "group.id", "population");
                rk = test_create_handle(RD_KAFKA_CONSUMER, conf);
        } else
                rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        err = rd_kafka_metadata(rk, 0, NULL, &md, tmout_multip(5000));
        if (!err)
                rd_kafka_metadata_destroy(md);

        ids = rd_kafka_brokers_learned_ids(rk, &cnt);
        rd_free(ids);

        if (max == 0 || max >= needed) {
                TEST_ASSERT(!err, "metadata: %s", rd_kafka_err2str(err));
                TEST_ASSERT(cnt == 3, "expected 3 learned brokers, not %d",
                            (int)cnt);
                err = population_wait_fatal(rk, 1000);
                TEST_ASSERT(!err, "expected no fatal error, not %s",
                            rd_kafka_err2name(err));
                /* Drive the logical coordinator's real FindCoordinator path. */
                if (role == 2) {
                        rd_kafka_error_t *error =
                            rd_kafka_init_transactions(rk, tmout_multip(5000));
                        TEST_ASSERT(!error, "init transactions: %s",
                                    error ? rd_kafka_error_string(error) : "");
                } else if (role == 1) {
                        rd_kafka_topic_partition_list_t *subscription =
                            rd_kafka_topic_partition_list_new(1);
                        rd_kafka_topic_partition_list_add(subscription,
                                                         "population", -1);
                        TEST_CALL_ERR__(rd_kafka_subscribe(rk, subscription));
                        rd_kafka_topic_partition_list_destroy(subscription);
                        TEST_CALL_ERR__(rd_kafka_mock_topic_create(
                            mcluster, "population", 1, 1));
                        test_consumer_wait_assignment(rk, rd_true);
                }

        } else {
                err = population_wait_fatal(rk, tmout_multip(5000));
                TEST_ASSERT(err == RD_KAFKA_RESP_ERR__BROKER_POPULATION,
                            "expected fatal error %s, not %s",
                            rd_kafka_err2name(
                                RD_KAFKA_RESP_ERR__BROKER_POPULATION),
                            rd_kafka_err2name(err));
                /* A learned broker can still be added once the
                 * bootstrap broker it overlapped is decommissioned,
                 * within the limit. */
                TEST_ASSERT((int)cnt <= max - (needed - 4),
                            "expected at most %d learned brokers, not %d",
                            max - (needed - 4), (int)cnt);
        }

        rd_kafka_destroy(rk);
        test_curr->is_fatal_cb = NULL;
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief partition.population.max refuses metadata that would grow the
 *        partitions past it with a typed fatal error; a topic that fits
 *        exactly, its UA handle and 10 partitions, is produced to.
 */
static void do_test_partition_population_max(int max) {
        rd_kafka_t *rk;
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        const char *topic = test_mk_topic_name(__FUNCTION__, 1);
        char max_str[16];
        rd_kafka_resp_err_t err;

        SUB_TEST_QUICK("partition.population.max %d, 10 partitions", max);

        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 10, 1);

        test_conf_init(&conf, NULL, 10);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        rd_snprintf(max_str, sizeof(max_str), "%d", max);
        test_conf_set(conf, "partition.population.max", max_str);
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);
        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        TEST_CALL_ERR__(rd_kafka_producev(rk, RD_KAFKA_V_TOPIC(topic),
                                          RD_KAFKA_V_VALUE("v", 1),
                                          RD_KAFKA_V_END));

        if (max == 0 || max >= 11) {
                TEST_CALL_ERR__(rd_kafka_flush(rk, tmout_multip(5000)));
                err = population_wait_fatal(rk, 1000);
                TEST_ASSERT(!err, "expected no fatal error, not %s",
                            rd_kafka_err2name(err));
        } else {
                /* The fatal error fails the queued message. */
                test_curr->ignore_dr_err = rd_true;
                err = population_wait_fatal(rk, tmout_multip(5000));
                TEST_ASSERT(err == RD_KAFKA_RESP_ERR__PARTITION_POPULATION,
                            "expected fatal error %s, not %s",
                            rd_kafka_err2name(
                                RD_KAFKA_RESP_ERR__PARTITION_POPULATION),
                            rd_kafka_err2name(err));
        }

        rd_kafka_destroy(rk);
        test_curr->ignore_dr_err = rd_false;
        test_curr->is_fatal_cb   = NULL;
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}

/**
 * @brief Every path that creates a partition handle is admitted by
 *        partition.population.max before it allocates: a refused topic,
 *        desired partition or assignment fails with
 *        ERR__PARTITION_POPULATION and raises it as the fatal error.
 */
static void do_test_partition_population_paths(void) {
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        rd_kafka_topic_t *rkt;
        rd_kafka_topic_partition_list_t *parts;
        rd_kafka_resp_err_t err;
        char errstr[512];

        SUB_TEST_QUICK();

        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;

        /* Assignment: the topic's UA handle and two partitions do not fit
         * in 2. The first pass can retain its admitted local handles. */
        test_conf_init(&conf, NULL, 10);
        test_conf_set(conf, "partition.population.max", "2");
        test_conf_set(conf, "group.id", "population");
        test_conf_set(conf, "enable.auto.commit", "false");
        rk    = test_create_handle(RD_KAFKA_CONSUMER, conf);
        parts = rd_kafka_topic_partition_list_new(2);
        rd_kafka_topic_partition_list_add(parts, "assigned", 0)->offset = 0;
        rd_kafka_topic_partition_list_add(parts, "assigned", 1)->offset = 0;
        err = rd_kafka_assign(rk, parts);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__PARTITION_POPULATION,
                    "assign: expected %s, not %s",
                    rd_kafka_err2name(RD_KAFKA_RESP_ERR__PARTITION_POPULATION),
                    rd_kafka_err2name(err));
        TEST_ASSERT(rd_atomic32_get(&rk->rk_partition_cnt) <= 2,
                    "assign: population exceeds 2: %d",
                    rd_atomic32_get(&rk->rk_partition_cnt));
        TEST_ASSERT(rk->rk_consumer.assignment.all->cnt == 0,
                    "refused first pass must not mark an assignment");
        err = rd_kafka_fatal_error(rk, errstr, sizeof(errstr));
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__PARTITION_POPULATION,
                    "assign: expected fatal %s, not %s",
                    rd_kafka_err2name(RD_KAFKA_RESP_ERR__PARTITION_POPULATION),
                    rd_kafka_err2name(err));
        rd_kafka_topic_partition_list_destroy(parts);
        rd_kafka_destroy_flags(rk, RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);

        /* The same assignment fits in 3: every handle is counted. */
        test_conf_init(&conf, NULL, 10);
        test_conf_set(conf, "partition.population.max", "3");
        test_conf_set(conf, "group.id", "population");
        test_conf_set(conf, "enable.auto.commit", "false");
        rk    = test_create_handle(RD_KAFKA_CONSUMER, conf);
        parts = rd_kafka_topic_partition_list_new(2);
        rd_kafka_topic_partition_list_add(parts, "assigned", 0)->offset = 0;
        rd_kafka_topic_partition_list_add(parts, "assigned", 1)->offset = 0;
        TEST_CALL_ERR__(rd_kafka_assign(rk, parts));
        TEST_ASSERT(rd_atomic32_get(&rk->rk_partition_cnt) == 3,
                    "assign: expected 3 handles, not %d",
                    rd_atomic32_get(&rk->rk_partition_cnt));
        TEST_CALL_ERR__(rd_kafka_assign(rk, NULL));
        rd_kafka_topic_partition_list_destroy(parts);
        rd_kafka_destroy_flags(rk, RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);

        /* Topic: each topic's UA handle counts; producing to a second topic
         * and then creating a third are refused. */
        test_conf_init(&conf, NULL, 10);
        test_conf_set(conf, "partition.population.max", "1");
        rk  = test_create_handle(RD_KAFKA_PRODUCER, conf);
        rkt = rd_kafka_topic_new(rk, "first", NULL);
        TEST_ASSERT(rkt, "first topic: %s",
                    rd_kafka_err2name(rd_kafka_last_error()));
        err = rd_kafka_producev(rk, RD_KAFKA_V_TOPIC("second"),
                                RD_KAFKA_V_VALUE("v", 1), RD_KAFKA_V_END);
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__PARTITION_POPULATION,
                    "producev: expected %s, not %s",
                    rd_kafka_err2name(RD_KAFKA_RESP_ERR__PARTITION_POPULATION),
                    rd_kafka_err2name(err));
        TEST_ASSERT(!rd_kafka_topic_new(rk, "third", NULL),
                    "third topic: expected refusal");
        TEST_ASSERT(rd_kafka_last_error() ==
                        RD_KAFKA_RESP_ERR__PARTITION_POPULATION,
                    "third topic: expected %s, not %s",
                    rd_kafka_err2name(RD_KAFKA_RESP_ERR__PARTITION_POPULATION),
                    rd_kafka_err2name(rd_kafka_last_error()));
        TEST_ASSERT(rk->rk_topic_cnt == 1 &&
                        rd_atomic32_get(&rk->rk_partition_cnt) == 1,
                    "third topic: expected 1 topic and 1 handle, not %d "
                    "and %d",
                    rk->rk_topic_cnt, rd_atomic32_get(&rk->rk_partition_cnt));
        err = rd_kafka_fatal_error(rk, errstr, sizeof(errstr));
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__PARTITION_POPULATION,
                    "topic: expected fatal %s, not %s",
                    rd_kafka_err2name(RD_KAFKA_RESP_ERR__PARTITION_POPULATION),
                    rd_kafka_err2name(err));
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);

        /* Desired partition: a simple consumer's start is refused. */
        test_conf_init(&conf, NULL, 10);
        test_conf_set(conf, "partition.population.max", "1");
        rk  = test_create_handle(RD_KAFKA_CONSUMER, conf);
        rkt = rd_kafka_topic_new(rk, "desired", NULL);
        TEST_ASSERT(rkt, "desired topic: %s",
                    rd_kafka_err2name(rd_kafka_last_error()));
        TEST_ASSERT(rd_kafka_consume_start(rkt, 0, RD_KAFKA_OFFSET_BEGINNING) ==
                            -1 &&
                        rd_kafka_last_error() ==
                            RD_KAFKA_RESP_ERR__PARTITION_POPULATION,
                    "consume_start: expected %s, not %s",
                    rd_kafka_err2name(RD_KAFKA_RESP_ERR__PARTITION_POPULATION),
                    rd_kafka_err2name(rd_kafka_last_error()));
        TEST_ASSERT(rd_atomic32_get(&rk->rk_partition_cnt) == 1,
                    "consume_start: expected 1 handle, not %d",
                    rd_atomic32_get(&rk->rk_partition_cnt));
        err = rd_kafka_fatal_error(rk, errstr, sizeof(errstr));
        TEST_ASSERT(err == RD_KAFKA_RESP_ERR__PARTITION_POPULATION,
                    "consume_start: expected fatal %s, not %s",
                    rd_kafka_err2name(RD_KAFKA_RESP_ERR__PARTITION_POPULATION),
                    rd_kafka_err2name(err));
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);

        test_curr->is_fatal_cb = NULL;

        SUB_TEST_PASS();
}

/* Thread-exit interceptors run after list removal and before final broker
 * destruction. Hold the actual bootstrap broker there while adding another. */
struct population_exit_gate {
        mtx_t lock;
        cnd_t changed;
        int entered;
        int released;
};
static rd_kafka_resp_err_t population_thread_exit(
    rd_kafka_t *rk, rd_kafka_thread_type_t type, const char *name, void *opaque) {
        struct population_exit_gate *gate = opaque;
        if (type != RD_KAFKA_THREAD_BROKER || !strstr(name, "bootstrap"))
                return RD_KAFKA_RESP_ERR_NO_ERROR;
        mtx_lock(&gate->lock);
        gate->entered = 1;
        cnd_broadcast(&gate->changed);
        while (!gate->released)
                cnd_wait(&gate->changed, &gate->lock);
        mtx_unlock(&gate->lock);
        return RD_KAFKA_RESP_ERR_NO_ERROR;
}
static rd_kafka_resp_err_t population_on_new(
    rd_kafka_t *rk, const rd_kafka_conf_t *conf, void *opaque,
    char *errstr, size_t errstr_size) {
        return rd_kafka_interceptor_add_on_thread_exit(
            rk, "population final destruction", population_thread_exit, opaque);
}
static void do_test_population_retained_objects(void) {
        struct population_exit_gate gate = {0};
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_kafka_mock_cluster_t *cluster;
        const char *bootstraps;
        const rd_kafka_metadata_t *md;
        int added, held, entered;
        int64_t deadline;
        rd_kafka_resp_err_t fatal;
        char reason[512];
        SUB_TEST_QUICK();
        mtx_init(&gate.lock, mtx_plain);
        cnd_init(&gate.changed);
        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;
        cluster = test_mock_cluster_new(1, &bootstraps);
        test_conf_init(&conf, NULL, 10);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "broker.population.max", "2");
        rd_kafka_conf_interceptor_add_on_new(conf, "population gate", population_on_new, &gate);
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);
        if (!rd_kafka_metadata(rk, 0, NULL, &md, tmout_multip(5000)))
                rd_kafka_metadata_destroy(md);
        deadline = test_clock() + tmout_multip(5000) * 1000LL;
        mtx_lock(&gate.lock);
        while (!gate.entered && test_clock() < deadline)
                cnd_timedwait_ms(&gate.changed, &gate.lock, 100);
        entered = gate.entered;
        mtx_unlock(&gate.lock);
        held = rd_atomic32_get(&rk->rk_broker_population);
        added = rd_kafka_brokers_add(rk, "127.0.0.1:1");
        fatal = rd_kafka_fatal_error(rk, reason, sizeof(reason));
        mtx_lock(&gate.lock);
        gate.released = 1;
        cnd_broadcast(&gate.changed);
        mtx_unlock(&gate.lock);
        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(cluster);
        cnd_destroy(&gate.changed);
        mtx_destroy(&gate.lock);
        TEST_ASSERT(entered, "bootstrap broker did not reach final-destruction gate");
        TEST_ASSERT(held == 2, "list removal refunded a live broker: %d", held);
        TEST_ASSERT(added == 0 && fatal == RD_KAFKA_RESP_ERR__BROKER_POPULATION,
                    "replacement must refuse while old object lives: added %d, %s", added, rd_kafka_err2name(fatal));

        /* Keep a desired handle past removal from its topic's desired list. */
        {
                rd_kafka_toppar_t *old, *replacement;
                rd_kafka_topic_t *topic;
                test_conf_init(&conf, NULL, 10);
                test_conf_set(conf, "partition.population.max", "2");
                rk = test_create_handle(RD_KAFKA_PRODUCER, conf);
                old = rd_kafka_toppar_get2(rk, "retained", 0, 0, 1);
                TEST_ASSERT(old, "create original desired partition");
                topic = old->rktp_rkt;
                rd_kafka_topic_wrlock(topic);
                rd_kafka_toppar_lock(old);
                rd_kafka_toppar_desired_del(old);
                rd_kafka_toppar_unlock(old);
                rd_kafka_topic_wrunlock(topic);
                replacement = rd_kafka_toppar_get2(rk, "retained", 1, 0, 1);
                TEST_ASSERT(!replacement, "removed but retained handle still consumes its place");
                TEST_ASSERT(rd_atomic32_get(&rk->rk_partition_cnt) == 2, "UA and retained partition stay admitted");
                rd_kafka_toppar_destroy(old);
                rd_kafka_destroy(rk);
        }
        test_curr->is_fatal_cb = NULL;
        SUB_TEST_PASS();
}

int main_0146_metadata_mock(int argc, char **argv) {
        TEST_SKIP_MOCK_CLUSTER(0);
        int variation;

        /* No need to test the "roundrobin" assignor case,
         * as this is just for checking the two code paths:
         * EAGER or COOPERATIVE one, and "range" is EAGER too. */
        do_test_metadata_persists_in_cache("range");
        do_test_metadata_persists_in_cache("cooperative-sticky");

        do_test_metadata_call_before_join();

        do_test_fast_metadata_refresh(0);
        do_test_fast_metadata_refresh(1);

        do_test_stale_metadata_doesnt_migrate_partition();

        do_test_cluster_id_not_overread();

        do_test_broker_population_max(rd_false, 0);
        do_test_broker_population_max(rd_false, 4);
        do_test_broker_population_max(rd_false, 3);
        do_test_broker_population_max(rd_true, 5);
        do_test_broker_population_max(rd_true, 4);
        do_test_broker_population_max(2, 5);
        do_test_broker_population_max(2, 4);

        do_test_partition_population_max(0);
        do_test_partition_population_max(11);
        do_test_partition_population_max(10);
        do_test_partition_population_paths();
        do_test_population_retained_objects();

        for (variation = 0; variation < 4; variation++) {
                do_test_metadata_update_operation(
                        variation / 2, /* 0-1: consumer, 2-3 producer */
                        variation % 2  /* 1-3: second leader change,
                                        * 0-2: single leader change */);
        }

        return 0;
}
