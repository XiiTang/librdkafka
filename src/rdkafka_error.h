/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2020-2022, Magnus Edenhill
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


#ifndef _RDKAFKA_ERROR_H_
#define _RDKAFKA_ERROR_H_

#include <stdarg.h>

typedef struct rd_kafka_broker_s rd_kafka_broker_t;

/**
 * @name Public API complex error type implementation.
 *
 */

struct rd_kafka_error_s {
        rd_kafka_broker_reply_t *reply; /**< Owned correlated wire result. */
        rd_kafka_resp_err_t code; /**< Error code. */
        char *errstr;             /**< Human readable error string, allocated
                                   *   with the rd_kafka_error_s struct
                                   *   after the struct.
                                   *   Possibly NULL. */
        rd_bool_t fatal;          /**< This error is a fatal error. */
        rd_bool_t retriable;      /**< Operation is retriable. */
        rd_bool_t
            txn_requires_abort; /**< This is an abortable transaction error.*/
};


/* Decoder-only construction. Strings are copied with their decoded lengths. */
rd_kafka_broker_reply_t *rd_kafka_broker_reply_new(
    rd_kafka_broker_t *rkb, const rd_kafka_buf_t *request, int16_t code,
    rd_kafka_reply_target_t target, const char *name, size_t name_size,
    int32_t partition, int8_t resource_type,
    const char *explanation, size_t explanation_size);
rd_kafka_broker_reply_t *
rd_kafka_broker_reply_copy(const rd_kafka_broker_reply_t *reply);
void *rd_kafka_broker_reply_copy_opaque(const void *reply, void *opaque);

rd_kafka_error_t *rd_kafka_error_new_with_reply(
    rd_kafka_resp_err_t code, const rd_kafka_broker_reply_t *reply,
    const char *fmt, ...) RD_FORMAT(printf, 3, 4);

rd_kafka_error_t *
rd_kafka_error_new_v(rd_kafka_resp_err_t code, const char *fmt, va_list ap);

rd_kafka_error_t *rd_kafka_error_copy(const rd_kafka_error_t *src);

void *rd_kafka_error_copy_opaque(const void *error, void *opaque);

void rd_kafka_error_set_fatal(rd_kafka_error_t *error);
void rd_kafka_error_set_retriable(rd_kafka_error_t *error);
void rd_kafka_error_set_txn_requires_abort(rd_kafka_error_t *error);


rd_kafka_error_t *rd_kafka_error_new_fatal(rd_kafka_resp_err_t code,
                                           const char *fmt,
                                           ...) RD_FORMAT(printf, 2, 3);
rd_kafka_error_t *rd_kafka_error_new_retriable(rd_kafka_resp_err_t code,
                                               const char *fmt,
                                               ...) RD_FORMAT(printf, 2, 3);
rd_kafka_error_t *
rd_kafka_error_new_txn_requires_abort(rd_kafka_resp_err_t code,
                                      const char *fmt,
                                      ...) RD_FORMAT(printf, 2, 3);


rd_kafka_resp_err_t rd_kafka_error_to_legacy(rd_kafka_error_t *error,
                                             char *errstr,
                                             size_t errstr_size);
size_t rd_kafka_broker_reply_size(const rd_kafka_broker_reply_t *);
rd_kafka_broker_reply_t *rd_kafka_broker_reply_write(
    rd_tmpabuf_t *, const rd_kafka_broker_reply_t *);

#endif /* _RDKAFKA_ERROR_H_ */
