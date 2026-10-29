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


/**
 * @name Public API complex error type implementation.
 *
 */

#include "rdkafka_int.h"
#include "rdkafka_error.h"
#include "rdkafka_broker.h"

#include <stdarg.h>


void rd_kafka_error_destroy(rd_kafka_error_t *error) {
        if (error) {
                rd_free(error->reply);
                rd_free(error);
        }
}


/**
 * @brief Creates a new error object using the optional va-args format list.
 */
rd_kafka_error_t *
rd_kafka_error_new_v(rd_kafka_resp_err_t code, const char *fmt, va_list ap) {
        rd_kafka_error_t *error;
        ssize_t strsz = 0;

        if (fmt && *fmt) {
                va_list ap2;
                va_copy(ap2, ap);
                strsz = rd_vsnprintf(NULL, 0, fmt, ap2) + 1;
                va_end(ap2);
        }

        error                     = rd_malloc(sizeof(*error) + strsz);
        error->reply              = NULL;
        error->code               = code;
        error->fatal              = rd_false;
        error->retriable          = rd_false;
        error->txn_requires_abort = rd_false;

        if (strsz > 0) {
                error->errstr = (char *)(error + 1);
                rd_vsnprintf(error->errstr, strsz, fmt, ap);
        } else {
                error->errstr = NULL;
        }

        return error;
}

rd_kafka_error_t *rd_kafka_error_copy(const rd_kafka_error_t *src) {
        rd_kafka_error_t *error;
        ssize_t strsz = 0;

        if (src->errstr) {
                strsz = strlen(src->errstr) + 1;
        }

        error                     = rd_malloc(sizeof(*error) + strsz);
        error->reply              = rd_kafka_broker_reply_copy(src->reply);
        error->code               = src->code;
        error->fatal              = src->fatal;
        error->retriable          = src->retriable;
        error->txn_requires_abort = src->txn_requires_abort;

        if (strsz > 0) {
                error->errstr = (char *)(error + 1);
                rd_strlcpy(error->errstr, src->errstr, strsz);
        } else {
                error->errstr = NULL;
        }

        return error;
}

/**
 * @brief Same as rd_kafka_error_copy() but suitable for
 *        rd_list_copy(). The \p opaque is ignored.
 */
void *rd_kafka_error_copy_opaque(const void *error, void *opaque) {
        return rd_kafka_error_copy(error);
}


rd_kafka_error_t *
rd_kafka_error_new(rd_kafka_resp_err_t code, const char *fmt, ...) {
        rd_kafka_error_t *error;
        va_list ap;

        va_start(ap, fmt);
        error = rd_kafka_error_new_v(code, fmt, ap);
        va_end(ap);

        return error;
}

rd_kafka_error_t *
rd_kafka_error_new_fatal(rd_kafka_resp_err_t code, const char *fmt, ...) {
        rd_kafka_error_t *error;
        va_list ap;

        va_start(ap, fmt);
        error = rd_kafka_error_new_v(code, fmt, ap);
        va_end(ap);

        rd_kafka_error_set_fatal(error);

        return error;
}

rd_kafka_error_t *
rd_kafka_error_new_retriable(rd_kafka_resp_err_t code, const char *fmt, ...) {
        rd_kafka_error_t *error;
        va_list ap;

        va_start(ap, fmt);
        error = rd_kafka_error_new_v(code, fmt, ap);
        va_end(ap);

        rd_kafka_error_set_retriable(error);

        return error;
}

rd_kafka_error_t *
rd_kafka_error_new_txn_requires_abort(rd_kafka_resp_err_t code,
                                      const char *fmt,
                                      ...) {
        rd_kafka_error_t *error;
        va_list ap;

        va_start(ap, fmt);
        error = rd_kafka_error_new_v(code, fmt, ap);
        va_end(ap);

        rd_kafka_error_set_txn_requires_abort(error);

        return error;
}


rd_kafka_resp_err_t rd_kafka_error_code(const rd_kafka_error_t *error) {
        return error ? error->code : RD_KAFKA_RESP_ERR_NO_ERROR;
}

const char *rd_kafka_error_name(const rd_kafka_error_t *error) {
        return error ? rd_kafka_err2name(error->code) : "";
}

const char *rd_kafka_error_string(const rd_kafka_error_t *error) {
        if (!error)
                return "";
        return error->errstr ? error->errstr : rd_kafka_err2str(error->code);
}

int rd_kafka_error_is_fatal(const rd_kafka_error_t *error) {
        return error && error->fatal ? 1 : 0;
}

int rd_kafka_error_is_retriable(const rd_kafka_error_t *error) {
        return error && error->retriable ? 1 : 0;
}

int rd_kafka_error_txn_requires_abort(const rd_kafka_error_t *error) {
        return error && error->txn_requires_abort ? 1 : 0;
}



void rd_kafka_error_set_fatal(rd_kafka_error_t *error) {
        error->fatal = rd_true;
}

void rd_kafka_error_set_retriable(rd_kafka_error_t *error) {
        error->retriable = rd_true;
}

void rd_kafka_error_set_txn_requires_abort(rd_kafka_error_t *error) {
        error->txn_requires_abort = rd_true;
}


/**
 * @brief Converts a new style error_t error to the legacy style
 *        resp_err_t code and separate error string, then
 *        destroys the the error object.
 *
 * @remark The \p error object is destroyed.
 */
rd_kafka_resp_err_t rd_kafka_error_to_legacy(rd_kafka_error_t *error,
                                             char *errstr,
                                             size_t errstr_size) {
        rd_kafka_resp_err_t err = error->code;

        rd_snprintf(errstr, errstr_size, "%s", rd_kafka_error_string(error));

        rd_kafka_error_destroy(error);

        return err;
}

/**@}*/

/* Projection plus length-bearing text, usable in an owner's contiguous backing. */
size_t rd_kafka_broker_reply_size(const rd_kafka_broker_reply_t *src) {
        return src ? sizeof(*src) + strlen(src->peer) + 1 + src->name_size + 1 +
                         src->explanation_size + 1 : 0;
}
static rd_kafka_broker_reply_t *rd_kafka_broker_reply_copy_into(
    rd_kafka_broker_reply_t *dst, const rd_kafka_broker_reply_t *src) {
        char *p = (char *)(dst + 1);
        size_t peer_size = strlen(src->peer) + 1;
        *dst = *src;
        dst->peer = p;
        memcpy(p, src->peer, peer_size);
        p += peer_size;
        dst->name = p;
        if (src->name_size)
                memcpy(p, src->name, src->name_size);
        p[src->name_size] = '\0';
        p += src->name_size + 1;
        dst->explanation = src->explanation_size ? p : NULL;
        if (src->explanation_size)
                memcpy(p, src->explanation, src->explanation_size);
        p[src->explanation_size] = '\0';
        return dst;
}
rd_kafka_broker_reply_t *rd_kafka_broker_reply_copy(const rd_kafka_broker_reply_t *src) {
        return src ? rd_kafka_broker_reply_copy_into(
            rd_malloc(rd_kafka_broker_reply_size(src)), src) : NULL;
}
rd_kafka_broker_reply_t *rd_kafka_broker_reply_write(
    rd_tmpabuf_t *tbuf, const rd_kafka_broker_reply_t *src) {
        return src ? rd_kafka_broker_reply_copy_into(
            rd_tmpabuf_alloc(tbuf, rd_kafka_broker_reply_size(src)), src) : NULL;
}

rd_kafka_broker_reply_t *rd_kafka_broker_reply_new(
    rd_kafka_broker_t *rkb, const rd_kafka_buf_t *request, int16_t code,
    rd_kafka_reply_target_t target, const char *name, size_t name_size,
    int32_t partition, int8_t resource_type,
    const char *explanation, size_t explanation_size) {
        rd_kafka_broker_reply_t src = RD_ZERO_INIT;
        rd_kafka_broker_reply_t *dst;
        src.api_key = request->rkbuf_reqhdr.ApiKey;
        src.api_version = request->rkbuf_reqhdr.ApiVersion;
        src.code = code;
        src.target = target;
        src.name = name;
        src.name_size = name_size;
        src.partition = partition;
        src.resource_type = resource_type;
        src.explanation = explanation;
        src.explanation_size = explanation && explanation_size ?
            explanation_size : 0;
        rd_kafka_broker_lock(rkb);
        src.broker_id = rkb->rkb_nodeid;
        src.peer = rkb->rkb_nodename;
        dst = rd_kafka_broker_reply_copy(&src);
        rd_kafka_broker_unlock(rkb);
        return dst;
}

const rd_kafka_broker_reply_t *
rd_kafka_error_broker_reply(const rd_kafka_error_t *error) {
        return error ? error->reply : NULL;
}

void *rd_kafka_broker_reply_copy_opaque(const void *reply, void *opaque) {
        return rd_kafka_broker_reply_copy(reply);
}

rd_kafka_error_t *rd_kafka_error_new_with_reply(
    rd_kafka_resp_err_t code, const rd_kafka_broker_reply_t *reply,
    const char *fmt, ...) {
        va_list ap;
        rd_kafka_error_t *error;
        va_start(ap, fmt);
        error = rd_kafka_error_new_v(code, fmt, ap);
        va_end(ap);
        error->reply = rd_kafka_broker_reply_copy(reply);
        return error;
}
