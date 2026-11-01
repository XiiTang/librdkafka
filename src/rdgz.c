/*
 * librd - Rapid Development C library
 *
 * Copyright (c) 2012-2022, Magnus Edenhill
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

#include "rd.h"
#include "rdgz.h"

#include <zlib.h>


#define RD_GZ_CHUNK 262144

void *rd_gz_decompress_bounded(const void *compressed,
                       int compressed_len,
                       uint64_t *decompressed_lenp, uint64_t maximum) {
        int pass           = 1;
        char *decompressed = NULL;

        /* First pass (1): calculate decompressed size.
         * Second pass (2): perform actual decompression.
         *
         * Both passes read every gzip member (RFC 1952 2.2) to its end and
         * require all input to be consumed: input ending inside a member,
         * or bytes after a member that do not start another one, fail the
         * call, as does output beyond \p maximum for all members together
         * (Boundless #442). */

        *decompressed_lenp = 0;

        for (; pass <= 2; pass++) {
                z_stream strm = RD_ZERO_INIT;
                char buf[512];
                char *p      = decompressed;
                /* Pass 2: room left in the buffer pass 1 sized. */
                uint64_t len = *decompressed_lenp;
                /* Output of all members so far: inflateReset2() clears
                 * strm.total_out at each member. */
                uint64_t total = 0;
                uInt room;
                int r;

                if ((r = inflateInit2(&strm, 15 + 32)) != Z_OK)
                        goto fail;

                strm.next_in  = (void *)compressed;
                strm.avail_in = compressed_len;

                for (;;) {
                        if (pass == 1) {
                                /* Use dummy output buffer */
                                strm.next_out = (unsigned char *)buf;
                                room          = sizeof(buf);
                        } else {
                                /* Use real output buffer */
                                strm.next_out = (unsigned char *)p;
                                room = (uInt)RD_MIN(len, (uint64_t)UINT_MAX);
                        }
                        strm.avail_out = room;

                        r = inflate(&strm, Z_NO_FLUSH);

                        total += room - strm.avail_out;
                        if (pass == 2) {
                                /* Advance output pointer (in pass 2). */
                                p += room - strm.avail_out;
                                len -= room - strm.avail_out;
                        }
                        if (total > maximum) {
                                inflateEnd(&strm);
                                goto fail;
                        }

                        if (r == Z_STREAM_END) {
                                if (strm.avail_in == 0)
                                        break; /* Last member complete */
                                /* Another member follows: gzip only. */
                                if (inflateReset2(&strm, 15 + 16) != Z_OK) {
                                        inflateEnd(&strm);
                                        goto fail;
                                }
                                continue;
                        }

                        /* Z_BUF_ERROR: no progress is possible, as the input
                         * ended inside a member or pass 2 would produce more
                         * than pass 1 counted. Any other code is an error. */
                        if (r != Z_OK) {
                                inflateEnd(&strm);
                                goto fail;
                        }
                }


                if (pass == 1) {
                        *decompressed_lenp = total;
                        if (!(decompressed = rd_malloc(
                                  (size_t)(*decompressed_lenp) + 1))) {
                                inflateEnd(&strm);
                                return NULL;
                        }
                        /* For convenience of the caller we nul-terminate
                         * the buffer. If it happens to be a string there
                         * is no need for extra copies. */
                        decompressed[*decompressed_lenp] = '\0';
                }

                inflateEnd(&strm);
        }

        return decompressed;

fail:
        if (decompressed)
                rd_free(decompressed);
        return NULL;
}

void *rd_gz_decompress(const void *input, int length, uint64_t *output_length) { return rd_gz_decompress_bounded(input,length,output_length,INT_MAX); }



/**
 * @name Unit tests (Boundless #442)
 * @{
 */
#include "rdunittest.h"

/** @brief Compresses \p in as one gzip member into \p out. */
static size_t ut_gz_member(const char *in, unsigned char *out, size_t size) {
        z_stream strm = RD_ZERO_INIT;
        size_t len;
        int r = deflateInit2(&strm, 6, Z_DEFLATED, 15 + 16, 8,
                             Z_DEFAULT_STRATEGY);
        rd_assert(r == Z_OK);
        strm.next_in   = (unsigned char *)in;
        strm.avail_in  = (uInt)strlen(in);
        strm.next_out  = out;
        strm.avail_out = (uInt)size;
        r = deflate(&strm, Z_FINISH);
        rd_assert(r == Z_STREAM_END);
        len = (size_t)strm.total_out;
        deflateEnd(&strm);
        return len;
}

/** @brief Decodes \p in; returns 1 when it decodes to exactly \p expect,
 *         0 when refused, -1 for any other output. */
static int ut_gz_decodes(const unsigned char *in,
                         size_t len,
                         uint64_t maximum,
                         const char *expect) {
        uint64_t outlen = 0;
        char *out = rd_gz_decompress_bounded(in, (int)len, &outlen, maximum);
        int r;
        if (!out)
                return 0;
        r = expect && outlen == strlen(expect) &&
                    !memcmp(out, expect, (size_t)outlen)
                ? 1
                : -1;
        rd_free(out);
        return r;
}

int unittest_gzip(void) {
        static const char first[]  = "first member's records, first member's";
        static const char second[] = "second member's records";
        unsigned char a[256], b[256], in[600];
        size_t la = ut_gz_member(first, a, sizeof(a));
        size_t lb = ut_gz_member(second, b, sizeof(b));
        char both[sizeof(first) + sizeof(second)];
        int r;

        rd_snprintf(both, sizeof(both), "%s%s", first, second);
        memcpy(in, a, la);
        memcpy(in + la, b, lb);

        r = ut_gz_decodes(a, la, 1 << 20, first);
        RD_UT_ASSERT(r == 1, "one member: %d", r);

        r = ut_gz_decodes(in, la + lb, 1 << 20, both);
        RD_UT_ASSERT(r == 1, "two members decode in order: %d", r);

        /* The bound covers all members together. */
        r = ut_gz_decodes(in, la + lb, strlen(both) - 1, NULL);
        RD_UT_ASSERT(r == 0, "members over the bound together: %d", r);
        r = ut_gz_decodes(in, la + lb, strlen(both), both);
        RD_UT_ASSERT(r == 1, "members at the bound: %d", r);

        memcpy(in + la, "garbage!", 8);
        r = ut_gz_decodes(in, la + 8, 1 << 20, NULL);
        RD_UT_ASSERT(r == 0, "bytes after the member: %d", r);

        r = ut_gz_decodes(a, la - 8, 1 << 20, NULL);
        RD_UT_ASSERT(r == 0, "member without its trailer: %d", r);

        r = ut_gz_decodes(a, la - 12, 1 << 20, NULL);
        RD_UT_ASSERT(r == 0, "member cut inside its deflate data: %d", r);

        memcpy(in, a, la);
        memcpy(in + la, b, lb - 4);
        r = ut_gz_decodes(in, la + lb - 4, 1 << 20, NULL);
        RD_UT_ASSERT(r == 0, "second member cut short: %d", r);

        r = ut_gz_decodes(a, 0, 1 << 20, NULL);
        RD_UT_ASSERT(r == 0, "no input: %d", r);

        RD_UT_PASS();
}

/**@}*/
