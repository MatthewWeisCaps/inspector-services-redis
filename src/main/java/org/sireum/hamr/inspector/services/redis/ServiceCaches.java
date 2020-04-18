/*
 * Copyright (c) 2020, Matthew Weis, Kansas State University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sireum.hamr.inspector.services.redis;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.sireum.hamr.inspector.common.Msg;
import org.sireum.hamr.inspector.services.Session;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.Duration;

@Slf4j
@UtilityClass
class ServiceCaches {

    private static final Cache<Tuple2<Session, Long>, Msg> ID_CACHE = Caffeine.newBuilder()
            .maximumSize(8192)
            .expireAfterAccess(Duration.ofMinutes(5))
            .build();

    static void put(@NotNull Session session, long id, @NotNull Msg msg) {
        final Tuple2<Session, Long> pair = Tuples.of(session, id);
        ID_CACHE.put(pair, msg);
    }

    /**
     *
     * Returns the {@link Msg} corresponding to a given session and uuid, if one has been cached. If none has been
     * cached, returns null.
     *
     * It is NOT safe to assume this function returns a non-null value after calling
     * {@link ServiceCaches#put(Session, long, Msg)} because cached key/value pairs can be removed at anytime.
     *
     * @param session the session to lookup a message id from
     * @param id the message's uuid
     * @return The {@link Msg} corresponding to the given session and uuid
     */
    @Nullable
    static Msg get(@NotNull Session session, long id) {
        final Tuple2<Session, Long> pair = Tuples.of(session, id);
        return ID_CACHE.getIfPresent(pair);
    }
}