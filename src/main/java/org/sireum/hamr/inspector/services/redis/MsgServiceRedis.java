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

import art.DataContent;
import art.Empty;
import art.UPort;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.sireum.hamr.inspector.common.ArtUtils;
import org.sireum.hamr.inspector.common.InspectionBlueprint;
import org.sireum.hamr.inspector.common.Msg;
import org.sireum.hamr.inspector.services.MsgService;
import org.sireum.hamr.inspector.services.RecordId;
import org.sireum.hamr.inspector.services.Session;
import org.sireum.hamr.inspector.services.SessionService;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveStreamOperations;
import org.springframework.data.redis.stream.StreamReceiver;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.NoSuchElementException;

import static org.springframework.data.domain.Range.Bound.*;

@Slf4j
@Controller
public class MsgServiceRedis implements MsgService {

    private static final Empty EMPTY = Empty.apply();
    public static final Msg INVALID_MSG = new Msg(null, null, null, null, EMPTY, -1, -1);
    public static final Duration POLL_TIMEOUT = Duration.ofSeconds(2);

    private final SessionService sessionService;
    private final InspectionBlueprint inspectionBlueprint;
    private final ArtUtils artUtils;

    private final ReactiveRedisTemplate<String, String> template;

    private ReactiveStreamOperations<String, String, String> streamOps;

    final Cache<String, DataContent> parseCache = Caffeine.newBuilder()
            .<String, DataContent>weigher((key, value) -> approxMinMemUsageBytes(key))
            .maximumWeight(64_000_000) // ~ 0.064 GB // todo add to properties or better yet delegate to spring cache..
            .expireAfterAccess(Duration.ofMinutes(5))
            .build();

    /**
     * A source that periodically polls the server and forwards any new messages to all subscribers of a live streams.
     * This is the only source of polling to support all stream message forwarding.
     *
     * Note that published messages (currently only injections) are NOT affected by polling.
     */
    private StreamReceiver<String, MapRecord<String, String, String>> streamReceiver;

    public MsgServiceRedis(ReactiveRedisTemplate<String, String> template, SessionService sessionService, InspectionBlueprint inspectionBlueprint, ArtUtils artUtils) {
        this.template = template;
        this.sessionService = sessionService;
        this.inspectionBlueprint = inspectionBlueprint;
        this.artUtils = artUtils;
    }

    @PostConstruct
    private void postConstruct() {
        streamOps = template.opsForStream();

        final ReactiveRedisConnectionFactory connectionFactory = template.getConnectionFactory();
        final var options = StreamReceiver.StreamReceiverOptions.builder()
                .pollTimeout(POLL_TIMEOUT)
                .build();
        streamReceiver = StreamReceiver.create(connectionFactory, options);
    }

    @Override
    public @NotNull Flux<Msg> live(@NotNull Session session, @NotNull Range<RecordId> range) {
        if (range.getUpperBound().isBounded()) {
            return streamOps.range(session.getName() + "-stream", format(range))
                    .transform(flux -> RECORD_TRANSFORMER(session, flux));
        } else {
            if (range.getLowerBound().isBounded()) {
                final ReadOffset readOffset = range.getLowerBound().getValue()
                        .map(lower -> ReadOffset.from(format(lower))).get();

                return streamReceiver.receive(StreamOffset.create(session.getName() + "-stream", readOffset))
                        .transform(flux -> RECORD_TRANSFORMER(session, flux));
            } else {
                return streamReceiver.receive(StreamOffset.fromStart(session.getName() + "-stream"))
                        .transform(flux -> RECORD_TRANSFORMER(session, flux));
            }
        }
    }

    @Override
    public @NotNull Flux<Msg> replay(@NotNull Session session, @NotNull Range<RecordId> range) {
        return streamOps.range(session.getName() + "-stream", format(range))
                .transform(flux -> RECORD_TRANSFORMER(session, flux));
    }

    // todo change docs to only support this method for existent parts of streams (even if hot)
    @Override
    public @NotNull Flux<Msg> replayReverse(@NotNull Session session, @NotNull Range<RecordId> range) {
        return streamOps.reverseRange(session.getName() + "-stream", format(range))
                .transform(flux -> RECORD_TRANSFORMER(session, flux));
    }

    @Override
    public @NotNull Mono<Long> count(@NotNull Session session) {
        return streamOps.size(session.getName() + "-stream");
    }

    private Flux<Msg> RECORD_TRANSFORMER(Session session, Flux<MapRecord<String, String, String>> flux) {
        return flux
                .map(Record::getValue)
                // if the message contains a "stop" key then it is a special indicator that the session
                // has stopped. This message will contain a "stop" field with a reason string and a "timestamp"
                .takeWhile(value -> value.get("stop") == null)
                .index().map(indexedValue -> {
            final long id = indexedValue.getT1();

            final Msg cachedMsg = ServiceCaches.get(session, id);
            if (cachedMsg != null) {
                return cachedMsg;
            }

            try {
                final var it = indexedValue.getT2();

                long ts = Long.parseLong(it.getOrDefault("timestamp", "-1"));
                if (ts == -1) {
                    log.error("Unable to parse timestamp of msg id={} data={}.", id, it);
                    return INVALID_MSG;
                }

                final int srcId = Integer.parseInt(it.getOrDefault("src", "-1"));
                final UPort src;
                if (srcId != -1) {
                    src = artUtils.getPort(srcId);
                } else {
                    log.error("Unable to parse src port of msg id={} data={}.", id, it);
                    return INVALID_MSG;
                }

                final int dstId = Integer.parseInt(it.getOrDefault("dst", "-1"));
                final UPort dst;
                if (dstId != -1) {
                    dst = artUtils.getPort(dstId);
                } else {
                    log.error("Unable to parse dst port of msg id={} data={}.", id, it);
                    return INVALID_MSG;
                }

                final String data = it.getOrDefault("data", "");
                final DataContent dataContent = parseCache.get(data, json -> inspectionBlueprint.deserializer().apply(json));
                if (dataContent == null) {
                    log.error("Unable to parse data content of msg id={} data={}.", id, it);
                    return INVALID_MSG;
                }

                final Msg msg = new Msg(src, dst, artUtils.getBridge(src), artUtils.getBridge(dst), dataContent, ts, id);
                ServiceCaches.put(session, id, msg);
                return msg;

            } catch (NumberFormatException | NoSuchElementException e) {
                log.error("Unable to parse incoming message", e);
            }

            return INVALID_MSG;
        }).filter(msg -> msg != INVALID_MSG);
    }

    /**
     * Convert a generic spring {@link Range} of {@link RecordId}s into a redis-friendly String format.
     *
     * @param range the {@link Range} of {@link RecordId}s to convert
     * @return the resulting redis-friendly {@link Range} of Strings
     */
    private static Range<String> format(@NotNull Range<RecordId> range) {
        return Range.of(format(range.getLowerBound()), format(range.getUpperBound()));
    }

    /**
     * Converts a generic spring {@link org.springframework.data.domain.Range.Bound} of {@link RecordId} into a
     * redis-friendly {@link org.springframework.data.domain.Range.Bound} of Strings.
     *
     * @param bound the {@link RecordId}-containing {@link org.springframework.data.domain.Range.Bound} to convert
     * @return a String-containing {@link org.springframework.data.domain.Range.Bound} in a redis-friendly format
     */
    private static Range.Bound<String> format(@NotNull Range.Bound<RecordId> bound) {
        return bound.getValue()
                .map(recordId -> bound.isInclusive() ? inclusive(format(recordId)) : exclusive(format(recordId)))
                .orElse(unbounded());
    }

    /**
     * Formats a generic spring data {@link RecordId} into a redis-friendly string format.
     *
     * @param recordId the {@link RecordId} to covert
     * @return a redis-friendly string representation of the recordId
     */
    private static String format(@NotNull RecordId recordId) {
        return recordId.timestamp() + "-" + recordId.sequence();
    }

    /*
     * There are many exceptions, but this approximates string size.
     */
    private static int approxMinMemUsageBytes(String string) {
        // formula from:
        // https://www.javamex.com/tutorials/memory/string_memory_usage.shtml
        return 8 * (((2 * string.length()) + 35) / 8);
    }

}