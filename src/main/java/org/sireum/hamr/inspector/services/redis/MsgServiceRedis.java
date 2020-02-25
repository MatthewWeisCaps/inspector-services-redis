package org.sireum.hamr.inspector.services.redis;

import art.DataContent;
import art.Empty;
import art.UPort;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.sireum.hamr.inspector.common.InspectionBlueprint;
import org.sireum.hamr.inspector.common.ArtUtils;
import org.sireum.hamr.inspector.common.Msg;
import org.sireum.hamr.inspector.services.MsgService;
import org.sireum.hamr.inspector.services.Session;
import org.sireum.hamr.inspector.services.SessionService;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
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
            .<String, DataContent>weigher((key, value) -> Math.max(key.length(), 1))
            .maximumWeight(16384)
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
    public @NotNull Flux<Msg> replayThenLive(@NotNull Session session) {
        return sessionService.startTimeOf(session).flatMapMany(startTime ->
                streamReceiver.receive(StreamOffset.fromStart(session.getName() + "-stream"))
                        .transform(flux -> RECORD_TRANSFORMER(startTime, session.getName(), flux)));
    }

    @Override
    public @NotNull Flux<Msg> replay(@NotNull Session session) {
        return sessionService.startTimeOf(session).flatMapMany(startTime ->
                streamOps.read(StreamOffset.fromStart(session.getName() + "-stream"))
                        .transform(flux -> RECORD_TRANSFORMER(startTime, session.getName(), flux)));
    }

    @Override
    public @NotNull Flux<Msg> live(@NotNull Session session) {
        return sessionService.startTimeOf(session).flatMapMany(startTime ->
                streamReceiver.receive(StreamOffset.latest(session.getName() + "-stream"))
                        .transform(flux -> RECORD_TRANSFORMER(startTime, session.getName(), flux)));
    }

    @Override
    public @NotNull Mono<Long> count(@NotNull Session session) {
        return streamOps.size(session.getName() + "-stream");
    }

    private Flux<Msg> RECORD_TRANSFORMER(long startTime, String key, Flux<MapRecord<String, String, String>> flux) {
        return flux.map(Record::getValue)
                // if the message contains a "stop" key then it is a special indicator that the session
                // has stopped. This message will contain a "stop" field with a reason string and a "timestamp"
                .takeWhile(value -> value.get("stop") == null)
                .index().map(indexedValue -> {
            final long id = indexedValue.getT1();

            final Msg cachedMsg = ServiceCaches.get(key, id);
            if (cachedMsg != null) {
                return cachedMsg;
            }

            try {
                final var it = indexedValue.getT2();

                long ts = Long.parseLong(it.getOrDefault("timestamp", "-1"));
                if (ts != -1) {
                    ts -= startTime;
                } else {
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
                final DataContent dataContent = parseCache.get(data, json -> inspectionBlueprint.deserializeFn().apply(json));
                if (dataContent == null) {
                    log.error("Unable to parse data content of msg id={} data={}.", id, it);
                    return INVALID_MSG;
                }

                final Msg msg = new Msg(src, dst, artUtils.getBridge(src), artUtils.getBridge(dst), dataContent, ts, id);
                ServiceCaches.put(key, id, msg);
                return msg;

            } catch (NumberFormatException | NoSuchElementException e) {
                log.error("Unable to parse incoming message", e);
            }

            return INVALID_MSG;
        }).filter(msg -> msg != INVALID_MSG);
    }

}