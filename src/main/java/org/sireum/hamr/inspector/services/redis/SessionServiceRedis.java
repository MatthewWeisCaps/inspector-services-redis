package org.sireum.hamr.inspector.services.redis;

import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.sireum.hamr.inspector.services.Session;
import org.sireum.hamr.inspector.services.SessionService;
import org.sireum.hamr.inspector.services.SessionStatus;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

import javax.annotation.PostConstruct;

@Slf4j
@Controller
public class SessionServiceRedis implements SessionService {

    private static final int MAX_KEY_QUERY_BUFFER_SIZE = 65536;

    private ReactiveValueOperations<String, String> valueOps;

    private final ReactiveStringRedisTemplate template;

    private Flux<GroupedFlux<Session, SessionStatus>> liveSessionStatusHotFlux;

    public SessionServiceRedis(ReactiveStringRedisTemplate template) {
        this.template = template;
    }

    @PostConstruct
    private void postConstruct() {
        valueOps = template.opsForValue();

        liveSessionStatusHotFlux = template.listenToChannel("inspector-channel")
                .map(ReactiveSubscription.Message::getMessage)
                .map(message -> {
                    final int dashIndex = message.indexOf('-');
                    final String session = message.substring(0, dashIndex);
                    final String content = message.substring(dashIndex);

                    if (content.equals("-start")) {
                        return Tuples.of(session, true);
                    } else if (content.equals("-stop")) {
                        return Tuples.of(session, false);
                    } else {
                        throw new RuntimeException("ClientStateService received unparsable string: " + message);
                    }
                })
                .onErrorContinue((throwable, message) ->
                        log.error("ClientStateService received invalid input on inspector-channel: {}", message))
                .groupBy(tuple -> new Session(tuple.getT1()),
                        tuple -> tuple.getT2() ? SessionStatus.RUNNING : SessionStatus.COMPLETED);
    }

    @NotNull
    @Override
    public Flux<Session> sessions() {
        // Scanning the key space can have issues on larger dbs if the return size of "scan()" becomes too large.

        // Because the Inspector's streams are uniquely numbered using the atomic increasing-only value "numSessions,"
        // it is possible to avoid this by:
        //      (1) Query the value of numSessions to see what the largest possible max stream value is
        //      (2) Check whether or not a key exists for all values 1 .. n
        //      (3) Filter the output to remove deleted keys

        // Keys are checked to see if they exist because it's possible someone may want to delete old sessions
        // without renaming or changing their current sessions. Use querySessionsUnsafe() to skip this check.

        // This method does NOT check if all three components ("n-start, n-stop, and n-stream") of a session's key
        // triplet are contained in the database. Only the "n-stream" key is checked. This is because it's assumed any
        // key deletion procedure will handle all three at once.

        return valueOps.get("numSessions")
                .flatMapMany(countString -> {
                    final int count = Integer.parseInt(countString);

                    // buffer size is rounded up to next power-of-two by filterWhen function so capping size
                    // prevents issues from massive calls suddenly rounding up.
                    final int bufferSize = Math.min(count, MAX_KEY_QUERY_BUFFER_SIZE);

                    if (count != bufferSize) {
                        log.warn("numSessions = {} > {}, so keys will be queried in chunks.", count, bufferSize);
                    }

                    return Flux.range(1, count)
                            .map(n -> new Session(Integer.toString(n)))
                            .filterWhen(n -> template.hasKey(n + "-stream"), bufferSize);
                });
    }

    @NotNull
    @Override
    public Mono<Long> startTimeOf(@NotNull Session session) {
        return valueOps.get(session.getName() + "-start").map(Long::parseLong);
    }

    @NotNull
    @Override
    public Mono<Long> stopTimeOf(@NotNull Session session) {
        return valueOps.get(session.getName() + "-stop").map(Long::parseLong);
    }

    @NotNull
    @Override
    public Mono<SessionStatus> statusOf(@NotNull Session session) {
        return stopTimeOf(session).map(any -> SessionStatus.COMPLETED)
                .switchIfEmpty(startTimeOf(session).map(any -> SessionStatus.RUNNING));
    }

    @NotNull
    @Override
    public Flux<GroupedFlux<Session, SessionStatus>> liveStatusUpdates() {
        return liveSessionStatusHotFlux;
    }

}









