package org.sireum.hamr.inspector.services.redis;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.sireum.hamr.inspector.common.Msg;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@UtilityClass
class ServiceCaches {

    // must be concurrent or two runs of a session can cause ConcurrentModificationException in put method
    private static final Map<String, Cache<Long, Msg>> ID_CACHE = Collections.synchronizedMap(new HashMap<>());

//    /*
//     * It is NOT safe to assume {@link ServiceCaches#get(String, long)} returns a non-null value after calling
//     * {@link ServiceCaches#contains(String, long)} because cached key/value pairs can be removed at anytime.
//     *
//     * Keeping private because, if needed in the future, this warning should be above the implementation.
//     */
//    private static boolean contains(@NotNull String sessionName, long id) {
//        if (ID_CACHE.containsKey(sessionName)) {
//            return ID_CACHE.get(sessionName).getIfPresent(id) != null;
//        }
//
//        return false;
//    }

    static void put(@NotNull String sessionName, long id, @NotNull Msg msg) {
        ID_CACHE.computeIfAbsent(sessionName, it ->
                Caffeine.newBuilder().maximumSize(16384).expireAfterAccess(Duration.ofMinutes(5)).build());

        ID_CACHE.get(sessionName).put(id, msg);
    }

    /**
     *
     * Returns the {@link Msg} corresponding to a given session and uuid, if one has been cached. If none has been
     * cached, returns null.
     *
     * @param sessionName the session to lookup a message id from
     * @param id the message's uuid
     * @return The {@link Msg} corresponding to the given session and uuid
     */
    @Nullable
    static Msg get(@NotNull String sessionName, long id) {
        if (ID_CACHE.containsKey(sessionName)) {
            return ID_CACHE.get(sessionName).getIfPresent(id);
        }
        return null;
    }
}