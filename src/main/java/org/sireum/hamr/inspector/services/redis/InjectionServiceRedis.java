package org.sireum.hamr.inspector.services.redis;

import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.sireum.hamr.inspector.common.InspectionBlueprint;
import org.sireum.hamr.inspector.common.Injection;
import org.sireum.hamr.inspector.services.InjectionService;
import org.sireum.hamr.inspector.services.Session;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Controller;

@Slf4j
@Controller
public class InjectionServiceRedis implements InjectionService {

    private final StringRedisTemplate template;

    private final InspectionBlueprint inspectionBlueprint;

    public InjectionServiceRedis(StringRedisTemplate template, InspectionBlueprint inspectionBlueprint) {
        this.template = template;
        this.inspectionBlueprint = inspectionBlueprint;
    }

    @Override
    public void inject(@NotNull Session session, @NotNull Injection injection) {
        final int bridgeId = injection.bridge().id().toInt();
        final int portId = injection.port().id().toInt();

        final String dataContentString = inspectionBlueprint.serializeFn().apply(injection.dataContent());

        final String key = String.format("%s-pubsub", session);
        final String message = String.format("%d,%d,%s", bridgeId, portId, dataContentString);

        log.info("Injecting {} to {}", message, key);
        template.convertAndSend(key, message);
    }
}









