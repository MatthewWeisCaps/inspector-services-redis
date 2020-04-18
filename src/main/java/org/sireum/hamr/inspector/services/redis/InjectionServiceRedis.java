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

        final String dataContentString = inspectionBlueprint.serializer().apply(injection.dataContent());

        final String key = String.format("%s-pubsub", session);
        final String message = String.format("%d,%d,%s", bridgeId, portId, dataContentString);

        log.info("Injecting {} to {}", message, key);
        template.convertAndSend(key, message);
    }
}









