package com.leonardonogueira.nosqlmsg.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class KafkaOrderConsumers {

    private static final Logger log = LoggerFactory.getLogger(KafkaOrderConsumers.class);

    private final List<Map<String, Object>> processedEvents = new CopyOnWriteArrayList<>();

    /**
     * CONSUMER GROUP 1: 'audit-consumer-group'
     * Responsavel por registrar logs de auditoria e compliance de todos os eventos.
     */
    @KafkaListener(topics = KafkaTopicConfig.ORDERS_TOPIC, groupId = "audit-consumer-group")
    public void consumeForAudit(
            @Payload String payload,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("🛡️ [CONSUMER GROUP 1: AUDIT] Processado com sucesso! Topico='{}' | Particao={} | Offset={} | Key='{}'",
                KafkaTopicConfig.ORDERS_TOPIC, partition, offset, key);

        processedEvents.add(Map.of(
                "consumerGroup", "audit-consumer-group",
                "partition", partition,
                "offset", offset,
                "key", key,
                "processedAt", Instant.now().toString(),
                "payload", payload
        ));
    }

    /**
     * CONSUMER GROUP 2: 'notification-consumer-group'
     * Repare no FAN-OUT do Kafka: Este grupo consome exatamente as MESMAS mensagens
     * do Grupo 1, no seu proprio ritmo e com seu proprio Offset individual!
     */
    @KafkaListener(topics = KafkaTopicConfig.ORDERS_TOPIC, groupId = "notification-consumer-group")
    public void consumeForNotification(
            @Payload String payload,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("🔔 [CONSUMER GROUP 2: NOTIFICATION] Disparando Push/SMS! Topico='{}' | Particao={} | Offset={} | Key='{}'",
                KafkaTopicConfig.ORDERS_TOPIC, partition, offset, key);

        processedEvents.add(Map.of(
                "consumerGroup", "notification-consumer-group",
                "partition", partition,
                "offset", offset,
                "key", key,
                "processedAt", Instant.now().toString(),
                "payload", payload
        ));
    }

    public List<Map<String, Object>> getProcessedEvents() {
        return Collections.unmodifiableList(processedEvents);
    }
}

