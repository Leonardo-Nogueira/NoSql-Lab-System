package com.leonardonogueira.nosqlmsg.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Service
public class KafkaOrderProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaOrderProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public KafkaOrderProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public record OrderEvent(
            String orderId,
            String customerId,
            BigDecimal amount,
            String eventType, // "ORDER_CREATED", "PAYMENT_AUTHORIZED", "ORDER_SHIPPED"
            Instant timestamp
    ) {}

    /**
     * PUBLICAÇÃO ASSÍNCRONA NO KAFKA:
     * - A 'key' (orderId) garante que TODOS os eventos deste mesmo pedido caiam
     *   SEMPRE na mesma partição do Kafka (mantendo ordem estrita FIFO)!
     */
    public CompletableFuture<Map<String, Object>> publishOrderEvent(
            String orderId,
            String customerId,
            BigDecimal amount,
            String eventType) {

        OrderEvent event = new OrderEvent(orderId, customerId, amount, eventType, Instant.now());

        try {
            String jsonPayload = objectMapper.writeValueAsString(event);

            // Dispara para o Kafka passando a Chave (orderId) e o Payload JSON:
            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(
                    KafkaTopicConfig.ORDERS_TOPIC,
                    orderId, // KEY (Garante a partição por hash!)
                    jsonPayload
            );

            return future.thenApply(result -> {
                var metadata = result.getRecordMetadata();
                log.info("🚀 [KAFKA PRODUCER] Evento '{}' enviado para Topico='{}' | Particao={} | Offset={}",
                        eventType, metadata.topic(), metadata.partition(), metadata.offset());

                return Map.of(
                        "status", "PUBLISHED_TO_KAFKA",
                        "topic", metadata.topic(),
                        "partitionAssigned", metadata.partition(),
                        "offsetAssigned", metadata.offset(),
                        "messageKey", orderId,
                        "payload", event
                );
            });

        } catch (Exception e) {
            log.error("💥 Erro ao serializar evento Kafka: {}", e.getMessage());
            CompletableFuture<Map<String, Object>> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }
    }
}

